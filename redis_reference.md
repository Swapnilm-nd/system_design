# Redis Reference — DIS Window Handler (AN-34875 / AN-35636)

What Redis functionality the DIS window handler feature actually uses, and how each piece
works. Covers the new methods added to `RedisUtils` (`frs_src/utils.py`) for the per-vehicle
window buffer and lock, and how `Recognize` (`frs_src/api_interface.py`) calls into them.
Does not re-document the pre-existing `RedisUtils` methods (DIS prediction caching, cluster
DCT tracking, etc.) — only the new surface built for this feature.

> **Known issue as of this writing**: `Recognize._append_to_dis_window` builds the per-image
> `record` dict with `"device_capture_time": row["device_capture_time"]` — a raw
> `datetime.datetime`, not a string. `json.dumps(record)` cannot serialize a `datetime` object,
> so this raises `TypeError` on every call today. It's caught by the method's own
> `try/except`, so it fails silently rather than breaking Recognize's response — but it means
> the window buffer isn't actually being populated right now. This doc describes the intended,
> working shape (`.isoformat()` on that field) — flagging the gap here rather than pretending
> it's fixed.

## What's stored, where

| Redis key | Type | Purpose |
|---|---|---|
| `vehicle_window:{tenant_id:vehicle_id}` | **Sorted Set (ZSET)** | Buffered per-image records for one vehicle's currently-open window, scored by `device_capture_time` (epoch seconds) |
| `vehicle_lock:{tenant_id:vehicle_id}` | **String** | Present = `dis_window_handler` (Loop 2) is currently sweeping this vehicle; value = an owner token (UUID) |

Both key names embed `{tenant_id:vehicle_id}` — note the literal curly braces — which is a
**Redis Cluster hash tag**, explained below. Built by two small helpers:

```python
@staticmethod
def _vehicle_window_key(tenant_id, vehicle_id):
    return f"vehicle_window:{{{tenant_id}:{vehicle_id}}}"

@staticmethod
def _vehicle_lock_key(tenant_id, vehicle_id):
    return f"vehicle_lock:{{{tenant_id}:{vehicle_id}}}"
```

For `tenant_id=42, vehicle_id=100` these produce `vehicle_window:{42:100}` and
`vehicle_lock:{42:100}`.

### Why a Sorted Set, not a List

The window needs to come back sorted by `device_capture_time` for the split-merge step
(`embed_sim_dct_level_assignment`) with no separate sort pass. A Redis **List** only preserves
insertion order (whatever order Recognize happened to process requests in); a **Sorted Set**
keeps every member ordered by a numeric **score** at all times — `ZADD` inserts a member at its
correct sorted position, and `ZRANGE key 0 -1` always returns members in ascending-score order.
Here, score = `device_capture_time.timestamp()` (epoch seconds — the same datetime→float
conversion already used elsewhere in this file, e.g. `update_clusters_dct_and_vehicle_id`).

This also tolerates the rare out-of-order arrival documented in `analysis_for_design.md`
(~0.001% of images arrive with a `device_capture_time` earlier than images already buffered for
that request/window): a late record just slots into its correct sorted position. A Redis
**Stream** (`XADD`/`XRANGE`) was considered and rejected for this reason — Streams require each
new entry's ID to be strictly greater than the previous one, so using `device_capture_time` as
the stream ID would make `XADD` reject exactly this rare-but-real case.

### Why the hash tag (`{tenant_id:vehicle_id}`)

This service runs Redis in **Cluster mode** (`rediscluster.RedisCluster`), where keys are
sharded across nodes by a hash of the key name. `append_to_vehicle_window` needs to atomically
touch *both* the lock key and the window key in one Lua script (see below) — and Redis Cluster
refuses to run a multi-key script unless every key in it hashes to the *same* node ("slot"),
raising a `CROSSSLOT` error otherwise.

A **hash tag** — the `{...}` portion of a key — tells Redis Cluster "only hash *this* substring
to decide the slot, ignore the rest of the key." Both `vehicle_window:{42:100}` and
`vehicle_lock:{42:100}` share the hash tag `42:100`, so they're guaranteed to land on the same
node regardless of their different prefixes — which is what makes the atomic multi-key script
possible. No other key in `frs_src/utils.py` uses this technique; every other Redis key in this
codebase is single-key-per-operation, so this is a new pattern introduced specifically for this
feature.

## The methods

### `append_to_vehicle_window(tenant_id, vehicle_id, device_capture_time, record_json)`

Called once per image from `Recognize._append_to_dis_window`, gated by
`cfg.DIS_WINDOW.enabled_vehicle_ids`. Adds one record to the window — but only if the vehicle
isn't currently locked (being swept by Loop 2).

```python
append_script = """
    if redis.call('EXISTS', KEYS[1]) == 1 then
        return 0
    end
    redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
    return 1
"""
lock_key = self._vehicle_lock_key(tenant_id, vehicle_id)
window_key = self._vehicle_window_key(tenant_id, vehicle_id)
score = device_capture_time.timestamp()
appended = self._r.eval(append_script, 2, lock_key, window_key, score, record_json)
```

**Why `EVAL` (a Lua script) instead of two plain Python calls**: "check the lock, then append"
has to be one indivisible step. Redis executes commands one at a time, single-threaded — so a
script passed to `EVAL` runs to completion with nothing else interleaved. If this were two
separate round trips (`EXISTS` then `ZADD`), `dis_window_handler` could set the lock in the gap
between them, and the append would slip through into a window that's already being read —
exactly the race the design doc calls out.

**`eval(script, numkeys, ...)` breakdown**: the `2` right after the script tells Redis "the next
2 arguments are key names, not plain values." So `lock_key` → `KEYS[1]`, `window_key` →
`KEYS[2]`, and everything after that (`score`, `record_json`) → `ARGV[1]`, `ARGV[2]`. Redis needs
this split (rather than parsing key names out of the script text) mainly for Cluster routing —
it inspects `KEYS[]` alone to decide which node should run the script, and to enforce the
same-slot rule above.

**Return value**: `1`/`0` from the script → cast to `bool`. `False` means the vehicle was
locked; the record is silently dropped (logged at `info`), not raised as an error — this call is
best-effort by design (see `_append_to_dis_window`'s own `try/except`), and a dropped image here
is an accepted rare miss, the same spirit as the design's tolerance for rare discontinuities.

### `get_vehicle_window_length(tenant_id, vehicle_id)`

```python
return self._r.zcard(self._vehicle_window_key(tenant_id, vehicle_id))
```

`ZCARD` = "how many members does this Sorted Set have" — an O(1) count, no need to fetch the
data itself. Used to support `cfg.DIS_WINDOW.record_limit_per_vehicle`: `dis_window_handler`
checks this cheaply on each sweep tick to decide whether a vehicle's window should be
force-closed early, before its normal 30-minute boundary arrives.

### `get_active_vehicles()`

```python
active_vehicles = []
for key in self._r.scan_iter("vehicle_window:{*"):
    inner = key[len("vehicle_window:{") : -1]
    tenant_id_str, vehicle_id_str = inner.split(":", 1)
    active_vehicles.append((int(tenant_id_str), int(vehicle_id_str)))
return active_vehicles
```

How `dis_window_handler` discovers which vehicles currently have an open (non-empty) window,
without maintaining a separate registry key. `scan_iter` is Redis's cursor-based key
enumeration (`SCAN`, not `KEYS` — `KEYS` blocks the whole server while it walks every key;
`SCAN` walks incrementally in small batches, safe to run against a live production Redis). The
pattern `"vehicle_window:{*"` matches any key starting with that literal prefix — every
`vehicle_window:{...}` key, regardless of what's inside the braces. Same idiom this codebase
already uses elsewhere (`get_dct_vehicle_id_of_all_clusters` scans `"lat:*"`).

A separate `SADD`-based registry set (one global key listing all active vehicles) was
considered and rejected: it would be a *third* key, unrelated to the `{tenant_id:vehicle_id}`
hash tag, so it couldn't be updated atomically inside `append_to_vehicle_window`'s Lua script
without a `CROSSSLOT` error. `scan_iter` sidesteps that entirely — no extra key to keep in sync.

### `acquire_vehicle_lock(tenant_id, vehicle_id)`

```python
token = str(uuid.uuid4())
acquired = self._r.set(
    self._vehicle_lock_key(tenant_id, vehicle_id),
    token,
    nx=True,   # only set the key if it does not already exist
    ex=cfg.DIS_WINDOW.lock_ttl_seconds,  # auto-expire after this many seconds
)
return token if acquired else None
```

`SET key value NX EX <seconds>` is Redis's standard building block for a distributed lock:
- `NX` ("not exists") makes the `SET` a no-op — and report failure — if the key is already
  present. This is what gives you mutual exclusion: only one caller's `SET NX` can ever succeed
  for a given key at a time.
- `EX <seconds>` attaches a TTL so the key auto-deletes on its own after that many seconds.
  This is a **safety net**, not something normal operation depends on — `dis_window_handler`
  explicitly releases the lock when it's done. It exists purely so that if the process crashes
  or is killed mid-sweep while holding a lock, Redis cleans it up after
  `cfg.DIS_WINDOW.lock_ttl_seconds` (120s) instead of that vehicle staying locked forever.
- The **token** (a fresh UUID per call) is stored as the lock's value, not just a placeholder —
  it's what makes `release_vehicle_lock` safe (see next).

Returns the token on success (the caller needs it later to release), or `None` if the vehicle
was already locked.

### `release_vehicle_lock(tenant_id, vehicle_id, token)`

```python
release_script = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
"""
return bool(self._r.eval(release_script, 1, lock_key, token))
```

A **check-and-delete**, not a plain `DEL` — it only deletes the lock if its current value still
matches the token this caller was given when it acquired the lock. Why this matters: the lock
has a TTL as a safety net (above). Suppose a sweep takes unusually long and the TTL expires
before the sweep finishes — some *other* process could then acquire the lock in the meantime.
If the original (slow) sweep then just did a plain `DEL` when it finally finished, it would
delete the *new* holder's lock, not its own — breaking mutual exclusion for whoever holds it
now. Checking the token first means "only delete this if it's still mine." Same reasoning as the
`append_to_vehicle_window` script: this has to be one atomic `EVAL`, not a separate `GET` then
`DEL`, or another process could re-acquire the lock in the gap between those two calls.

### `pull_and_clear_vehicle_window(tenant_id, vehicle_id)`

```python
pull_and_clear_script = """
    local records = redis.call('ZRANGE', KEYS[1], 0, -1)
    redis.call('DEL', KEYS[1])
    return records
"""
return self._r.eval(pull_and_clear_script, 1, window_key)
```

Called by `dis_window_handler` only while it holds the vehicle's lock. `ZRANGE key 0 -1` reads
every member of the Sorted Set, in ascending score order (i.e. already sorted by
`device_capture_time`) — `0` and `-1` are start/stop indexes meaning "from the first element to
the last," the standard Redis idiom for "give me everything." Then `DEL` empties the window so
the vehicle starts its next window fresh. One `EVAL` again, for the same reason as above: if
this were `ZRANGE` then a separate `DEL`, a worker's `append_to_vehicle_window` call landing in
that gap would append into a window this process is about to wipe, silently losing that record
even though the vehicle wasn't locked at the moment the append's own lock-check ran.

## Wiring into `Router` (dual-write dispatch)

`Router` (`frs_src/utils.py`) is the existing facade every service actually calls through — it
decides whether a given method call goes to the plain-Redis client, the Valkey client, or both,
based on `cfg.INMEMORY_CACHE.{redis,valkey}_{read,write}`. It does this by an explicit allowlist,
`self.write_methods` — any *mutating* method must be listed there by exact name, or `Router`
treats it as a read and only sends it to whichever single backend has `*_read=True` (today,
Valkey only). All four mutating methods from this feature are registered:

```python
self.write_methods = {
    ...,
    "append_to_vehicle_window",
    "acquire_vehicle_lock",
    "release_vehicle_lock",
    "pull_and_clear_vehicle_window",
}
```

**Caveat noted in code**: if `redis_write` and `valkey_write` were ever both enabled at once
(they aren't today), `acquire_vehicle_lock`/`release_vehicle_lock` would run against two
independent, unsynchronized Redis clusters — each cluster's `SET NX` could succeed or fail
differently, so the two backends could disagree about who currently holds a given vehicle's
lock. Harmless under the current Valkey-only config; flagged for whoever changes those flags
during a future cache migration.

---

# Redis command reference — sliding-window split-merge design (current)

Every Redis data type and command referenced by
[`dis_sliding_window_split_merge_design.md`](dis_sliding_window_split_merge_design.md), the
current, actively-developed DIS design (a different, newer architecture from the vehicle-window
design documented above — session-based buffer + matrix resolution, not a single per-vehicle
window ZSET). Organized by the six Redis structures that design defines (§2), plus the scripting
primitive that ties several of them together atomically.

## Data types used

| Type | Structure(s) | Why this type |
|---|---|---|
| **String** | `dis_device_lock` (§2.5) | Simplest possible shape for a lock — one key, one value (the holder's token), one expiry. |
| **Hash** | `dis_saved_sessions` (§2.2), `dis_session_meta` (§2.3) | Field-value pairs under one key per device — natural fit for "many sessions, each with their own data, sharing one device-scoped key." |
| **Set** | `dis_active_devices` (§2.4) | Unordered collection of unique members (`{tenant_id}:{device_id}` strings) — membership only, no ordering or scoring needed. |
| **Sorted Set (ZSET)** | `dis_session_by_end` (§2.6) | Needs members kept in score order (`end_dct`) at all times, for the backward walk (§4.3) to traverse without re-sorting on every call. |
| **Sorted Set (ZSET)** | `dis_embed_list` (§2.1) | Pinned as a ZSET (score = dct, member = serialized `(session_id, embedding)`) rather than Redis's native `LIST` type, which has no built-in sorted-insert — same reasoning the vehicle-window design above already worked through for its own analogous buffer ("Why a Sorted Set, not a List"). Gives sorted insertion, cheap pop-the-oldest (`ZPOPMIN`), and cheap neighbor (`prev`/`next`) lookups for free, all of which §4.2's matrix needs on every insert. One consequence: since `session_id` lives inside the member string, relabeling an entry (merges, splits) is a `ZREM`+`ZADD` pair, not an in-place field update. |

## Hash commands (`dis_saved_sessions`, `dis_session_meta`)

- **`HGET key field`** — read one field's value. Used to read one session's saved dct list
  (`dis_saved_sessions`) or one compound metadata field (`dis_session_meta`) before modifying it.
- **`HSET key field value`** — write one field's value. The write half of every read-modify-write
  on these hashes (append a dct, update `start_dct`/`end_dct`, etc.).
- **`HGETALL key`** — return every field *and* value in the hash in one round trip. §2.2 calls
  this out explicitly for the historical path's original "fetch every saved session" access
  pattern — since superseded for candidate search by the `dis_session_by_end` index (§2.6), but
  still the natural op for anything that genuinely needs every saved session's full data at once.
- **`HKEYS key`** — return just the field *names* (i.e. the session_ids), no values. Cheaper than
  `HGETALL` when only the set of ids is needed, not their data.
- **`HMGET key field1 field2 ...`** — read several *specific* fields in one call, without fetching
  the whole hash. This is exactly how `dis_session_meta`'s compound-field design (§2.3) reads one
  session's full metadata: `HMGET` its six `{session_id}:*` field names at once, rather than one
  `HGET` per field.
- **`HINCRBY key field increment`** — atomically add an integer to a field's current value, no
  read-modify-write needed. This is specifically why `dis_session_meta` uses compound fields
  instead of one JSON blob per session (§2.3) — `member_count` needs this atomic increment on
  every pop-type event, which a JSON blob can't support natively.
- **`HDEL key field`** — remove one field from the hash. Used at session evacuation (§5.2 step 6)
  to delete a finalized session's saved-dct-list and metadata fields.

## Set commands (`dis_active_devices`)

- **`SADD key member`** — add a member to the set (a no-op if already present). Adds a device on
  its first-ever live-path insert (§2.4).
- **`SREM key member`** — remove a member. Removes a device once it has no live entries and no
  saved sessions left (§2.4, §5.2 step 6).
- **`SMEMBERS key`** (or `SSCAN` for a very large set, incremental/cursor-based rather than one
  blocking call) — enumerate every member. What `sessions_eviction` iterates each cycle instead
  of a full keyspace `SCAN` across the whole Redis instance.

## Sorted Set commands (`dis_session_by_end`, `dis_embed_list`)

- **`ZADD key score member`** — add a member with a score, or update its score if the member
  already exists. Adds/updates a session's entry in `dis_session_by_end` whenever its `end_dct`
  changes (§2.6); also the insert op for `dis_embed_list` (§2.1) — every new image, and every
  `ZREM`+`ZADD` re-insert when relabeling an entry during a merge or split.
- **`ZREM key member`** — remove a member regardless of its score. Removes a merged-away or
  evacuated session's entry from the index; also the first half of relabeling a `dis_embed_list`
  entry (§2.1), since `session_id` lives inside the member string rather than a separate field.
- **`ZRANGEBYSCORE key min max`** — return members whose score falls in `[min, max]`, ascending
  order; Redis syntax `(value` makes a bound exclusive (matching this design's `gap < threshold`
  convention throughout — see §1). The general range-query primitive behind any "what falls in
  this dct window" check.
- **`ZREVRANGEBYSCORE key max min`** — the same range query, but returned in *descending* score
  order. This is specifically what §4.3's backward walk uses to traverse a device's saved sessions
  from most-recent to oldest.
- **`ZRANGE key rank1 rank2`** (or `ZRANGEBYSCORE` around a given member) — used against
  `dis_embed_list` (§2.1) to read a newly-inserted entry's immediate `prev`/`next` neighbors by
  their rank, exactly what the 8-case matrix (§4.2) compares against.
- **`ZPOPMIN key`** — atomically remove and return the lowest-scored member of `dis_embed_list`
  (§2.1) — exactly "pop the front/oldest entry to hold the list at size `N`" (§4.2 step 1) in one
  call.

## String / lock commands (`dis_device_lock`)

- **`SET key value NX PX <ms>`** — one atomic command combining three things: set the key's value,
  **only if it does not already exist** (`NX` — this is what makes it work as a mutual-exclusion
  primitive: only one caller's `SET ... NX` can ever succeed for a given key at a time), and
  **auto-expire it after `<ms>` milliseconds** (`PX` — the safety net against a crashed holder;
  `EX` is the same idea in whole seconds rather than milliseconds). This is the lock's acquire
  step (§2.5, §7.3).
- **`GET key`** — read a string's current value. Used in the lock's release script to check
  whether the key still holds *this caller's* token before deleting it.
- **`DEL key`** — delete a key outright. Never called unconditionally on the lock (that would risk
  deleting a different caller's lock, see §2.5) — only ever reached via the token-compare-then-`DEL`
  Lua script below.

## Scripting

- **`EVAL script numkeys key... arg...`** (Lua) — runs a script server-side as one atomic,
  uninterruptible step; nothing else can execute on that Redis instance between the script's
  individual commands. Two places in this design specifically need that atomicity:
  - **Lock release** (§2.5, §7.3): `GET` the lock key, compare its value to the caller's token,
    `DEL` only on a match. If this were two separate round trips (`GET` then `DEL`), another
    process could acquire the lock in the gap between them, and the blind `DEL` would delete that
    new holder's lock instead of a no-op.
  - **`insert_and_resolve_session`** (§7.3): the entire routing/insert/pop/8-case-matrix sequence
    (§4.1–§4.2) runs as one script or Lua-orchestrated sequence, so that a pop, a matrix
    resolution, a merge's saved-data reconciliation, and every metadata/index update they trigger
    all land as one indivisible unit — no other worker's operation on that device can interleave
    partway through, which is also why the per-device lock (§6) is acquired *before* this runs and
    held across the whole sequence rather than relying on `EVAL`'s atomicity alone (`EVAL`
    guarantees no interleaving *within* the script, not exclusivity *across* separate calls).

## Not used in this design (called out for the same reasons the vehicle-window design above rejected them)

- **`KEYS pattern`** — blocks the whole Redis server while it walks every key; never appropriate
  against a live instance. `dis_active_devices` (§2.4) exists specifically so nothing in this
  design needs `SCAN` (the safe, incremental alternative) or `KEYS` at all for device enumeration.
- **Redis Streams (`XADD`/`XRANGE`)** — require each new entry's ID to be strictly greater than
  the previous one, which doesn't tolerate the out-of-order arrivals this design explicitly
  accommodates (§2.1's sorted insertion, §4.1's mixed-request handling) — same reasoning the
  vehicle-window design above already worked through.
