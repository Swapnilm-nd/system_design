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
