# DIS Sliding-Window Split-Merge: Implementation Specification

Complete specification of the production mechanism for driver-invariant-session (DIS) split-merge
processing: a per-device, size-bounded sliding embedding buffer with inline (per-image) session
resolution, backed by a saved-session store and a periodic finalization pass. This document
describes the system to be built — the algorithm, every parameter, every case and condition, the
known accepted gaps, and the concrete repository changes required to implement it.

---

## 1. System overview

Two components run per deployment:

- **The Recognize service** (existing production service, extended) — handles every incoming
  request synchronously. In addition to its existing work, it inserts each image into a
  per-device Redis embedding buffer and resolves that image's session membership immediately,
  inline, before responding.
- **`sessions_eviction`** (new service) — a single, non-replicated process that periodically runs
  a two-phase sweep (§5) over each device's buffer, then finalizes sessions that have gone stale:
  computing a session-level driver-id decision, persisting it, and evacuating that session's Redis
  state.

A "device" throughout means the `(tenant_id, device_id)` pair — `device_id` alone is not
unique across tenants, so every key and every piece of per-device state is scoped to the pair.
`device_id` is unique per vehicle, so device-level scoping is equivalent to vehicle-level scoping
without depending on `vehicle_id` directly.

Session membership is decided by two signals only: **cosine similarity between consecutive
images' face embeddings** (threshold `0.4`) and **the time gap between their
`device_capture_time` values** (dct-gap threshold, `10 minutes` by convention — see §3 for
parameter status). Both signals use the same pass/fail convention: the check passes on a **strict**
comparison (`cosine > threshold`, `gap < threshold`) and fails on the boundary value itself
(`cosine == threshold` or `gap == threshold` both count as fail). Both must pass for two images to
be considered the same session. A missing or invalid embedding on either side forces a split
(never a merge).

---

## 2. Redis data model

All keys are scoped `{tenant_id}:{device_id}` unless noted.

### 2.1 Embedding list — `dis_embed_list:{tenant_id}:{device_id}`

A Redis **Sorted Set (ZSET)**, scored by `device_capture_time` (dct), **bounded to `N` entries**
(§3). Each member is a serialized `(session_id, embedding)` pair. A native Redis `LIST` was
considered and rejected — Lists have no built-in sorted insert, so every insert would need its own
read-scan-reinsert to find the correct position; a ZSET gives sorted insertion, cheap pop-the-
oldest, and cheap neighbor (`prev`/`next`) lookups natively, all of which §4.2's matrix needs on
every single insert.

- `dct` (the score) — the sort key, and the input to the dct-gap-threshold check against
  neighbors. `ZADD` inserts a new entry at its correct sorted position in one call; `ZRANGE`
  around a given member's rank reads its immediate `prev`/`next` neighbors; `ZPOPMIN` atomically
  removes and returns the lowest-scored (oldest) entry — exactly "pop the front entry to hold the
  list at size `N`" (§4.2 step 1) in one call.
- `embedding` — the input to the similarity check against neighbors, packed into the member string
  alongside `session_id` (e.g. a small JSON or binary encoding of both).
- `session_id` — the currently-assigned session tag for this image, part of the same member
  string as `embedding`. This is what the live-path matrix (§4.2) reads and, in some cases,
  relabels for other entries too — since `session_id` lives inside the member string rather than
  a separate field, **relabeling an entry means `ZREM` the old member and `ZADD` a new one at the
  same score** with the updated `session_id` baked in, not an in-place field update. This is the
  concrete cost of every merge (§4.2 case 2, relabeling every `next`-tagged live entry) and every
  split (cases 3/5/7, relabeling everything peeled off) — one `ZREM`+`ZADD` pair per relabeled
  entry, `O(log N)` each.

`tenant_id`/`device_id` are not stored per-entry (implicit from the key). `image_id` is not
stored either — finalization (§5) queries the source-of-truth DB by dct range, not by per-image
lookup, so no per-entry identifier is needed in Redis.

### 2.2 Saved sessions — `dis_saved_sessions:{tenant_id}:{device_id}`

A single Redis **HASH per device**. Fields are `session_id`; values are that session's dct list
(JSON-encoded, since Redis hash field values are plain strings). This is where a session's data
lives once some or all of its members have popped out of the embedding list.

- The access pattern is always "given `(tenant_id, device_id)`, fetch every saved session and
  scan for a dct-range match" — never a lookup by session_id alone, and never indexed by `dis`
  (one session can legitimately span more than one `dis` value, since session membership is
  decided purely by similarity/dct-gap, independent of `dis` boundaries — so `dis` is not a valid
  index for this data). A single HASH supports the real access pattern natively via
  `HGETALL`/`HKEYS` in one call.
- Appending a dct to one session is a per-field operation: `HGET` that field, deserialize, append,
  reserialize, `HSET` back. Cost is proportional to that one session's own list size, not the
  whole hash.
- This hash accumulates until `sessions_eviction` finalizes and evacuates a session. Its size stays
  bounded as long as the finalization cadence (§5) keeps up with session-creation rate.

### 2.3 Session metadata — `dis_session_meta:{tenant_id}:{device_id}`

A single Redis **HASH per device**. Unlike §2.2, fields are **compound** —
`{session_id}:tenant_drp`, `{session_id}:start_dct`, `{session_id}:end_dct`,
`{session_id}:created_at`, `{session_id}:last_updated_at`, `{session_id}:member_count` — rather
than one JSON blob per session_id. This is specifically so `member_count` (below) can be updated
with a native atomic `HINCRBY` on its own compound field, instead of a read-modify-write of an
entire blob for a single-field increment; a full read of one session's metadata is a `HMGET` of its
six compound field names. `tenant_id`/`device_id` are not stored here either, same reasoning as
§2.1 — already implicit from the key.

- **`start_dct`/`end_dct`** are a cheap, always-available summary of the session's own *saved*
  range — the same value that's derivable from the first/last elements of its dct list in §2.2,
  kept alongside as a fast lookup rather than requiring a deserialize of that list every time the
  range is needed. The rule that governs when they change: **they change only when the saved dct
  list itself changes.** That happens two ways — a live-list pop (§4.2 step 1) or
  `sessions_eviction`'s Phase 1 forced flush (§5.1) moving a member from live to saved, or a historical-path write
  (§4.3.2), which writes to the saved list *directly*, with no separate pop step at all. A
  historical attach therefore updates `start_dct`/`end_dct` in the very same step that bumps
  `last_updated_at` (§4.3.2) — that's not an exception to "assignment doesn't touch these fields,"
  it's the same rule: for historical writes, "assignment" and "the saved list just changed" are one
  event. What genuinely never touches these fields is a **live-path assignment to an already-
  existing session** (joining an established session while still live, or being relabeled during a
  merge) — that data hasn't reached the saved list yet, and won't update `start_dct`/`end_dct`
  until it eventually pops. A newly-created session (live fresh-id, or any historical-path
  creation) simply initializes both to its own first member's dct at creation time — the trivial
  case, not a third rule.
- **`last_updated_at`** is what `sessions_eviction`'s Phase 2 (§5.2) uses to decide staleness, and
  what its Phase 1 (§5.1) uses to decide which live sessions have gone idle. It is
  **assignment-driven, never pop-driven**: every operation that tags an image with a session_id —
  live-path matrix assignment (§4.2, always run, no skip case) or any historical-path outcome
  (§4.3.2) — bumps it to wall-clock **now** (never backdated to the underlying image's own dct,
  even for old historical data — see the rationale in §5.1). No pop-type event ever touches it,
  including Phase 1's forced flush, deliberately: that's what lets it correctly answer "is this
  session still receiving new members" independent of how often existing members happen to
  physically move from live to saved.
- `created_at` and `last_updated_at` start out equal at session creation; `last_updated_at` moves
  forward only on a later assignment event.
- `tenant_drp` is cached here (copied from the tenant record at session-creation time) purely to
  support §5.2 step 2's partition-pruned DB query — it's a genuinely derived value, not implicit
  from the key, unlike `tenant_id`/`device_id`.
- **`member_count`** is the session's saved member count, read directly by §5.2 step 5 for
  `dis_session_details.num_images` — `O(1)` instead of deserializing the full dct list (§2.2) just
  to take its length. It follows the **exact same update rule as `start_dct`/`end_dct`**: it
  changes only when the saved dct list itself changes, via `HINCRBY {session_id}:member_count 1` on
  each individual pop (§4.2 step 1, `sessions_eviction`'s Phase 1) or `HINCRBY ... <group size>` on
  a historical-path write (§4.3.2), which sets a group's worth of members at once since historical
  writes land directly in the saved list. A live-path assignment to an existing session never
  touches it, for the same reason it never touches `start_dct`/`end_dct` — that member hasn't
  reached the saved list yet. A merge (§4.2 case 2) sums the surviving and abandoned ids'
  `member_count`s; a "breaking" split (§4.3.2) apportions the original count across however many
  session-sourced groups the merge walk (§4.3.1) produces, each keeping its own group's size.

### 2.4 Device enumeration set — `dis_active_devices`

A single **global** Redis `SET` (not scoped per-device — its whole purpose is cross-device
enumeration), members are `{tenant_id}:{device_id}` strings.

- **Added to**: on a device's first-ever live-path insert (§4.2), if not already present.
- **Removed from**: once a device has both an empty `dis_embed_list` and an empty
  `dis_saved_sessions` (checked at the end of `sessions_eviction`'s per-device pass, §5.1/§5.2) —
  including the case where a tenant's `DIS_WINDOW.enabled` flag is turned off and its devices'
  buffered state fully drains out with nothing new arriving to replace it (§7.2).
- This is what `sessions_eviction` iterates each cycle instead of a full Redis keyspace `SCAN`, and
  what its Phase 2 saved-session scan uses for its own per-device enumeration — both phases share
  this one structure, no separate scored index needed.

### 2.5 Per-device lock — `dis_device_lock:{tenant_id}:{device_id}`

A plain Redis **string** key, holding a caller-generated token as its value, with expiry set
directly on the acquiring `SET` (no separate TTL bookkeeping). This is the single mutual-exclusion
point every mutating path in this design funnels through (§6): live-path inserts, the historical
path, and both phases of `sessions_eviction`'s sweep all contend for the *same* key per device,
regardless of which of the three is calling.

**The protocol every caller must follow, without exception:**

1. **Generate a fresh, unique token** for this specific acquisition attempt (e.g. a UUID) — never
   reuse a token across separate acquire calls, even by the same process.
2. **Acquire** via `SET dis_device_lock:{tenant_id}:{device_id} <token> NX PX <ttl>` against the
   single backend designated by the `redis_read`/`valkey_read` flag (§6) — never the dual-write
   Router. If the `SET` fails (key already held), follow the still-open locked-device fallback
   policy (§8) rather than busy-looping indefinitely.
3. **Hold the lock across the entire mutating sequence**, not just a single Redis call — this is
   already required explicitly for live-path inserts (§4.2), the historical path (§4.3), and
   `sessions_eviction`'s Phase 1 + Phase 2 together (§5, §6); this section just states it as one
   uniform rule rather than repeating it per caller.
4. **Release only via the token-compare-delete Lua script** (§7.3) — `GET` the key, compare
   against the token from step 1, `DEL` only on a match. Never an unconditional `DEL`: if this
   caller's own acquisition already expired under `PX` and someone else has since acquired the
   lock, an unconditional delete would remove the *new* holder's lock instead of a no-op.
5. **Always release on every exit path**, including error/exception — a caller that mutates state
   and then fails before releasing must still release in a `finally`-equivalent block. The `PX`
   expiry is a backstop against a crash that skips this entirely, not a substitute for it; relying
   on the TTL alone would hold every other caller for that device up to the full TTL on every
   failure, not just genuine crashes.

### 2.6 Session ordering index — `dis_session_by_end:{tenant_id}:{device_id}`

One per-device Redis **Sorted Set**, scored by `end_dct`, member `session_id`. Purely additive:
`dis_saved_sessions` (§2.2) and `dis_session_meta` (§2.3) are unchanged by this; it exists solely
to drive an efficient ordered traversal of a device's saved sessions for §4.3's backward walk.

- **Why it's needed:** §2.2's own access pattern for the historical path is "fetch every saved
  session and scan for a dct-range match" — an `O(K)` operation per historical arrival, where `K`
  is the device's saved-session count. §4.3's backward walk needs sessions in strict chronological
  order to work at all; without an index, producing that order means fetching and sorting every
  saved session's cached range on every historical arrival — expensive exactly when finalization is
  already lagging and `K` has grown large, the worst time for it to also slow down further.
- **Why only one index, not two:** since sessions never wrongly overlap (§4.4), ordering by
  `start_dct` and ordering by `end_dct` produce the *identical* relative order among sessions for a
  device — if session A entirely precedes session B, `A.start <= A.end < B.start <= B.end` holds
  regardless of which endpoint you sort by. The walk only needs one consistent ordering to traverse
  sessions in; once positioned at a given session during the walk, its *other* boundary value
  (`start_dct`) is read directly off `dis_session_meta` (§2.3), not via a second index. An earlier
  version of this design used two indexes to support independent range queries per direction — that
  approach is superseded by §4.3's walk, which doesn't need independent range queries at all.
- **Maintenance:** kept in sync with `end_dct` (§2.3) — same update triggers, same events. Session
  creation adds an entry. A forward attach or a normal pop that advances `end_dct` updates the
  entry. A "breaking" truncation updates the old session's entry and adds fresh entries for the two
  new pieces. A merge removes the abandoned id's entry and updates the surviving id's entry to the
  combined range's `end_dct`.
- **Use:** `ZREVRANGEBYSCORE dis_session_by_end +inf -inf` (or a bounded start point) walks a
  device's saved sessions from most-recent to oldest — exactly the traversal §4.3's backward walk
  needs, in `O(log K)` per step instead of `O(K)` to re-derive the order from scratch each time.

---

## 3. Parameters

| Parameter | Purpose | Status |
|---|---|---|
| `embed_list_max_size` (`N`) | Max size of the embedding list (§2.1) | **Locked at `8`.** |
| `similarity_threshold` | Cosine-similarity cutoff for the live-path matrix and historical attach | **Locked at `0.4`**, from the offline DIS Split-Merge research. Confirmed *not* to unify with the existing production `cosine_similarity_threshold` (`0.53`) — that threshold answers a different question (matching a face to a known driver identity across visits, used in `new_face_clustering.py`), not whether two consecutive frames belong to the same continuous visit. Kept distinct. |
| `dct_gap_threshold_seconds` | Max time gap between consecutive same-session images | Presumed `600` (10 min) by convention. **The value itself is still never independently confirmed as a hard decision** — the single most load-bearing unconfirmed number in this design. (The comparison convention *is* now fixed: `gap < threshold` passes, `gap >= threshold` fails — see §1.) |
| `finalize_cron_interval_seconds` | How often `sessions_eviction` runs its sweep, **and** the single staleness bar both of its phases check `last_updated_at` (§2.3) against — Phase 1 (§5.1) uses it to decide whether to flush a session's remaining live entries to saved, Phase 2 (§5.2) uses it to decide whether to finalize a saved session | **Locked at `3600`** (1 hour). |
| `device_lock_wait_timeout_seconds` / retry policy | Concurrency control on the per-device lock (§6) | Deferred — tied to the still-open "locked-device fallback behavior" question in §8. |

There is deliberately only **one** timing parameter governing staleness, not two: earlier drafts of
this design had a separate `idle_eviction_seconds` (for Phase 1) and `finalize_unchanged_threshold_seconds`
(for Phase 2), which required an explicit ordering constraint between them to avoid Phase 2
finalizing a session before Phase 1 had flushed its live remnants. Collapsing both to
`finalize_cron_interval_seconds` removes that constraint by construction — there's only one number,
so it can't be misconfigured relative to itself. The tradeoff accepted knowingly: idle detection is
now tied to the sweep's own cadence rather than independently tunable, so the worst-case time to
catch an idle session stays roughly the same (between one and two sweep intervals) but can no
longer be tightened without also changing how often the whole sweep runs.

All "deferred" parameters need an owner and a value before implementation is complete; none of
them are currently blocking the *shape* of the design, only its tuning.

---

## 4. Processing algorithm

Applies per incoming request. A request may carry multiple images.

### 4.1 Routing

A request's images are **not** routed as a single unit — there's no guarantee every image in one
request falls on the same side of `dis_embed_list`'s **current start_dct** (the dct of its current
front/oldest live entry). Sort the request's images by dct first, then partition every image
individually:

- **Group L** — images with `dct >= start_dct` → live path (§4.2).
- **Group H** — images with `dct < start_dct` → historical/backward-walk path (§4.3).

**Group L is processed first, in full, before Group H begins — this order is required, not
incidental.** Group L's own processing (§4.2) can trigger pops that create or extend the *most
recent* saved session's boundary, purely as a side effect of buffer pressure from Group L's own
insertions — independent of anything to do with Group H. Group H's backward walk (§4.3) starts
from whatever is currently the most recent saved session, so it needs that boundary to already
reflect anything Group L's processing produced. Concrete example: `start_dct = 10:00`, Group H has
an image at `09:58`, and Group L's insertions push a pre-existing live entry at `10:02` out of the
buffer and into `dis_saved_sessions`. Gap `09:58` → `10:02` is 4 minutes, a genuine forward-
extension candidate — but only if that pop has already happened by the time Group H is evaluated.
Processing Group H first would miss it entirely and could wrongly create a standalone session that
should have extended forward. Group L never depends on Group H the other way around: `start_dct`
only ever advances forward as the live buffer fills, so Group H's membership — fixed by the
*original* `start_dct` at the moment this request began — remains entirely valid no matter how far
Group L's own processing subsequently advances `start_dct`; nothing in Group H's set of images
needs to be re-evaluated after Group L runs.

- **Group L**: each image processed **one at a time, in increasing dct order**, exactly as §4.2
  already describes — never as a batch, even within one multi-image request.
- **Group H**: processed as one pass **in decreasing dct order**, per §4.3's backward walk.

### 4.2 Live path — the embedding-list matrix

Per image, in order:

1. Insert the image into the embedding list at its correct sorted (dct) position, then pop the
   front (oldest) entry to hold the list at size `N`.
   - Popping is **unconditional**: push the popped entry's dct onto its session's field in
     `dis_saved_sessions` (§2.2), no check performed. Also update that session's `start_dct`/
     `end_dct` and `HINCRBY` its `member_count` in `dis_session_meta` (§2.3) accordingly, and
     update its entry in `dis_session_by_end` (§2.6) if `end_dct` moved. **Never touch
     `last_updated_at` here** — popping is a storage-tier move, not new information about the
     session.
2. Determine the newly-inserted image's own `session_id` via the **8-case matrix**, comparing it
   against its actual immediate neighbors (`prev`, `next`) now present in the list, on two signals
   — similarity (`cosine > 0.4`) and dct-gap (`gap < 600`, pending §3) — plus whether `prev` and
   `next` already carry the *same* underlying session_id or a *different* one:

| Case | prev ✓? | next ✓? | prev/next same session? | Resolution |
|---|---|---|---|---|
| 1 | ✓ | ✓ | same | new record carries that shared id |
| 2 | ✓ | ✓ | different | **merge**: new record carries `prev`'s id; every `next`-tagged *live* entry gets relabeled to `prev`'s id |
| 3 | ✓ | ✗ | same | new record carries `prev`'s id; every entry after it still sharing the old id gets a **fresh** id (split) |
| 4 | ✓ | ✗ | different | new record carries `prev`'s id, nothing else changes |
| 5 | ✗ | ✓ | same | new record gets a **fresh** id; that same fresh id is carried to `next` and everything after it still sharing the old id |
| 6 | ✗ | ✓ | different | new record carries `next`'s id, nothing else changes |
| 7 | ✗ | ✗ | same | new record gets its **own** fresh id (isolated); `next` and everything after it still sharing the old id gets a **different** fresh id (three-way split) |
| 8 | ✗ | ✗ | different | new record gets its own fresh id, nothing else changes |

   - Whichever session_id the new record ends up carrying, bump that session's `last_updated_at`
     to now in `dis_session_meta` (§2.3) — this is an assignment event. The same applies to every
     id a split (cases 3/5/7) peels off or a merge (case 2) folds in.
   - **Splits (3/5/7) never need to worry about pre-existing saved data for the newly-created id**,
     since it never existed before this moment — every id a split produces gets a fresh
     `dis_session_meta` record the same way any other fresh-id creation does (`start_dct = end_dct`
     = that portion's own first live member's dct, `member_count = 0` until its own members
     eventually pop, `created_at = last_updated_at = now`), with a fresh entry in the range index
     (§2.6). Only **merges (case 2) touch a pre-existing id**, which is where the reconciliation
     below applies.
   - **Merges also reconcile pre-existing saved data**, not just live entries: if the abandoned
     (`next`) id has an existing `dis_saved_sessions` field, merge its dct list into the surviving
     (`prev`) id's list (properly ordered), combine session metadata (`start_dct = min`,
     `end_dct = max`, `member_count` = the sum, of both sides, `last_updated_at = now`), remove the
     abandoned id's entry from the range index and update the surviving id's entry to the
     combined range's `end_dct`, and delete the abandoned id's now-empty saved-session and metadata entries.
     This closes the "merge-orphan" scenario: without it, a session that already has some members
     popped to saved (while newer members remain live) can have its live remainder absorbed into a
     different session via merge, permanently stranding its saved data under a session_id nothing
     will ever reference again. A concrete sequence produces this — see §6 — so this reconciliation
     is required, not optional hardening.
- **The live path can split an already-established session on its own** (cases 3/5/7) — unlike a
  merge, a split never touches pre-existing saved data, since whichever portion keeps the old id
  keeps its saved history untouched, and the newly-split-off portion has none yet. This is in
  addition to, not instead of, the historical/saved "breaking" resolution in §4.3.2, which covers
  the case where a session has no live representation left at all.

### 4.3 Historical/backward-walk path

Applies to Group H (§4.1) — only ever entered after Group L has been fully processed, per §4.1's
ordering requirement. All mutations in this path — walking saved sessions, deciding an outcome,
writing to `dis_saved_sessions` and `dis_session_meta` — happen under the same per-device lock used
by the live path and by `sessions_eviction` (§6), held across the *entire* walk, however many
sessions it ends up touching.

**Lock-duration tradeoff, accepted as-is:** a Group H spanning a large historical backlog (e.g. a
device that was offline a long time before a large delayed batch arrives) can touch an unbounded
number of saved sessions in one pass, holding the per-device lock for correspondingly longer and
blocking other work on that device — live inserts, `sessions_eviction` — for the duration. This is
a real latency cost, not a correctness problem (everything stays safely serialized), and is
accepted rather than capped; revisit if it becomes an operational issue (e.g. a max-sessions-per-
pass limit, continuing the walk on a subsequent call). This cost is higher than it might first
appear: the merge below reads each touched session's *entire* saved dct list (§2.2), not just its
cached range summary, so the walk's cost scales with the total number of saved members across every
session it touches, not just the count of sessions.

**4.3.1 Backward merge walk.** Two sequences, both traversed in strictly decreasing dct order:
Group H's own images (already sorted descending, §4.1), and the device's saved sessions, visited
one at a time in descending order via `ZREVRANGEBYSCORE dis_session_by_end` (§2.6) — within
whichever session is currently being visited, its own full dct list (§2.2) is read and consumed in
descending order too.

At each step, compare the largest remaining element from each sequence and consume the larger one.
**On an exact tie, the session-sourced element is always consumed first** — an arriving image is
treated as *not* greater than an existing confirmed member at the same dct, deferring to it rather
than triggering a group switch on its own. Group consecutive same-source picks together; a new
group starts every time the source switches. When the current session's own list is exhausted,
advance to the next-older session (via the index) and continue; when Group H is exhausted, stop —
any remaining session-sourced elements simply become the final group(s), and (since sessions never
overlap, §4.4) the next-older session's `end_dct` should already be below whatever's left, a
consistency check rather than something the ordinary path needs to act on.

This directly discovers every position where an existing session's own continuity is actually
interrupted, rather than inferring it from the arriving images' mutual continuity with *each
other*. That distinction matters concretely: two arriving images can be close to each other in time
and still need two independent resolutions, if a confirmed existing member sits between them.
Worked example: `Sn = [10:00, 10:08, 10:16, 10:24]` (every adjacent gap a legitimate 8 minutes), a
historical batch arrives with `[10:04, 10:20]` (only 16 minutes apart from each other, but each
lands in a *different* gap of `Sn`). Merging `Sn`'s descending list `[10:24, 10:16, 10:08, 10:00]`
against the arriving descending list `[10:20, 10:04]`:

`10:24`(session) → `10:20`(**arrival**, switch) → `10:16`(session, switch) → `10:08`(session, same)
→ `10:04`(**arrival**, switch) → `10:00`(session, switch, arrivals now exhausted)

Five groups: `[10:24]`, `[10:20]`, `[10:16, 10:08]`, `[10:04]`, `[10:00]` — two independent
interruptions correctly discovered as separate, not collapsed into one.

**4.3.2 Resolving each resulting group:**

- **Session-sourced groups** — pieces of an existing session's own confirmed data, produced
  whenever that session's continuity survives one or more stretches uninterrupted by an arrival.
  Across the *whole* walk, the single **oldest** (last-consumed, chronologically earliest)
  session-sourced group keeps that session's original id — its `dis_session_meta` entry has
  `end_dct` rewritten down to this group's own latest dct, `member_count` reduced to this group's
  own size, `last_updated_at` bumped to now (a material change to this session's identity), and its
  `dis_session_by_end` entry updated to match. **Every other** session-sourced group — anything
  isolated between two interruptions, or the newest remaining piece before the first one — gets a
  fresh `dis_session_meta` record and range-index entry the same as any other new-session creation
  (`start_dct`/`end_dct` = this group's own min/max, `member_count` = this group's own size,
  `created_at = last_updated_at = now`).
- **Arrival-sourced groups** — resolved by what brackets them in the merge sequence, which the walk
  already knows without any further search:
  - **Bracketed by session-sourced groups belonging to the *same* original session on both sides**
    → **interior** → **breaking**, unconditionally, treating the whole group as one unit: this
    group becomes its own new session, no further check needed. Session data on both sides is what
    makes this safe without a mutual-continuity check — anything genuinely disconnected within an
    interior group would already have been separated by session-sourced elements sitting between
    it, per the merge itself (§4.3.1's worked example). Purely positional otherwise — saved
    sessions generally don't retain embeddings for interior (non-latest) members, so no similarity
    comparison is possible or needed. The mere presence of dct's landing precisely inside an
    assumed-continuous gap is treated as sufficient proof the gap wasn't actually continuous.
  - **Bracketed by session-sourced groups belonging to *two different* sessions, or on only one
    side** (the **gap** between two sessions where one's list was just exhausted and the
    next-older one is starting; or the **leading edge**, before `start_dct` itself; or the
    **trailing edge**, past the oldest saved session) → **run a mutual-continuity internal-split
    first, same similarity/dct-gap rule used everywhere else, then resolve each resulting
    sub-group independently.** Unlike the interior case, nothing session-sourced separates
    arrival elements that both happen to fall in open space between two sessions (or past an open
    edge) — the merge only ever switches groups when a *session*-sourced element interrupts an
    arrival run, so several mutually-unrelated arrivals can end up merged into one group here
    purely because nothing existed to interrupt them, even if they're far apart from each other
    and from both bracketing sessions. Concrete example: `Sn = [10:00]`, next-older
    `S_{i-1} = [08:00]`, an arriving batch of `[09:52, 09:20, 08:05]` — all three get consumed
    consecutively by the walk (nothing session-sourced falls between them), but `09:52` is a
    genuine backward-extension candidate for `Sn`, `08:05` a genuine forward-extension candidate
    for `S_{i-1}`, and `09:20` is close to neither — resolving them as one aggregate group would
    silently drag all three into whichever side the *group's* overall range happened to favor.
    Splitting first by mutual continuity separates them correctly before resolution runs.

    For each resulting sub-group: close to the newer session's `start_dct` only (backward
    extension) or the older session's `end_dct` only (forward extension) → attach directly,
    extending `start_dct`/`end_dct`/`member_count` (§2.3) and bumping `last_updated_at` on that
    session, and updating its range-index entry; close to **both** → **bridging** — do not merge,
    do not guess, the sub-group becomes its own new session regardless of which side it's closer
    to, with fresh `dis_session_meta` and range-index entries; close to **neither** → **no match**
    — the sub-group becomes one brand-new saved session outright, same metadata treatment as
    bridging. On the leading or trailing edge, where only one bounding session exists, bridging is
    impossible by construction — there's nothing on the open side to be close to, so only one
    direction of extension (or no-match) applies.

### 4.4 Why finalization can safely use a session's own tight dct range

Because both the live-path split (§4.2, cases 3/5/7) and the historical "breaking" split (§4.3.2,
resolved via the backward merge walk of §4.3.1) always narrow a session's own range to exclude
whatever caused the split — the live-path
version by re-tagging live entries before any of them ever reach the saved store, the historical
version by directly narrowing already-saved data — and because merges now reconcile saved data
instead of orphaning it (§4.2, §6), a session's own `[start_dct, end_dct]` (§2.3) is guaranteed to
never wrongly include another session's members. This is what makes §5.2's DB range-query approach
safe.

---

## 5. Finalization (`sessions_eviction`)

`sessions_eviction` runs on its own cadence (`finalize_cron_interval_seconds`, §3). Each cycle, for
every device in `dis_active_devices` (§2.4), it runs a **two-phase sweep**, with both phases for a
given device executed under that device's per-device lock (§6), acquired once and held across both.

### 5.1 Phase 1 — live-list walk

This phase replaces what would otherwise need two separate mechanisms (a whole-device idle check,
and a way to keep a still-active device's individual stale sessions from being finalized off an
incomplete range): a single per-session sweep of the live buffer.

Walk `dis_embed_list` front-to-back (oldest dct first). For each entry, look up its session's
`last_updated_at` in `dis_session_meta` (§2.3):

- **If stale** (`now - last_updated_at > finalize_cron_interval_seconds`): push this entry into
  `dis_saved_sessions` the same way a normal pop does (§4.2 step 1) — including updating that
  session's `start_dct`/`end_dct`/`member_count` and its range-index entries (§2.6) — but **do not
  touch `last_updated_at`**. Leaving it alone is
  deliberate: bumping it here would make the session look fresh again the instant it's flushed,
  and Phase 2 would skip finalizing it this cycle for no reason. Continue to the next entry.
- **If not stale**: stop the walk entirely. This is a heuristic, not a strict guarantee — it
  assumes staleness roughly tracks position in the dct-sorted list (older entries near the front
  are more likely to belong to sessions that have gone quiet), which holds in the ordinary case
  but isn't airtight if sessions ever interleave (e.g. a brief driver handoff and back). If a stale
  session sits further back and gets missed this cycle, that fails safe — it's caught on the next
  hourly pass, not silently corrupted.

A fully idle device is just the case where every one of its live sessions goes stale — this walk
catches that automatically, with no separate device-level idle signal needed.

**Why `last_updated_at` is always "now," never backdated to the underlying image's dct — for both
this phase and every assignment event in §4.2/§4.3.2:** the field exists to answer "is this session
likely to receive more data," which is a question about arrival/processing patterns, not content
age. A historical group showing up at all is itself evidence the pipeline has some delay, and
delayed pipelines don't reliably deliver everything in one shot — more of the same delayed batch
could still be in transit. Backdating risks finalizing a session as soon as it's first seen (its
`last_updated_at` looks stale immediately), only to have a related delayed continuation arrive
later to find the session already finalized and evacuated, with no mechanism anywhere in this
design to un-finalize or amend a decision once made. Using "now" costs at most one extra
`finalize_cron_interval_seconds` window of delay for data that's genuinely done — a small,
bounded cost against an unbounded one.

### 5.2 Phase 2 — saved-session scan

Runs immediately after Phase 1 for the same device, under the same lock. Because Phase 1 already
folded any stale session's live remnants into `dis_saved_sessions` first — checked against the
*same* `finalize_cron_interval_seconds` bar (§3) — this phase always sees the *complete* range for
anything it finalizes; there's no separate, independently-tunable threshold that could fall out of
sync with Phase 1's.

1. Scan `dis_saved_sessions` for this device for sessions eligible to finalize: a session is
   eligible once `now - last_updated_at > finalize_cron_interval_seconds` (§2.3). No
   separate check against `created_at` is needed — at creation `created_at` and `last_updated_at`
   start out equal, and `last_updated_at` only ever moves forward on an assignment event, so a
   single check against `last_updated_at` alone already captures both "freshly created and
   untouched" and "touched a while ago and now stale."
2. For each eligible session: fetch `predicted_driver_id` values from the source-of-truth DB.
   `arcface_return_details_v2` has no `device_capture_time`, `tenant_id`, or `device_id` column of
   its own — this is a **join**, not a single-table range query: filter `image_details` by
   `(tenant_id, device_id)` and `device_capture_time` in `[start_dct, end_dct]` (§4.4), then join
   to `arcface_return_details_v2` on `audit_id` for `predicted_driver_id`. The cardinality of that
   join (whether a given `audit_id` can have more than one `arcface_return_details_v2` row, e.g.
   due to `retroactive_completed` reprocessing) is not yet confirmed against real data — see §8.
3. Compute `final_predicted_driver_id`: unanimity among valid (`>0`) `predicted_driver_id` values,
   else `-1`. Apply the same authorization/group-assignment gating already used for the real-time
   per-image decision, so the session-level id stays consistent with what a real-time caller would
   have been told for the same images.
4. **Update the arcface table directly** with `final_predicted_driver_id` for this session's
   images — this decision is written there, not into a separate results table. The `UPDATE`
   explicitly filters on `tenant_drp` (cached in `dis_session_meta`, §2.3) in addition to the
   `device_capture_time` range, for partition pruning against `arcface_return_details_v2`'s
   `LIST(tenant_drp)` + weekly `timestamp` range partitioning. `device_id` for the row persisted
   next is already known — it's the session's own scoping identity (§1), not something to look up
   here.
5. Persist one row per session to `dis_session_details` (§7.4 for schema) — this table records
   which session existed, its boundaries/identity, and its member count, not the prediction
   outcome itself. `final_assigned_driver_id` is a `NOT NULL` column on this table but is not used
   to carry the outcome here — every row writes the sentinel `-1` into it (matching the existing
   "`-1` = unknown" convention used elsewhere for `driver_id`), and `num_images` is populated
   directly from `dis_session_meta`'s `member_count` (§2.3) — no need to deserialize the dct list
   just to take its length. The real decision lives only in the arcface table (step 4); no
   `session_id` is added to the arcface table in either direction.
6. Evacuate: delete that session's `dis_saved_sessions` and `dis_session_meta` fields. If, after
   this, the device has no remaining live entries and no remaining saved sessions, remove it from
   `dis_active_devices` (§2.4) too — otherwise it lingers in that set indefinitely, and every
   future cycle wastes a check on a device with nothing left to do (this is also what makes
   disabling a tenant's `DIS_WINDOW.enabled` flag mid-flight safe with no special handling: new
   inserts simply stop, §7.2, and the existing buffered/saved state drains through this same
   two-phase sweep like any other device, eventually removing itself from `dis_active_devices` once
   fully finalized).

---

## 6. Concurrency model

**Processes, not threads**, for both the Recognize service (multiple independent worker
processes, one per queue consumer) and `sessions_eviction` (a single, non-replicated process).
Reason: the split/merge work is CPU-bound (embedding math, similarity checks) and won't
parallelize across threads under Python's GIL; separate processes also isolate failures from each
other.

**Per-device wait/lock**: since the ingestion queue gives zero per-device ordering or
exclusivity, mutating operations on a device's embedding list (insert, pop, relabel) are
serialized via a per-device wait/lock — a worker processing an image for a given device waits
for any other in-flight mutation on that same device to finish first, rather than racing. This
same lock covers the **historical path** (§4.3.2) for its full read-modify-write sequence, and
**both phases of `sessions_eviction`'s sweep** (§5.1, §5.2) for a given device, acquired once and
held across both — every mutating path for a device funnels through one mutual-exclusion point.
The lock's storage (`dis_device_lock:{tenant_id}:{device_id}`) and the exact acquire/hold/release
protocol every caller must follow are specified in §2.5.

**`sessions_eviction`'s Phase 2 takes this same lock across its full sequence.** For whichever device's
session it is currently finalizing, it acquires that device's lock *before* reading the session's
data and holds it across the entire read → DB query → compute → persist → evacuate sequence,
releasing it only once that session's finalization is fully done. This closes a race that would
otherwise exist: without it, a live insert or a historical attach could land on the same session in
the gap between reading it (to decide the outcome) and deleting it — appending real data that then
gets silently wiped by the delete, with a decision already persisted that never saw it. Holding the
lock for the full sequence means that insert or attach simply waits until finalization for that
device completes, instead of racing it. This lock is scoped **per device**, matching the
insert-side lock's own granularity — it does not block unrelated devices while one device's
session is being finalized.

**The lock targets a single backend, never the dual-write path.** A lock's `SET NX PX` is a
race-sensitive primitive: if it were dual-written to two independent backends (as this codebase's
existing `Router.write_methods` migration mechanism does for ordinary state writes), two workers
could each win the NX-set on a *different* backend simultaneously and both believe they hold the
lock — silently defeating every guarantee above. Instead, `acquire_device_lock`/
`release_device_lock` (§7.3) always target whichever single backend the existing `redis_read`/
`valkey_read` flag currently designates as authoritative (confirmed mutually exclusive at startup)
— never both, regardless of what `redis_write`/`valkey_write` say. Every worker consults the same
flag, so all lock contention happens against one instance, and `SET NX PX` is atomic within a
single instance.

**Merge-orphan scenario — confirmed real, and fixed (§4.2).** A live-path merge (§4.2, `prev✓
next✓`) relabels the abandoned (`next`) session's *live* entries to the surviving (`prev`) id.
Without reconciling saved data too, this can permanently strand pre-existing saved data under the
abandoned id. Concrete sequence: device `V`, one real driver, a ~13-minute gap in captures splits
one continuous visit into two live session ids before they reconnect — session `A` with live
members `08:50`, `08:55`; session `B` forms independently at `09:08` (gap to `A`'s `08:55` = 13
min, over threshold, correctly a fresh id), grows to `09:13`, `09:20`, then pops its oldest member
(`09:08`) to saved while `09:13`/`09:20` remain live. A bridging arrival at `09:04` (captured
before `09:08` but processed after it — ordinary arrival reordering within the live window) lands
with neighbors `08:55` (`A`, prev) and `09:13` (`B`'s now-earliest live entry, next); both gaps
pass, triggering a merge — `B`'s live entries relabel to `A`, but without the fix, `B`'s saved
`[09:08]` would be left stranded under an id nothing will ever reference again. §4.2's merge step
now explicitly reconciles this: merging saved data into the surviving id, combining metadata, and
deleting the abandoned id's now-empty entries — all within the same per-device lock this section
already requires for merges, no new locking needed.

---

## 7. Repository changes required

### 7.1 Config

New `DIS_WINDOW` config section, containing every parameter in §3:
`embed_list_max_size`, `similarity_threshold`, `dct_gap_threshold_seconds`,
`finalize_cron_interval_seconds`, `device_lock_wait_timeout_seconds`.

Rollout scope is **not** a static config allowlist. It's a per-tenant flag,
`tenant.config["DIS_WINDOW"]["enabled"]` (boolean, default absent/false), stored in the existing
`tenant_details.config` JSON column (`frs_src/orm.py`) — the same column `sync_vls_configs.py`
already uses for other per-tenant settings blobs. Modeled after that column rather than
`tenant_details.vls_enabled` specifically, because `vls_enabled` is auto-synced from an external
system (IDMS, reflecting customer entitlement) via an hourly cron, and this flag is an internal
engineering rollout control with no external system of record — it needs its own manual toggle
(a one-off script updating specific `tenant_id`s through the normal ORM path, which gets audit-
logged via `tenant_details_audit` for free), not folded into that sync job. When enabled for a
tenant, it applies to every device under that tenant — no separate per-device allowlist.

### 7.2 Recognize service extension

After existing per-image prediction, for any device whose tenant has
`tenant.config["DIS_WINDOW"]["enabled"]` set: build the per-image record (existing fields +
embedding + raw `predicted_driver_id`) and run the full routing/resolve step (§4.1) against that
device's embedding list — which, per §4.1's Group L/Group H split, may invoke *both* the live-path
matrix (§4.2) and the historical backward-walk (§4.3) for a single incoming request, not §4.2
alone. This must be:

- **Best-effort and non-fatal** — a failure here logs and continues; it must never block or fail
  the service's existing synchronous response path.
- **Inline, not a blind append** — unlike a simple queue-append, this step does real work
  (routing decision, insert, pop, neighbor-matrix resolution, possible relabeling) before returning.

### 7.3 New Redis methods

- `insert_and_resolve_session(tenant_id, device_id, image_record)` — the core op implementing
  §4.1-§4.2: acquire the per-device lock, run the full routing/insert/pop/matrix logic (including
  merge-time saved-data reconciliation and session-metadata updates) as one Lua script or
  Lua-orchestrated sequence for atomicity, release the lock.
- `resolve_historical_group(tenant_id, device_id, group_records)` — implements §4.3's backward
  merge walk for Group H, under the same per-device lock for its full read-decide-write
  sequence, however many sessions the walk ends up touching.
- `pop_and_save(tenant_id, device_id, popped_entry)` — the unconditional pop-push described in
  §4.2, including the `start_dct`/`end_dct`/`member_count` update in `dis_session_meta` (never
  `last_updated_at`) and the corresponding update to that session's entry in `dis_session_by_end`
  (§2.6).
- `sweep_idle_sessions(tenant_id, device_id)` — implements §5.1's Phase 1 live-list walk, used by
  `sessions_eviction`.
- `finalize_and_evacuate_session(tenant_id, device_id, session_id)` — used by `sessions_eviction`'s
  Phase 2 (§5.2): read the session's saved dct range and `member_count`, delete its
  `dis_saved_sessions`/`dis_session_meta` fields and its entry in the range index (§2.6) after
  persistence succeeds, and remove the device from `dis_active_devices` if it's now fully empty.
- `acquire_device_lock(tenant_id, device_id)` / `release_device_lock(tenant_id, device_id, token)`
  — **not** registered in the dual-write Router (see §6 for why); each targets a single backend,
  selected by the existing `redis_read`/`valkey_read` flag. Acquire is `SET ... NX PX <ttl>`
  against that one backend; release is a Lua script doing `GET`-compare-`DEL`, only deleting if the
  stored token still matches the caller's own, so a worker whose lock already expired and got
  re-acquired by someone else can't delete the new holder's lock.

All new Redis-mutating methods **other than the lock pair above** must be registered wherever the
codebase's dual-write routing config (`Router.write_methods` in `frs_src/utils.py`) lives, or they
will silently write to only one backend instead of all configured ones.

### 7.4 New database table

`dis_session_details` — already exists as a migration
(`db_migrations/frs/upgrade_20260908_create_dis_session_details_table.sql`); this section matches
that schema. It records that a session existed, its identity/boundaries, and its member count.
Despite having a `final_assigned_driver_id` column, it does **not** functionally carry the
prediction outcome — `final_predicted_driver_id` is written directly into the arcface table
instead (§5.2, step 4), and this table's own `final_assigned_driver_id` column is always written as
the sentinel `-1` (§5.2, step 5) because it's `NOT NULL` in the migration.

| Column | Type | Notes |
|---|---|---|
| `id` | `TEXT` primary key | |
| `tenant_id` | `INTEGER NOT NULL` | |
| `device_id` | `BIGINT NOT NULL` | the session's own scoping identity (§1) — already known throughout, not recovered from anywhere at finalization time |
| `session_id` | `TEXT NOT NULL` | |
| `start_device_capture_time`, `end_device_capture_time` | `TIMESTAMP NOT NULL` | the session's own tight dct range (§4.4) — this is what §5.2's finalization query uses, and what makes that query safe against interlopers |
| `final_assigned_driver_id` | `BIGINT NOT NULL` | always `-1` — not the outcome storage location (see above); the real decision lives only in the arcface table |
| `num_images` | `INTEGER NOT NULL` | the session's actual member count at finalization time |
| `created_at` | `TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP` | |

Indexes: `(tenant_id, device_id)` and `(tenant_id, device_id, session_id)`.

**Pending:** the migration file currently has this column named `vehicle_id`, predating the
device_id rename applied throughout this design — it needs updating to `device_id` (and its two
indexes renamed to match) directly in the migration file, outside the scope of this document.

### 7.5 `sessions_eviction`

A new, standalone process implementing the two-phase sweep logic (§5.1, §5.2):

- Follows a drain-loop shape: on each cycle, iterate `dis_active_devices` (§2.4), run
  Phase 1 then Phase 2 for each device under its lock, then sleep until the next
  `finalize_cron_interval_seconds` tick.
- Single instance, not replicated — its work is periodic and comparatively cheap, not a per-image
  hot path.
- Needs graceful-shutdown handling so an in-flight finalization batch can complete before the
  process exits, rather than leaving a session half-evacuated.
- Opens/commits/closes a database session per finalization batch, not one held for the process's
  entire lifetime.
- Packaging: one supervisor/process-manager entry (no replica group, no health-check aggregator),
  and a corresponding container image entry, matching whatever pattern the deployment already
  uses for other single-instance batch/cron-style services.

---

## 8. Open items requiring a decision before implementation

1. **`dct_gap_threshold_seconds`** — presumed 600s by convention, the value itself never
   independently confirmed as a hard decision (the comparison convention is fixed — see §1).
2. **`device_lock_wait_timeout_seconds`** — needs an owner and a value.
3. **Locked-device fallback behavior** — what a worker does when it cannot acquire a device's
   lock (retry-then-drop vs. some other redelivery/backoff mechanism), especially given the lock
   is now acquired far more frequently (potentially every image) than a coarser, once-per-window
   design would need.
4. **`image_details.audit_id → arcface_return_details_v2.audit_id` join cardinality** (§5.2 step
   2) — `arcface_return_details_v2`'s `retroactive_completed` flag suggests a given `audit_id`
   could have more than one row over time. If so, this design needs to say which row wins (most
   recent by `timestamp`, only where `retroactive_completed` is true, or otherwise) — needs
   verification against real data before implementation.
5. **`dis_session_details` migration's `vehicle_id` → `device_id` rename** (§7.4) — needs to be
   applied to the actual migration file, outside the scope of this document.
