# DIS Sliding-Window Split-Merge: Implementation Specification

Complete specification of the production mechanism for driver-invariant-session (DIS) split-merge
processing: a per-vehicle, size-bounded sliding embedding buffer with inline (per-image) session
resolution, backed by a saved-session store and a periodic finalization pass. This document
describes the system to be built — the algorithm, every parameter, every case and condition, the
known accepted gaps, and the concrete repository changes required to implement it.

---

## 1. System overview

Two components run per deployment:

- **The Recognize service** (existing production service, extended) — handles every incoming
  request synchronously. In addition to its existing work, it inserts each image into a
  per-vehicle Redis embedding buffer and resolves that image's session membership immediately,
  inline, before responding.
- **`window_handler`** (new service) — a single, non-replicated process that periodically scans
  saved (already-evicted) sessions and finalizes ones that have gone stale: computing a
  session-level driver-id decision, persisting it, and evacuating that session's Redis state.

A "vehicle" throughout means the `(tenant_id, vehicle_id)` pair — `vehicle_id` alone is not
unique across tenants, so every key and every piece of per-vehicle state is scoped to the pair.

Session membership is decided by two signals only: **cosine similarity between consecutive
images' face embeddings** (threshold `0.4`) and **the time gap between their
`device_capture_time` values** (dct-gap threshold, `10 minutes` by convention — see §3 for
parameter status). Both must pass for two images to be considered the same session. A missing or
invalid embedding on either side forces a split (never a merge).

---

## 2. Redis data model

All keys are scoped `{tenant_id}:{vehicle_id}` unless noted.

### 2.1 Embedding list — `dis_embed_list:{tenant_id}:{vehicle_id}`

A list, sorted by `device_capture_time` (dct), **bounded to `N` entries** (§3). Each entry is a
triple: **`(session_id, embedding, dct)`**.

- `dct` — the sort key, and the input to the dct-gap-threshold check against neighbors.
- `embedding` — the input to the similarity check against neighbors. Also what gets copied out
  when an entry pops, if it is currently the latest live member of its session (§2.3).
- `session_id` — the currently-assigned session tag for this image. This is what the live-path
  matrix (§4.2) reads and, in some cases, relabels for other entries too.

`tenant_id`/`vehicle_id` are not stored per-entry (implicit from the key). `image_id` is not
stored either — finalization (§5) queries the source-of-truth DB by dct range, not by per-image
lookup, so no per-entry identifier is needed in Redis.

### 2.2 Saved sessions — `dis_saved_sessions:{tenant_id}:{vehicle_id}`

A single Redis **HASH per vehicle**. Fields are `session_id`; values are that session's dct list
(JSON-encoded, since Redis hash field values are plain strings). This is where a session's data
lives once some or all of its members have popped out of the embedding list.

- The access pattern is always "given `(tenant_id, vehicle_id)`, fetch every saved session and
  scan for a dct-range match" — never a lookup by session_id alone, and never indexed by `dis`
  (one session can legitimately span more than one `dis` value, since session membership is
  decided purely by similarity/dct-gap, independent of `dis` boundaries — so `dis` is not a valid
  index for this data). A single HASH supports the real access pattern natively via
  `HGETALL`/`HKEYS` in one call.
- Appending a dct to one session is a per-field operation: `HGET` that field, deserialize, append,
  reserialize, `HSET` back. Cost is proportional to that one session's own list size, not the
  whole hash.
- This hash accumulates until `window_handler` finalizes and evacuates a session. Its size stays
  bounded as long as the finalization cadence (§5) keeps up with session-creation rate.

### 2.3 Retained boundary embedding — `dis_boundary_embed:{tenant_id}:{vehicle_id}`

A single Redis **HASH per vehicle**, same shape as §2.2. Fields are `session_id`; each value is
`[dct, embedding]` — the dct alongside the embedding, not the embedding alone, so a lookup here
never needs a round-trip to §2.2 just to know which dct the retained embedding belongs to.

For whichever session is currently the **latest-popped** (its most-recently-evicted member is
also its most-recent member overall), that member's `[dct, embedding]` is retained here,
separately from the plain dct list in §2.2, under its own **grace-period TTL** (distinct from the
blanket backstop TTL in §2.5). This exists solely to support forward-extension: a late arrival
whose dct is just past a saved session's known latest member needs *something* to compare
embeddings against, since §2.2 only stores dct, not embeddings.

- **Maintenance**: on every pop, unconditionally overwrite this session's field with the
  just-popped entry's `[dct, embedding]` — no check needed. This value can be transiently "behind"
  reality if the session still has more-recent members still live in the embedding list, but
  that's never a problem: the only time it's ever read is for a late arrival whose dct is older
  than the embedding list's current start_dct (§4.1), which by definition predates anything still
  live, including any of this session's own still-live members. The latest-popped-so-far
  `[dct, embedding]` is always the correct reference point at the moment it's actually needed.
- Not needed for backward or interior attachment (§4.3.1) — those are pure dct-range checks, no
  embedding required.

### 2.4 Session metadata

Per session: `tenant_id`, `tenant_drp`, `device_id`, `session_id`, `start_dct`, `end_dct`,
`created_at`, `last_updated_at`. `start_dct`/`end_dct` here are a cheap, always-available summary
of the session's own range — the same value that's derivable from the first/last elements of its
dct list in §2.2, kept alongside as a fast lookup rather than requiring a deserialize of that list
every time the range is needed. `last_updated_at` is what `window_handler` (§5) uses to decide
staleness.

`created_at` and `last_updated_at` start out equal at session creation (nothing has updated the
session beyond its own creation yet); `last_updated_at` moves forward only when the session
actually receives a new member later.

### 2.5 Blanket key TTL

A single, generous TTL applied to every key type above, as a leak backstop only. This is not the
real cleanup mechanism — that is the finalization cron (§5) — it exists purely so a bug or an
unswept edge case cannot leak Redis memory forever. Value: deferred (§3).

---

## 3. Parameters

| Parameter | Purpose | Status |
|---|---|---|
| `embed_list_max_size` (`N`) | Max size of the embedding list (§2.1) | **Not set.** Needs a real-data sizing analysis: how many images can arrive between the first and last member of a typical long session, so legitimate same-session neighbors don't get evicted before ever being compared. |
| `similarity_threshold` | Cosine-similarity cutoff for the live-path matrix and historical attach | `0.4`, from the offline DIS Split-Merge research. Decide whether to instead reuse the existing production `cosine_similarity_threshold` (`0.53`) used elsewhere in the embedding space, for consistency. |
| `dct_gap_threshold_seconds` | Max time gap between consecutive same-session images | Presumed `600` (10 min) by convention. **Never independently confirmed as a hard decision** — the single most load-bearing unconfirmed number in this design. |
| `boundary_embedding_grace_period_seconds` | TTL on the retained boundary embedding (§2.3) | Deferred. |
| `idle_eviction_seconds` | Force-evict a record stuck in the embedding list with nothing pressuring it out (a vehicle that stops sending entirely) | Deferred. |
| `finalize_unchanged_threshold_seconds` | How long a saved session must go untouched before `window_handler` finalizes it | Deferred. |
| `finalize_cron_interval_seconds` | How often `window_handler` runs its scan | Deferred. |
| `vehicle_lock_wait_timeout_seconds` / retry policy | Concurrency control on the per-vehicle lock (§6) | Deferred. |
| `redis_key_ttl_seconds` | Blanket backstop TTL (§2.5) | Deferred. |

All "deferred" parameters need an owner and a value before implementation is complete; none of
them are currently blocking the *shape* of the design, only its tuning.

---

## 4. Processing algorithm

Applies per incoming request. A request may carry multiple images.

### 4.1 Routing

Compare the request's dct(s) against `dis_embed_list`'s **current start_dct** (the dct of its
current front/oldest live entry):

- **`≥` start_dct** → live path (§4.2). Each image is processed **one at a time, in sequence** —
  never as a batch, even within one multi-image request.
- **`<` start_dct** → historical/group path (§4.3). Here the request's images **are** treated as
  one group for the routing decision (see §4.3.1 for the one place they get separated back out).

### 4.2 Live path — the embedding-list matrix

Per image, in order:

1. Insert the image into the embedding list at its correct sorted (dct) position, then pop the
   front (oldest) entry to hold the list at size `N`.
   - Popping is **unconditional**: push the popped entry's dct onto its session's field in
     `dis_saved_sessions` (§2.2), no check performed. If this entry is the session's current
     latest, also update its retained boundary embedding (§2.3).
2. Determine the newly-inserted image's own `session_id` from its actual immediate neighbors
   (`prev`, `next`) now present in the list:
   - **If `prev` and `next` both exist and already share the same `session_id`**: the new record
     carries that shared id, unconditionally. No similarity or dct-gap check is run — the new
     record sits positionally inside an already-established session, and that's treated as
     sufficient on its own.
   - **Otherwise** (no `prev`, no `next`, or `prev`/`next` belong to different sessions): check
     each neighbor independently on two signals — similarity (`cosine > 0.4`) and dct-gap (within
     threshold):

| prev ✓? | next ✓? | Resolution |
|---|---|---|
| ✓ | ✓ | **merge**: new record carries `prev`'s id; every `next`-tagged *live* entry gets relabeled to `prev`'s id |
| ✓ | ✗ | new record carries `prev`'s id |
| ✗ | ✓ | new record carries `next`'s id |
| ✗ | ✗ | new record gets its own fresh id |

- **Merges touch only live entries** — relabeling never reaches into `dis_saved_sessions`. Whether
  this can actually leave pre-existing saved data orphaned under `next`'s abandoned id is flagged
  as unresolved in §6, not confirmed as a real gap.
- With this rule, the live path never splits an already-established session on its own — a
  session's members, once linked while live, stay linked for as long as they remain live. The
  only place a session can still be split is the historical/saved "breaking" check in §4.3.2,
  step 3, which only ever applies once a session has no live representation left.

### 4.3 Historical/group path

Applies to a whole request whose dct(s) are `<` the embedding list's current start_dct.

**4.3.1 Internal split check.** First, check whether the request's own images internally split
(via the same similarity/dct-gap rule, applied just within this one arrival). If they do, each
resulting sub-group is handled independently through §4.3.2. This applies uniformly regardless of
image count — a single-image "group" and a five-image group follow the exact same rule, with no
special-casing for size.

**4.3.2 Per (sub-)group, check in this order:**

1. **Backward or forward extension** against an existing saved session:
   - *Backward*: does the group sit just before a session's earliest known dct, within gap
     threshold? Pure dct-range check, no embedding needed.
   - *Forward*: does the group sit just past a session's latest known dct, within gap threshold?
     Checked via that session's retained boundary embedding (§2.3) plus the dct-gap check.
   - If either matches: attach directly. No split.
2. **Bridging two different saved sessions** — the group's dct sits close enough to plausibly
   extend *either* of two independent, unrelated saved sessions. Resolved as: **do not merge, do
   not guess.** The group becomes its own new session regardless of which side it's closer to.
3. **Breaking an existing saved session** — the group's dct(s) land strictly between two dct
   values that are *both* already-confirmed members of the *same* saved session (an interior
   sandwich). This always triggers a three-way split, regardless of how many images are in the
   group.

   Worked example: `S1 = [10:00, 10:01, 10:15, 10:30, 10:50, 10:55, 10:56]`. A group arrives with
   `[10:41, 10:45, 10:49]` — falls between `S1`'s `10:30` and `10:50`:
   - `S1` truncates to `[10:00, 10:01, 10:15, 10:30]` (everything before the gap, same old id)
   - a new session takes the back half: `[10:50, 10:55, 10:56]` (new id)
   - the arriving group becomes its own new session: `[10:41, 10:45, 10:49]` (another new id)

   This check is purely positional — saved sessions generally don't retain embeddings for interior
   (non-latest) members, so no similarity comparison is possible or needed. The mere presence of
   dct's landing precisely inside an assumed-continuous gap is treated as sufficient proof the gap
   wasn't actually continuous.
4. **No match to anything above** → the (sub-)group becomes one brand-new saved session outright —
   all its own dct's stored together under one new session_id.

### 4.4 Why finalization can safely use a session's own tight dct range

Because the historical "breaking" split (§4.3.2 step 3) always narrows a session's own range to
exclude whatever caused the split, a session's own `[min(dct), max(dct)]` (derivable from the
first/last elements of its saved dct list, since pops/appends always happen in ascending order,
keeping that list naturally sorted) is guaranteed to never wrongly include another session's
members. This is what makes §5's DB range-query approach safe.

---

## 5. Finalization (`window_handler`)

Runs on its own cadence (`finalize_cron_interval_seconds`, §3). Per pass:

1. Scan `dis_saved_sessions` across active vehicles for sessions eligible to finalize: a session
   is eligible once `now - last_updated_at > finalize_unchanged_threshold_seconds` (§2.4). No
   separate check against `created_at` is needed — at creation `created_at` and `last_updated_at`
   start out equal, and `last_updated_at` only ever moves forward when the session actually
   receives a new member, so a single check against `last_updated_at` alone already captures both
   "freshly created and untouched" and "touched a while ago and now stale."
2. For each eligible session: fetch `predicted_driver_id` values from the source-of-truth DB, by
   querying that session's own tight `[min(dct), max(dct)]` range (§4.4) against the image-records
   table for that `(tenant_id, vehicle_id)` — a range query, not a per-image/`image_id` lookup
   (the saved dct list doesn't carry `image_id`).
3. Compute `final_predicted_driver_id`: unanimity among valid (`>0`) `predicted_driver_id` values,
   else `-1`. Apply the same authorization/group-assignment gating already used for the real-time
   per-image decision, so the session-level id stays consistent with what a real-time caller would
   have been told for the same images.
4. **Update the arcface table directly** with `final_predicted_driver_id` for this session's
   images — this decision is written there, not into a separate results table. Also recover
   `device_id` from the arcface table at this step, for the row persisted next.
5. Persist one row per session to `dis_session_details` (§7.4 for schema) — this table records
   which session existed and its boundaries/identity, not the prediction outcome itself.
6. Evacuate: delete that session's `dis_saved_sessions` field and, if present, its retained
   boundary embedding.

---

## 6. Concurrency model

**Processes, not threads**, for both the Recognize service (multiple independent worker
processes, one per queue consumer) and `window_handler` (a single, non-replicated process).
Reason: the split/merge work is CPU-bound (embedding math, similarity checks) and won't
parallelize across threads under Python's GIL; separate processes also isolate failures from each
other.

**Per-vehicle wait/lock**: since the ingestion queue gives zero per-vehicle ordering or
exclusivity, mutating operations on a vehicle's embedding list (insert, pop, relabel) are
serialized via a per-vehicle wait/lock — a worker processing an image for a given vehicle waits
for any other in-flight mutation on that same vehicle to finish first, rather than racing.

**`window_handler` takes the same per-vehicle lock.** For whichever vehicle's session it is
currently finalizing, `window_handler` acquires that vehicle's lock *before* reading the
session's data and holds it across the entire read → DB query → compute → persist → evacuate
sequence, releasing it only once that session's finalization is fully done. This closes a race
that would otherwise exist: without it, a live insert could land on the same session in the gap
between `window_handler` reading it (to decide the outcome) and deleting it — appending real data
that then gets silently wiped by the delete, with a decision already persisted that never saw it.
Holding the lock for the full sequence means that insert simply waits until finalization for that
vehicle completes, instead of racing it. This lock is scoped **per vehicle**, matching the
insert-side lock's own granularity — it does not block unrelated vehicles while one vehicle's
session is being finalized.

**One flagged-but-unconfirmed concern, not stated as settled**: a "merge-orphan" scenario was
proposed — the idea that when two sessions merge in the live buffer (§4.2), pre-existing saved
data under the abandoned id's `dis_saved_sessions` field could be left permanently unreconciled.
On working through concrete, mechanically-consistent examples, no valid sequence of arrivals could
actually be constructed that produces this: any attempt to give the abandoned ("next") session
pre-existing saved data at merge time either breaks that session's own chain formation earlier on,
or gets absorbed into it via the historical-attach path rather than staying a separate merge
participant. This is noted here as **unresolved, not as an accepted gap** — either a valid
triggering sequence still needs to be found, or this concern should be dropped as a non-issue.

---

## 7. Repository changes required

### 7.1 Config

New `DIS_WINDOW` config section, containing every parameter in §3:
`embed_list_max_size`, `similarity_threshold`, `dct_gap_threshold_seconds`,
`boundary_embedding_grace_period_seconds`, `idle_eviction_seconds`,
`finalize_unchanged_threshold_seconds`, `finalize_cron_interval_seconds`,
`vehicle_lock_wait_timeout_seconds`, `redis_key_ttl_seconds`, plus:

- `enabled_vehicle_ids = []` — explicit allowlist controlling rollout scope, empty (feature off
  everywhere) by default.

### 7.2 Recognize service extension

After existing per-image prediction, for any vehicle in `enabled_vehicle_ids`: build the per-image
record (existing fields + embedding + raw `predicted_driver_id`) and run the full insert/resolve
step (§4.1-§4.2) against that vehicle's embedding list. This must be:

- **Best-effort and non-fatal** — a failure here logs and continues; it must never block or fail
  the service's existing synchronous response path.
- **Inline, not a blind append** — unlike a simple queue-append, this step does real work
  (routing decision, insert, pop, neighbor-matrix resolution, possible relabeling) before returning.

### 7.3 New Redis methods

- `insert_and_resolve_session(tenant_id, vehicle_id, image_record)` — the core op implementing
  §4.1-§4.2: acquire the per-vehicle lock, run the full routing/insert/pop/matrix logic as one Lua
  script or Lua-orchestrated sequence for atomicity, release the lock.
- `resolve_historical_group(tenant_id, vehicle_id, group_records)` — implements §4.3 for the
  historical/group path.
- `pop_and_save(tenant_id, vehicle_id, popped_entry)` — the unconditional pop-push described in
  §4.2, including the boundary-embedding update when applicable.
- `finalize_and_evacuate_session(tenant_id, vehicle_id, session_id)` — used by `window_handler`
  (§5): read the session's saved dct range, delete its fields/boundary embedding after
  persistence succeeds.
- `acquire_vehicle_lock(tenant_id, vehicle_id)` / `release_vehicle_lock(tenant_id, vehicle_id,
  token)` — standard `SET ... NX PX <ttl>` acquire / Lua check-and-delete release, safe against a
  crashed holder via the TTL.

All new Redis-mutating methods must be registered wherever the codebase's dual-write routing
config lives, or they will silently write to only one backend instead of all configured ones.

### 7.4 New database table

`dis_session_details` (working name) — records that a session existed and its identity/
boundaries; it does **not** carry the prediction outcome, since `final_predicted_driver_id` is
written directly into the arcface table instead (§5, step 4):

| Column | Notes |
|---|---|
| `id` | UUID primary key |
| `tenant_id`, `vehicle_id` | |
| `session_id` | |
| `start_timestamp`, `end_timestamp` | the session's own tight dct range (§4.4) — this is what §5's finalization query uses, and what makes that query safe against interlopers |
| `device_id` | recovered from the arcface table during the finalization pass (§5, step 4), not populated from Redis-side data |
| `created_at` | |

### 7.5 `window_handler` service

A new, standalone process:

- Follows a drain-loop shape: on each cycle, scan `dis_saved_sessions` across active vehicles,
  finalize every eligible session (§5), then sleep until the next `finalize_cron_interval_seconds`
  tick.
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

1. **`embed_list_max_size` (`N`)** — no value yet; needs a real-data sizing analysis.
2. **`dct_gap_threshold_seconds`** — presumed 600s by convention, never independently confirmed
   as a hard decision.
3. **`similarity_threshold` (0.4) vs. an existing, separately-tuned production threshold (0.53)**
   used elsewhere in the same embedding space — decide whether to unify or keep them distinct.
4. **All other deferred parameters in §3** (grace period, idle-eviction, finalize threshold/
   interval, lock wait timeout, blanket TTL) — need an owner and a value.
5. **Locked-vehicle fallback behavior** — what a worker does when it cannot acquire a vehicle's
   lock (retry-then-drop vs. some other redelivery/backoff mechanism), especially given the lock
   is now acquired far more frequently (potentially every image) than a coarser, once-per-window
   design would need.
6. **`dis_session_details` table name/columns** — as listed in §7.4, open to adjustment.
7. **Merge-orphan concern (§6)** — unresolved, not confirmed as a real gap. Either find a valid,
   mechanically-consistent sequence that actually triggers it, or drop it from the design.
