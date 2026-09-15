# Cloud based DIS: Production Implementation Design (Two-Loop, Additive)

Working design reconciling `system_design.md` (the original two-loop split-merge design) with
what actually exists in production code, **updated** with a substantially different Redis
mechanism worked out in a later design session: a sliding, size-bounded embedding buffer with
inline split/merge resolution, replacing the earlier simple "one Redis LIST per vehicle, fixed
30-minute window, sweep everything on a timer" approach. **Not yet implemented** — this is a
design/plan document only.

Section 1-2 (production-codebase facts, the additive-not-replacement framing) are carried
forward unchanged from the earlier pass, since they remain true regardless of which Redis
mechanism sits underneath. Sections 3 onward describe the new mechanism in full detail.

---

## 1. Key reframing vs. `system_design.md`

The original design described the new Redis-append step as an append-only rewrite of the
Recognize worker (no session-boundary logic, no synchronous decision). Exploration of the current
`Recognize` implementation found that today's SQS→callback contract is fully synchronous:
`Recognize` computes `final_predicted_driver_id` and POSTs it to the caller's `callback_url`
**before the SQS message is even deleted** (`frs.py:143-160`). Deferring that decision to a
window close minutes later would break this contract for every caller.

Resolved (via user clarification) as **additive, not a replacement**:

- **The Recognize service, extended.** It keeps doing everything it does
  today — embedding, proximity-based per-image prediction, DIS unanimity vote, authorization/
  group-assignment gating, synchronous callback to the caller, write to
  `arcface_return_details_v2` — **unchanged**. One new step is added, and (per the mechanism
  below) it is now considerably more than a blind append: each image is inserted into a
  per-vehicle sliding embedding buffer and immediately resolved into a session_id inline,
  gated by a per-vehicle wait/lock folded around the mutating Redis ops.
- **window_handler = `dis_window_handler.py`, a brand-new single-instance-class service** — but its job
  has changed shape. It is no longer a fixed-interval "sweep every vehicle's whole window and
  split it" process. It is now a **finalization cron**: periodically scan saved (already-evicted)
  sessions per vehicle for ones that have gone stale, compute their session-level
  `final_predicted_driver_id` (through the *same* authorization/group-assignment gating Recognize
  uses today), persist one row per session to a **new** table, and evacuate that session's Redis
  state. It has no relationship to the caller's callback.
- **Rollout is scoped to an explicit allowlist of vehicle ids** — a plain config list, not the
  tenant-config registry/extensibility framework referenced by the old (unfound)
  `AN-35636_design.md` draft, which is explicitly out of scope for this pass.

## 2. What already exists in the codebase (confirmed by exploration, file:line as of the earlier session)

- `Recognize` already parses `vehicle_id`/`dis`/`tenant_id` and assembles a 14-field per-image
  record (`frs_src/api_interface.py:179-205`) — everything `system_design.md`'s storage table
  lists (the doc says 13 fields; 14 actually exist — a documentation discrepancy, not a gap).
- Per-image proximity prediction (`_predict_drivers_using_proximity`,
  `frs_src/api_interface.py:572-632`) is genuinely stateless-across-requests (tenant-scoped model
  only, no DIS/Redis-accumulated state) — safe to leave exactly where it is, i.e. in the
  Recognize service.
- `arcface_return_details_v2` (`frs_src/orm.py:532-547`) is the real-time per-request table;
  this design does **not** touch it.
- No Redis distributed-lock primitive exists yet anywhere in the codebase (`SET NX` pattern
  absent everywhere) — only a Postgres-row lock (`lock_tenant`/`unlock_tenant`,
  `frs_src/utils.py:2160-2247`) and one Lua-script atomic-update precedent
  (`update_clusters_dct_and_vehicle_id`, `frs_src/utils.py:2657-2670`), which is the style
  template to follow for the new lock/insert/pop ops below.
- `Router.write_methods` (`frs_src/utils.py:2854-2861`) is an explicit allow-list — any new
  Redis-mutating method must be added there or it silently won't dual-write to Valkey (current
  runtime config routes all traffic to Valkey only).
- No cosine-similarity helper needs to be invented — `sklearn.metrics.pairwise.cosine_similarity`
  is the established idiom (`frs_src/new_face_clustering.py`, `frs_src/mistakes_flagger.py`), and
  `cfg.ARCFACE.cosine_similarity_threshold` (0.53, `frs_src/config.py:416`) is the existing
  canonical embedding-space threshold — this design's own similarity threshold (0.4, see §8)
  is a distinct, separately-tuned value from the offline DIS Split-Merge research, not this one;
  worth an explicit decision on whether to reuse 0.53 or keep 0.4.
- Closest existing "continuous sweep loop" precedent: `vls_notification_poller.py` (drain-until-
  empty per category, then `time.sleep(...)`) — this remains the shape `dis_window_handler.py`
  should follow for its cron loop, not `frs.py`'s SQS-consumer shape. Note it has **no signal
  handling** today — window_handler should add graceful-shutdown handling that
  `vls_notification_poller.py` lacks, so an in-flight finalization pass can finish before the
  process exits.
- Closest existing "single-instance service" packaging: `vls_driver_data_deletion.py` +
  `supervisor_configs/vls_driver_data_deletion.conf` + `docker/VLSDriverDataDeletion_Dockerfile`
  (one `[program:...]` block, no health-check aggregator, no replica group — unlike the ×8
  `vls_recognize` group). **This still fits**, since window_handler remains a single, non-replicated
  process even under the new mechanism (its finalization work is still cheap/periodic, not a
  per-image hot path).
- Bulk multi-row insert idiom: `session.bulk_insert_mappings(orm.Model, list_of_dicts)` +
  one `flush()` (used in `frs_src/vls_kpi.py:217,370`) — still the idiom window_handler should use
  when a finalization pass persists several sessions in one go.
- DB session lifecycle convention across the codebase is "short-lived session per unit of work" —
  window_handler should open/commit/close a DB session per finalization batch, not once at startup.
- **The "earlier dummy implementation" (`loop1_receiver`/`loop2_sweep` under supervisord) cited
  in `system_design.md` as precedent does not exist anywhere in this repo or its git history** —
  confirmed via full-history search. Treat that reference as aspirational description of the
  intended shape, not an actual reference implementation to consult.
- `AN-35636_design.md` (the earlier draft with tenant enable/disable + registry-driven
  extensibility) also does not exist anywhere in this repo's history.
- `system_design.md`'s `cluster_linkage` table doesn't exist by that exact name — closest match
  is `cluster_linkage_details` (`frs_src/orm.py:651-670`); not directly relevant since
  window_handler writes to a new table, not that one.

---

## 3. Why the Redis mechanism changed

The original two-loop design used one Redis LIST per vehicle (`vehicle_window:{tenant}:{vehicle}`),
appended to freely, and split/finalized in one shot every fixed `window_length_minutes` (30)
via a global sweep. Problems with this that motivated the redesign:

- **Fixed clock-aligned windows measurably fragment sessions.** `notebooks/window_level_discontinuity_check.ipynb`
  measured real image-level discontinuity at 0.49–0.73% for 10/15/30/60-minute fixed windows —
  2-3 orders of magnitude worse than the 0.001% measured for exact-request grouping
  (`notebooks/image_level_discontinuity_check.ipynb`). A window boundary can arbitrarily slice
  through a real, continuous session just because two requests straddle a clock boundary.
- **Unbounded memory per window.** The old mechanism holds every image (with its embedding) for
  the full window regardless of traffic; `scripts/online_vehicle_redis_storage_analysis.py`
  measured real fleet-wide peaks of ~332MB at a 30-min window (naive per-record model). A
  size-bounded buffer (below) gives a predictable memory ceiling independent of traffic bursts.
- **Split/merge decisions deferred to sweep time** means a vehicle's true session structure is
  unknown until its window closes, and a burst of activity right at window-close inflates the
  work done in one sweep pass.

The replacement: split/merge decisions are resolved **incrementally, per image, at arrival
time**, against a small sliding window of recent images — not deferred to a periodic sweep.

---

## 4. Redis data model

All keys below are scoped `{tenant_id}:{vehicle_id}` unless noted; "vehicle" throughout means
the `(tenant_id, vehicle_id)` pair, since `vehicle_id` alone is not unique across tenants.

### 4.1 Embedding list (`dis_embed_list:{tenant_id}:{vehicle_id}`)

A list, sorted by `device_capture_time` (dct), **bounded to `N` entries** (value deferred — see
§12, item 1). Each entry: **`(session_id, embedding, dct)`**.

- `dct` — the sort key, and the input to the dct-gap-threshold check against neighbors.
- `embedding` — the input to the similarity-threshold check against neighbors; also what gets
  retained (copied out) when this entry pops, if it's currently the latest live member of its
  session (see §4.3).
- `session_id` — the currently-assigned session tag; this is what §5.2's case matrix reads and
  relabels.

`tenant_id`/`vehicle_id` are not stored per-entry (implicit from the key). `image_id` is not
stored either — finalization (§6) queries the production DB by dct range, not by per-image
lookup, so no per-entry identifier is needed in Redis.

### 4.2 Saved (historical/evicted) sessions (`dis_saved_sessions:{tenant_id}:{vehicle_id}`)

A single Redis **HASH per vehicle**, not indexed by `dis` and not one Redis key per session.
Fields are `session_id`, values are that session's dct list (JSON-encoded, since Redis hash
field values are plain strings).

- The real access pattern is always "given `(tenant, vehicle)`, fetch every saved session and
  iterate to find the dct-range match" — never "look up by session_id directly," and never by
  `dis` (one session can legitimately span more than one `dis`, since DIS Split-Merge ignores
  `dis` boundaries entirely — so nothing about `dis` identity is a reliable index for this). A
  single HASH supports the actual access pattern natively via `HGETALL`/`HKEYS` in one call.
- **Appending to one session's field is a per-field operation** — `HGET` that field, deserialize,
  append the new dct, reserialize, `HSET` back — proportional to that one session's own list
  size, not the whole hash.
- **A known, accepted limitation**: when two sessions merge in the live buffer (case 2, §5.2),
  relabeling only ever touches *live* entries in the embedding list — this hash is left
  untouched by a merge. If one of the merged sessions already had earlier members saved here
  under its now-abandoned id, that saved fragment is never reconciled into the surviving id.
  This is intentional, not an oversight: anything still *live* at merge time correctly ends up
  under the merged id once it eventually pops (relabeling happens before popping); only data
  that was *already* saved before the merge is left behind, and that's treated as a tolerable,
  permanent loss rather than adding merge-time reconciliation complexity.
- **This hash accumulates until finalized.** As long as the finalization cron (§6) runs often
  enough relative to session creation rate, it stays bounded; if the cron fell behind, this hash
  could grow unbounded for a busy vehicle. No hard cap is proposed here — rely on the cron
  cadence.

### 4.3 Retained boundary embedding (per session, transient)

For whichever session is currently the **latest-popped** (i.e., its most-recently-evicted member
is also its most-recent member overall), its embedding is retained separately from the plain dct
list, under its own **grace-period eviction policy** (a TTL distinct from the blanket backstop
TTL in §4.6). This exists solely to support the **forward-extension** check: a late arrival whose
dct is just past a saved session's known latest member needs *something* to compare embeddings
against, since the saved-session hash (§4.2) only stores dct, not embeddings.

- **Maintenance rule**: on every pop, unconditionally overwrite this session's retained embedding
  with the just-popped entry's embedding. This is correct even though it can be transiently
  "behind" reality (if the session still has other, more-recent members still live in list one) —
  because the *only* time this retained value is ever consulted is for a late arrival whose dct is
  older than the embedding list's own current start_dct (§5.1), which by definition predates
  anything still live, including any of this session's own still-live members. So using the
  latest-popped-so-far embedding is always the correct reference point when it's actually needed.
- **Not needed for backward or interior attachment** — those are resolved by pure dct-range logic
  (§5.3), no embedding required. Only forward-extension needs this.

### 4.4 Existence range (per session, transient, in-list-time only)

`(existence_start, existence_end)` per session — but this concept **only exists and only matters
while that session (or its immediate neighbors) still has live representation in the embedding
list.** It is used purely as a routing aid for a new arrival that lands inside an already-carved
gap between two currently-live-adjacent sessions (see §5.2's existence-range pre-check). Once a
session's members have fully left the embedding list, existence range is **dropped and never
maintained or consulted again** — from that point on, everything about that session is governed
by its plain saved dct range (§4.2) and (if applicable) its retained boundary embedding (§4.3).
Existence range does **not** need to be persisted, gathered, or reconciled for historical/fully-
saved sessions — only the tight, own-member dct range matters there, which is also what makes the
finalization DB query (§6) safe (see §5.4's note on why a split's own-range narrowing prevents
interlopers from corrupting a query).

`last_session_end_time` — a single per-vehicle value tracking the current frontier-most known
session's own end dct. Seeds a brand-new session's existence range start (open-ended on the far
end) when a genuinely fresh, forward-progressing arrival doesn't match anything already carved.

### 4.5 Session metadata

Per session: `created_at`, `last_updated_at` (epoch or timestamp). Used by the finalization cron
(§6) to decide staleness, and to reduce (not eliminate — see §7) the race between a live insert
and a concurrent finalize-and-evacuate of the same session.

### 4.6 Blanket key TTL (backstop only)

A single, generous TTL applied to all of the above key types, as a leak backstop only — **not**
the real cleanup mechanism (that's the finalization cron). Value: deferred (owner: user).

---

## 5. Processing algorithm

Applies **per incoming request** (a request may carry multiple images).

### 5.1 Routing: does this request belong to the live buffer, or the saved/historical world?

Compare the request's dct(s) against `dis_embed_list:{tenant}:{vehicle}`'s **current start_dct**
(the dct of its current front/oldest live entry) — this single comparison is the gate, full stop.

- **If the request's dct(s) are `≥` the embedding list's current start_dct**: live path, §5.2.
  Each image in the request is processed **one at a time**, in sequence — never as a batch, even
  within one multi-image request.
- **If `<`**: historical/group path, §5.3. Here, by contrast, the request's images **are**
  treated as one group for the routing decision (see §5.3's own internal-split step for the one
  place images within the group still get separated back out).

### 5.2 Live path — the embedding-list 8-case matrix

Per image, in order:

1. **Existence-range pre-check**: if this image's dct falls inside an already-carved gap between
   two currently live-adjacent sessions (from a prior split event — see the worked example in
   §5.3.2, applied live), attach directly to that gap's owning session. No similarity check. This
   only ever applies when the surrounding sessions genuinely still have live representation —
   see §4.4.
2. **Otherwise** (the normal/fresh case): insert the image into the embedding list at its correct
   sorted (dct) position, then pop the front (oldest) entry to hold the list at size `N`.
   - Popping is **unconditional**: push the popped entry's dct onto its session's field in
     `dis_saved_sessions` (§4.2), no check performed. If this entry is the session's current
     latest, also update its retained boundary embedding (§4.3).
   - Then determine the newly-inserted image's own `session_id` via the 8-case matrix, comparing
     it against its actual immediate neighbors (`prev`, `next`) now present in the list — the two
     signals being similarity (`cosine > 0.4`) and dct-gap (within threshold):

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

   - Splits (3/5/7) never need to worry about pre-existing saved/historical data for the *newly
     created* id, since it never existed before this moment — only merges (case 2) touch a
     *pre-existing* id, which is where §4.2's accepted merge-orphan limitation applies.
   - A split (3/5/7) also carves new existence-range boundaries for the resulting sessions (§4.4)
     while they still have live representation.

### 5.3 Historical/group path

Applies to a whole request (all its images together) whose dct(s) are `<` the embedding list's
current start_dct.

**5.3.1 Internal split check.** First, check whether the request's own images internally split
(via the same similarity/dct-gap rule, applied just within this one arrival) — if they do, each
resulting sub-group is handled independently through the steps below. This applies uniformly
regardless of image count — a single-image "group" and a five-image group are handled by the
exact same rule, with no special-casing for size.

**5.3.2 Per (sub-)group, check in order:**

1. **Backward or forward extension** against an existing saved session — pure dct-range check
   (backward: does it sit just before a session's earliest known dct, within gap threshold; no
   embedding needed) or forward-extension (does it sit just past a session's latest known dct,
   within gap threshold — checked via that session's retained boundary embedding, §4.3, plus the
   dct-gap check). If it matches, attach directly. No split.
2. **Bridging two different saved sessions** (the group's dct sits close enough to plausibly
   extend *either* of two independent, unrelated saved sessions) — resolved as: **do not merge,
   do not guess.** The group becomes its own new session regardless. Ambiguity is not resolved by
   picking a side.
3. **Breaking an existing saved session** — the group's dct(s) land strictly between two dct
   values that are *both* already-confirmed members of the *same* saved session (an interior
   sandwich). This always triggers a three-way split, regardless of how many images are in the
   group. Worked example: `S1 = [10:00, 10:01, 10:15, 10:30, 10:50, 10:55, 10:56]`, a group
   arrives with `[10:41, 10:45, 10:49]` (falls between `S1`'s `10:30` and `10:50`) →
   - `S1` truncates to `[10:00, 10:01, 10:15, 10:30]` (everything before the gap, same old id)
   - a new session takes the back half: `[10:50, 10:55, 10:56]` (new id)
   - the arriving group becomes its own new session: `[10:41, 10:45, 10:49]` (another new id)

   This is purely positional — no embedding comparison is possible or needed, since saved
   sessions generally don't retain embeddings for interior (non-latest) members. The mere
   presence of dct's that weren't already accounted for, landing precisely in an assumed-
   continuous gap, is treated as sufficient proof that the gap wasn't actually continuous.
4. **No match to anything above** → the (sub-)group becomes one brand-new saved session outright
   — all its own dct's stored together under one new session_id in `dis_saved_sessions`.

### 5.4 Why finalization can safely use a session's own tight dct range

Because splits (both live, §5.2, and historical, §5.3.2 step 3) always narrow a session's own
range to exclude whatever caused the split, a session's own `[min(dct), max(dct)]` — derivable
from the first/last elements of its saved dct list (§4.2), since pops/appends always happen in
ascending order, keeping that list naturally sorted — is guaranteed to never wrongly include
another session's members. This is what makes §6's DB range-query finalization approach safe,
without needing the wider, harder-to-maintain existence-range concept (§4.4) to persist beyond
the live phase.

---

## 6. Finalization (the new window_handler — `dis_window_handler.py`)

No longer a fixed-interval "sweep every vehicle's window" process. Runs on its own cadence
(cron interval, deferred parameter) and, per pass:

1. Scan `dis_saved_sessions` across active vehicles for sessions eligible to finalize: unchanged
   (no append) for longer than a threshold period, using `created_at`/`last_updated_at`
   (§4.5) — the exact combined eligibility logic (e.g. requiring both a minimum age since
   creation *and* enough idle time since last update) is a deferred design detail, not fully
   specified; the intent is simply that neither a too-new nor a too-recently-touched session gets
   swept.
2. For each eligible session: **fetch `predicted_driver_id` values from the production DB**, by
   querying that session's own tight `[min(dct), max(dct)]` range (§5.4) against the source
   image-records table for that `(tenant_id, vehicle_id)` — **not** a per-image/`image_id` lookup
   (the saved dct list doesn't carry `image_id`), and **not** the wider existence-range concept
   (dropped once historical, §4.4).
3. Compute `final_predicted_driver_id`: unanimity among valid (`>0`) `predicted_driver_id` values,
   else `-1` — same rule shape as today's `_predict_a_driver_from_predictions`
   (`frs_src/api_interface.py:634-652`), and the same authorization/group-assignment gating
   Recognize applies today (`apply_gating`, carried forward from the earlier design pass — see
   §2), so the session-level id stays consistent with what a real-time caller would have been
   told for the same images.
4. Persist one row per session (§10 for the table shape).
5. **Evacuate**: delete that session's `dis_saved_sessions` field and, if present, its retained
   boundary embedding.

Packaging/process shape (unchanged from the earlier pass — see §2): follows
`vls_notification_poller.py`'s drain-loop shape, packaged like `vls_driver_data_deletion.py`
(single `[program:...]` supervisor block, no replica group), with graceful-shutdown handling so
an in-flight finalization batch can complete before exit.

---

## 7. Concurrency model

**Processes, not threads** — both for the Recognize service (multiple worker processes, matching
production's existing shape: independent OS processes, one per SQS consumer, no thread pool) and
window_handler (a single, non-replicated finalization-cron process). Reasons: the split/merge work is
CPU-bound (embedding math, similarity checks) and won't parallelize across threads under
Python's GIL; separate processes also isolate failures.

**Per-vehicle wait/lock**: since the queue gives zero per-vehicle ordering or exclusivity,
mutating operations on a vehicle's embedding list (insert, pop, relabel) are serialized via a
per-vehicle wait/lock — a worker processing an image for a given vehicle waits for any other
in-flight mutation on that same vehicle to finish first, rather than racing. (Exact
timeout/retry policy: deferred parameter.)

**Accepted race window**: the finalization cron's staleness check and its actual
finalize-and-evacuate are not fully atomic against a concurrent live insert landing in the same
narrow window — explicitly **accepted as a rare, tolerable miss**, consistent with this design's
general tolerance for rare edge-case misses elsewhere (out-of-order arrivals, the merge-orphan
limitation in §4.2). No compare-and-delete/CAS mechanism is planned to close this fully.

---

## 8. Config (`frs_src/config.py`)

New `cfg.DIS_WINDOW` section — supersedes the field list from the earlier pass where the
mechanism has changed underneath:

- `enabled_vehicle_ids = []` — unchanged from the earlier pass: explicit allowlist, empty by
  default.
- `embed_list_max_size` (the `N` parameter, §4.1) — **no value set; deferred to real-data sizing**
  analysis, same category of work as the earlier window-length capacity study
  (`scripts/online_vehicle_redis_storage_analysis.py`), not yet run for this parameter.
- `similarity_threshold = 0.4` — reused from the offline DIS Split-Merge research; note this is a
  *different* value from `cfg.ARCFACE.cosine_similarity_threshold` (0.53) already in the
  codebase — an open decision (§12) on whether to actually reuse 0.53 instead for consistency.
- `dct_gap_threshold_seconds` — presumed 600 (10 min), reused from the same offline research
  convention as the similarity threshold, but **never independently re-confirmed with an actual
  number written down in this session** — same open item flagged in the earlier pass, still open.
- `boundary_embedding_grace_period_seconds` — deferred.
- `idle_eviction_seconds` (for a record stuck in the embedding list with nothing pressuring it
  out) — deferred.
- `finalize_unchanged_threshold_seconds` — deferred.
- `finalize_cron_interval_seconds` — deferred.
- `vehicle_lock_wait_timeout_seconds` / retry policy — deferred.
- `redis_key_ttl_seconds` (blanket backstop, §4.6) — deferred.
- `window_length_minutes`, `sweep_poll_interval_seconds` (as a *fixed-window* concept),
  `lock_ttl_seconds` (as originally scoped for a 30-min-cadence lock) — **carried over from the
  earlier pass, now superseded.** There is no fixed clock window left in this mechanism; do not
  reintroduce `window_length_minutes` as a live-processing parameter. `finalize_cron_interval_seconds`
  above replaces `sweep_poll_interval_seconds`'s role.

---

## 9. Redis methods (`frs_src/utils.py`, on `RedisUtils`, inherited by `ValkeyUtils`)

Naming follows the existing `prefix:{tenant_id}:{vehicle_id}` convention:

- `dis_embed_list:{tenant_id}:{vehicle_id}` — the embedding list (§4.1).
- `dis_saved_sessions:{tenant_id}:{vehicle_id}` — the saved-sessions HASH (§4.2).
- `dis_boundary_embed:{tenant_id}:{vehicle_id}:{session_id}` — retained boundary embedding
  (§4.3), own TTL.
- `dis_vehicle_lock:{tenant_id}:{vehicle_id}` — the per-vehicle wait/lock (§7).

New methods (all must be added to `Router.write_methods`, `frs_src/utils.py:2854-2861`, or they
silently misroute to a single non-dual-write backend — easy to forget, confirmed required):

- `insert_and_resolve_session(tenant_id, vehicle_id, image_record)` — the core op implementing
  §5.1-§5.2: acquire the per-vehicle lock, run the routing/insert/pop/8-case logic as one Lua
  script or Lua-orchestrated sequence (following the `update_clusters_dct_and_vehicle_id` Lua
  style, `frs_src/utils.py:2657-2670`, as the atomicity template), release the lock. This
  **replaces** the earlier pass's simple `append_to_vehicle_window` — it is no longer a blind
  append, it performs the full inline resolution.
- `resolve_historical_group(tenant_id, vehicle_id, group_records)` — implements §5.3 for the
  historical/group path.
- `pop_and_save(tenant_id, vehicle_id, popped_entry)` — implements the unconditional pop-push
  described in §5.2, including the boundary-embedding update when applicable.
- `finalize_and_evacuate_session(tenant_id, vehicle_id, session_id)` — used by window_handler (§6): read
  the session's saved dct range, delete its fields/boundary embedding after persistence succeeds.
- `acquire_vehicle_lock` / `release_vehicle_lock` — same safe-lock shape as the earlier pass
  (`SET ... NX PX <ttl>` / Lua check-and-delete), now invoked far more frequently (per-image, not
  per-window-close) — see §12, item 6 on re-confirming the locked-vehicle fallback behavior given
  this changed frequency.

---

## 10. Table (`frs_src/orm.py` + migration)

`dis_session_details` (working name) — column set updated to match the new mechanism (no more
fixed `window_id`, since there's no fixed window left):

- `id` (uuid PK)
- `tenant_id`, `vehicle_id`
- `session_id`
- `start_timestamp` / `end_timestamp` — the session's own tight dct range (§5.4) — this is what
  §6's finalization query uses, and what makes the query safe against interlopers
- `final_predicted_driver_id`
- `suppression_reason`
- `num_images`
- `finalize_reason` — new vs. the earlier pass: distinguishes *which* path produced this session
  (e.g. live 8-case resolution vs. historical breaking vs. fresh/no-match) for observability, not
  strictly required for correctness
- `created_at`

---

## 11. Packaging

Unchanged from the earlier pass (§2) — still applies as-is:

- `supervisor_configs/dis_window_handler.conf` — single `[program:dis_window_handler]` block,
  copying `vls_driver_data_deletion.conf`'s shape.
- `docker/DisWindowHandler_Dockerfile` — copy of `VLSDriverDataDeletion_Dockerfile`'s shape.

---

## 12. Open items requiring sign-off before/at implementation

1. **`embed_list_max_size` (N) has no value** — needs a real-data sizing analysis (how many
   images can arrive between the first and last member of a typical long session, to know how
   large the buffer needs to be before legitimate same-session neighbors risk getting evicted
   before comparison).
2. **`dct_gap_threshold_seconds` has no independently-confirmed value** — presumed 600s (10 min)
   by convention, never written down as a hard decision in this design session (same open item
   carried from the earlier pass).
3. **`similarity_threshold` (0.4) vs. the existing `cfg.ARCFACE.cosine_similarity_threshold`
   (0.53)** — decide whether to reuse the existing production value or keep this design's own,
   separately-tuned one.
4. **All other deferred parameters in §8** (grace period, idle-eviction, finalize threshold/
   interval, lock wait timeout, blanket TTL) — owner: user, not yet set.
5. **Finalization eligibility logic** (§6 step 1) — the exact combination of `created_at`/
   `last_updated_at` conditions is not fully specified, only conceptually agreed.
6. **Locked-vehicle fallback behavior** given the much higher lock-acquisition frequency under
   this mechanism (§7, §9) — the earlier pass proposed retry-then-drop for a locked vehicle
   (instead of `system_design.md`'s original SQS `ChangeMessageVisibility` redelivery, which no
   longer directly applies now that Recognize keeps its full synchronous job). That answer was
   reached when the lock was rare (once per 30-min window close); it's now acquired far more
   often (potentially every image). Re-confirm retry-then-drop is still the right call at this
   higher frequency, rather than assuming the earlier answer transfers unchanged.
7. **`dis_session_details` table name/columns** — as listed in §10, open to adjustment.

---

## 13. Verification plan (once implementation is approved)

- Unit-level: exercise the 8-case matrix (§5.2) and the historical/group path (§5.3) against
  hand-built fixtures covering every case in the table, the breaking worked example, the
  backward/forward/bridging/no-match outcomes, and the internal-split-of-a-group case —
  following the existing `testing/unit_tests/` conventions.
- Integration: run `dis_window_handler.py` locally against a test Redis + test Postgres DB with a
  synthetic `dis_saved_sessions` pre-populated, confirm lock acquire/release, finalize-and-
  evacuate correctness, and correct session rows land in `dis_session_details`.
- Confirm `Recognize`'s existing synchronous response/callback behavior is provably unchanged for
  vehicles both inside and outside `cfg.DIS_WINDOW.enabled_vehicle_ids` (regression-check against
  `testing/testcases/test_recognize_old.py`/`testing/testscripts/recognize.py`) — this matters
  more now than under the old mechanism, since the new per-image insert/resolve step does
  meaningfully more work inline than a blind append did.
- End-to-end on a small `enabled_vehicle_ids` allowlist in a non-prod environment before any
  wider rollout — this is a new production service touching Redis/Postgres, so no direct-to-prod
  testing.
