# DIS Production System Design — Fixed 30-min Window, Two-Loop Architecture

Working design for the AN-35636 production DIS (Driver Invariant Session) split-merge service,
worked out conversationally in this session. Captures the algorithm/architecture decisions made
so far; supporting capacity numbers come from the analyses in this same folder (see
`analysis_for_design.md` and `outputs/`).

## Window scheme

Fixed, clock-aligned window, **`window_length = 30 minutes`** — chosen from the four window
lengths studied in `notebooks/online_vehicle_redis_storage_analysis.ipynb` (10/15/30/60 min).
Every vehicle shares the same window boundaries; a request's window is a pure function of its
own arrival `timestamp`, no per-vehicle dynamic state needed to decide which window it belongs
to.

## Two loops

### Loop 1 — Recognize workers (append)

On each incoming request: for every image in it, compute/attach its embedding and append the
image (with its embedding) into that vehicle's Redis-resident window state. No session-boundary
logic runs here — this loop only accumulates; splitting/merging into sessions happens once, at
window close, in Loop 2.

Before appending, a worker must check whether the vehicle is currently locked (see below) — this
check should be folded into the same atomic Redis operation that performs the append, rather
than a separate read-then-write, to close the race where Loop 2 locks the vehicle in the gap
between a worker's lock-check and its actual write.

### Loop 2 — window-close sweep

A single, non-replicated process that:

1. **Monitors** Redis storage and the window timer for every vehicle.
2. On window end for a vehicle: **locks** that vehicle (see "Locking" below) so Loop 1 stops
   accepting new writes for it.
3. Atomically **pulls and clears** all accumulated records for that vehicle from Redis (single
   Redis operation — same "pull-and-clear" shape as the earlier fixed-window design).
4. **Creates sessions** from the pulled records: sort by `device_capture_time`, split into a new
   session whenever consecutive-image cosine similarity drops below the similarity threshold or
   the time gap exceeds the DCT threshold (the DIS Split-Merge rule,
   `embed_sim_dct_level_assignment`) — a missing/invalid embedding forces a split.
5. **Assigns** `final_predicted_driver_id` per session: unanimity among valid (`>0`) raw
   `predicted_driver_id` values in that session, else `-1`.
6. **Persists** each session's result to the DB.
7. **Evacuates** — Redis storage for that vehicle is already empty from step 3 (pull-and-clear);
   nothing further to remove.
8. **Unlocks** the vehicle, so it starts a fresh window and Loop 1 resumes accepting writes for
   it.

## Concurrency: processes, not threads

Both loops run as separate OS processes (Loop 1: multiple Recognize worker processes; Loop 2:
exactly one sweep process) — not threads within one process. Two reasons:

- **CPU-bound work**: embedding computation and similarity/DCT checks won't parallelize across
  threads under Python's GIL; separate processes give real parallelism.
- **Matches existing precedent**: the production `Recognize` service already runs as
  independent OS processes (one per SQS consumer, no thread pool), and this project's own
  earlier dummy implementation already ran this exact shape under supervisord (multiple
  `loop1_receiver` processes + exactly one `loop2_sweep` process).

This means the loop1↔loop2 lock must be a real cross-process lock living in Redis, not an
in-process `threading.Lock`.

## Locking: pausing a vehicle without blocking a worker

The queue (SQS) is non-FIFO with no per-vehicle affinity — a worker can't simply "skip" a
locked vehicle's message and pull a different one on demand; it has to receive whatever the
queue hands it. Mechanism:

- **Lock**: a plain Redis key, e.g. `vehicle_lock:{tenant_id}:{vehicle_id}`, set by Loop 2 right
  before it starts evacuating a vehicle's window, deleted once that vehicle's sessions are
  finalized and persisted.
- **Worker behavior on a locked vehicle**: if a worker receives a message for a vehicle that's
  currently locked, it does **not** process it. Instead it calls `ChangeMessageVisibility` on
  that message to reset its visibility timeout to 0 (or a short delay), releasing it back onto
  the queue immediately, then loops back to `ReceiveMessage` for its next message — which, since
  the queue holds many other vehicles' messages, is very likely a different, unlocked vehicle.
  The released message becomes redeliverable (to it or another worker) once Loop 2 clears the
  lock.
- This closes the requirement "avoid processing this vehicle's requests, go handle others, come
  back to this vehicle's requests later" without any worker blocking or idling.

**Open decision, not yet made**: whether the redelivery mechanism is exactly
`ChangeMessageVisibility(0)` or something else — flagged here as the concrete decision point,
not assumed settled.

## Per-record storage model (measured, not estimated)

Each record (= one image) held in Redis during its open window costs:

| Component | Bytes | Source |
|---|---|---|
| JSON metadata (13 fields: `image_id`, `face_s3_path`, `file_name`, `dis`, `device_id`, `vehicle_id`, `tenant_id`, `tenant_drp`, `device_capture_time`, lat+long, `device_release_version`, `device_merge_config_version`, `face_params`) + JSON structure overhead + `predicted_driver_id` | 662 | field-by-field breakdown in `system_design_backup/checks/notebooks/incoming_requests_rate_analysis.ipynb` |
| Embedding (512-dim) | ~4096 (real dtype measured per image, not assumed) | `scripts/online_vehicle_redis_storage_analysis.py`, loaded from each vehicle's real `.joblib` embeddings file |
| **Total per record** | **~4758** | |

This is the naive/worst-case cost (every record keeps its own embedding for the whole window,
no boundary-only optimization) — a capacity ceiling to design against, not the final optimized
footprint.

## Real capacity numbers at WL = 30 min

From `outputs/online_storage_summary_by_window_length.csv` (6,364 vehicles, 336 windows, 0
vehicle errors):

- **Peak online vehicles**: 4,752 (window starting 2026-07-29 18:30)
- **Peak fleet-wide Redis storage**: ~332 MB (window starting 2026-07-28 21:00)
- **Mean fleet-wide storage**: ~114 MB · **Median**: ~58 MB, across all 336 windows

## Open items

- Redelivery mechanism for a locked-vehicle message (`ChangeMessageVisibility` value vs.
  alternative) — flagged above, not decided.
- Idle-timeout / force-close for a vehicle that goes quiet mid-window — not addressed by this
  design as written; a vehicle with no further arrivals still needs its window eventually
  closed.
- Tenant-level enable/disable config and the extensibility (pure-function, registry-driven)
  structure from the earlier `AN-35636_design.md` draft aren't yet re-confirmed against this
  specific two-loop shape — worth revisiting once this design is otherwise settled.
