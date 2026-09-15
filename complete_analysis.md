# System Design Analysis — Productionizing DIS Split-Merge (AN-34882)

This document collects every analysis done so far for the AN-34882 feasibility study
in one place, in plain language. Source code and notebooks live in
`DIS_logic/implementation/codes/` and `DIS_logic/implementation/notebooks/`; a parallel
hands-on Redis workspace and its own findings log live in `test_ws/redis/CLAUDE.md`.
This file is the synthesis across both.

All numbers below come from the same **6,364-vehicle valid sample** (see
`implementation/CLAUDE.md` for how that sample was chosen) unless stated otherwise — not
the full production fleet, which is believed to be on the order of ~100K vehicles.

---

## 1. The problem, in plain terms

An earlier study picked **DIS Split-Merge** (`embed_sim_dct_level_assignment`, run at a
0.4 face-similarity threshold and a 10-minute time-gap threshold) as the best method for
assigning a driver ID to each image. It works by comparing each image to the one
captured right before it: if the faces look similar enough *and* they weren't captured
too far apart in time, they're treated as the same session and get the same driver ID.

That validation was done **offline** — the algorithm could see a vehicle's entire week of
images at once before deciding anything. A production system can't do that: it has to
decide incrementally, as images arrive, without waiting days for the full picture, and it
has to do this for far more vehicles than the ~10K-vehicle sample it was tuned on.

This document is about what breaks, and what it costs, when that offline method gets
squeezed into a real-time, memory-constrained system.

---

## 2. What the raw device data actually looks like

Every image carries a `dis` — a session ID the *device itself* assigns, independent of
the DIS Split-Merge algorithm above. Two structural questions matter for the redesign:

### 2a. Does a `dis` naturally stay together in true time?

**Test**: sort a vehicle's entire image stream by `device_capture_time` (true
chronological order — not the time it arrived at the server). Does each `dis`'s images
form one unbroken block, or does another `dis`'s image land in the middle of it?

**Finding: essentially never.** Across all 6,487,886 `dis`s in the sample, only **one**
was fragmented this way, and even that single case isn't a real counterexample — the two
"halves" were **23 hours apart**, with 105 other images from 21 other `dis`s in between.
No real session stays open for 23 hours. This looks like the device reusing the same
raw `dis` value for two unrelated occurrences roughly a day apart, not an interrupted
session. Treat the true fragmentation rate as **0%**.

**Takeaway**: `dis` boundaries are clean and reliable *in true time*. Whatever
complexity this project deals with elsewhere does not come from messy session structure
at the source.

*(Script: `codes/dis_dct_sort_fragmentation_check.py` → `notebooks/dis_dct_sort_fragmentation_check.ipynb`)*

### 2b. Does that cleanliness survive once you window by arrival time instead?

A production system can't sort by true time — it only knows what's arrived so far. So it
has to batch images into **arrival-time windows** and process one window at a time. Three
windowing schemes were tested, at several window lengths:

- **Fixed clock-aligned** — windows at fixed clock boundaries (e.g. every 3 hours on the
  clock, `00:00–03:00`, `03:00–06:00`, …).
- **Cascading, no cutoff** — a window opens on the first image after the last one closed,
  and only closes once its fixed length has elapsed.
- **Cascading with an idle-gap cutoff** — same as above, but also closes early if the
  vehicle goes quiet for longer than some cutoff.

**Finding: fragmentation reappears, and grows sharply as the window shrinks.**

| Scheme | Window | Fragmented `dis`s | Vehicles affected | Mean fragmented rate per vehicle |
|---|---|---|---|---|
| Fixed clock-aligned | 30 min | 1.39% | **98.7%** | 7.10% |
| Fixed clock-aligned | 3h | 0.31% | 78.6% | 2.34% |
| Fixed clock-aligned | 6h | 0.16% | 61.4% | 1.28% |
| Fixed clock-aligned | 12h | 0.08% | 44.1% | 0.61% |
| Fixed clock-aligned | 24h | 0.06% | 35.7% | 0.33% |
| Cascading, no cutoff | 3h | 0.23% | 70.1% | 1.84% |
| Cascading, no cutoff | 6h | 0.10% | 43.5% | 0.87% |
| Cascading, no cutoff | 12h | 0.03% | 15.5% | 0.32% |
| Cascading + gap cutoff (best cutoff per length) | 3h | 0.23% | 69.5% | 1.79% |
| Cascading + gap cutoff (best cutoff per length) | 6h | 0.09% | 42.2% | 0.79% |
| Cascading + gap cutoff (best cutoff per length) | 12h | 0.02% | 13.2% | 0.18% |
| Cascading + gap cutoff (best cutoff per length) | 24h | 0.0086% | 6.5% | 0.083% |

Reading this: at a 24-hour window with a well-tuned gap cutoff, fragmentation is almost
negligible (matches the ~0% true-time finding). At **30 minutes**, it's nearly
universal — **98.7% of vehicles** have at least one fragmented `dis`, and on average
**7% of a vehicle's sessions** get split apart purely because of arrival-time windowing,
not because the sessions were actually messy.

Cascading windows consistently fragment less than fixed clock-aligned windows at the same
length (a fixed clock boundary can slice through an active session regardless of what's
happening; a cascading window only starts a new one when needed). Adding a gap cutoff to
a cascading window reduces fragmentation further still.

*(Scripts: `codes/dis_windowed_group_check*.py` family → comparison outputs in
`data/*_fragmentation_comparison*.csv`)*

---

## 3. What DIS Split-Merge itself does to `dis` (the "raw" baseline)

Section 2 is about structural fragmentation *before* the assignment algorithm runs at
all. This section is about what the algorithm itself does — run **unwindowed**, full
7-day visibility, exactly as originally validated — to the raw `dis` grouping.

| | Count |
|---|---|
| Total raw `dis`s | 6,487,949 |
| `dis`s the algorithm **splits** into 2+ sessions | 58,524 (0.90%) |
| Final resulting sessions | 604,374 |
| Sessions that are a **merge** of 2+ different `dis`s | 338,961 (56.08% of all sessions) |
| `dis`s absorbed into some merge | 6,328,001 (97.53% of all `dis`s) |

**Takeaway**: splitting a `dis` is rare (~1%). **Merging is the dominant, near-universal
effect** — the raw `dis` count collapses roughly **10.7x** into final sessions, and
merge activity happens in all but 17 of 6,363 vehicles (99.7%). This is the real value
DIS Split-Merge adds over the raw device grouping — but it depends on being able to see
`dis`s that may be far apart in time, which is exactly what a short, memory-constrained
window prevents.

*(Script: `codes/dis_split_merge_raw_analysis.py`)*

---

## 4. Timing characteristics — how much data, how often

### 4a. Gaps between consecutive arrivals

| | Overall (any `dis`) | Same-`dis` only |
|---|---|---|
| Mean | 387.5 s | 124.3 s |
| Median | 0.15 s | **0.0 s** |
| Max (tail) | 591,001 s (~6.8 days) | 225,765 s (~2.6 days) |

Over half of consecutive same-`dis` pairs share an *identical* arrival timestamp
(batched uploads), and both distributions are heavily right-tailed — a handful of very
long gaps pull the mean far above the median.

### 4b. Incoming volume per window size (10 min–24h swept)

Two views were built: **per-vehicle** (how big is a typical single vehicle's window) and
**fleet-wide** (how much total volume hits the system at once, across all 6,364 vehicles,
in a given clock-aligned window). The fleet-wide numbers are what matter for sizing a
shared system:

| Window | Fleet images (mean/median/max) | Fleet distinct sessions ("batches") (mean/median/max) |
|---|---|---|
| 15 min | 12,556 / 6,533 / 40,001 | 10,230 / 4,710 / 34,639 |
| 1 hour | 50,224 / 26,228 / 139,696 | 40,920 / 19,510 / 120,386 |
| 6 hours | 301,344 / 262,977 / 763,243 | 245,521 / 207,742 / 651,143 |
| 24 hours | 1,205,375 / 1,276,551 / 1,441,811 | 982,083 / 1,033,770 / 1,170,625 |

*(Notebook: `notebooks/timestamp_gap_analysis.ipynb`)*

---

## 5. Redis storage design

### 5a. What gets stored

One record per image: `tenant_id, vehicle_id, image_id, dis, timestamp,
device_capture_time, embedding`, looked up by `tenant_id`/`vehicle_id`/`dis`/time — never
by similarity search. The algorithm only ever compares two *already-identified* images,
so a vector-search index isn't needed; a plain keyed store is the right fit.

**Layout decided**: a Redis `HASH` keyed by `f"{tenant_id}:{vehicle_id}"`, with `image_id`
as the field name inside it. Rejected alternatives: a separate Redis instance or logical
database per tenant/vehicle pair — neither gives real per-entity memory isolation, and
both collapse under fleet-scale vehicle counts or Redis Cluster mode.

### 5b. Field sizes — confirmed from real production data

| Field | Postgres type | Current (text) size | Packed-binary alternative |
|---|---|---|---|
| `tenant_id` | `integer` | 4-5 bytes | 4 bytes (native `int32`) |
| `vehicle_id` | `integer` | 6-7 bytes | 4 bytes (native `int32`) |
| `image_id` | `text` | 32 bytes (hex string) | 16 bytes (it's really a 128-bit id written as hex) |
| `dis` | `text` (too big for a 64-bit int) | 22-25 bytes | 16 bytes (bigint packing) |
| `timestamp` | `timestamp without time zone` | 26 bytes | 8 bytes (int64 epoch — matches Postgres's own internal form) |
| `device_capture_time` | `timestamp without time zone` | 26 bytes | 8 bytes (same) |
| `embedding` | derived, 512-dim | 4096 bytes (`float64`) | 2048 (`float32`) / 1024 (`float16`, unvalidated precision risk) |

None of the binary-packing options are adopted yet — every capacity number below assumes
today's simplest approach (text encoding, `float64` embeddings), so they're a
**pessimistic** baseline, not a best case.

### 5c. Capacity: how much can 16 GB actually hold?

Record size today (text-encoded, max observed field sizes): **4,217 bytes**. That gives
**~4.07 million records** in 16 GB.

Translating "records" into "time of data held," using the fleet-wide arrival rate from
§4b:

| Fleet size | 16 GB holds roughly |
|---|---|
| 6,364 vehicles (this sample) | **~3 days** |
| 1,000,000 vehicles (hypothetical, same per-vehicle rate) | **~30 minutes** |

**This is the headline number for the whole feasibility study.** At real production
scale, 16 GB cannot function as a historical store — it can only ever be a bounded
"working set" of whatever's currently active, sized by an eviction/TTL policy (see §6).
Switching to the packed-binary encoding in §5b would roughly **halve the record size and
double every number above** (e.g. ~1 hour instead of ~30 minutes at 1M vehicles) — a real
lever, but not one that changes the fundamental conclusion.

*(Scripts: `codes/redis_field_size_check*.py`, `codes/redis_field_datatype_report.py`,
`codes/redis_embedding_storage_capacity.py`; hands-on Redis prototyping in
`test_ws/redis/src/`)*

### 5d. Records per vehicle, at various fleet sizes

The same fixed 16 GB budget (~4,073,955 records, current text-encoded ~4,217-byte
record), divided evenly across different total fleet sizes:

| Total vehicles | Records per vehicle (16 GB ÷ fleet size) |
|---|---|
| 5,000 | ~815 |
| 10,000 | ~407 |
| 50,000 | ~81 |
| 100,000 | ~41 |
| 200,000 | ~20 |
| 500,000 | **~8** |

Switching to the packed-binary encoding (§5b, ~2,104 bytes/record → ~7.76M records total)
roughly doubles every row above — e.g. ~16 records/vehicle at 500,000 instead of ~8 — but
doesn't change the shape of the problem.

This is the same §5c/§7 finding stated a different way, and it's a sharper way to feel
it: 16 GB is a **fixed pie**, so more vehicles just means a thinner slice per vehicle —
there's no scaling relief here. For context, a single *typical* 15-minute window for one
vehicle averaged 10 images and ran up to 379 in a busy one (§4b). At 200,000 vehicles,
each vehicle's whole share of the budget (~20 records) barely covers one average window;
at 500,000, ~8 records/vehicle doesn't even cover that, let alone the busy-window tail or
more than one session held open at a time.

---

## 6. Open questions — not yet resolved

Ranked by how much each still blocks a real design:

1. **TTL per record/session (the biggest one).** How long should a record stay resident
   before it's evicted? Directly trades off memory footprint against how late an
   out-of-order/delayed image can still arrive and correctly merge into its session.
   Subtlety: the algorithm's own 10-minute threshold is measured on *true* time
   (`device_capture_time`), but a Redis TTL can only be keyed on *arrival* time
   (`timestamp`) — and those two diverge by an amount that hasn't been measured yet
   (`timestamp − device_capture_time`, the upload-latency distribution). That's the next
   concrete measurement needed to size TTL correctly instead of guessing.
2. **`maxmemory-policy` (eviction policy).** `allkeys-lru`/`allkeys-lfu` evict silently —
   dropping a still-needed record is a correctness bug here, not just a performance one,
   since the algorithm has no way to detect that eviction happened. `noeviction` fails
   loudly instead but pushes backpressure handling onto the pipeline. Undecided.
3. **Secondary indexing.** The `HASH` layout only supports point lookup by `image_id`.
   Nothing yet answers "what's currently open for this vehicle" as an indexed query.
   Not designed.
4. **Whether to adopt binary field packing** (§5b) — would roughly double every capacity
   number, not yet implemented anywhere.
5. **Why 16 GB specifically.** Unconfirmed whether this is a hard infra ceiling or a
   soft starting point — matters for how alarming the "~30 min at 1M vehicles" finding
   actually is.
6. **Windowed assignment-rate experiment (in progress).** A direct test of how much of
   DIS Split-Merge's accuracy survives at short, sub-hour window lengths (10/15/30
   min/1h, fixed and cascading schemes), measured against the unwindowed baseline from
   §3. Code is written (`codes/windowed_embed_sim_dct_level_assignment_short_windows*.py`)
   and running on the EC2 instance; results aren't back yet. This is the number that
   will make §2b's fragmentation curve and §5c's capacity curve concrete in terms of
   actual assignment quality, not just structural counts.

---

## 7. The central tension

Two independent curves point at the same place:

- **§5c (Redis capacity)** says the memory budget forces the working-set window down to
  roughly **30 minutes** at real fleet scale.
- **§2b (fragmentation vs. window length)** says that at roughly that same **30-minute**
  window, fragmentation is already **nearly universal** (98.7% of vehicles affected,
  ~7% of sessions per vehicle split apart) — purely from arrival-time windowing, not from
  anything wrong with the underlying data (§2a showed the raw data is clean).

These aren't two separate problems — they're the same constraint viewed from two
directions, and they intersect at a bad point: the window length memory forces us toward
is also where the windowing scheme itself starts breaking down the sessions DIS
Split-Merge exists to reconstruct. Item 6 in §6 (the assignment-rate experiment) is what
will turn "fragmentation is high here" into "and here's how much that actually costs in
assignment accuracy" — that number is the one this whole feasibility study has been
building toward.
