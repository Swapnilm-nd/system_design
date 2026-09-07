# Analysis for Design

Findings from analysis run in this folder, feeding into the AN-35636 production DIS design.
Each section: question asked, method, outcome, and what it implies for the design.

## 1. Image-level discontinuity in device_capture_time order

**Question**: When two separate incoming requests (arrivals, identified by distinct `timestamp`
values) for the same vehicle are pooled and sorted by `device_capture_time`, do their images
stay contiguous (all of one request's images before/after all of the other's), or do they
interleave? Every interleaved image is a "discontinuity" — different from the earlier
`implementation/codes/base_discontinuity_check.py` check, which measured discontinuity in
*arrival* order and grouped by `dis` rather than by request/image directly.

**Method**: `notebooks/image_level_discontinuity_check.ipynb`. Per vehicle: sort all images by
`device_capture_time`, run-length encode the `timestamp` (request id) label over that order —
consecutive same-label images form one contiguous run. A request whose images land in more than
one run has been split by another request's images; every image in a non-first run for its
label is counted as discontinuous. Equivalent to checking every pair of requests directly, but
computed in one linear pass instead of O(requests²). Validated against a hand-worked toy example
before running at scale. Run over all 6,364 `valid_tenant_vehicle_pairs` vehicles.

**Outcome**:

| Metric | Count | Total | % |
|---|---|---|---|
| Discontinuous images | 87 | 8,437,627 | 0.0010% |
| Affected requests | 46 | 6,874,579 | 0.0007% |

- 6,363 of 6,364 vehicles processed successfully (1 missing source CSV — a stale entry in
  `valid_tenant_vehicle_pairs.csv`, unrelated to the logic).
- Per-vehicle median discontinuity is 0% — the handful of affected vehicles (see
  `outputs/vehicle_level_dct_discontinuity_results.csv`, sorted by `discontinuous_pct`) each
  have only 1-2 affected requests.
- Full per-vehicle results: `outputs/vehicle_level_dct_discontinuity_results.csv`; fleet summary:
  `outputs/overall_dct_discontinuity_summary.csv`; distribution plot:
  `outputs/dct_discontinuity_pct_distribution.png`.

**Implication for design**: real requests almost never arrive so out-of-order (relative to
`device_capture_time`) that one request's images get sandwiched inside another's. This is a much
weaker signal than the arrival-order-based discontinuity previously measured, and supports the
production design's "accepted approximation" that a late/out-of-order arrival is rare enough to
treat as a permanent, accepted miss rather than something requiring active reconciliation.

## 2. Window-level discontinuity, fixed clock-aligned windows (10/15/30/60 min)

**Question**: Same interleaving test as #1, but instead of grouping by individual request
(`timestamp`), first bucket requests into **fixed, clock-aligned windows** of length `WL`
(`window_id = floor(epoch_seconds(timestamp) / (WL*60))` — the same scheme as the production
"method one" fixed-window design). Does any image whose request falls in window `w1` interleave,
in `device_capture_time` order, with images from a different window `w2`? Run for
`WL ∈ {10, 15, 30, 60}` minutes.

**Method**: `notebooks/window_level_discontinuity_check.ipynb`. Reuses the exact run-length
interleaving core from notebook #1 unchanged — only the grouping label changes, from raw request
`timestamp` to `window_id` derived from it. Each vehicle's CSV is read once and window ids are
recomputed for all 4 window lengths from the same loaded data. Validated against a toy example
before running at scale. Run over all 6,364 `valid_tenant_vehicle_pairs` vehicles.

**Outcome**:

| Window length | Discontinuous images | Total images | Image % | Affected windows | Total windows | Window % |
|---|---|---|---|---|---|---|
| 10 min | 61,811 | 8,437,627 | 0.733% | 7,702 | 1,591,407 | 0.484% |
| 15 min | 51,641 | 8,437,627 | 0.612% | 6,133 | 1,134,540 | 0.541% |
| 30 min | 41,048 | 8,437,627 | 0.486% | 3,223 | 635,134 | 0.507% |
| 60 min | 43,697 | 8,437,627 | 0.518% | 2,132 | 352,342 | 0.605% |

- Same 6,363/6,364 vehicles processed (same 1 missing source CSV as analysis #1).
- Full results: `outputs/vehicle_level_window_discontinuity_results.csv` (long format, one row
  per vehicle x window length); fleet summary: `outputs/overall_window_discontinuity_summary.csv`;
  plots: `outputs/window_discontinuity_pct_distribution_by_wl.png`,
  `outputs/overall_window_discontinuity_vs_wl.png`.

**Implication for design**: image-level discontinuity is **2-3 orders of magnitude higher** with
fixed windows (0.49-0.73%) than with exact-request grouping (0.001%, analysis #1) — expected,
since a fixed window's boundaries are arbitrary relative to the data and can slice through a
cluster of temporally-close requests that would otherwise sit cleanly together. Image-level %
drops as `WL` grows from 10 to 30 min (larger windows absorb more of what would otherwise be
inter-window interleaving), but **ticks back up at 60 min** — a real, non-monotonic effect worth
noting rather than assuming discontinuity strictly decreases with window size. This is a direct,
quantified cost of the fixed clock-aligned window scheme (vs. per-request or dynamic/idle-cutoff
schemes) that should factor into the `window_scheme`/`window_length_hours` choice in the
production design.
