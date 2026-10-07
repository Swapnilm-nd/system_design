"""
Incoming-request rate (per second / per minute / per hour) and images-per-request
(batch size) analysis, for the DIS round-trip/fleet-scale cost model in
dis_group_l_buffer_ram_vs_redis_native_analysis.md and
dis_group_l_batch_processing_proposal.md (where both were previously only
illustrative placeholder numbers, e.g. "R=0.1 req/s/device").

Run this ON THE CLOUD INSTANCE (same convention as every other script in this
folder - the real/full-scale per-image CSVs live there, not in a local Mac
checkout, or at least not at full fleet scale). It does NOT touch Redis or
Postgres at all - it's a pure offline computation over real per-image
timestamps, writing its results to OUTPUT_DIR as CSVs for
`notebooks/incoming_request_rate_and_batch_size_analysis.ipynb` (run locally,
after copying that folder back) to summarize and plot.

## What's a "request" here

A **request** = one reconstructed arrival: consecutive rows (per device, in
timestamp order) sharing the same `dis` value, capped at GROUP_SIZE_MAX images
-- identical rule to `system_design_backup/.../scripts/redis_load_analysis.py`'s
arrival reconstruction (itself matching `scripts/send_ready_for_queue.py`'s
grouping). This is deliberately *not* "one row = one request" -- a single
Recognize call can carry multiple images, which is exactly the `K` (images per
request) variable the round-trip cost model needs a real distribution for,
not an assumed one.

GROUP_SIZE_MAX's default (5) is inherited from the synthetic top_10k dataset's
own generation config (`fixed_window_and_window_timer_condition_method_one/src/
config.py`), not an independently-confirmed real production limit -- if the
instance's data was generated with a different cap, or if this is run against
a genuinely different (non-top_10k) data source, override it via the
GROUP_SIZE_MAX env var.

## Outputs are bounded by device-count / time-span / batch-size, not row-count

Deliberately does NOT write one row per arrival or per image (at true fleet
scale -- hundreds of thousands of devices, not top_10k's 9.6K sample -- that
could be hundreds of millions of rows). Every output file's size instead
scales with:
  - number of devices (per-device summary),
  - number of time buckets (per-bucket time series - e.g. ~604,800 buckets
    for 7 days at 1-second granularity, trivially small),
  - max images-per-request (the batch-size histogram).
All three stay small regardless of how many devices/images the real fleet has.

## Modeling choices (read before trusting the numbers)

- **Request/arrival time**: the CSV's own `timestamp` column (when the image
  was received by the system), not `device_capture_time` (when captured
  on-device) -- same convention as every prior windowing/rate script in this
  repo. An arrival's own timestamp is its *last* image's timestamp (the
  arrival is "complete" at that point) -- matching redis_load_analysis.py.
- **A "device"** is the `(tenant_id, device_id)` pair -- DIS's own identity
  key (`dis_buffer_last_update:{tenant_id}:{device_id}`, etc. in
  frs_src/utils.py), not `(tenant_id, vehicle_id)` (which only matters here
  for the per-file glob, a storage-layout detail of how this dataset happens
  to be sharded, not the actual DIS-relevant identity).
- **No internal-split simulation, no embeddings read at all** -- this script
  only needs `image_id, timestamp, tenant_id, device_id, dis`, so it's far
  cheaper than the embedding-size scripts in this folder and doesn't need the
  `embeddings/*.joblib` files at all.

## Reused patterns (see these files for the proven originals)

- Arrival reconstruction + vectorized run/sub-run bucketing:
  `system_design_backup/.../scripts/redis_load_analysis.py`'s `build_arrivals`.
- Per-vehicle-file `ProcessPoolExecutor` harness + `tqdm`:
  `scripts/online_vehicle_redis_storage_analysis.py`.
- `_env` config-override helper: both of the above.
"""
import glob
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm


def _env(name, default=None, cast=str):
    raw = os.environ.get(name)
    if raw is None:
        return default
    if cast is bool:
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    return cast(raw)


# ---- Config (env-var overridable) -- instance paths are the primary default,
# since this script's home is the cloud instance, not this local checkout. ----
PROCESSED_CSV_GLOB = _env(
    "PROCESSED_CSV_GLOB",
    "/home/ubuntu/dis_analysis/top_10k/processed_csv/processed_images_*.csv",
)
# Fallback smoke-test dataset if the glob above matches nothing (e.g. running
# this locally against the smaller sample already synced to this Mac).
FALLBACK_CSV_GLOB = _env(
    "FALLBACK_CSV_GLOB",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "top_10k", "processed_csv", "processed_images_*.csv",
    ),
)
OUTPUT_DIR = _env(
    "OUTPUT_DIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs"),
)

GROUP_SIZE_MAX = _env("GROUP_SIZE_MAX", 5, int)  # see module docstring - origin/caveat
BUCKETS = {"1s": "1s", "1min": "1min", "1h": "1h"}  # per the task's exact ask

# 0/blank disables -- use the full dataset. Set e.g. to 1 for a fast smoke test.
LIMIT_TO_FIRST_N_DAYS = _env("LIMIT_TO_FIRST_N_DAYS", 0, int)

MAX_WORKERS = _env("MAX_WORKERS", 10, int)


def _resolve_files():
    files = sorted(glob.glob(PROCESSED_CSV_GLOB))
    if files:
        return files, PROCESSED_CSV_GLOB
    files = sorted(glob.glob(FALLBACK_CSV_GLOB))
    if files:
        print(f"No files matched {PROCESSED_CSV_GLOB!r} -- falling back to {FALLBACK_CSV_GLOB!r}")
        return files, FALLBACK_CSV_GLOB
    raise FileNotFoundError(
        f"No files matched either {PROCESSED_CSV_GLOB!r} or {FALLBACK_CSV_GLOB!r}"
    )


def process_device_file(path):
    """Everything computed per-device-file, in a worker process: arrival
    reconstruction, per-bucket request/image counts, and this device's own
    summary stats. Returns a dict (never raises - errors are captured and
    reported in the 'error' field so one bad file doesn't kill the whole run).
    """
    try:
        df = pd.read_csv(path, usecols=["timestamp", "tenant_id", "device_id", "dis"])
        n_images_raw = len(df)
        # Keep a few RAW (unparsed) timestamp strings around before overwriting
        # the column - if every single one fails to parse (format mismatch on
        # this environment/dataset), this is what actually shows the real
        # format on the instance instead of a silent, opaque "0 rows survived".
        raw_timestamp_sample = df["timestamp"].head(3).tolist() if n_images_raw else []
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        dropped_missing_timestamp = int(df["timestamp"].isna().sum())
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

        if LIMIT_TO_FIRST_N_DAYS and len(df):
            cutoff = df["timestamp"].min() + pd.Timedelta(days=LIMIT_TO_FIRST_N_DAYS)
            df = df[df["timestamp"] < cutoff].reset_index(drop=True)

        if df.empty:
            return {
                "path": path, "tenant_id": None, "device_id": None,
                "n_images": 0, "n_requests": 0, "bucket_rows": [],
                "batch_size_counts": {}, "device_summary": None,
                "n_images_raw": n_images_raw,
                "raw_timestamp_sample": raw_timestamp_sample if dropped_missing_timestamp == n_images_raw else [],
                "dropped_missing_timestamp": dropped_missing_timestamp, "error": None,
            }

        tenant_id = df["tenant_id"].iloc[0]
        device_id = df["device_id"].iloc[0]

        # Arrival reconstruction - identical rule to redis_load_analysis.py's
        # build_arrivals: consecutive rows sharing `dis` (already single-device
        # here, so no tenant_id/device_id key-change check needed), capped at
        # GROUP_SIZE_MAX.
        key_change = df["dis"] != df["dis"].shift()
        run_id = key_change.cumsum()
        position_in_run = df.groupby(run_id).cumcount()
        sub_run = position_in_run // GROUP_SIZE_MAX

        arrivals = df.groupby([run_id, sub_run]).agg(
            n_images_in_request=("timestamp", "size"),
            request_ts=("timestamp", "max"),
        ).reset_index(drop=True)

        assert arrivals["n_images_in_request"].sum() == len(df)
        assert arrivals["n_images_in_request"].max() <= GROUP_SIZE_MAX

        batch_size_counts = arrivals["n_images_in_request"].value_counts().to_dict()

        bucket_rows = []
        for label, freq in BUCKETS.items():
            bucket_start = arrivals["request_ts"].dt.floor(freq)
            g = (
                arrivals.assign(bucket_start=bucket_start)
                .groupby("bucket_start")
                .agg(n_requests=("n_images_in_request", "size"), n_images=("n_images_in_request", "sum"))
                .reset_index()
            )
            for _, row in g.iterrows():
                bucket_rows.append({
                    "granularity": label,
                    "bucket_start": row["bucket_start"],
                    "n_requests": int(row["n_requests"]),
                    "n_images": int(row["n_images"]),
                })

        device_summary = {
            "tenant_id": tenant_id,
            "device_id": device_id,
            "n_images": int(len(df)),
            "n_requests": int(len(arrivals)),
            "mean_images_per_request": float(arrivals["n_images_in_request"].mean()),
            "median_images_per_request": float(arrivals["n_images_in_request"].median()),
            "max_images_per_request": int(arrivals["n_images_in_request"].max()),
            "timestamp_min": df["timestamp"].min(),
            "timestamp_max": df["timestamp"].max(),
            "dropped_missing_timestamp": dropped_missing_timestamp,
        }

        return {
            "path": path, "tenant_id": tenant_id, "device_id": device_id,
            "n_images": len(df), "n_requests": len(arrivals),
            "bucket_rows": bucket_rows, "batch_size_counts": batch_size_counts,
            "device_summary": device_summary,
            "n_images_raw": n_images_raw, "raw_timestamp_sample": [],
            "dropped_missing_timestamp": dropped_missing_timestamp, "error": None,
        }
    except Exception as e:
        return {
            "path": path, "tenant_id": None, "device_id": None,
            "n_images": 0, "n_requests": 0, "bucket_rows": [],
            "batch_size_counts": {}, "device_summary": None,
            "n_images_raw": 0, "raw_timestamp_sample": [],
            "dropped_missing_timestamp": None, "error": str(e),
        }


def main():
    files, source_glob = _resolve_files()
    print(f"Found {len(files):,} per-device files matching {source_glob!r}")
    print(f"GROUP_SIZE_MAX={GROUP_SIZE_MAX}  BUCKETS={list(BUCKETS)}  "
          f"LIMIT_TO_FIRST_N_DAYS={LIMIT_TO_FIRST_N_DAYS or 'disabled'}  MAX_WORKERS={MAX_WORKERS}")

    device_summaries = []
    bucket_rows_all = []
    batch_size_counts_total = {}
    errors = []
    total_images_raw = 0
    total_dropped_missing_timestamp = 0
    raw_timestamp_samples = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_device_file, p): p for p in files}
        for future in tqdm(as_completed(futures), total=len(futures)):
            path = futures[future]
            try:
                result = future.result(timeout=300)
            except Exception as e:
                result = {"path": path, "error": str(e), "device_summary": None,
                          "bucket_rows": [], "batch_size_counts": {}, "n_images_raw": 0,
                          "raw_timestamp_sample": [], "dropped_missing_timestamp": None}

            if result.get("error"):
                errors.append((result["path"], result["error"]))
                continue
            if result["device_summary"] is not None:
                device_summaries.append(result["device_summary"])
            bucket_rows_all.extend(result["bucket_rows"])
            for size, count in result["batch_size_counts"].items():
                batch_size_counts_total[size] = batch_size_counts_total.get(size, 0) + count
            total_images_raw += result.get("n_images_raw") or 0
            total_dropped_missing_timestamp += result.get("dropped_missing_timestamp") or 0
            if result.get("raw_timestamp_sample") and len(raw_timestamp_samples) < 5:
                raw_timestamp_samples.append((result["path"], result["raw_timestamp_sample"]))

    print(f"\nDevices processed OK: {len(device_summaries):,}   errored: {len(errors):,}")
    if errors:
        print("Sample errors (up to 5):")
        for path, err in errors[:5]:
            print(f"  {path}: {err}")

    print(f"\nTotal raw image rows read: {total_images_raw:,}")
    print(f"Rows dropped for unparseable/missing timestamp: {total_dropped_missing_timestamp:,} "
          f"({100 * total_dropped_missing_timestamp / total_images_raw:.1f}% of raw rows)"
          if total_images_raw else "")

    # Fail loud and diagnosable instead of an opaque KeyError deep inside
    # pandas groupby/sort_values on an empty, columnless frame (what an older
    # pandas build does when every device's timestamps failed to parse and
    # bucket_rows_all ends up completely empty).
    if not bucket_rows_all:
        print("\n" + "!" * 78)
        print("No arrivals/bucket rows were reconstructed from ANY processed file.")
        if total_images_raw and total_dropped_missing_timestamp == total_images_raw:
            print("Every single raw row's timestamp failed to parse (100% dropped) -")
            print("this is almost certainly a timestamp format mismatch on this")
            print("environment/dataset, not a real empty dataset. Raw (unparsed)")
            print("timestamp samples from the affected files:")
            for path, sample in raw_timestamp_samples:
                print(f"  {path}: {sample}")
            print("\nFix: adjust the pd.to_datetime(...) call in process_device_file")
            print("(e.g. pass an explicit format= matching what's printed above),")
            print("then re-run.")
        else:
            print("Raw row / dropped-row counts above didn't show a clear 100% drop -")
            print("investigate per_device_summary (once any non-empty devices exist)")
            print("or re-run with MAX_WORKERS=1 on one file to inspect directly.")
        print("!" * 78)
        raise SystemExit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ---- 1. Per-device summary (bounded by device count) ----
    device_summary_df = pd.DataFrame(device_summaries)
    device_summary_path = os.path.join(OUTPUT_DIR, "per_device_summary.csv")
    device_summary_df.to_csv(device_summary_path, index=False)

    # ---- 2. Per-device, per-bucket request/image counts (bounded by
    # device_count x time_buckets, still tiny relative to raw image count) ----
    bucket_df = pd.DataFrame(bucket_rows_all)
    # Re-attach tenant_id/device_id isn't needed here since bucket_rows don't
    # carry device identity (aggregated fleet-wide below) - kept lightweight
    # on purpose; see per_device_summary.csv for the per-device view.

    # ---- 3. Fleet-wide (overall) per-bucket request/image counts - the
    # primary "frequency of incoming requests" output ----
    overall_bucket_df = (
        bucket_df.groupby(["granularity", "bucket_start"], as_index=False)
        .agg(n_requests=("n_requests", "sum"), n_images=("n_images", "sum"))
        .sort_values(["granularity", "bucket_start"])
        .reset_index(drop=True)
    )
    overall_bucket_path = os.path.join(OUTPUT_DIR, "overall_request_rate_by_bucket.csv")
    overall_bucket_df.to_csv(overall_bucket_path, index=False)

    # ---- 4. Images-per-request (batch size) histogram - the primary "images
    # per request" output. Bounded by GROUP_SIZE_MAX distinct values. ----
    batch_size_df = (
        pd.Series(batch_size_counts_total, name="n_requests")
        .rename_axis("images_per_request")
        .reset_index()
        .sort_values("images_per_request")
        .reset_index(drop=True)
    )
    batch_size_path = os.path.join(OUTPUT_DIR, "images_per_request_histogram.csv")
    batch_size_df.to_csv(batch_size_path, index=False)

    print("\nWrote outputs:")
    for p in (device_summary_path, overall_bucket_path, batch_size_path):
        print(f"  {p}  ({os.path.getsize(p):,} bytes)")

    total_requests = int(batch_size_df["n_requests"].sum())
    total_images = int((batch_size_df["images_per_request"] * batch_size_df["n_requests"]).sum())
    print(f"\nTotal requests (arrivals) reconstructed: {total_requests:,}  (from {total_images:,} images)")
    if total_requests:
        weighted_mean = total_images / total_requests
        print(f"Fleet-wide mean images/request: {weighted_mean:.3f}")
    print("\nImages-per-request histogram:")
    print(batch_size_df.to_string(index=False))

    print("\nOverall request-rate summary by granularity:")
    for label in BUCKETS:
        s = overall_bucket_df.loc[overall_bucket_df["granularity"] == label, "n_requests"]
        if len(s):
            print(f"  {label:>5}: buckets={len(s):>8,}  mean={s.mean():10.3f}  "
                  f"median={s.median():10.1f}  p95={s.quantile(.95):10.1f}  max={s.max():>8}")


if __name__ == "__main__":
    main()
