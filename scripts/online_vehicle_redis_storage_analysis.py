"""
Online-vehicle count and raw Redis storage sizing, at fixed clock-aligned window lengths.

Run this ON THE CLOUD INSTANCE (real face embeddings only exist there, in
`top_10k/embeddings/embeddings_{tenant_id}_{vehicle_id}.joblib` -- not present in a local Mac
checkout). It does NOT touch a running Redis at all -- it's a pure offline computation over real
per-image timestamps and real embedding sizes, writing its results to OUTPUT_DIR as CSVs for
`notebooks/online_vehicle_redis_storage_analysis.ipynb` (run locally, after copying that folder
back) to analyze and plot.

For each window length WL in WINDOW_LENGTHS_MINUTES, and for each fixed clock-aligned window of
that length, reports: (1) number of distinct "online" vehicles (vehicles with >=1 record whose
arrival `timestamp` falls in that window), (2) number of records in that window, per vehicle and
fleet-wide, (3) Redis storage in bytes that would be required to hold those records, per vehicle
and fleet-wide.

## Modeling choices (read before trusting the numbers)

- **One record = one image.** No dis/arrival-message grouping is simulated here (contrast with
  `redis_load_analysis.py`'s arrival reconstruction) -- this is a raw per-image ingestion view.
- **Every record caches its own real embedding** -- no dis split-merge boundary-caching
  optimization is applied (contrast with `redis_load_analysis.py`'s "only 1-2 embeddings per
  arrival" model). This is deliberately the naive/worst-case "what would it cost to just hold
  everything until the window closes" number, matching the `_if_all_cached` comparison variant
  in `redis_load_analysis.py` -- here it's the primary metric, not a side comparison.
- **Embedding size is measured, not assumed**: loaded from each vehicle's own `.joblib` file via
  `.nbytes`, per image_id. Falls back to `DEFAULT_EMBEDDING_BYTES` (512-dim float64 = 4096) only
  when an image_id has no entry in its vehicle's embeddings dict -- tracked separately
  (`embeddings_missing`) rather than silently assumed.
- **Arrival/window time**: the CSV's own `timestamp` column (when the image was received by the
  system), not `device_capture_time` (when captured on-device) -- same convention as every prior
  windowing script in this repo.
- **No upper bound**: raw computed bytes, no clipping against any Redis memory ceiling.
- **A "vehicle"** is the `(tenant_id, vehicle_id)` pair, matching the per-file naming
  (`processed_images_{tenant_id}_{vehicle_id}.csv`), since `vehicle_id` alone isn't guaranteed
  unique across tenants.

## Reused patterns (see these files for the proven originals)

- Window bucketing: `system_design_backup/.../scripts/redis_load_analysis.py`'s resolution-safe
  `.dt.floor(...)` + epoch-`Timedelta` division (deliberately not `astype("int64") // window_ns`,
  which that file's own comment flags as unsafe across pandas datetime64 storage units).
- Multi-window-length-in-one-pass-per-vehicle harness + `ProcessPoolExecutor(..., fork)`:
  `implementation/codes/dis_windowed_group_check_fixed_windows_multi_short.py`.
- Embeddings path template + `joblib.load(...)` usage:
  `implementation/codes/redis_field_datatype_report.py` / `top_10k/codes/final_prediction.py`.
"""
import json
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import joblib
import numpy as np
import pandas as pd
from tqdm import tqdm


def _env(name, default=None, cast=str):
    raw = os.environ.get(name)
    if raw is None:
        return default
    if cast is bool:
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    return cast(raw)


# ---- Config (env-var overridable) -- instance paths are the primary default, since this
# script's home is the cloud instance, not this local checkout. ----
TOP_10K_DIR = _env("TOP_10K_DIR", "/home/ubuntu/dis_analysis/top_10k")
VALID_PAIRS_FILE = _env("VALID_PAIRS_FILE", "/home/ubuntu/dis_analysis/valid_tenant_vehicle_pairs.csv")
OUTPUT_DIR = _env(
    "OUTPUT_DIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs"),
)

PROCESSED_IMAGES_TEMPLATE = os.path.join(TOP_10K_DIR, "processed_csv", "processed_images_{tenant_id}_{vehicle_id}.csv")
EMBEDDINGS_TEMPLATE = os.path.join(TOP_10K_DIR, "embeddings", "embeddings_{tenant_id}_{vehicle_id}.joblib")

WINDOW_LENGTHS_MINUTES = [10, 15, 30, 60]

# ---- Byte-size constants -- see this module's docstring for where these come from ----
METADATA_BYTES_PER_RECORD = _env("METADATA_BYTES_PER_RECORD", 662, int)
EMBEDDING_DIM = 512
DEFAULT_EMBEDDING_BYTES = EMBEDDING_DIM * 8  # 4096, float64 -- fallback only, see docstring

MAX_WORKERS = _env("MAX_WORKERS", 10, int)

_EPOCH = pd.Timestamp("1970-01-01")


def process_vehicle(tenant_id, vehicle_id):
    """Returns a list of dicts, one per (window_length_minutes, window), for this vehicle."""
    try:
        df = pd.read_csv(
            PROCESSED_IMAGES_TEMPLATE.format(tenant_id=tenant_id, vehicle_id=vehicle_id),
            usecols=["image_id", "timestamp"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        dropped_missing_timestamp = int(df["timestamp"].isna().sum())
        df = df.dropna(subset=["timestamp"])

        embeddings = joblib.load(EMBEDDINGS_TEMPLATE.format(tenant_id=tenant_id, vehicle_id=vehicle_id))

        def embedding_bytes(image_id):
            vector = embeddings.get(image_id)
            if vector is None:
                return DEFAULT_EMBEDDING_BYTES, False
            return np.asarray(vector).nbytes, True

        embedding_info = df["image_id"].map(embedding_bytes)
        df["embedding_bytes"] = embedding_info.map(lambda t: t[0])
        df["embedding_found"] = embedding_info.map(lambda t: t[1])
        df["storage_bytes"] = METADATA_BYTES_PER_RECORD + df["embedding_bytes"]

        rows = []
        for window_minutes in WINDOW_LENGTHS_MINUTES:
            # Pure Timedelta arithmetic instead of `.dt.floor(f"{window_minutes}min")` --
            # frequency-alias strings ("min" vs. "T") aren't consistently accepted across pandas
            # versions, and this needs to run on whatever old/pinned env happens to be on the
            # instance. Integer floor-division of two Timedeltas has been stable for a very long
            # time and sidesteps that entirely.
            window_length = pd.Timedelta(minutes=window_minutes)
            window_id = ((df["timestamp"] - _EPOCH) // window_length).astype("int64")
            window_start = _EPOCH + window_id * window_length

            g = df.assign(window_id=window_id, window_start=window_start).groupby(
                ["window_id", "window_start"], as_index=False
            ).agg(
                record_count=("image_id", "size"),
                storage_bytes=("storage_bytes", "sum"),
                embeddings_missing=("embedding_found", lambda s: int((~s).sum())),
            )

            for _, row in g.iterrows():
                rows.append({
                    "tenant_id": tenant_id,
                    "vehicle_id": vehicle_id,
                    "window_length_minutes": window_minutes,
                    "window_id": int(row["window_id"]),
                    "window_start": row["window_start"],
                    "record_count": int(row["record_count"]),
                    "storage_bytes": int(row["storage_bytes"]),
                    "embeddings_missing": int(row["embeddings_missing"]),
                    "dropped_missing_timestamp": dropped_missing_timestamp,
                    "error": None,
                })
        return rows

    except Exception as e:
        err = str(e)
        return [
            {
                "tenant_id": tenant_id, "vehicle_id": vehicle_id,
                "window_length_minutes": window_minutes,
                "window_id": None, "window_start": None,
                "record_count": None, "storage_bytes": None,
                "embeddings_missing": None, "dropped_missing_timestamp": None,
                "error": err,
            }
            for window_minutes in WINDOW_LENGTHS_MINUTES
        ]


OVERALL_COLUMNS = [
    "window_length_minutes", "window_id", "window_start",
    "n_online_vehicles", "total_records", "total_storage_bytes",
]


def aggregate_overall(vehicle_window_df):
    ok = vehicle_window_df[vehicle_window_df["error"].isna()]
    if ok.empty:
        # Older pandas can drop the group-key columns entirely when grouping an empty frame
        # (as_index=False doesn't reliably save you) -- return an explicitly-shaped empty frame
        # instead of letting the caller's sort_values/plotting code KeyError on a missing column.
        return pd.DataFrame(columns=OVERALL_COLUMNS)
    return (
        ok.groupby(["window_length_minutes", "window_id", "window_start"], as_index=False)
        .agg(
            n_online_vehicles=("vehicle_id", "nunique"),
            total_records=("record_count", "sum"),
            total_storage_bytes=("storage_bytes", "sum"),
        )
        .sort_values(["window_length_minutes", "window_id"])
        .reset_index(drop=True)
    )


def summarize_by_window_length(vehicle_window_df, overall_df):
    rows = []
    for window_minutes in WINDOW_LENGTHS_MINUTES:
        veh = vehicle_window_df[vehicle_window_df["window_length_minutes"] == window_minutes]
        n_vehicles_total = veh[["tenant_id", "vehicle_id"]].drop_duplicates().shape[0]
        n_vehicles_errored = veh.loc[veh["error"].notna(), ["tenant_id", "vehicle_id"]].drop_duplicates().shape[0]

        overall = overall_df[overall_df["window_length_minutes"] == window_minutes]
        peak_vehicles_row = overall.loc[overall["n_online_vehicles"].idxmax()] if len(overall) else None
        peak_storage_row = overall.loc[overall["total_storage_bytes"].idxmax()] if len(overall) else None

        rows.append({
            "window_length_minutes": window_minutes,
            "vehicles_total": n_vehicles_total,
            "vehicles_errored": n_vehicles_errored,
            "total_windows": len(overall),
            "peak_n_online_vehicles": int(peak_vehicles_row["n_online_vehicles"]) if peak_vehicles_row is not None else None,
            "peak_n_online_vehicles_window_start": peak_vehicles_row["window_start"] if peak_vehicles_row is not None else None,
            "peak_total_storage_bytes": int(peak_storage_row["total_storage_bytes"]) if peak_storage_row is not None else None,
            "peak_total_storage_bytes_window_start": peak_storage_row["window_start"] if peak_storage_row is not None else None,
            "mean_n_online_vehicles": overall["n_online_vehicles"].mean() if len(overall) else None,
            "median_n_online_vehicles": overall["n_online_vehicles"].median() if len(overall) else None,
            "mean_total_storage_bytes": overall["total_storage_bytes"].mean() if len(overall) else None,
            "median_total_storage_bytes": overall["total_storage_bytes"].median() if len(overall) else None,
        })
    return pd.DataFrame(rows)


def main():
    print(f"WINDOW_LENGTHS_MINUTES={WINDOW_LENGTHS_MINUTES}  "
          f"METADATA_BYTES_PER_RECORD={METADATA_BYTES_PER_RECORD}  "
          f"DEFAULT_EMBEDDING_BYTES={DEFAULT_EMBEDDING_BYTES}  MAX_WORKERS={MAX_WORKERS}")

    valid_pairs_df = pd.read_csv(VALID_PAIRS_FILE)
    print(f"valid_tenant_vehicle_pairs shape = {valid_pairs_df.shape}")
    pairs = list(valid_pairs_df[["tenant_id", "vehicle_id"]].itertuples(index=False, name=None))

    all_rows = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_vehicle, t, v): (t, v) for t, v in pairs}
        for future in tqdm(as_completed(futures), total=len(futures)):
            tenant_id, vehicle_id = futures[future]
            try:
                all_rows.extend(future.result(timeout=300))
            except Exception as e:
                all_rows.extend([
                    {
                        "tenant_id": tenant_id, "vehicle_id": vehicle_id,
                        "window_length_minutes": window_minutes,
                        "window_id": None, "window_start": None,
                        "record_count": None, "storage_bytes": None,
                        "embeddings_missing": None, "dropped_missing_timestamp": None,
                        "error": str(e),
                    }
                    for window_minutes in WINDOW_LENGTHS_MINUTES
                ])

    vehicle_window_df = pd.DataFrame(all_rows)
    n_error_vehicles = vehicle_window_df.loc[vehicle_window_df["error"].notna(), ["tenant_id", "vehicle_id"]].drop_duplicates().shape[0]
    print(f"vehicles processed = {len(pairs)}, vehicles with errors = {n_error_vehicles}")

    if n_error_vehicles:
        sample_errors = vehicle_window_df.loc[vehicle_window_df["error"].notna(), "error"].drop_duplicates().head(5)
        print(f"\nSample distinct error message(s) (up to 5 of {vehicle_window_df['error'].dropna().nunique()} distinct):")
        for msg in sample_errors:
            print(f"  - {msg}")

    # Write the raw per-vehicle rows (error strings included) BEFORE any further aggregation --
    # this is the expensive part (one file read + one embeddings load per vehicle), so a bug in
    # the aggregation step below must never cost re-running it to get a diagnosable artifact.
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    vehicle_window_path = os.path.join(OUTPUT_DIR, "vehicle_window_online_storage_results.csv")
    vehicle_window_df.to_csv(vehicle_window_path, index=False)
    print(f"\nWrote {vehicle_window_path}  ({os.path.getsize(vehicle_window_path):,} bytes)")

    overall_df = aggregate_overall(vehicle_window_df)
    summary_df = summarize_by_window_length(vehicle_window_df, overall_df)

    overall_path = os.path.join(OUTPUT_DIR, "overall_window_online_storage_results.csv")
    summary_path = os.path.join(OUTPUT_DIR, "online_storage_summary_by_window_length.csv")

    overall_df.to_csv(overall_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    print("Wrote outputs:")
    for p in (overall_path, summary_path):
        print(f"  {p}  ({os.path.getsize(p):,} bytes)")

    print("\nSummary by window length:")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
