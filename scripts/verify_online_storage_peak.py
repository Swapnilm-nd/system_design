"""
Independent verification that the "peak" numbers in
`online_storage_summary_by_window_length.csv` really are the peak, by recomputing
everything from the raw per-vehicle-window rows in `vehicle_window_online_storage_results.csv`
-- does NOT reuse `aggregate_overall`/`summarize_by_window_length` from
`online_vehicle_redis_storage_analysis.py`, since re-running the same code can't catch a bug in
that code. This is a from-scratch recomputation, cross-checked against both output files.

Checks, per window_length_minutes:
1. Re-aggregates raw rows -> (window_id, window_start): n_online_vehicles, total_records,
   total_storage_bytes -- compares every row against `overall_window_online_storage_results.csv`.
2. Finds the true max of total_storage_bytes and n_online_vehicles from that recomputation and
   compares against the claimed peak in `online_storage_summary_by_window_length.csv` (value AND
   which window_start it occurred at -- a peak at the right value but wrong window is still wrong).
3. Flags duplicate (tenant_id, vehicle_id) rows within the same window (would silently
   double-count a vehicle's storage into the total).
4. Sanity-checks storage_bytes against record_count (storage_bytes must be >=
   record_count * METADATA_BYTES_PER_RECORD -- embeddings only ever add bytes, never subtract).

Run locally (no embeddings needed -- pure CSV cross-checking): python3 verify_online_storage_peak.py
"""
import os
import sys

import pandas as pd

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs")
VEHICLE_WINDOW_PATH = os.path.join(OUTPUT_DIR, "vehicle_window_online_storage_results.csv")
OVERALL_PATH = os.path.join(OUTPUT_DIR, "overall_window_online_storage_results.csv")
SUMMARY_PATH = os.path.join(OUTPUT_DIR, "online_storage_summary_by_window_length.csv")

METADATA_BYTES_PER_RECORD = 662  # must match online_vehicle_redis_storage_analysis.py


def fail(msg):
    print(f"  FAIL: {msg}")
    return False


def verify_window_length(window_minutes, vehicle_window_df, overall_df, summary_df):
    print(f"\n=== window_length_minutes = {window_minutes} ===")
    ok = True

    veh = vehicle_window_df[vehicle_window_df["window_length_minutes"] == window_minutes]
    veh_clean = veh[veh["error"].isna()].copy()
    print(f"  raw rows: {len(veh)} total, {len(veh_clean)} without error, "
          f"{len(veh) - len(veh_clean)} error rows")

    # ---- Check 3: duplicate (tenant_id, vehicle_id) within the same window ----
    dup_mask = veh_clean.duplicated(subset=["tenant_id", "vehicle_id", "window_id"], keep=False)
    if dup_mask.any():
        n_dup = dup_mask.sum()
        ok = fail(f"{n_dup} duplicate (tenant_id, vehicle_id, window_id) rows found -- these "
                   f"would double-count a vehicle's storage into the total. Sample:\n"
                   f"{veh_clean.loc[dup_mask].head(5).to_string(index=False)}")
    else:
        print("  OK: no duplicate (tenant_id, vehicle_id) rows within any single window.")

    # ---- Check 4: storage_bytes >= record_count * METADATA_BYTES_PER_RECORD ----
    floor_violation = veh_clean["storage_bytes"] < veh_clean["record_count"] * METADATA_BYTES_PER_RECORD
    if floor_violation.any():
        ok = fail(f"{floor_violation.sum()} rows have storage_bytes below the metadata-only "
                   f"floor (record_count * {METADATA_BYTES_PER_RECORD}) -- embeddings can't "
                   f"subtract bytes. Sample:\n"
                   f"{veh_clean.loc[floor_violation].head(5).to_string(index=False)}")
    else:
        print(f"  OK: every row's storage_bytes >= record_count * {METADATA_BYTES_PER_RECORD} (metadata floor).")

    # ---- Check 1: from-scratch re-aggregation vs. overall_window_online_storage_results.csv ----
    recomputed = (
        veh_clean.groupby(["window_id", "window_start"], as_index=False)
        .agg(
            n_online_vehicles=("vehicle_id", "nunique"),
            total_records=("record_count", "sum"),
            total_storage_bytes=("storage_bytes", "sum"),
        )
    )
    claimed = overall_df[overall_df["window_length_minutes"] == window_minutes].copy()

    merged = recomputed.merge(
        claimed, on=["window_id", "window_start"], how="outer",
        suffixes=("_recomputed", "_claimed"), indicator=True,
    )

    only_one_side = merged[merged["_merge"] != "both"]
    if len(only_one_side):
        ok = fail(f"{len(only_one_side)} windows appear in only one of the two files "
                   f"(recomputed-from-raw vs. overall_window_online_storage_results.csv):\n"
                   f"{only_one_side[['window_id', 'window_start', '_merge']].to_string(index=False)}")

    both = merged[merged["_merge"] == "both"]
    for col in ["n_online_vehicles", "total_records", "total_storage_bytes"]:
        mismatch = both[both[f"{col}_recomputed"] != both[f"{col}_claimed"]]
        if len(mismatch):
            ok = fail(f"{len(mismatch)} windows disagree on {col} between recomputed-from-raw "
                       f"and the overall CSV:\n"
                       f"{mismatch[['window_start', f'{col}_recomputed', f'{col}_claimed']].head(5).to_string(index=False)}")

    if len(only_one_side) == 0 and ok:
        print(f"  OK: all {len(both)} windows' n_online_vehicles/total_records/total_storage_bytes "
              f"exactly match a from-scratch re-aggregation of the raw per-vehicle rows.")

    # ---- Check 2: is the claimed peak actually the max? ----
    summary_row = summary_df[summary_df["window_length_minutes"] == window_minutes].iloc[0]

    true_peak_storage = recomputed.loc[recomputed["total_storage_bytes"].idxmax()]
    claimed_peak_storage_bytes = summary_row["peak_total_storage_bytes"]
    claimed_peak_storage_start = pd.Timestamp(summary_row["peak_total_storage_bytes_window_start"])

    if int(true_peak_storage["total_storage_bytes"]) != int(claimed_peak_storage_bytes):
        ok = fail(f"peak_total_storage_bytes mismatch: summary claims "
                   f"{claimed_peak_storage_bytes:,.0f}, true max of the recomputed data is "
                   f"{true_peak_storage['total_storage_bytes']:,.0f} "
                   f"(at {true_peak_storage['window_start']})")
    elif pd.Timestamp(true_peak_storage["window_start"]) != claimed_peak_storage_start:
        ok = fail(f"peak_total_storage_bytes VALUE matches ({claimed_peak_storage_bytes:,.0f}) "
                   f"but the claimed window_start ({claimed_peak_storage_start}) differs from "
                   f"where the true max actually occurs ({true_peak_storage['window_start']}) "
                   f"-- likely a tie broken differently, or a stale/mismatched summary row.")
    else:
        print(f"  OK: peak_total_storage_bytes ({claimed_peak_storage_bytes:,.0f} bytes at "
              f"{claimed_peak_storage_start}) matches the true max of the raw data exactly.")

    true_peak_vehicles = recomputed.loc[recomputed["n_online_vehicles"].idxmax()]
    claimed_peak_vehicles = summary_row["peak_n_online_vehicles"]
    claimed_peak_vehicles_start = pd.Timestamp(summary_row["peak_n_online_vehicles_window_start"])

    if int(true_peak_vehicles["n_online_vehicles"]) != int(claimed_peak_vehicles):
        ok = fail(f"peak_n_online_vehicles mismatch: summary claims {claimed_peak_vehicles}, "
                   f"true max of the recomputed data is {true_peak_vehicles['n_online_vehicles']} "
                   f"(at {true_peak_vehicles['window_start']})")
    elif pd.Timestamp(true_peak_vehicles["window_start"]) != claimed_peak_vehicles_start:
        ok = fail(f"peak_n_online_vehicles VALUE matches ({claimed_peak_vehicles}) but the "
                   f"claimed window_start ({claimed_peak_vehicles_start}) differs from where the "
                   f"true max actually occurs ({true_peak_vehicles['window_start']}).")
    else:
        print(f"  OK: peak_n_online_vehicles ({claimed_peak_vehicles} at "
              f"{claimed_peak_vehicles_start}) matches the true max of the raw data exactly.")

    return ok


def main():
    for path in (VEHICLE_WINDOW_PATH, OVERALL_PATH, SUMMARY_PATH):
        if not os.path.exists(path):
            print(f"Missing input file: {path}")
            sys.exit(1)

    vehicle_window_df = pd.read_csv(VEHICLE_WINDOW_PATH, parse_dates=["window_start"])
    overall_df = pd.read_csv(OVERALL_PATH, parse_dates=["window_start"])
    summary_df = pd.read_csv(SUMMARY_PATH, parse_dates=[
        "peak_n_online_vehicles_window_start", "peak_total_storage_bytes_window_start",
    ])

    window_lengths = sorted(summary_df["window_length_minutes"].unique())
    results = {
        wl: verify_window_length(wl, vehicle_window_df, overall_df, summary_df)
        for wl in window_lengths
    }

    print("\n" + "=" * 60)
    all_ok = all(results.values())
    for wl, passed in results.items():
        print(f"  window_length_minutes={wl}: {'PASS' if passed else 'FAIL'}")
    print("=" * 60)
    print("ALL CHECKS PASSED" if all_ok else "AT LEAST ONE CHECK FAILED -- see FAIL lines above")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
