"""
Data Integrity Module

This module provides functions to check for illogical and stuck values in dataframes,
specifically for meteorological data.
"""

import pandas as pd
import numpy as np
from functools import reduce
from . import config
from . import logger_config

# Get a logger for this module
logger = logger_config.get_logger(__name__)


def check_stuck_values(df, base_column, n_intervals=None, exclude_zero=False):
    """
    Detect stuck values where mean, min, max, and stddev remain exactly constant
    for n_intervals.

    Args:
        df: DataFrame containing the data.
        base_column: Base name of the sensor (e.g., 'met_WindSpeedRot').
        n_intervals: Number of consecutive intervals to consider 'stuck'.
        exclude_zero: If True, a stuck value of 0 is not treated as stuck.

    Returns:
        pd.Series: A boolean mask where True indicates a stuck value that should be Nullified.
    """
    n_intervals = n_intervals or config.MET_STUCK_INTERVALS
    stats = ["mean", "min", "max", "stddev"]
    
    # Filter for existing columns only
    cols = [f"{base_column}_{stat}" for stat in stats if f"{base_column}_{stat}" in df.columns]
    
    if not cols:
        return pd.Series(False, index=df.index)

    # Sort to ensure temporal order
    df_sorted = df.sort_values(["StationId", "TimeStamp"])

    # 1. Check if StationId preserved
    same_station = df_sorted["StationId"] == df_sorted["StationId"].shift(1)

    # 2. Check if all value columns are identical to previous row
    # Use reduce to combine boolean series for all columns
    all_vals_same = reduce(
        lambda acc, col: acc & (df_sorted[col] == df_sorted[col].shift(1)),
        cols,
        pd.Series(True, index=df_sorted.index)
    )

    is_same = same_station & all_vals_same

    # Exclude stuck zeros if requested
    if exclude_zero:
        mean_col = f"{base_column}_mean"
        if mean_col in df_sorted.columns:
            # If current value is 0, it doesn't count as a "stuck" event
            is_same &= (df_sorted[mean_col] != 0)

    # Find n_intervals consecutive "is_same" flags.
    # We verify that a run of (n-1) 'is_same' flags exists ending at 'i'.
    # This implies T[i] == T[i-1] == ... == T[i-(n-1)]
    stuck_at_end = is_same.copy()
    # Convert to float for safe shifting (avoids object-dtype warning on fillna)
    is_same_float = is_same.astype(float)
    
    for k in range(1, n_intervals - 1):
        stuck_at_end &= is_same_float.shift(k).fillna(0.0).astype(bool)

    # Backfill the True status to cover the entire stuck sequence
    stuck_mask_sorted = stuck_at_end.copy()
    stuck_at_end_float = stuck_at_end.astype(float)
    
    for k in range(1, n_intervals):
        # Propagate "stuck detected" backwards k steps
        stuck_mask_sorted |= stuck_at_end_float.shift(-k).fillna(0.0).astype(bool)

    # Realign with original index
    return stuck_mask_sorted.reindex(df.index, fill_value=False)


def scan_met_integrity(df, period_start=None, period_end=None):
    """
    Scans the dataframe for range and stuck checks on met data.
    Also checks for completeness per station if period_start and period_end are provided.
    Returns a list of issues found.
    """
    if df.empty:
        return []

    issues = []
    
    # --- Completeness Check ---
    if period_start and period_end and 'StationId' in df.columns:
        # Ensure start/end are datetimes
        if isinstance(period_start, str):
            period_start = pd.to_datetime(period_start)
    # Define checks: (column_base, (min_val, max_val))
    checks = [
        ("met_WindSpeedRot", config.MET_WINDSPEED_RANGE),
        ("met_WinddirectionRot", config.MET_WINDDIRECTION_RANGE),
        ("met_Pressure", config.MET_PRESSURE_RANGE),
        ("met_TemperatureTen", config.MET_TEMPERATURE_RANGE),
    ]

    issues = []
    
    # --- Completeness Check ---
    if period_start and period_end and 'StationId' in df.columns:
        if isinstance(period_start, str):
            period_start = pd.to_datetime(period_start)
        if isinstance(period_end, str):
            period_end = pd.to_datetime(period_end)

        # 0. Global System Connectivity (Row Existence)
        # Checks if any row exists for a timestamp, regardless of sensor data
        global_comp = check_completeness(df, period_start, period_end)
        global_missing_set = set(global_comp['missing_timestamps'])
        
        if global_comp['completeness_percentage'] < 100.0:
             missing_ts_list = global_comp['missing_timestamps']
             truncated_missing = [ts.isoformat() for ts in missing_ts_list[:10]]
             if len(missing_ts_list) > 10:
                 truncated_missing.append(f"... and {len(missing_ts_list) - 10} more")

             issues.append({
                "type": "system_completeness",
                "station_id": "ALL",
                "sensor": "Global Connectivity",
                "count": global_comp['missing_count'],
                "total_expected": global_comp['total_expected'],
                "completeness_pct": global_comp['completeness_percentage'],
                "missing_timestamps": truncated_missing,
                "range_start": period_start.isoformat(),
                "range_end": period_end.isoformat()
             })

        # 1. System-wide Completeness per Sensor (Union of all stations)
        # Checks if *at least one* station has data for each sensor
        # EXCLUDING intervals already covered by Global Connectivity gaps
        for (sensor, _) in checks:
             col_mean = f"{sensor}_mean"
             if col_mean not in df.columns:
                 continue
                 
             # Filter to valid rows for this sensor
             valid_sensor_df = df[df[col_mean].notna()]
             
             system_comp = check_completeness(valid_sensor_df, period_start, period_end)
             sensor_missing_set = set(system_comp['missing_timestamps'])
             
             # Deduplicate: Remove timestamps that are already globally missing
             true_sensor_gaps = sorted(list(sensor_missing_set - global_missing_set))
             
             if len(true_sensor_gaps) > 0:
                 # Recalculate stats based on true gaps
                 missing_count = len(true_sensor_gaps)
                 # Completeness here is a bit tricky to define if we exclude global gaps
                 # We can define it as: (Total Expected - True Gaps) / Total Expected * 100
                 # Or just report it relative to "Connected Time".
                 # For simplicity/consistency, let's keep it relative to Total Expected, 
                 # but specific to this sensor's *additional* failure.
                 
                 comp_pct = round(((system_comp['total_expected'] - missing_count) / system_comp['total_expected']) * 100, 2)

                 truncated_missing = [ts.isoformat() for ts in true_sensor_gaps[:10]]
                 if len(true_sensor_gaps) > 10:
                     truncated_missing.append(f"... and {len(true_sensor_gaps) - 10} more")

                 issues.append({
                    "type": "system_completeness",
                    "station_id": "ALL",
                    "sensor": sensor,
                    "count": missing_count,
                    "total_expected": system_comp['total_expected'],
                    "completeness_pct": comp_pct,
                    "missing_timestamps": truncated_missing,
                    "range_start": period_start.isoformat(),
                    "range_end": period_end.isoformat()
                 })

        # 2. Per-Station Completeness
        for station_id in df['StationId'].unique():
            station_df = df[df['StationId'] == station_id]
            
            # Reuse existing check_completeness
            comp_result = check_completeness(station_df, period_start, period_end)
            
            if comp_result['completeness_percentage'] < 100.0:
                 # Truncate missing timestamps for report
                 missing_ts_list = comp_result['missing_timestamps']
                 truncated_missing = [ts.isoformat() for ts in missing_ts_list[:10]]
                 if len(missing_ts_list) > 10:
                     truncated_missing.append(f"... and {len(missing_ts_list) - 10} more")

                 issues.append({
                    "type": "completeness",
                    "station_id": int(station_id),
                    "count": comp_result['missing_count'],
                    "total_expected": comp_result['total_expected'],
                    "completeness_pct": comp_result['completeness_percentage'],
                    "missing_timestamps": truncated_missing,
                    "range_start": period_start.isoformat(),
                    "range_end": period_end.isoformat()
                 })

            # 3. Per-Station Empty Rows (Present but all sensors NaN)
            # Identify columns to check (mean value of each sensor)
            present_sensor_cols = [f"{s}_mean" for s, _ in checks if f"{s}_mean" in df.columns]
            
            if present_sensor_cols:
                # Check if ALL checks' mean columns are NaN for a row
                empty_mask = station_df[present_sensor_cols].isna().all(axis=1)
                
                if empty_mask.any():
                     empty_rows = station_df[empty_mask]
                     issues.append({
                        "type": "empty_row",
                        "station_id": int(station_id),
                        "sensor": "ALL",
                        "count": len(empty_rows),
                        "range_start": empty_rows['TimeStamp'].min().isoformat() if not empty_rows.empty else None,
                        "range_end": empty_rows['TimeStamp'].max().isoformat() if not empty_rows.empty else None,
                        "indices": empty_rows.index.tolist()
                     })

                # 4. Per-Station Sensor Gaps (Row present, specific sensor NaN, not all NaN)
                # We reuse empty_mask to strictly distinguish from "Empty Row"
                non_empty_rows = station_df[~empty_mask]
                
                if not non_empty_rows.empty:
                    for col in present_sensor_cols:
                        sensor_name = col.replace("_mean", "")
                        # Check where this specific sensor is NaN in otherwise valid rows
                        gap_mask = non_empty_rows[col].isna()
                        
                        if gap_mask.any():
                            gap_rows = non_empty_rows[gap_mask]
                            issues.append({
                                "type": "sensor_gap",
                                "station_id": int(station_id),
                                "sensor": sensor_name,
                                "count": len(gap_rows),
                                "range_start": gap_rows['TimeStamp'].min().isoformat(),
                                "range_end": gap_rows['TimeStamp'].max().isoformat(),
                                "indices": gap_rows.index.tolist()
                            })

    stats_to_check = ["mean", "min", "max"]

    for base_col, (v_min, v_max) in checks:
        # --- Stuck Value Checks ---
        stuck_mask = check_stuck_values(df, base_col)

        if stuck_mask.any():
            stuck_rows = df[stuck_mask]
            
            # Record issue
            # Let's iterate over unique stations in stuck_rows to be more accurate in the report
            for station_id in stuck_rows['StationId'].unique():
                 station_stuck = stuck_rows[stuck_rows['StationId'] == station_id]
                 issues.append({
                    "type": "stuck_value",
                    "station_id": int(station_id),
                    "sensor": base_col,
                    "count": len(station_stuck),
                    "range_start": station_stuck['TimeStamp'].min(),
                    "range_end": station_stuck['TimeStamp'].max(),
                    "sample_value": station_stuck.iloc[0].get(f'{base_col}_mean', 'N/A'),
                    "indices": station_stuck.index.tolist()
                 })

        # --- Range Checks ---
        for stat in stats_to_check:
            col = f"{base_col}_{stat}"
            if col not in df.columns:
                continue

            # Identify values outside [v_min, v_max]
            illogical_mask = df[col].between(v_min, v_max, inclusive="both")
            is_outlier = (df[col].notna()) & (~illogical_mask)

            if is_outlier.any():
                outlier_rows = df[is_outlier]
                
                # Similar grouping for outliers
                for station_id in outlier_rows['StationId'].unique():
                    station_outliers = outlier_rows[outlier_rows['StationId'] == station_id]
                    issues.append({
                        "type": "out_of_range",
                        "station_id": int(station_id),
                        "column": col,
                        "count": len(station_outliers),
                        "range_start": station_outliers['TimeStamp'].min(),
                        "range_end": station_outliers['TimeStamp'].max(),
                        "bounds": (v_min, v_max),
                        "indices": station_outliers.index.tolist()
                    })

    return issues


def check_met_integrity(df):
    """
    Performs range and stuck checks on met data.
    Modified values are logged and replaced with NaN.
    """
    if df.empty:
        return df

    df_clean = df.copy()
    issues = scan_met_integrity(df)
    
    for issue in issues:
        # Log summary (Replicating original logging format roughly)
        if issue["type"] == "stuck_value":
            logger.warning(
                f"STUCK VALUES: Station {issue['station_id']} | Sensor: {issue['sensor']} | "
                f"Count: {issue['count']} | "
                f"Range: {issue['range_start']} to {issue['range_end']} | "
                f"Sample: {issue['sample_value']} | Action: Nullify"
            )
            
            # Nullify all related stat columns
            base_col = issue['sensor']
            for stat in ["mean", "min", "max", "stddev"]:
                col = f"{base_col}_{stat}"
                if col in df_clean.columns:
                    df_clean.loc[issue['indices'], col] = np.nan
                    
        elif issue["type"] == "out_of_range":
             logger.warning(
                f"ILLOGICAL VALUES: Station {issue['station_id']} | Column: {issue['column']} | "
                f"Count: {issue['count']} | "
                f"Range: {issue['range_start']} to {issue['range_end']} | "
                f"Bounds: [{issue['bounds'][0]}, {issue['bounds'][1]}] | Action: Nullify"
            )
             df_clean.loc[issue['indices'], issue['column']] = np.nan

    return df_clean


def check_completeness(df, start_time, end_time, frequency="10min"):
    """
    Check for missing timestamps in the dataframe within the specified range.
    
    Returns:
        dict: Summary of missing values (count, missing_timestamps).
    """
    # Create the full expected range
    full_range = pd.date_range(start=start_time, end=end_time, freq=frequency)
    total_expected = len(full_range)

    if df.empty:
        return {
            "missing_count": total_expected,
            "total_expected": total_expected,
            "missing_timestamps": full_range,
            "completeness_percentage": 0.0
        }

    # Conver time to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df["TimeStamp"]):
        try:
             # operating on a copy to avoid side effects if df is used later
             # though strictly check_completeness shouldn't modify inputs
             timestamps = pd.to_datetime(df["TimeStamp"])
        except Exception:
             return {
                "missing_count": -1,
                "total_expected": -1,
                "missing_timestamps": [],
                "completeness_percentage": 0.0,
                "error": "TimeStamp column invalid"
            }
    else:
        timestamps = df["TimeStamp"]

    # Filter for relevant range efficiently
    relevant_timestamps = timestamps[
        (timestamps >= start_time) & (timestamps <= end_time)
    ]
    
    # Use set difference for speed
    actual_set = set(relevant_timestamps)
    missing = [ts for ts in full_range if ts not in actual_set]
    
    missing_count = len(missing)
    completeness = (1 - (missing_count / total_expected)) * 100 if total_expected > 0 else 0.0
    
    return {
        "missing_count": missing_count,
        "total_expected": total_expected,
        "missing_timestamps": pd.DatetimeIndex(missing),
        "completeness_percentage": round(completeness, 2)
    }