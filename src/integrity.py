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


def check_met_integrity(df):
    """
    Performs range and stuck checks on met data.
    Modified values are logged and replaced with NaN.
    """
    if df.empty:
        return df

    df_clean = df.copy()

    # Define checks: (column_base, (min_val, max_val))
    checks = [
        ("met_WindSpeedRot", config.MET_WINDSPEED_RANGE),
        ("met_WinddirectionRot", config.MET_WINDDIRECTION_RANGE),
        ("met_Pressure", config.MET_PRESSURE_RANGE),
        ("met_TemperatureTen", config.MET_TEMPERATURE_RANGE),
    ]

    stats_to_check = ["mean", "min", "max"]

    for base_col, (v_min, v_max) in checks:
        # --- Stuck Value Checks ---
        stuck_mask = check_stuck_values(df, base_col)

        if stuck_mask.any():
            stuck_rows = df[stuck_mask]
            
            # Log summary
            logger.warning(
                f"STUCK VALUES: Station {stuck_rows['StationId'].iloc[0]} | Sensor: {base_col} | "
                f"Count: {len(stuck_rows)} | "
                f"Range: {stuck_rows['TimeStamp'].min()} to {stuck_rows['TimeStamp'].max()} | "
                f"Sample: {stuck_rows.iloc[0].get(f'{base_col}_mean', 'N/A')} | Action: Nullify"
            )

            # Nullify all related stat columns
            for stat in ["mean", "min", "max", "stddev"]:
                col = f"{base_col}_{stat}"
                if col in df_clean.columns:
                    df_clean.loc[stuck_mask, col] = np.nan

        # --- Range Checks ---
        for stat in stats_to_check:
            col = f"{base_col}_{stat}"
            if col not in df_clean.columns:
                continue

            # Identify values outside [v_min, v_max]
            illogical_mask = df_clean[col].between(v_min, v_max, inclusive="both")
            # We want to finding outliers, so invert (~)
            # Note: between() returns True for valid, False for outliers or NaN. 
            # We must be careful not to flag NaNs as illogical here (they are just missing).
            # So: Invalid if NOT NaN AND NOT BETWEEN.
            is_outlier = (df_clean[col].notna()) & (~illogical_mask)

            if is_outlier.any():
                outlier_rows = df[is_outlier]
                
                logger.warning(
                    f"ILLOGICAL VALUES: Station {outlier_rows['StationId'].iloc[0]} | Column: {col} | "
                    f"Count: {len(outlier_rows)} | "
                    f"Range: {outlier_rows['TimeStamp'].min()} to {outlier_rows['TimeStamp'].max()} | "
                    f"Bounds: [{v_min}, {v_max}] | Action: Nullify"
                )
                
                df_clean.loc[is_outlier, col] = np.nan

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