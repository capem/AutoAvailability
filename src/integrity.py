"""
Data Integrity Module

This module provides functions to check for illogical and stuck values in dataframes,
specifically for meteorological data.
"""

import pandas as pd
import numpy as np
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
    if n_intervals is None:
        n_intervals = config.MET_STUCK_INTERVALS

    stats = ["mean", "min", "max", "stddev"]
    cols = [f"{base_column}_{stat}" for stat in stats]

    # Ensure all columns exist
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return pd.Series(False, index=df.index)

    # Sort by StationId and TimeStamp to ensure correct ordering for shift operations
    # We operate on a sorted copy to avoid modifying the input or relying on input order
    df_sorted = df.sort_values(["StationId", "TimeStamp"])

    # Calculate 'is_same' for all columns combined across the entire sorted dataframe
    # We compare with the previous row.
    # Valid transitions must match values AND have the same StationId.

    # 1. Check if StationId is same as previous
    same_station = df_sorted["StationId"] == df_sorted["StationId"].shift(1)

    # 2. Check if values are same as previous for all stats
    # We use a combined boolean mask
    all_vals_same = pd.Series(True, index=df_sorted.index)

    # Get shifted comparisons for all columns at once (vectorized)
    # Using explicit loop over columns is fine as len(cols) is small (4)
    # but we avoid the O(N) groupby loop.
    for col in cols:
        # Check equality with previous row
        # Handle NaN: if both are NaN, they are "same" in this context?
        # Original logic: NaN == NaN is False (default pandas).
        # If data has NaNs, they shouldn't trigger "stuck" probably.
        all_vals_same &= df_sorted[col] == df_sorted[col].shift(1)

    # A row 'i' is the same as 'i-1' if station is same and values are same
    is_same = same_station & all_vals_same

    # If excluding zero, we mask out rows where the mean value is 0
    # (If current value is 0, it contributes to a "stuck zero" sequence, which we ignore)
    if exclude_zero:
        mean_col = f"{base_column}_mean"
        if mean_col in df_sorted.columns:
            is_nonzero = df_sorted[mean_col] != 0
            # If current is 0, it cannot be a "stuck non-zero" repetition
            is_same &= is_nonzero

    # Find n_intervals consecutive "is_same" flags.
    # For n=3, we need T1, T2, T3 to be same.
    # matches: T2==T1 (is_same[T2]), T3==T2 (is_same[T3]).
    # We need (n_intervals - 1) consecutive True values in is_same.

    stuck_at_end = is_same.copy()
    for k in range(1, n_intervals - 1):
        # We perform bitwise AND with shifted versions
        # Shift(k) puts T at i-k into i.
        # logical_and(i, i-1) -> sequence of 2.
        # This confirms that the chain of 'same' holds for the required length
        # Use float casting to avoid future warning
        stuck_at_end &= is_same.astype(float).shift(k).fillna(0).astype(bool)

    # stuck_at_end marks the END of a stuck sequence.
    # We need to mark the whole sequence (backfill).
    stuck_mask_sorted = stuck_at_end.copy()
    for k in range(1, n_intervals):
        # Shift(-k) moves value from i to i-k (backwards in time / upwards in DF)
        # Because we enforced StationId equality in 'is_same', we won't bleed across stations
        # unless n_intervals > distinct readings in a station, but shift(-k) would just take from next station?
        # WAIT: shift(-k) takes FROM future (next rows).
        # If `stuck_at_end` at index `i` is True, it means `i` is the end of a stuck chain within the SAME station.
        # As established, `i` is at least `start + n-1`.
        # Shifting this True back to `i-k` (where k < n) will land on rows `i-1`...`i-(n-1)`.
        # These rows MUST belong to the same station because `stuck_at_end` implies `n-1` same-station transitions.
        # We use float casting to avoid future warning
        stuck_mask_sorted |= stuck_at_end.astype(float).shift(-k).fillna(0).astype(bool)

    # Realign with original index
    stuck_mask = stuck_mask_sorted.reindex(df.index, fill_value=False)

    return stuck_mask


def check_met_integrity(df):
    """
    Performs range and stuck checks on met data.
    Modified values are logged and replaced with NaN.
    """
    if df.empty:
        return df

    df_clean = df.copy()

    # Range checks
    checks = [
        ("met_WindSpeedRot", config.MET_WINDSPEED_RANGE),
        ("met_WinddirectionRot", config.MET_WINDDIRECTION_RANGE),
        ("met_Pressure", config.MET_PRESSURE_RANGE),
        ("met_TemperatureTen", config.MET_TEMPERATURE_RANGE),
    ]

    stats = ["mean", "min", "max"]  # stddev range check? usually not unless specified.

    for base_col, (v_min, v_max) in checks:
        # User requested to include 0 in stuck checks for wind speed
        stuck_mask = check_stuck_values(df, base_col)

        if stuck_mask.any():
            stuck_rows = df[stuck_mask]
            stuck_count = len(stuck_rows)
            min_time = stuck_rows["TimeStamp"].min()
            max_time = stuck_rows["TimeStamp"].max()

            # Get a sample value (mean of the first stuck row)
            sample_val = stuck_rows.iloc[0].get(f"{base_col}_mean", "N/A")

            logger.warning(
                f"STUCK VALUES DETECTED: Station {stuck_rows['StationId'].iloc[0]} | Sensor: {base_col} | "
                f"Count: {stuck_count} | Range: {min_time} to {max_time} | "
                f"Sample Value: {sample_val} | Replaced with NaN"
            )

            # Apply replacement for all stats of this sensor
            for stat in ["mean", "min", "max", "stddev"]:
                col = f"{base_col}_{stat}"
                if col in df_clean.columns:
                    df_clean.loc[stuck_mask, col] = np.nan

        # Range checks for mean, min, max
        for stat in stats:
            col = f"{base_col}_{stat}"
            if col in df_clean.columns:
                # Mask for illogical values (outside range)
                # We skip NaNs already there
                illogical_mask = (df_clean[col].notna()) & (
                    (df_clean[col] < v_min) | (df_clean[col] > v_max)
                )

                if illogical_mask.any():
                    illogical_rows = df[illogical_mask]
                    illogical_count = len(illogical_rows)
                    min_time = illogical_rows["TimeStamp"].min()
                    max_time = illogical_rows["TimeStamp"].max()

                    logger.warning(
                        f"ILLOGICAL VALUES DETECTED: Station {illogical_rows['StationId'].iloc[0]} | Column: {col} | "
                        f"Count: {illogical_count} | Range: {min_time} to {max_time} | "
                        f"Bounds: [{v_min}, {v_max}] | Replaced with NaN"
                    )
                    df_clean.loc[illogical_mask, col] = np.nan

    return df_clean
