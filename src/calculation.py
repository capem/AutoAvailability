import os
import numpy as np
import pandas as pd
from scipy.interpolate import interp1d

# Import centralized logging and configuration
from . import config
from . import logger_config

# Get a logger for this module
logger = logger_config.get_logger(__name__)

def read_csv_data(data_type, period):
    """
    Read data directly from CSV files in the unified data directory.

    Args:
        data_type: Type of data (met, tur, grd, etc.)
        period: Period in YYYY-MM format

    Returns:
        DataFrame with the query results
    """
    # Get file information
    # Construct path to the CSV file in the new unified directory
    data_type_upper = data_type.upper()
    csv_file = f"./monthly_data/data/{data_type_upper}/{period}-{data_type}.csv"

    # Define dtype specifically for 'met' data
    read_options = {}
    if data_type == 'met':
        read_options['dtype'] = {'StationId': 'Int64'} # Use nullable integer type

    # Read CSV file directly into pandas DataFrame
    try:
        # Check if the CSV file exists before attempting to read
        if not os.path.exists(csv_file):
             raise FileNotFoundError(f"CSV file not found at {csv_file}")
        # Pass dtype option if defined
        df = pd.read_csv(csv_file, **read_options)
        logger.debug(f"[CALC] Read {csv_file} with options: {read_options}. Dtypes: {df.dtypes}")
    except Exception as e:
        raise ValueError(f"Error reading CSV file {csv_file}: {str(e)}")

    # Return the dataframe
    return df


# ============================= ALARM PERIOD CALCULATION FUNCTIONS =============================

def cascade_alarm_times(alarm_df):
    """
    Determine the maximum end time of previous alarms to handle overlapping alarms.

    This function calculates the cumulative maximum of TimeOff values and shifts them
    to create a TimeOffMax column that represents the latest end time of any previous alarm.

    Args:
        alarm_df: DataFrame containing alarm data with TimeOn and TimeOff columns

    Returns:
        DataFrame with added TimeOffMax column
    """
    alarm_df.reset_index(inplace=True, drop=True)
    # Calculate cumulative maximum of TimeOff values (shifted by 1)
    alarm_df["TimeOffMax"] = alarm_df.TimeOff.cummax().shift()

    # Set the first TimeOffMax to the first TimeOn (no previous alarms)
    alarm_df.at[0, "TimeOffMax"] = alarm_df.at[0, "TimeOn"]

    return alarm_df


def apply_cascade_method(alarm_summary_df):
    """
    Apply the cascade method to determine real alarm periods by handling overlapping alarms.

    This function:
    1. Groups alarms by turbine (StationNr)
    2. Applies the cascade_alarm_times function to each group
    3. Calculates the real period of each alarm based on overlapping conditions
    4. Separates periods by error type (Siemens vs Tarec)

    Args:
        alarm_summary_df: DataFrame containing alarm summary data

    Returns:
        DataFrame with calculated real alarm periods
    """
    # Sort by alarm start time and ID
    alarm_summary_df.sort_values(["StationNr", "TimeOn", "ID"], inplace=True)

    # Apply cascade function to each turbine group
    processed_df = alarm_summary_df.groupby("StationNr", group_keys=True).apply(
        cascade_alarm_times, include_groups=False
    )

    # Define masks for different alarm overlap scenarios
    # Root alarms: Start after any previous alarm ends
    is_root_alarm = processed_df.TimeOn.values >= processed_df.TimeOffMax.values

    # Child alarms: Start during a previous alarm but end after it
    is_child_alarm = (
        (processed_df.TimeOn.values < processed_df.TimeOffMax.values) &
        (processed_df.TimeOff.values > processed_df.TimeOffMax.values)
    )

    # Embedded alarms: Completely contained within a previous alarm
    is_embedded_alarm = processed_df.TimeOff.values <= processed_df.TimeOffMax.values

    # Set the new start time based on the overlap scenario
    processed_df.loc[is_root_alarm, "NewTimeOn"] = processed_df.loc[is_root_alarm, "TimeOn"]
    processed_df.loc[is_child_alarm, "NewTimeOn"] = processed_df.loc[is_child_alarm, "TimeOffMax"]
    processed_df.loc[is_embedded_alarm, "NewTimeOn"] = processed_df.loc[is_embedded_alarm, "TimeOff"]

    # Reset index for further processing
    processed_df.reset_index(inplace=True)

    # Calculate the EffectiveAlarmTime of each alarm
    end_times = processed_df.TimeOff
    start_times = processed_df.NewTimeOn
    processed_df["EffectiveAlarmTime"] = abs(end_times - start_times)

    # Separate periods by error type
    mask_siemens_errors = processed_df["Error Type"] == 1
    mask_tarec_errors = processed_df["Error Type"] == 0

    # Assign periods to their respective error type columns
    processed_df["Period Siemens(s)"] = processed_df[mask_siemens_errors].EffectiveAlarmTime
    processed_df["Period Tarec(s)"] = processed_df[mask_tarec_errors].EffectiveAlarmTime

    return processed_df


def convert_to_10min_intervals(alarm_df, alarm_type="1-0"):
    """
    Convert alarm periods to 10-minute intervals for time series analysis.

    This function:
    1. Rounds alarm start and end times to 10-minute intervals
    2. Explodes the data to create a row for each 10-minute interval
    3. Calculates the actual duration of the alarm within each 10-minute interval
    4. Assigns durations to appropriate error type columns

    Args:
        alarm_df: DataFrame containing alarm data with NewTimeOn and TimeOff columns
        alarm_type: Type of alarm data being processed ("1-0" for normal alarms, "2006" for special case)

    Returns:
        DataFrame with alarms broken down into 10-minute intervals
    """
    # Round start and end times to 10-minute intervals
    alarm_df["TimeOnRound"] = alarm_df["NewTimeOn"].dt.ceil("10min")
    alarm_df["TimeOffRound"] = alarm_df["TimeOff"].dt.ceil("10min")

    # Create a row for each 10-minute interval between start and end times
    alarm_df["TimeStamp"] = alarm_df.apply(
        lambda row: pd.date_range(row["TimeOnRound"], row["TimeOffRound"], freq="10min"),
        axis=1,
    )
    # Explode the DataFrame to create a row for each timestamp
    alarm_df = alarm_df.explode("TimeStamp")

    # Initialize period columns for normal alarms
    if alarm_type != "2006":
        alarm_df["EffectiveAlarmTime"] = pd.Timedelta(0)
        alarm_df["Period Siemens(s)"] = pd.Timedelta(0)
        alarm_df["Period Tarec(s)"] = pd.Timedelta(0)

    # Create temporary DataFrames for calculating actual start and end times within each interval
    start_times_df = alarm_df[["TimeStamp", "NewTimeOn"]].copy()
    end_times_df = alarm_df[["TimeStamp", "TimeOff"]].copy()

    # Adjust timestamp for start times to previous interval
    start_times_df["TimeStamp"] = start_times_df["TimeStamp"] - pd.Timedelta(minutes=10)

    # Calculate actual start and end times within each 10-minute interval
    alarm_df["10minTimeOn"] = start_times_df[["TimeStamp", "NewTimeOn"]].max(1).values
    alarm_df["10minTimeOff"] = end_times_df[["TimeStamp", "TimeOff"]].min(1).values

    # Calculate the real period within each 10-minute interval
    alarm_df["EffectiveAlarmTime"] = alarm_df["10minTimeOff"] - alarm_df["10minTimeOn"]

    # Assign periods to their respective error type columns for normal alarms
    if alarm_type != "2006":
        mask_siemens_errors = alarm_df["Error Type"] == 1
        mask_tarec_errors = alarm_df["Error Type"] == 0
        alarm_df.loc[mask_siemens_errors, "Period Siemens(s)"] = alarm_df.loc[mask_siemens_errors, "EffectiveAlarmTime"]
        alarm_df.loc[mask_tarec_errors, "Period Tarec(s)"] = alarm_df.loc[mask_tarec_errors, "EffectiveAlarmTime"]

    return alarm_df


def handle_alarm_code_1005_overlap(alarm_df):
    """
    Handle special case of alarm code 1005 overlapping with other alarms.
    This function:
    1. Removes zero-duration alarms (except code 1005)
    2. Adjusts alarm periods to account for overlaps with alarm code 1005
    3. Handles multiple 1005 alarms overlapping with the same alarm
    4. Reapplies the cascade method to recalculate real periods
    Args:
        alarm_df: DataFrame containing alarm data with processed periods
    Returns:
        DataFrame with adjusted alarm periods accounting for code 1005 overlaps
    """
    # Select only necessary columns to avoid modifying others unintentionally
    # Added a check for column existence for robustness
    required_cols = [
        "TimeOn", "TimeOff", "StationNr", "Alarmcode", "Parameter", "ID",
        "NewTimeOn", "OldTimeOn", "OldTimeOff", "UK Text", "Error Type",
        "EffectiveAlarmTime"
    ]
    # Ensure all required columns are present, add if missing
    for col in required_cols:
        if col not in alarm_df.columns:
            # Add a sensible default if a column is missing
            alarm_df[col] = pd.NaT if 'Time' in col else None
            
    alarm_df = alarm_df[required_cols].copy()

    # Remove zero-duration alarms (except code 1005)
    is_zero_duration = (alarm_df['EffectiveAlarmTime'] <= pd.Timedelta(0)) & (alarm_df['Alarmcode'] != 1005)
    alarm_df = alarm_df[~is_zero_duration].copy()

    # Reset index after dropping rows
    alarm_df.reset_index(drop=True, inplace=True)

    # Set TimeOn to NewTimeOn for all alarms as the starting point for processing
    alarm_df["TimeOn"] = alarm_df["NewTimeOn"]

    # Restore original start times for code 1005 alarms for accurate cascading
    mask_1005 = alarm_df["Alarmcode"] == 1005
    alarm_df.loc[mask_1005, "TimeOn"] = alarm_df.loc[mask_1005, "OldTimeOn"]

    # Isolate all 1005 alarms for efficient lookup later.
    # Sorting is crucial for the chronological processing logic within the loop.
    alarms_code_1005 = alarm_df.query("Alarmcode == 1005").sort_values(
        ["StationNr", "TimeOn", "TimeOff"]
    ).reset_index(drop=True)

    # Get a unique list of stations that have at least one 1005 alarm.
    # We only need to process these stations.
    stations_with_1005 = alarms_code_1005["StationNr"].unique()

    # --- 2. CORE PROCESSING LOGIC ---

    # Initialize lists to store modifications. This is more efficient than
    # modifying the DataFrame row-by-row inside the loop.
    all_new_alarms = []
    indices_to_remove = set()

    # Process each station independently to avoid incorrect interactions.
    for station_nr in stations_with_1005:
        # Get all 1005 alarms for this station.
        station_1005_alarms = alarms_code_1005[
            alarms_code_1005["StationNr"] == station_nr
        ]

        # Get the indices of all non-1005 alarms for this station.
        station_alarms_indices = alarm_df[
            (alarm_df["StationNr"] == station_nr) &
            (alarm_df["Alarmcode"] != 1005)
        ].index

        # Iterate through each non-1005 alarm to check for overlaps.
        for idx in station_alarms_indices:
            alarm = alarm_df.loc[idx]
            alarm_on = alarm["TimeOn"]
            alarm_off = alarm["TimeOff"]

            # Find all 1005 alarms that overlap with the current alarm's time range.
            # The condition `(startB < endA) & (endB > startA)` is the standard
            # and robust way to check for any temporal interval overlap.
            overlapping_1005 = station_1005_alarms[
                (station_1005_alarms["TimeOn"] < alarm_off) &
                (station_1005_alarms["TimeOff"] > alarm_on)
            ]

            # If there are no overlaps, we can skip to the next alarm.
            if overlapping_1005.empty:
                continue

            # --- 2a. Segment Creation Logic ---
            segments = []
            # Initialize a 'cursor' to track the start of the next potential valid segment.
            # It starts at the beginning of the current alarm.
            cursor = alarm_on

            # Chronologically process each overlapping 1005 alarm.
            for _, alarm_1005 in overlapping_1005.iterrows():
                t1005_on = alarm_1005["TimeOn"]
                t1005_off = alarm_1005["TimeOff"]

                # If there's a gap between the cursor and the start of this 1005 alarm,
                # that gap represents a valid, uninterrupted piece of the original alarm.
                if cursor < t1005_on:
                    segment_end = min(t1005_on, alarm_off)
                    segments.append((cursor, segment_end))

                # CRITICAL: Advance the cursor to the end of the current 1005 alarm.
                # Using `max` ensures that if multiple 1005s overlap *each other*,
                # the cursor correctly jumps to the end of the combined blackout period.
                cursor = max(cursor, t1005_off)

            # After checking all 1005s, if the cursor hasn't reached the end of the
            # original alarm, the remaining time is the final valid segment.
            if cursor < alarm_off:
                segments.append((cursor, alarm_off))

            # Clean up any zero-duration segments that might have been created.
            segments = [(s, e) for s, e in segments if s < e]

            # --- 2b. Applying the Results ---
            if not segments:
                # Case A: Alarm is completely covered by 1005s. Mark for removal.
                indices_to_remove.add(idx)
            elif len(segments) == 1:
                # Case B: Alarm is trimmed. Update the original alarm in place.
                alarm_df.loc[idx, "TimeOn"] = segments[0][0]
                alarm_df.loc[idx, "TimeOff"] = segments[0][1]
            else:
                # Case C: Alarm is split. Update the original alarm to be the first
                # segment, and create new alarm records for the subsequent segments.
                alarm_df.loc[idx, "TimeOn"] = segments[0][0]
                alarm_df.loc[idx, "TimeOff"] = segments[0][1]

                for seg_start, seg_end in segments[1:]:
                    new_alarm = alarm.copy()
                    new_alarm["TimeOn"] = seg_start
                    new_alarm["TimeOff"] = seg_end
                    all_new_alarms.append(new_alarm)

    # --- 3. FINAL DATAFRAME RECONSTRUCTION ---

    # Perform batch modifications for performance.
    if indices_to_remove:
        alarm_df.drop(index=list(indices_to_remove), inplace=True)

    if all_new_alarms:
        new_alarms_df = pd.DataFrame(all_new_alarms)
        alarm_df = pd.concat([alarm_df, new_alarms_df], ignore_index=True)


    # Reapply cascade method to recalculate real periods and re-sort the dataframe
    alarm_df = apply_cascade_method(alarm_df)

    return alarm_df


# ============================= DATA PROCESSING UTILITY FUNCTIONS =============================

def expand_to_full_time_range(df, time_range, station_ids):
    """
    Expand a DataFrame to include all timestamps in the specified time range for all specified stations.
    
    This function creates a cartesian product of stations and time (skeleton) and merges the 
    original data onto it. This ensures that every station-timestamp combination exists in 
    the result, filling missing data with NaN.

    Args:
        df: DataFrame to expand. Must contain "StationNr" and "TimeStamp" columns.
        time_range: Full range of timestamps to include.
        station_ids: Array/list of station IDs to include.

    Returns:
        DataFrame expanded to include all timestamps for all stations.
    """
    # Create the full skeleton of (StationNr, TimeStamp)
    # Note: We assume the station column is named 'StationNr' to match the alarm data
    full_idx = pd.MultiIndex.from_product([station_ids, time_range], names=["StationNr", "TimeStamp"])
    
    # Create skeleton DataFrame and reset index to get columns
    skeleton = pd.DataFrame(index=full_idx).reset_index()
    
    # Merge original data onto the skeleton (Left Join)
    return pd.merge(skeleton, df, on=["StationNr", "TimeStamp"], how="left")


def calculate_correction_factor(turbine_count, total_turbines=131, availability_loss=0.08):
    """
    Calculate the correction factor for energy production based on available turbines.

    This function implements the correction factor formula to account for wake effects
    when fewer turbines are operational.

    Args:
        turbine_count: Number of operational turbines
        total_turbines: Total number of turbines in the wind farm (default: 131)
        availability_loss: Total availability loss factor (default: 0.08)

    Returns:
        Correction factor value
    """
    # Inner function to calculate availability loss for a specific number of turbines
    def availability_loss_for_count(count):
        return availability_loss * (count - 1) / (total_turbines - 1)

    # Calculate and return the correction factor
    return (1 - availability_loss) / (1 - availability_loss_for_count(turbine_count))


def apply_energy_correction_factor(energy_values):
    """
    Apply correction factor to energy production values based on number of operational turbines.

    This function:
    1. Calculates the mean energy value
    2. Applies the correction factor based on the number of values (turbines)
    3. Rounds the result to 2 decimal places

    Args:
        energy_values: Series of energy production values

    Returns:
        Corrected energy production value
    """
    # Get the number of operational turbines
    turbine_count = len(energy_values)

    # Calculate the mean energy value
    mean_energy = energy_values.mean()

    # Apply correction factor and round to 2 decimal places
    corrected_energy = round(mean_energy * calculate_correction_factor(turbine_count), 2)

    return corrected_energy


def get_correction_factor_value(values):
    """
    Get the correction factor value based on the number of values.

    Args:
        values: Series of values

    Returns:
        Correction factor value
    """
    # Get the number of values (turbines)
    turbine_count = len(values)

    # Return the correction factor
    return calculate_correction_factor(turbine_count)


# ============================= POTENTIAL ENERGY CALCULATION FUNCTIONS =============================

def calculate_potential_energy_from_turbine(df):
    """
    Calculate potential energy production using turbine wind speed data (Case 2).

    This function:
    1. Loads the power curve from CB2.xlsx
    2. Creates an interpolation function from the power curve
    3. Uses turbine wind speed data (wtc_AcWindSp_mean)
    4. Applies the power curve to estimate potential energy production
    5. Returns NaN for rows where no wind data is available

    Args:
        df: DataFrame containing turbine data

    Returns:
        Series of potential energy values (with NaN for missing wind data)
    """
    # Load power curve data
    power_curve = pd.read_excel("./config/CB2.xlsx")
    power_curve = power_curve.astype(int).drop_duplicates()

    # Create interpolation function from power curve
    power_curve_interpolator = interp1d(
        power_curve.Wind,
        power_curve.Power,
        kind="linear",
        fill_value="extrapolate"
    )

    # Log debug information
    logger.debug(f"[CALC] calculate_potential_energy_from_turbine called with DataFrame of shape {df.shape}")
    logger.debug(f"[CALC] Available columns in DataFrame: {list(df.columns)}")

    # Define the turbine wind speed column to use
    turbine_wind_column = "wtc_AcWindSp_mean"
    logger.debug(f"[CALC] Looking for turbine wind speed column: {turbine_wind_column}")

    # Check if turbine wind speed column exists in the DataFrame
    if turbine_wind_column in df.columns:
        # Use turbine wind speed directly
        wind_speed = df[turbine_wind_column]

        # Create mask for missing wind data
        missing_wind_mask = wind_speed.isna()
        
        # Apply power curve and divide by 6 (10-minute intervals per hour)
        potential_energy = power_curve_interpolator(wind_speed) / 6
        
        # Set NaN for rows where wind data is missing
        potential_energy[missing_wind_mask] = np.nan
        
        return potential_energy
    else:
        raise ValueError(f"Turbine wind speed column '{turbine_wind_column}' not present in the DataFrame.")


def calculate_potential_energy_from_statistics(period):
    """
    Calculate potential energy production using statistical wind distribution data (Case 3).

    This function:
    1. Loads normalized wind distribution (NWD), seasonal wind factors (SWF), and power curve (CB2)
    2. Calculates normalized annual energy (NAE) using the wind distribution and power curve
    3. Adjusts NAE for the specific period using seasonal factors

    Args:
        period: Period in YYYY-MM format

    Returns:
        Potential energy value for the period
    """
    # Load required configuration files
    normalized_wind_dist = pd.read_excel("./config/NWD.xlsx", index_col=0)
    seasonal_wind_factors = pd.read_excel("./config/SWF.xlsx", index_col=0)
    power_curve = pd.read_excel("./config/CB2.xlsx")

    # Define constants
    park_wind_efficiency = 0.92  # PWE
    normalized_annual_energy = 0  # NAE

    # Process power curve data
    power_curve = power_curve.astype(int).drop_duplicates()
    power_curve_interpolator = interp1d(
        power_curve.Wind,
        power_curve.Power,
        kind="linear",
        fill_value="extrapolate"
    )

    # Calculate normalized annual energy using wind distribution
    wind_speed_bins = np.arange(1, 26, 1)
    for wind_speed in wind_speed_bins:
        normalized_annual_energy += (
            power_curve_interpolator(wind_speed) *
            normalized_wind_dist.loc[wind_speed].values[0]
        )

    # Apply park wind efficiency
    normalized_annual_energy *= park_wind_efficiency

    # Calculate potential energy for the specific period
    # Divide by 8760 (hours in year), by 6 (10-min intervals per hour), and multiply by seasonal factor
    potential_energy = (
        normalized_annual_energy *
        (1 / 8760) *
        (1 / 6) *
        seasonal_wind_factors.loc[int(period.split('-')[1])].values[0]
    )

    return potential_energy


# ============================= DATA LOADING FUNCTIONS =============================

class DataLoader:
    """
    Class containing methods to load and preprocess different types of data files.

    This class provides static methods to load:
    - Grid data (grd): Power output data
    - Counter data (cnt): Energy production counters
    - Summary data (sum): Alarm events
    - Turbine data (tur): Turbine operational data
    - Meteorological data (met): Wind speed and direction from met masts
    - Digital input data (din): Curtailment and other digital signals
    """

    # ------------------------------Grid Data-------------------------------------
    @staticmethod
    def load_grid_data(period):
        """
        Load grid data (power output) for the specified period.

        Args:
            period: Period in YYYY-MM format

        Returns:
            DataFrame containing grid data with parsed timestamps
        """
        # Load grid data from CSV
        grid_data = read_csv_data(data_type="grd", period=period)

        # Convert TimeStamp column to datetime
        grid_data["TimeStamp"] = pd.to_datetime(grid_data["TimeStamp"])

        return grid_data

    # ------------------------------Counter Data-------------------------------------
    @staticmethod
    def load_counter_data(period):
        """
        Load counter data (energy production) for the specified period.

        Args:
            period: Period in YYYY-MM format

        Returns:
            DataFrame containing counter data with parsed timestamps
        """
        # Load counter data from CSV
        counter_data = read_csv_data(data_type="cnt", period=period)

        # Convert TimeStamp column to datetime
        counter_data["TimeStamp"] = pd.to_datetime(counter_data["TimeStamp"])

        return counter_data

    # -----------------------------Alarm Summary Data---------------------------
    @staticmethod
    def load_alarm_data(period):
        """
        Load alarm summary data for the specified period.

        This function:
        1. Loads alarm data from CSV
        2. Cleans and preprocesses the data
        3. Filters to include only relevant turbines

        Args:
            period: Period in YYYY-MM format

        Returns:
            DataFrame containing processed alarm data
        """
        # Load alarm data from CSV
        alarm_data = read_csv_data("sum", period)

        # Remove rows with missing alarm codes
        alarm_data.dropna(subset=["Alarmcode"], inplace=True)

        # Convert time columns to datetime
        alarm_data["TimeOn"] = pd.to_datetime(alarm_data["TimeOn"], format="%Y-%m-%d %H:%M:%S.%f")
        alarm_data["TimeOff"] = pd.to_datetime(alarm_data["TimeOff"], format="%Y-%m-%d %H:%M:%S.%f")

        # Filter to include only relevant turbines (station numbers)
        alarm_data = alarm_data[alarm_data.StationNr >= 2307405]
        alarm_data = alarm_data[alarm_data.StationNr <= 2307535].reset_index(drop=True)

        # Reset index and convert data types
        alarm_data.reset_index(drop=True, inplace=True)
        alarm_data["Alarmcode"] = alarm_data.Alarmcode.astype(int)

        # Remove spaces from Parameter column
        alarm_data["Parameter"] = alarm_data.Parameter.str.replace(" ", "")

        return alarm_data

    # ------------------------------Turbine Data---------------------------
    @staticmethod
    def load_turbine_data(period):
        """
        Load turbine operational data for the specified period.

        Args:
            period: Period in YYYY-MM format

        Returns:
            DataFrame containing turbine data with parsed timestamps
        """
        # Load turbine data from CSV
        turbine_data = read_csv_data("tur", period)

        # Convert TimeStamp column to datetime
        turbine_data["TimeStamp"] = pd.to_datetime(turbine_data["TimeStamp"])

        return turbine_data

    # ------------------------------Meteorological Data---------------------------
    @staticmethod
    def load_met_data(period):
        """
        Load meteorological data for the specified period.

        This function:
        1. Loads met data from CSV
        2. Pivots the data to create columns for each station and measurement
        3. Flattens the multi-level column names

        Args:
            period: Period in YYYY-MM format

        Returns:
            DataFrame containing processed meteorological data
        """
        # Load meteorological data from CSV
        met_data = read_csv_data("met", period)

        # Convert TimeStamp column to datetime
        met_data["TimeStamp"] = pd.to_datetime(met_data["TimeStamp"])

        # Log debug information before pivoting
        logger.debug(f"[CALC] Pre-pivot columns: {list(met_data.columns)}")
        logger.debug(f"[CALC] Pre-pivot shape: {met_data.shape}")

        # Pivot the data to create columns for each station and measurement
        met_data = met_data.pivot_table(
            index="TimeStamp",
            columns="StationId",
            values=["met_WindSpeedRot_mean", "met_WinddirectionRot_mean"],
            aggfunc="mean",
        )

        # Convert multi-level column index to flat index
        met_data.columns = met_data.columns.to_flat_index()

        # Reset index to make TimeStamp a column again
        met_data.reset_index(inplace=True)

        # Create readable column names by joining the multi-level names
        met_data.columns = ["_".join(str(v) for v in tup) if type(tup) is tuple else tup for tup in met_data.columns]

        # Log debug information after pivoting
        logger.debug(f"[CALC] Post-pivot columns: {list(met_data.columns)}")
        logger.debug(f"[CALC] Post-pivot shape: {met_data.shape}")

        return met_data

    # ------------------------------Digital Input Data---------------------------
    @staticmethod
    def load_digital_input_data(period):
        """
        Load digital input data (curtailment signals) for the specified period.

        Args:
            period: Period in YYYY-MM format

        Returns:
            DataFrame containing digital input data with parsed timestamps
        """
        # Load digital input data from CSV
        digital_input_data = read_csv_data("din", period)

        # Convert TimeStamp column to datetime
        digital_input_data["TimeStamp"] = pd.to_datetime(digital_input_data["TimeStamp"])

        return digital_input_data

    # ------------------------------Load All Data---------------------------
    @staticmethod
    def load_all_data(period):
        """
        Load all data types for the specified period.

        Args:
            period: Period in YYYY-MM format

        Returns:
            Tuple of DataFrames (met_data, turbine_data, alarm_data, counter_data, grid_data, digital_input_data)
        """
        return (
            DataLoader.load_met_data(period),
            DataLoader.load_turbine_data(period),
            DataLoader.load_alarm_data(period),
            DataLoader.load_counter_data(period),
            DataLoader.load_grid_data(period),
            DataLoader.load_digital_input_data(period),
        )


# ============================= MAIN CALCULATION FUNCTION =============================

def full_calculation(period):
    """
    Perform the full availability calculation for the specified period.

    This function:
    1. Loads all required data for the period
    2. Performs data validation and cleaning
    3. Processes alarms and calculates real periods
    4. Calculates energy loss by category
    5. Returns a DataFrame with detailed availability results

    Potential Energy (Epot) Calculation Cascade:
    Case 1: AverageWithWakeLossAdjustments - For non-operational turbines, uses average energy from operational turbines 
            at the same timestamp with wake loss correction factor applied
    Case 2: Anemometer - Uses turbine-specific wind speed data with power curve interpolation
    Case 3: SWE (Statistical Wind Estimation) - Uses statistical wind distribution with seasonal factors
    Special Case: Epot=EnergyProduced - For operational turbines, actual energy produced is used as potential

    Args:
        period: Period in YYYY-MM format

    Returns:
        DataFrame containing detailed availability results
    """
    # Load all data files for the period
    met_data, turbine_data, alarm_data, counter_data, grid_data, digital_input_data = DataLoader.load_all_data(period)

    # ------------------------------------------------------------
    # Define period start and end times

    # Set period start to first day of month at midnight
    period_start = pd.Timestamp(f"{period}-01 00:00:00.000")

    # Set period end to the latest timestamp in the counter data
    period_end = counter_data.TimeStamp.max()

    # Create a full time range with 10-minute intervals for the entire period
    full_time_range = pd.date_range(period_start, period_end, freq="10min")

    # ----------------------Sanity check---------------------------
    # Filter out invalid data points using sanity checks

    # Check grid data for valid power values
    valid_grid_indices = grid_data.query(
        """-1000 <= wtc_ActPower_min <= 2600 & -1000 <= wtc_ActPower_max <= 2600 & -1000 <= wtc_ActPower_mean <= 2600"""
    ).index

    # Check counter data for valid energy accumulation values
    valid_counter_indices = counter_data.query(
        """-500 <= wtc_kWG1Tot_accum <= 500 & 0 <= wtc_kWG1TotE_accum <= 500"""
    ).index

    # Check turbine data for valid wind speed and direction values
    valid_turbine_indices = turbine_data.query(
        """0 <= wtc_AcWindSp_mean <= 50 & 0 <= wtc_ActualWindDirection_mean <= 360"""
    ).index

    # Check digital input data for valid curtailment values
    valid_digital_input_indices = digital_input_data.query(
        """0 <= wtc_PowerRed_timeon <= 600"""
    ).index

    # Note: Commented out code for saving outliers to Excel files
    # This could be uncommented if outlier analysis is needed

    # Apply the filters to keep only valid data
    grid_data = grid_data.loc[grid_data.index.isin(valid_grid_indices)]
    counter_data = counter_data.loc[counter_data.index.isin(valid_counter_indices)]
    turbine_data = turbine_data.loc[turbine_data.index.isin(valid_turbine_indices)]
    digital_input_data = digital_input_data.loc[digital_input_data.index.isin(valid_digital_input_indices)]

    # Create a full time range with 10-minute intervals for the entire period
    full_time_range = pd.date_range(period_start, period_end, freq="10min")

    # --------------------------Load error list-------------------------
    # Load the error list from Excel file
    error_list = pd.read_excel(config.ALARMS_FILE_PATH)

    # Convert Number column to integer and ensure no duplicates
    error_list.Number = error_list.Number.astype(int)
    error_list.drop_duplicates(subset=["Number"], inplace=True)

    # Rename Number column to match Alarmcode in alarm data
    error_list.rename(columns={"Number": "Alarmcode"}, inplace=True)

    # ------------------------------------------------------
    # Get alarm codes for error types 0 and 1 (Tarec and Siemens)
    alarm_codes_0_1 = error_list.loc[error_list["Error Type"].isin([1, 0])].Alarmcode

    # ------------------------------Process alarm data-------------------------------------

    # Store original TimeOn and TimeOff values
    alarm_data["OldTimeOn"] = alarm_data["TimeOn"]
    alarm_data["OldTimeOff"] = alarm_data["TimeOff"]

    # Log information about missing TimeOff values
    logger.info(f"[CALC] TimeOff NAs = {alarm_data.loc[alarm_data.Alarmcode.isin(alarm_codes_0_1)].TimeOff.isna().sum()}")

    if alarm_data.loc[alarm_data.Alarmcode.isin(alarm_codes_0_1)].TimeOff.isna().sum():
        logger.info(
            f"[CALC] Earliest TimeOn when TimeOff is NA = {alarm_data.loc[alarm_data.Alarmcode.isin(alarm_codes_0_1) & alarm_data.TimeOff.isna()].TimeOn.min()}"
        )

    # Fill missing TimeOff values with period end
    alarm_data.loc[alarm_data.Alarmcode.isin(alarm_codes_0_1), "TimeOff"] = alarm_data.loc[
        alarm_data.Alarmcode.isin(alarm_codes_0_1), "TimeOff"
    ].fillna(period_end)

    # ------------------------------Limit alarms to period boundaries----------------------

    # Cap alarms ending after period end
    alarm_data.loc[(alarm_data.TimeOff > period_end), "TimeOff"] = period_end

    # ------------------------------Keep only alarms active in period-------------
    alarm_data.reset_index(inplace=True, drop=True)

    # Drop alarms that ended before period start
    alarm_data.drop(
        alarm_data.query("(TimeOn < @period_start) & (TimeOff < @period_start) & Alarmcode.isin(@alarm_codes_0_1)").index,
        inplace=True,
    )

    # Drop alarms that start after period end
    alarm_data.drop(alarm_data.query("(TimeOn > @period_end)").index, inplace=True)
    alarm_data.reset_index(drop=True, inplace=True)

    # ------------------------------Handle alarms starting before period start-----------------
    warning_date = alarm_data.TimeOn.min()

    # Set start time to period start for alarms that started before
    alarm_data.loc[(alarm_data.TimeOn < period_start) & (alarm_data.Alarmcode.isin(alarm_codes_0_1)), "TimeOn"] = period_start

    # Drop non-error alarms that started before period start
    alarm_data.drop(
        alarm_data.query("~Alarmcode.isin(@alarm_codes_0_1) & (TimeOn < @period_start)").index,
        inplace=True,
    )
    alarm_data.reset_index(drop=True, inplace=True)

    # ------------------------------Match alarms with error types------------------------------
    # Merge alarm data with error list to get error types
    alarm_summary = pd.merge(alarm_data, error_list, on="Alarmcode", how="inner", sort=False)

    # Keep only alarms with error types 0 and 1
    alarm_summary = alarm_summary.loc[alarm_summary["Error Type"].isin([1, 0])]

    # ------------------------------Calculate real alarm periods------------------------------
    # Apply cascade method to handle overlapping alarms
    processed_alarms = apply_cascade_method(alarm_summary)
    processed_alarms = handle_alarm_code_1005_overlap(processed_alarms)

    # -------------------Process code 2006(DG:Local power limit - OEM) warnings (special case)-----------------------------
    logger.info("[CALC] Processing code 2006(DG:Local power limit - OEM) warnings")

    # Extract code 2006 alarms
    alarms_code_2006 = alarm_data.loc[(alarm_data["Alarmcode"] == 2006)].copy()
    alarms_code_2006["NewTimeOn"] = alarms_code_2006["TimeOn"]

    # Filter to include only alarms active during the period
    alarms_code_2006 = alarms_code_2006.query(
        "(@period_start < TimeOn < @period_end) | \
        (@period_start < TimeOff < @period_end) | \
        ((TimeOn < @period_start) & (@period_end < TimeOff))"
    )

    # Process code 2006 warnings if any exist
    if not alarms_code_2006.empty:
        # Fill missing TimeOff values
        alarms_code_2006["TimeOff"] = alarms_code_2006["TimeOff"].fillna(period_end)

        # Convert to 10-minute intervals
        alarms_code_2006_intervals = convert_to_10min_intervals(alarms_code_2006, "2006")

        # Aggregate by timestamp
        alarms_code_2006_intervals = alarms_code_2006_intervals.groupby("TimeStamp", group_keys=True).agg(
            {"EffectiveAlarmTime": "sum", "StationNr": "first"}
        )

        alarms_code_2006_intervals.reset_index(inplace=True)

        # Rename columns for clarity
        alarms_code_2006_intervals.rename(
            columns={"StationNr": "StationId", "EffectiveAlarmTime": "Duration 2006(s)"},
            inplace=True,
        )

        # Convert timedelta to seconds
        alarms_code_2006_intervals["Duration 2006(s)"] = alarms_code_2006_intervals["Duration 2006(s)"].dt.total_seconds().fillna(0)

    else:
        logger.info("[CALC] No code 2006 warnings found")
        alarms_code_2006_intervals = pd.DataFrame(columns=["TimeStamp", "Duration 2006(s)", "StationId"])

    # ----------------------- Convert other alarms to 10-minute intervals --------------------------------------
    logger.info("[CALC] Converting alarms to 10-minute intervals")

    # Filter out zero-duration alarms
    non_zero_alarms = processed_alarms.loc[(processed_alarms["EffectiveAlarmTime"].dt.total_seconds() != 0)].copy()

    # Convert to 10-minute intervals
    alarm_intervals = convert_to_10min_intervals(non_zero_alarms)
    alarm_intervals.reset_index(inplace=True, drop=True)

    # ----------------------- Aggregate alarms by station and timestamp ---------------------------------
    # Group by station and timestamp, summing durations
    aggregated_alarms = (
        alarm_intervals.groupby(["StationNr", "TimeStamp"], group_keys=True)
        .agg(
            {
                "EffectiveAlarmTime": "sum",
                "Period Tarec(s)": "sum",
                "Period Siemens(s)": "sum",
                "UK Text": "|".join,
            }
        )
        .reset_index()
    )

    # Expand to include all timestamps in the full time range
    # Ensure all stations are represented, even if they have no alarms/data
    # We use the fixed range of all stations in the park to capture every turbine
    # This range matches the filter in load_alarm_data (2307405 to 2307535)
    all_stations = np.arange(2307405, 2307536)

    # Use the helper function to expand to the full grid (Stations x Time)
    # This ensures even missing stations are present in the final DataFrame
    aggregated_alarms = expand_to_full_time_range(aggregated_alarms, full_time_range, station_ids=all_stations)
    
    # Rename columns to match expected output
    aggregated_alarms.rename(
        columns={
            "StationNr": "StationId",
            "Period Tarec(s)": "Period 0(s)",
            "Period Siemens(s)": "Period 1(s)",
        },
        inplace=True
    )

    # Convert timedeltas to seconds and fill NAs with 0
    aggregated_alarms["Period 0(s)"] = aggregated_alarms["Period 0(s)"].dt.total_seconds().fillna(0)
    aggregated_alarms["Period 1(s)"] = aggregated_alarms["Period 1(s)"].dt.total_seconds().fillna(0)
    aggregated_alarms["EffectiveAlarmTime"] = aggregated_alarms["EffectiveAlarmTime"].dt.total_seconds().fillna(0)


    logger.info("[CALC] Alarm aggregation completed")

    # ----------Merge with code 2006 warnings----------
    aggregated_alarms = pd.merge(
        aggregated_alarms, alarms_code_2006_intervals, on=["TimeStamp", "StationId"], how="left"
    ).reset_index(drop=True)

    # -------Merge with other data sources------
    # Merge with counter data (energy production)
    logger.info("[CALC] Merging alarm data with energy production data")
    results = pd.merge(aggregated_alarms, counter_data, on=["TimeStamp", "StationId"], how="left").reset_index(drop=True)

    # Merge with grid data (power output)
    results = pd.merge(results, grid_data, on=["TimeStamp", "StationId"], how="left")
    results.reset_index(drop=True, inplace=True)

    # Merge with turbine data (wind speed and direction)
    results = pd.merge(
        results,
        turbine_data[
            [
                "TimeStamp",
                "StationId",
                "wtc_AcWindSp_mean",
                "wtc_ActualWindDirection_mean",
            ]
        ],
        on=("TimeStamp", "StationId"),
        how="left",
    )

    # Merge with meteorological data
    logger.debug(f"[CALC] Pre-MET merge 'results' shape: {results.shape}")
    results = pd.merge(results, met_data, on="TimeStamp", how="left")
    logger.debug(f"[CALC] Post-MET merge 'results' columns: {list(results.columns)}")
    logger.debug(f"[CALC] Post-MET merge 'results' shape: {results.shape}")

    # Merge with digital input data (curtailment)
    results = pd.merge(results, digital_input_data, on=["StationId", "TimeStamp"], how="left")
    results = results.infer_objects()
    results["Duration 2006(s)"] = results["Duration 2006(s)"].fillna(0)

    # -------- Identify operational turbines --------------------------------------
    # Create a mask for operational turbines (no alarms, producing energy)
    operational_turbines_mask = (
        (results["wtc_kWG1TotE_accum"] > 0)                # Producing energy
        & (results["EffectiveAlarmTime"] == 0)                     # No active alarms
        & (results["wtc_ActPower_min"] > 0)                # Minimum power > 0
        & (results["Duration 2006(s)"] == 0)               # No code 2006 warnings
        & (                                                # Not curtailed or high power despite curtailment
            (results["wtc_PowerRed_timeon"] == 0)
            | ((results["wtc_PowerRed_timeon"] != 0) & (results["wtc_ActPower_max"] > 2200))
        )
    )

    # -------- Calculate potential energy from operational turbines -------------------------------------------
    # Extract data for operational turbines
    operational_turbines = results.loc[operational_turbines_mask].copy()

    # Add columns for correction factor calculation
    operational_turbines["Correction Factor"] = 0
    operational_turbines["Available Turbines"] = 0

    # Calculate potential energy using operational turbines
    potential_energy = (
        operational_turbines.groupby("TimeStamp", group_keys=True)
        .agg(
            {
                "wtc_kWG1TotE_accum": apply_energy_correction_factor,     # Apply correction factor to energy
                "Correction Factor": get_correction_factor_value,          # Get correction factor
                "Available Turbines": "count",                             # Count available turbines
            }
        )
        .copy()
    )

    # Rename column for clarity
    potential_energy = potential_energy.rename(columns={"wtc_kWG1TotE_accum": "Epot"})

    # Remove temporary columns from operational turbines
    del operational_turbines["Correction Factor"]
    del operational_turbines["Available Turbines"]

    # Merge potential energy back to operational turbines
    operational_turbines = pd.merge(operational_turbines, potential_energy, on="TimeStamp", how="left")

    # For operational turbines, potential energy equals actual energy
    operational_turbines["Epot"] = operational_turbines["wtc_kWG1TotE_accum"]
    
    # Add method column for operational turbines
    operational_turbines["Epot_Method"] = "Epot=EnergyProduced"

    # Process non-operational turbines
    non_operational_turbines = results.loc[~operational_turbines_mask].copy()

    # Merge potential energy to non-operational turbines
    non_operational_turbines = pd.merge(non_operational_turbines, potential_energy, on="TimeStamp", how="left")
    
    # Add method column for non-operational turbines
    non_operational_turbines["Epot_Method"] = "AverageWithWakeLossAdjustments"

    # If actual energy exceeds potential energy, use actual energy as potential
    mask_higher_actual = non_operational_turbines["Epot"] < non_operational_turbines["wtc_kWG1TotE_accum"]
    non_operational_turbines.loc[mask_higher_actual, "Epot"] = non_operational_turbines.loc[mask_higher_actual, "wtc_kWG1TotE_accum"]
    non_operational_turbines.loc[mask_higher_actual, "Epot_Method"] = "Epot=EnergyProduced"

    # Combine operational and non-operational turbines
    final_results = pd.concat([non_operational_turbines, operational_turbines], sort=False)

    # Sort by station and timestamp
    final_results = final_results.sort_values(["StationId", "TimeStamp"]).reset_index(drop=True)
    

    # Log debug information
    logger.debug(f"[CALC] Pre-Epot check 'final_results' columns: {list(final_results.columns)}")
    logger.debug(f"[CALC] Pre-Epot check 'final_results' shape: {final_results.shape}")

    # -------- Handle missing potential energy values --------------------------------------
    # Check if there are any NA values in the 'Epot' column
    if final_results["Epot"].isna().any():
        # Create mask for rows with missing Epot
        missing_epot_mask = final_results["Epot"].isna()
        missing_count = missing_epot_mask.sum()
        logger.info(f"[CALC] Found {missing_count} NA values in 'Epot' column, attempting to fill with turbine data (Case 2: Anemometer)")

        # Extract rows with missing Epot
        df_for_potential_energy = final_results.loc[missing_epot_mask]
        logger.debug(f"[CALC] DataFrame for potential energy calculation has shape {df_for_potential_energy.shape}")
        logger.debug(f"[CALC] Columns available: {list(df_for_potential_energy.columns)}")

        # Try to calculate potential energy using turbine data (Case 2: Anemometer)
        potential_energy_values_case2 = None
        try:
            potential_energy_values_case2 = calculate_potential_energy_from_turbine(df_for_potential_energy)
            logger.info("[CALC] Successfully calculated potential energy from turbine data (Case 2: Anemometer)")
            
            # Update Epot and Epot_Method for rows where we got valid values from Case 2
            valid_case2_mask = ~np.isnan(potential_energy_values_case2)
            if valid_case2_mask.any():
                # Create a mask for the original DataFrame that corresponds to valid Case 2 values
                case2_update_mask = missing_epot_mask.copy()
                case2_update_mask.loc[missing_epot_mask] = valid_case2_mask
                
                # Update Epot and Epot_Method for valid Case 2 values
                final_results.loc[case2_update_mask, "Epot_Method"] = "Anemometer"
                final_results.loc[case2_update_mask, "Epot"] = np.maximum(
                    potential_energy_values_case2[valid_case2_mask],
                    final_results.loc[case2_update_mask, "wtc_kWG1TotE_accum"].fillna(0).values,
                )
        except Exception as e:
            logger.error(f"[CALC] Error calculating potential energy from turbine data: {str(e)}")

        # For rows that are still missing Epot after Case 2, use statistical method (Case 3: SWE)
        still_missing_epot_mask = final_results["Epot"].isna()
        if still_missing_epot_mask.any():
            still_missing_count = still_missing_epot_mask.sum()
            logger.info(f"[CALC] Found {still_missing_count} NA values in 'Epot' column after Case 2, using statistical method (Case 3: SWE)")
            
            try:
                # Use the period to calculate potential energy from statistics
                potential_energy_values_case3 = calculate_potential_energy_from_statistics(period)
                logger.info("[CALC] Successfully calculated potential energy from statistical method (Case 3: SWE)")
                
                # Update Epot and Epot_Method for remaining missing values
                final_results.loc[still_missing_epot_mask, "Epot_Method"] = "SWE"
                final_results.loc[still_missing_epot_mask, "Epot"] = np.maximum(
                    potential_energy_values_case3,
                    final_results.loc[still_missing_epot_mask, "wtc_kWG1TotE_accum"].fillna(0).values,
                )
            except Exception as e:
                logger.error(f"[CALC] Error calculating potential energy from statistical method: {str(e)}")

    # -------- Calculate energy loss --------------------------------------
    # Calculate total energy loss (potential - actual)
    final_results["EL"] = final_results["Epot"].fillna(0) - final_results["wtc_kWG1TotE_accum"].fillna(0)

    # Ensure energy loss is not negative
    final_results["EL"] = final_results["EL"].clip(lower=0)

    # Calculate energy loss by error type
    # Energy loss due to Tarec errors (type 0)
    final_results["ELX"] = (
        (final_results["Period 0(s)"] / (final_results["Period 0(s)"] + final_results["Period 1(s)"]))
        * (final_results["EL"])
    ).fillna(0)

    # Energy loss due to Siemens errors (type 1)
    final_results["ELNX"] = (
        (final_results["Period 1(s)"] / (final_results["Period 0(s)"] + final_results["Period 1(s)"]))
        * (final_results["EL"])
    ).fillna(0)

    # Energy loss not attributed to specific error types
    final_results["EL_indefini"] = final_results["EL"] - (final_results["ELX"] + final_results["ELNX"])

    # -------- Calculate neighboring values for wind pattern detection --------------------------------------
    # Add columns for previous and next values in the time series

    # Wind speed from previous and next turbines at the same timestamp
    final_results["wind_speed_prev_turbine"] = final_results.groupby("TimeStamp", group_keys=True)["wtc_AcWindSp_mean"].shift()
    final_results["wind_speed_next_turbine"] = final_results.groupby("TimeStamp", group_keys=True)["wtc_AcWindSp_mean"].shift(-1)

    # Minimum power from previous and next turbines at the same timestamp
    final_results["min_power_prev_turbine"] = final_results.groupby("TimeStamp", group_keys=True)["wtc_ActPower_min"].shift()
    final_results["min_power_next_turbine"] = final_results.groupby("TimeStamp", group_keys=True)["wtc_ActPower_min"].shift(-1)

    # Alarm periods from previous and next turbines at the same timestamp
    final_results["alarm_period_prev_turbine"] = final_results.groupby("TimeStamp", group_keys=True)["EffectiveAlarmTime"].shift()
    final_results["alarm_period_next_turbine"] = final_results.groupby("TimeStamp", group_keys=True)["EffectiveAlarmTime"].shift(-1)

    # Calculate wind speed differences between neighboring turbines at the same timestamp
    final_results["wind_speed_diff_prev_turbine"] = final_results.wind_speed_prev_turbine - final_results.wtc_AcWindSp_mean
    final_results["wind_speed_diff_next_turbine"] = final_results.wind_speed_next_turbine - final_results.wtc_AcWindSp_mean

    # -------- Categorize energy loss by cause --------------------------------------

    # Energy loss due to curtailment (power reduction)
    curtailment_mask = (final_results["EL_indefini"] > 0) & (
        (final_results["wtc_PowerRed_timeon"] > 0) & (final_results["wtc_ActPower_max"] > 2300)
    )
    final_results.loc[curtailment_mask, "EL_PowerRed"] = final_results.loc[curtailment_mask, "EL_indefini"]
    final_results["EL_PowerRed"] = final_results["EL_PowerRed"].fillna(0)

    # Subtract curtailment energy loss from unidentified loss
    final_results["EL_indefini"] = final_results["EL_indefini"].fillna(0) - final_results["EL_PowerRed"]

    # Energy loss due to code 2006 warnings
    code_2006_mask = (final_results["EL_indefini"] > 0) & (final_results["Duration 2006(s)"] > 0)
    final_results.loc[code_2006_mask, "EL_2006"] = final_results.loc[code_2006_mask, "EL_indefini"]
    final_results["EL_2006"] = final_results["EL_2006"].fillna(0)

    # Subtract code 2006 energy loss from unidentified loss
    final_results["EL_indefini"] = final_results["EL_indefini"].fillna(0) - final_results["EL_2006"]

    # -------- Detect low wind conditions and post-alarm recovery --------------------------------------
    def detect_wind_patterns(df):
        """
        Detect patterns in wind speed and power to categorize energy loss.

        This function:
        1. Identifies localized low wind conditions by comparing wind speeds between neighboring turbines
        2. Detects transitions between operational states
        3. Categorizes energy loss by cause (low wind, post-low wind startup, post-alarm recovery)

        The function uses wind speed differences between neighboring turbines at the same timestamp
        to identify turbines experiencing localized wind issues, which helps distinguish between
        genuine low wind conditions and other causes of energy loss.

        Args:
            df: DataFrame containing turbine data for a single station

        Returns:
            DataFrame with categorized energy loss
        """
        # Identify significant wind speed differences between neighboring turbines
        wind_drop_condition = (
            (df.wind_speed_diff_prev_turbine > 1)                            # Wind speed lower than previous turbine
            & (df.wind_speed_diff_next_turbine > 1)                          # Wind speed lower than next turbine
            & ((df.wind_speed_prev_turbine >= 5) | (df.wind_speed_next_turbine >= 5) | (df.wtc_AcWindSp_mean >= 5))  # Sufficient wind somewhere
        )

        # Mask for low wind energy loss
        # Exclude cases where neighboring turbines are producing power
        low_wind_mask = ~(
            wind_drop_condition
            & (
                ((df.min_power_prev_turbine > 0) & (df.min_power_next_turbine > 0))    # Both neighboring turbines producing
                | ((df.min_power_prev_turbine > 0) & (df.alarm_period_next_turbine > 0))  # Previous turbine producing, next has alarm
                | ((df.min_power_next_turbine > 0) & (df.alarm_period_prev_turbine > 0))  # Next turbine producing, previous has alarm
            )
        ) & (df["EL_indefini"] > 0)                                          # Has unidentified energy loss

        # Convert to boolean type
        low_wind_mask = low_wind_mask.astype('boolean')

        # Mask for intervals following low wind
        post_low_wind_mask = low_wind_mask.shift().bfill()

        # Assign energy loss to low wind category
        df.loc[low_wind_mask, "EL_wind"] = df.loc[low_wind_mask, "EL_indefini"].fillna(0)
        df.loc[low_wind_mask, "Duration lowind(s)"] = 600  # 10 minutes in seconds

        # Assign energy loss to post-low wind startup category
        df.loc[post_low_wind_mask & ~low_wind_mask, "EL_wind_start"] = (
            df.loc[post_low_wind_mask & ~low_wind_mask, "EL_indefini"]
        ).fillna(0)
        df.loc[post_low_wind_mask & ~low_wind_mask, "Duration lowind_start(s)"] = 600

        # Mask for intervals following alarms (post-alarm recovery)
        # Identifies turbines that have energy loss but no current alarm, while the previous timestamp had an alarm
        post_alarm_mask = (df["EffectiveAlarmTime"] > 0).shift() & (df["EL_indefini"] > 0) & (df["EffectiveAlarmTime"] == 0)

        # Assign energy loss to post-alarm recovery category
        df.loc[~low_wind_mask & ~post_low_wind_mask & post_alarm_mask, "EL_alarm_start"] = (
            df.loc[~low_wind_mask & ~post_low_wind_mask & post_alarm_mask, "EL_indefini"]
        ).fillna(0)
        df.loc[~low_wind_mask & ~post_low_wind_mask & post_alarm_mask, "Duration alarm_start(s)"] = 600

        return df

    # Apply wind pattern detection to each turbine
    final_results = final_results.groupby("StationId", group_keys=False).apply(detect_wind_patterns, include_groups=True)

    # Calculate remaining unidentified energy loss
    final_results["EL_indefini_left"] = final_results["EL_indefini"].fillna(0) - (
        final_results["EL_wind"].fillna(0)
        + final_results["EL_wind_start"].fillna(0)
        + final_results["EL_alarm_start"].fillna(0)
    )

    # -------- Detect misassigned low wind alarms --------------------------------------
    # Identify alarms with "low wind" text that have neighboring turbines producing power
    # This suggests the wind issue is not widespread and might be misclassified
    misassigned_alarm_mask = final_results["UK Text"].str.contains("low wind") & (
        ((final_results["min_power_next_turbine"] > 0) & (final_results["min_power_prev_turbine"] > 0))  # Both neighboring turbines producing
        | ((final_results["min_power_next_turbine"] > 0) & (final_results["alarm_period_prev_turbine"] > 0))  # Next turbine producing, previous has alarm
        | ((final_results["min_power_prev_turbine"] > 0) & (final_results["alarm_period_next_turbine"] > 0))  # Previous turbine producing, next has alarm
    )

    # Assign Tarec energy loss to misassigned category
    final_results.loc[misassigned_alarm_mask, "EL_Misassigned"] = final_results.loc[misassigned_alarm_mask, "ELX"]
    final_results["EL_Misassigned"] = final_results["EL_Misassigned"].fillna(0)

    # -------- Finalize results --------------------------------------
    # Round numeric columns to 2 decimal places and convert to float32
    numeric_columns = list(set(final_results.columns) - set(("StationId", "TimeStamp", "UK Text", "Epot_Method")))
    final_results[numeric_columns] = final_results[numeric_columns].round(2).astype(np.float32)

    # Log the earliest alarm date
    logger.warning(f"[CALC] First date in alarm = {warning_date}")


    return final_results


if __name__ == "__main__":
    full_calculation("2025-04")
