"""
Data Exporter Module

This module handles exporting data from a SQL Server database to CSV files,
performing reconciliation. It integrates database interaction,
file handling, and progress reporting, exporting directly to CSV.
"""

import os
import argparse
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import pyodbc
import queue
import json
import hashlib
from contextlib import contextmanager
from datetime import datetime
from dateutil.relativedelta import relativedelta
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    TextColumn,
    Progress,
    TimeRemainingColumn,
)

# --- Configuration Constants (Merged from db_export.py) ---

# Database connection parameters
DB_CONFIG = {
    "server": "10.173.224.101",
    "database": "WpsHistory",
    "username": "odbc_user",
    "password": "0dbc@1cust",
    "driver": "{ODBC Driver 11 for SQL Server}",
}

# Table mappings between file types and SQL Server tables
TABLE_MAPPINGS = {
    "sum": "tblAlarmLog",
    "met": "tblSCMet",
    "tur": "tblSCTurbine",
    "grd": "tblSCTurGrid",
    "cnt": "tblSCTurCount",
    "din": "tblSCTurDigiIn",
}

# Column definitions for each table (excluding unique keys where appropriate for checksum)
# Using lists for easier maintenance
TABLE_COLUMNS = {
    "tblAlarmLog": ["[ID]", "[TimeOn]", "[TimeOff]", "[StationNr]", "[Alarmcode]", "[Parameter]"],
    "tblSCMet": ["[TimeStamp]", "[StationId]", "[met_WindSpeed]", "[met_WindDirection]", "[met_AirPressure]", "[met_AirTemperature]"],
    "tblSCTurbine": ["[TimeStamp]", "[StationId]", "[wtc_kWG1TotE_accum]", "[wtc_YawPos]", "[wtc_NacelPos]", "[wtc_GenRpm]", "[wtc_RotRpm]", "[wtc_PitchA]", "[wtc_PitchB]", "[wtc_PitchC]", "[wtc_PitchBatteryA]", "[wtc_PitchBatteryB]", "[wtc_PitchBatteryC]", "[wtc_PitchMotorTempA]", "[wtc_PitchMotorTempB]", "[wtc_PitchMotorTempC]", "[wtc_PitchConvTempA]", "[wtc_PitchConvTempB]", "[wtc_PitchConvTempC]", "[wtc_PitchCabTempA]", "[wtc_PitchCabTempB]", "[wtc_PitchCabTempC]", "[wtc_PitchRef]", "[wtc_PitchAngle]", "[wtc_PitchAngleA]", "[wtc_PitchAngleB]", "[wtc_PitchAngleC]", "[wtc_PitchBladeRootTempA]", "[wtc_PitchBladeRootTempB]", "[wtc_PitchBladeRootTempC]", "[wtc_PitchHubTemp]", "[wtc_PitchNacelleTemp]", "[wtc_PitchContrCabTemp]", "[wtc_PitchContrHubTemp]", "[wtc_PitchContrNacTemp]", "[wtc_PitchDrvCurrA]", "[wtc_PitchDrvCurrB]", "[wtc_PitchDrvCurrC]", "[wtc_PitchDrvVoltA]", "[wtc_PitchDrvVoltB]", "[wtc_PitchDrvVoltC]", "[wtc_PitchEncPosA]", "[wtc_PitchEncPosB]", "[wtc_PitchEncPosC]", "[wtc_PitchEncRefA]", "[wtc_PitchEncRefB]", "[wtc_PitchEncRefC]", "[wtc_PitchEncTempA]", "[wtc_PitchEncTempB]", "[wtc_PitchEncTempC]", "[wtc_PitchMotorTorqueA]", "[wtc_PitchMotorTorqueB]", "[wtc_PitchMotorTorqueC]", "[wtc_PitchSetPointA]", "[wtc_PitchSetPointB]", "[wtc_PitchSetPointC]", "[wtc_PitchStatusA]", "[wtc_PitchStatusB]", "[wtc_PitchStatusC]", "[wtc_PitchSupplyVoltA]", "[wtc_PitchSupplyVoltB]", "[wtc_PitchSupplyVoltC]", "[wtc_PitchTempA]", "[wtc_PitchTempB]", "[wtc_PitchTempC]", "[wtc_PitchVoltA]", "[wtc_PitchVoltB]", "[wtc_PitchVoltC]", "[wtc_Power]", "[wtc_PowerReact]", "[wtc_PowerFact]", "[wtc_VoltL1]", "[wtc_VoltL2]", "[wtc_VoltL3]", "[wtc_CurrL1]", "[wtc_CurrL2]", "[wtc_CurrL3]", "[wtc_Freq]", "[wtc_CosPhi]", "[wtc_GenBearTemp1]", "[wtc_GenBearTemp2]", "[wtc_GenWindTemp1]", "[wtc_GenWindTemp2]", "[wtc_GenWindTemp3]", "[wtc_GearBearTemp1]", "[wtc_GearBearTemp2]", "[wtc_GearBearTemp3]", "[wtc_GearOilTemp]", "[wtc_GearOilLevel]", "[wtc_HydrOilTemp]", "[wtc_HydrOilPress]", "[wtc_NacelTemp]", "[wtc_CabTemp]", "[wtc_AmbTemp]", "[wtc_TransfWindTemp1]", "[wtc_TransfWindTemp2]", "[wtc_TransfWindTemp3]", "[wtc_TransfOilTemp]", "[wtc_ConvWaterTemp]", "[wtc_ConvAirTemp]", "[wtc_ConvWindTemp1]", "[wtc_ConvWindTemp2]", "[wtc_ConvWindTemp3]", "[wtc_InvWaterTemp]", "[wtc_InvAirTemp]", "[wtc_InvWindTemp1]", "[wtc_InvWindTemp2]", "[wtc_InvWindTemp3]", "[wtc_YawBrakePress]", "[wtc_YawBrakeState]", "[wtc_YawMotorTemp1]", "[wtc_YawMotorTemp2]", "[wtc_YawMotorTemp3]", "[wtc_YawMotorTemp4]", "[wtc_YawMotorTemp5]", "[wtc_YawMotorTemp6]", "[wtc_YawMotorTemp7]", "[wtc_YawMotorTemp8]", "[wtc_YawSpeed]", "[wtc_YawState]", "[wtc_YawTorque1]", "[wtc_YawTorque2]", "[wtc_YawTorque3]", "[wtc_YawTorque4]", "[wtc_YawTorque5]", "[wtc_YawTorque6]", "[wtc_YawTorque7]", "[wtc_YawTorque8]", "[wtc_YawTwist]", "[wtc_YawVolt]", "[wtc_YawWindDir]", "[wtc_YawWindSpeed]"],
    "tblSCTurGrid": ["[TimeStamp]", "[StationId]", "[wtg_VoltL1]", "[wtg_VoltL2]", "[wtg_VoltL3]", "[wtg_CurrL1]", "[wtg_CurrL2]", "[wtg_CurrL3]", "[wtg_Freq]", "[wtg_Power]", "[wtg_PowerReact]", "[wtg_PowerFact]", "[wtg_CosPhi]"],
    "tblSCTurCount": ["[TimeStamp]", "[StationId]", "[wtm_OperTime]", "[wtm_ServTime]", "[wtm_ErrTime]", "[wtm_GridDropTime]", "[wtm_ExtErrTime]", "[wtm_IntErrTime]", "[wtm_WarnTime]", "[wtm_ExtWarnTime]", "[wtm_IntWarnTime]", "[wtm_StartTime]", "[wtm_StopTime]", "[wtm_ProdTime]", "[wtm_PauseTime]", "[wtm_ReadyTime]", "[wtm_StandbyTime]", "[wtm_MaintTime]", "[wtm_ManStopTime]", "[wtm_ManStartTime]", "[wtm_ManPauseTime]", "[wtm_ManReadyTime]", "[wtm_ManStandbyTime]", "[wtm_ManMaintTime]", "[wtm_ManOperTime]", "[wtm_ManServTime]", "[wtm_ManErrTime]", "[wtm_ManGridDropTime]", "[wtm_ManExtErrTime]", "[wtm_ManIntErrTime]", "[wtm_ManWarnTime]", "[wtm_ManExtWarnTime]", "[wtm_ManIntWarnTime]", "[wtm_ManStartTimeCnt]", "[wtm_ManStopTimeCnt]", "[wtm_ManProdTimeCnt]", "[wtm_ManPauseTimeCnt]", "[wtm_ManReadyTimeCnt]", "[wtm_ManStandbyTimeCnt]", "[wtm_ManMaintTimeCnt]", "[wtm_ManOperTimeCnt]", "[wtm_ManServTimeCnt]", "[wtm_ManErrTimeCnt]", "[wtm_ManGridDropTimeCnt]", "[wtm_ManExtErrTimeCnt]", "[wtm_ManIntErrTimeCnt]", "[wtm_ManWarnTimeCnt]", "[wtm_ManExtWarnTimeCnt]", "[wtm_ManIntWarnTimeCnt]", "[wtm_StartTimeCnt]", "[wtm_StopTimeCnt]", "[wtm_ProdTimeCnt]", "[wtm_PauseTimeCnt]", "[wtm_ReadyTimeCnt]", "[wtm_StandbyTimeCnt]", "[wtm_MaintTimeCnt]", "[wtm_OperTimeCnt]", "[wtm_ServTimeCnt]", "[wtm_ErrTimeCnt]", "[wtm_GridDropTimeCnt]", "[wtm_ExtErrTimeCnt]", "[wtm_IntErrTimeCnt]", "[wtm_WarnTimeCnt]", "[wtm_ExtWarnTimeCnt]", "[wtm_IntWarnTimeCnt]"],
    "tblSCTurDigiIn": ["[TimeStamp]", "[StationId]", "[wtd_StateWord1]", "[wtd_StateWord2]", "[wtd_StateWord3]", "[wtd_StateWord4]", "[wtd_StateWord5]", "[wtd_StateWord6]", "[wtd_StateWord7]", "[wtd_StateWord8]", "[wtd_StateWord9]", "[wtd_StateWord10]", "[wtd_WarnWord1]", "[wtd_WarnWord2]", "[wtd_WarnWord3]", "[wtd_WarnWord4]", "[wtd_WarnWord5]", "[wtd_WarnWord6]", "[wtd_WarnWord7]", "[wtd_WarnWord8]", "[wtd_WarnWord9]", "[wtd_WarnWord10]", "[wtd_ErrWord1]", "[wtd_ErrWord2]", "[wtd_ErrWord3]", "[wtd_ErrWord4]", "[wtd_ErrWord5]", "[wtd_ErrWord6]", "[wtd_ErrWord7]", "[wtd_ErrWord8]", "[wtd_ErrWord9]", "[wtd_ErrWord10]", "[wtd_ExtErrWord1]", "[wtd_ExtErrWord2]", "[wtd_ExtErrWord3]", "[wtd_ExtErrWord4]", "[wtd_ExtErrWord5]", "[wtd_ExtErrWord6]", "[wtd_ExtErrWord7]", "[wtd_ExtErrWord8]", "[wtd_ExtErrWord9]", "[wtd_ExtErrWord10]", "[wtd_IntErrWord1]", "[wtd_IntErrWord2]", "[wtd_IntErrWord3]", "[wtd_IntErrWord4]", "[wtd_IntErrWord5]", "[wtd_IntErrWord6]", "[wtd_IntErrWord7]", "[wtd_IntErrWord8]", "[wtd_IntErrWord9]", "[wtd_IntErrWord10]", "[wtd_GridDropWord1]", "[wtd_GridDropWord2]", "[wtd_GridDropWord3]", "[wtd_GridDropWord4]", "[wtd_GridDropWord5]", "[wtd_GridDropWord6]", "[wtd_GridDropWord7]", "[wtd_GridDropWord8]", "[wtd_GridDropWord9]", "[wtd_GridDropWord10]", "[wtd_ExtWarnWord1]", "[wtd_ExtWarnWord2]", "[wtd_ExtWarnWord3]", "[wtd_ExtWarnWord4]", "[wtd_ExtWarnWord5]", "[wtd_ExtWarnWord6]", "[wtd_ExtWarnWord7]", "[wtd_ExtWarnWord8]", "[wtd_ExtWarnWord9]", "[wtd_ExtWarnWord10]", "[wtd_IntWarnWord1]", "[wtd_IntWarnWord2]", "[wtd_IntWarnWord3]", "[wtd_IntWarnWord4]", "[wtd_IntWarnWord5]", "[wtd_IntWarnWord6]", "[wtd_IntWarnWord7]", "[wtd_IntWarnWord8]", "[wtd_IntWarnWord9]", "[wtd_IntWarnWord10]"]
}

# Columns to use for checksum calculation (often excludes simple primary keys like 'ID')
TABLE_CHECKSUM_COLUMNS = {
    "tblAlarmLog": ["[TimeOn]", "[TimeOff]", "[StationNr]", "[Alarmcode]", "[Parameter]"],
    # For others, assume all columns defined in TABLE_COLUMNS are relevant for checksum
    "tblSCMet": TABLE_COLUMNS["tblSCMet"],
    "tblSCTurbine": TABLE_COLUMNS["tblSCTurbine"],
    "tblSCTurGrid": TABLE_COLUMNS["tblSCTurGrid"],
    "tblSCTurCount": TABLE_COLUMNS["tblSCTurCount"],
    "tblSCTurDigiIn": TABLE_COLUMNS["tblSCTurDigiIn"],
}


# File extension to use for exports
FILE_EXTENSION = "csv"

# Output directories
BASE_DATA_PATH = "./monthly_data/data"  # Unified data directory
METADATA_EXTENSION = ".meta.json"

# Path to manual adjustments file
MANUAL_ADJUSTMENTS_FILE = "manual_adjustments.json"

# --- Logging Setup ---

# Use RichHandler for better console output
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",  # Simplified format for RichHandler
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
)
logger = logging.getLogger("rich")  # Use the rich logger consistently

# --- Database Connection Pool (from db_export.py) ---


class ConnectionPool:
    """A simple connection pool for reusing database connections"""

    def __init__(self, max_connections=5):
        """Initialize the connection pool"""
        self.pool = queue.Queue(max_connections)
        self.size = max_connections
        self._create_connections()

    def _create_connections(self):
        """Create initial connections and add them to the pool"""
        for _ in range(self.size):
            conn = self._create_connection()
            if conn:  # Only add if connection was successful
                self.pool.put(conn)

    def _create_connection(self):
        """Create a new database connection"""
        connection_string = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']}"
        )
        try:
            conn = pyodbc.connect(connection_string, timeout=10)  # Added timeout
            logger.debug("Database connection created successfully.")
            return conn
        except pyodbc.Error as e:
            logger.error(f"Failed to create database connection: {str(e)}")
            # Do not raise here, allow pool creation to potentially succeed with fewer connections
            return None  # Indicate failure

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = None
        try:
            conn = self.pool.get(block=True, timeout=30)  # Added timeout
            yield conn
        except queue.Empty:
            logger.error("Timeout waiting for database connection from pool.")
            raise TimeoutError(
                "Could not get database connection from pool"
            )  # Raise specific error
        finally:
            if conn:
                try:
                    # Simple check if connection is likely alive
                    if not conn.closed:
                        self.pool.put(conn)
                    else:
                        logger.warning("Connection was closed, creating a new one.")
                        self.pool.put(self._create_connection())

                except pyodbc.Error:
                    # If connection is broken, create a new one
                    logger.warning("Connection is broken, replacing with a new one")
                    try:
                        conn.close()
                    except Exception:
                        pass  # Ignore errors during close of broken connection
                    new_conn = self._create_connection()
                    if new_conn:
                        self.pool.put(new_conn)
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")
                    # Attempt to create a new connection if putting back failed
                    new_conn = self._create_connection()
                    if new_conn:
                        self.pool.put(new_conn)

    def close_all(self):
        """Close all connections in the pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get(block=False)
                conn.close()
                logger.debug("Closed connection from pool.")
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")


# --- DB Exporter Class ---


class DBExporter:
    """Handles exporting data from SQL Server to CSV files"""

    def __init__(self, connection_pool):
        """Initialize the exporter"""
        self.connection_pool = connection_pool
        self._load_error_list()
        self.manual_adjustments = self._load_manual_adjustments()

    def _load_error_list(self):
        """Loads and prepares the alarm error list from the Excel file."""
        try:
            excel_path = r"Alarmes List Norme RDS-PP_Tarec.xlsx"
            if not os.path.exists(excel_path):
                logger.error(f"Error list file not found at: {excel_path}")
                self.alarms_0_1 = pd.Series(dtype=int)
                return

            error_list = pd.read_excel(excel_path)
            error_list.Number = error_list.Number.astype(int)
            error_list.drop_duplicates(subset=["Number"], inplace=True)
            error_list.rename(columns={"Number": "Alarmcode"}, inplace=True)
            self.alarms_0_1 = error_list.loc[
                error_list["Error Type"].isin(
                    [0, 1]
                )  # Corrected isin([1, 0]) to isin([0, 1])
            ].Alarmcode
            logger.info(
                f"Loaded {len(self.alarms_0_1)} alarm codes for type 0/1 from {excel_path}"
            )
        except Exception as e:
            logger.error(f"Failed to load or process error list from Excel: {e}")
            self.alarms_0_1 = pd.Series(dtype=int)

    def _load_manual_adjustments(self):
        """Loads manual alarm adjustments from the JSON file."""
        try:
            if not os.path.exists(MANUAL_ADJUSTMENTS_FILE):
                logger.info(
                    f"Manual adjustments file not found at: {MANUAL_ADJUSTMENTS_FILE}. Creating empty file."
                )
                with open(MANUAL_ADJUSTMENTS_FILE, "w") as f:
                    json.dump({"adjustments": []}, f, indent=4)
                return {"adjustments": []}

            with open(MANUAL_ADJUSTMENTS_FILE, "r") as f:
                adjustments = json.load(f)
            logger.info(
                f"Loaded {len(adjustments.get('adjustments', []))} manual adjustments from {MANUAL_ADJUSTMENTS_FILE}"
            )
            return adjustments
        except Exception as e:
            logger.error(f"Failed to load or process manual adjustments: {e}")
            return {"adjustments": []}

    def _get_metadata_path(self, csv_path):
        """Constructs the metadata file path from the CSV path."""
        return csv_path + METADATA_EXTENSION

    def _read_metadata(self, metadata_path):
        """Reads the metadata JSON file."""
        if not os.path.exists(metadata_path):
            return None, None
        try:
            with open(metadata_path, "r") as f:
                metadata = json.load(f)
            return metadata.get("db_row_count"), metadata.get("db_checksum_agg")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(
                f"Could not read or parse metadata file {metadata_path}: {e}"
            )
            return None, None

    def _write_metadata(self, metadata_path, count, checksum_agg):
        """Writes the metadata to the JSON file."""
        metadata = {
            "db_row_count": count,
            "db_checksum_agg": checksum_agg,
            "last_updated": datetime.now().isoformat(),
        }
        try:
            os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=4)
            logger.debug(f"Metadata written to {metadata_path}")  # Changed to debug
        except IOError as e:
            logger.error(f"Could not write metadata file {metadata_path}: {e}")

    def _get_unique_keys(self, table_name):
        """Returns the list of unique key columns for a table."""
        if table_name == "tblAlarmLog":
            return ["ID"]
        else:
            return ["TimeStamp", "StationId"]

    def _get_checksum_columns(self, table_name):
        """Returns the comma-separated string of columns for checksum calculation."""
        columns = TABLE_CHECKSUM_COLUMNS.get(table_name)
        if columns:
            return ", ".join(columns)
        else:
            # Fallback: use all columns from TABLE_COLUMNS if not specified in TABLE_CHECKSUM_COLUMNS
            logger.warning(f"Checksum columns not explicitly defined for {table_name}. Using all columns from TABLE_COLUMNS.")
            return self._get_columns_for_table(table_name) # Reuse the other function

    def check_data_state(self, table_name, period_start, period_end):
        """
        Checks the current state (count and checksum) of data in the DB for the period.
        """
        query = ""
        checksum_cols = self._get_checksum_columns(table_name)

        try:
            with self.connection_pool.get_connection() as conn:
                if table_name == "tblAlarmLog":
                    if not hasattr(self, "alarms_0_1") or self.alarms_0_1.empty:
                        logger.warning(
                            "alarms_0_1 not loaded or empty for tblAlarmLog check. Attempting load."
                        )
                        self._load_error_list()
                        if self.alarms_0_1.empty:
                            logger.error(
                                "Failed to load alarms_0_1 for tblAlarmLog check."
                            )
                            return None, None

                    alarm_codes_tuple = tuple(self.alarms_0_1.tolist())
                    if not alarm_codes_tuple:
                        alarm_codes_tuple = ("NULL",)  # Avoid SQL syntax error

                    where_clause = f"""
                    WHERE ([Alarmcode] <> 50100)
                    AND (
                        ([TimeOff] BETWEEN '{period_start}' AND '{period_end}')
                        OR ([TimeOn] BETWEEN '{period_start}' AND '{period_end}')
                        OR ([TimeOn] <= '{period_start}' AND [TimeOff] >= '{period_end}')
                        OR ([TimeOff] IS NULL AND [Alarmcode] IN {alarm_codes_tuple})
                    )
                    """
                    query = f"""
                    SET NOCOUNT ON;
                    SELECT
                        COUNT_BIG(*),
                        CHECKSUM_AGG(CAST(BINARY_CHECKSUM({checksum_cols}) AS INT)) -- Cast needed for CHECKSUM_AGG
                    FROM [WpsHistory].[dbo].[tblAlarmLog]
                    {where_clause}
                    """
                else:
                    where_clause = f"WHERE TimeStamp >= '{period_start}' AND TimeStamp < '{period_end}'"
                    query = f"""
                    SET NOCOUNT ON;
                    SELECT
                        COUNT_BIG(*),
                        CHECKSUM_AGG(CAST(BINARY_CHECKSUM({checksum_cols}) AS INT)) -- Cast needed
                    FROM {table_name}
                    {where_clause}
                    """

                logger.debug(
                    f"Executing state check query for {table_name}"
                )  #: {query}") # Hide query details
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()

                if result:
                    count = result[0]
                    checksum_agg = result[1] if result[1] is not None else 0
                    logger.debug(  # Changed to debug
                        f"DB state for {table_name} ({period_start} to {period_end}): Count={count}, Checksum={checksum_agg}"
                    )
                    return count, checksum_agg
                else:
                    logger.warning(
                        f"Could not retrieve state for {table_name} for period."
                    )
                    return None, None

        except pyodbc.Error as e:
            logger.error(f"Database error during state check for {table_name}: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Unexpected error during state check for {table_name}: {e}")
            return None, None

    def _fetch_db_data(self, table_name, period_start, period_end):
        """Fetches the full dataset from the DB for the given table and period."""
        query = ""
        try:
            with self.connection_pool.get_connection() as conn:
                if table_name == "tblAlarmLog":
                    if not hasattr(self, "alarms_0_1") or self.alarms_0_1.empty:
                        self._load_error_list()
                        if self.alarms_0_1.empty:
                            logger.error(
                                "Failed to load alarms_0_1 for tblAlarmLog fetch."
                            )
                            return pd.DataFrame()
                    query = self.construct_query(
                        period_start, period_end, self.alarms_0_1
                    )
                else:
                    columns = self._get_columns_for_table(table_name)
                    query = f"""
                    SELECT {columns} FROM {table_name}
                    WHERE TimeStamp >= '{period_start}' AND TimeStamp < '{period_end}'
                    ORDER BY TimeStamp, StationId -- Add ordering for consistency
                    """
                logger.info(
                    f"Fetching data for {table_name} ({period_start} to {period_end})"
                )
                df = pd.read_sql(query, conn)
                logger.info(f"Fetched {len(df)} rows from DB for {table_name}")

                # Standardize TimeStamp columns
                for col in ["TimeStamp", "TimeOn", "TimeOff"]:
                    if col in df.columns:
                        # Use errors='coerce' to handle potential invalid date formats gracefully
                        df[col] = pd.to_datetime(df[col], errors="coerce")

                # Apply manual adjustments if this is the alarm table
                if table_name == "tblAlarmLog" and not df.empty:
                    df = self._apply_manual_adjustments(df)

                return df
        except Exception as e:
            logger.error(f"Failed to fetch data for {table_name}: {e}")
            return pd.DataFrame()

    def _apply_manual_adjustments(self, df):
        """Apply manual adjustments to alarm data."""
        if not self.manual_adjustments.get("adjustments"):
            return df

        df_adjusted = df.copy()
        adjustments_applied = 0

        for adjustment in self.manual_adjustments["adjustments"]:
            mask = df_adjusted["ID"] == adjustment["id"]
            if mask.any():
                try:
                    time_off = pd.to_datetime(adjustment["time_off"], errors="coerce")
                    if pd.notna(time_off):  # Check if conversion was successful
                        df_adjusted.loc[mask, "TimeOff"] = time_off
                        adjustments_applied += 1
                    else:
                        logger.warning(
                            f"Invalid time_off format in adjustment for ID {adjustment['id']}: {adjustment['time_off']}"
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to apply adjustment for alarm ID {adjustment['id']}: {e}"
                    )

        if adjustments_applied > 0:
            logger.info(
                f"Applied {adjustments_applied} manual adjustments to alarm data"
            )

        return df_adjusted

    def _hash_row(self, row, columns_to_hash):
        """Creates a hash for a pandas Series based on specified columns."""
        # Ensure consistent string representation, handle NaT/None
        hash_input = "".join(
            str(row[col]) if pd.notna(row[col]) else ""
            for col in columns_to_hash
            if col in row.index
        )
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _reconcile_and_export(
        self,
        table_name,
        period_start,
        period_end,
        output_path,
        db_count,
        db_checksum,
        update_mode,
    ):
        """Performs data reconciliation and exports the final CSV based on update_mode."""
        try:
            # 1. Fetch current data from DB
            db_df = self._fetch_db_data(table_name, period_start, period_end)
            if db_df.empty and db_count > 0:
                logger.warning(
                    f"DB fetch for {table_name} returned empty but DB count was {db_count}."
                )

            # 2. Read existing CSV if relevant for append/check modes
            existing_df = pd.DataFrame()
            if update_mode != "force-overwrite" and os.path.exists(output_path):
                try:
                    existing_df = pd.read_csv(output_path)
                    logger.info(
                        f"Read {len(existing_df)} rows from existing file {output_path}"
                    )
                    # Convert timestamp columns in existing data
                    for col in ["TimeStamp", "TimeOn", "TimeOff"]:
                        if col in existing_df.columns:
                            existing_df[col] = pd.to_datetime(
                                existing_df[col], errors="coerce"
                            )
                except Exception as e:
                    logger.error(
                        f"Failed to read or parse existing CSV {output_path}: {e}. Treating as empty."
                    )
                    existing_df = pd.DataFrame()

            # --- Handle different update modes ---

            if update_mode == "force-overwrite":
                logger.info(
                    f"Mode 'force-overwrite': Exporting fresh data for {table_name} to {output_path}"
                )
                final_df = db_df
            elif update_mode == "check":
                logger.info(
                    f"Mode 'check': Comparing DB state with existing file for {table_name}"
                )
                # Compare counts and checksums (checksum comparison might be unreliable if data types changed)
                existing_count = len(existing_df)
                # Note: Recalculating checksum on existing_df might be needed for accurate check
                # but can be slow. Relying on metadata count for now.
                meta_count, meta_checksum = self._read_metadata(
                    self._get_metadata_path(output_path)
                )

                if meta_count is not None and meta_checksum is not None:
                    if db_count == meta_count and db_checksum == meta_checksum:
                        logger.info(
                            f"Data for {table_name} appears unchanged based on metadata. No export needed."
                        )
                        return "NO_CHANGE"
                    else:
                        logger.warning(
                            f"Data change detected for {table_name} based on metadata (DB: {db_count}/{db_checksum}, Meta: {meta_count}/{meta_checksum}). Recommend re-export."
                        )
                        # In 'check' mode, we don't export, just report.
                        return "CHANGE_DETECTED"
                else:
                    logger.warning(
                        f"Metadata not found or invalid for {output_path}. Cannot perform check. Recommend re-export."
                    )
                    return "METADATA_MISSING"

            elif update_mode == "append":
                logger.info(
                    f"Mode 'append': Reconciling DB data with existing file for {table_name}"
                )
                if existing_df.empty:
                    logger.info(
                        f"Existing file {output_path} not found or empty. Exporting current DB data."
                    )
                    final_df = db_df
                else:
                    # Ensure consistent columns before merge
                    all_cols = list(set(db_df.columns) | set(existing_df.columns))
                    db_df = db_df.reindex(columns=all_cols)
                    existing_df = existing_df.reindex(columns=all_cols)

                    unique_keys = self._get_unique_keys(table_name)
                    if not all(key in db_df.columns for key in unique_keys) or not all(
                        key in existing_df.columns for key in unique_keys
                    ):
                        logger.error(
                            f"Unique key columns missing in DB or existing data for {table_name}. Cannot reconcile. Overwriting."
                        )
                        final_df = db_df
                    else:
                        # Merge based on unique keys
                        merged_df = pd.merge(
                            db_df.add_suffix("_db"),
                            existing_df.add_suffix("_ex"),
                            left_on=[k + "_db" for k in unique_keys],
                            right_on=[k + "_ex" for k in unique_keys],
                            how="outer",
                            indicator=True,
                        )

                        # Identify new, deleted, and potentially updated rows
                        new_rows = merged_df[merged_df["_merge"] == "left_only"].copy()
                        deleted_rows = merged_df[
                            merged_df["_merge"] == "right_only"
                        ].copy()
                        common_rows = merged_df[merged_df["_merge"] == "both"].copy()

                        # Restore original column names for new rows
                        new_rows.columns = [
                            c.replace("_db", "") for c in new_rows.columns
                        ]

                        # Keep deleted rows (as per 'append' preserving deletions)
                        deleted_rows.columns = [
                            c.replace("_ex", "") for c in deleted_rows.columns
                        ]

                        # For common rows, check for updates (simple check: keep DB version)
                        # A more robust check would compare non-key columns
                        common_rows_final = common_rows[
                            [
                                c
                                for c in common_rows.columns
                                if c.endswith("_db") or c in unique_keys
                            ]
                        ].copy()
                        common_rows_final.columns = [
                            c.replace("_db", "") for c in common_rows_final.columns
                        ]

                        # Combine results
                        final_df = pd.concat(
                            [common_rows_final, new_rows, deleted_rows],
                            ignore_index=True,
                        )
                        # Drop the merge indicator if it exists
                        if "_merge" in final_df.columns:
                            final_df.drop(columns=["_merge"], inplace=True)

                        logger.info(
                            f"Reconciliation for {table_name}: {len(new_rows)} new, {len(deleted_rows)} deleted (kept), {len(common_rows)} common/updated."
                        )

            else:
                logger.error(
                    f"Invalid update_mode: {update_mode}. Defaulting to force-overwrite."
                )
                final_df = db_df

            # 4. Export the final DataFrame
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            final_df.to_csv(
                output_path, index=False
            )  # , date_format='%Y-%m-%d %H:%M:%S.%f') # Ensure consistent date format if needed
            logger.info(f"Successfully exported {len(final_df)} rows to {output_path}")

            # 5. Write metadata
            self._write_metadata(
                self._get_metadata_path(output_path), db_count, db_checksum
            )
            return "EXPORT_DONE"

        except Exception as e:
            logger.exception(
                f"Error during reconcile/export for {table_name}: {e}"
            )  # Use exception for stack trace
            return "EXPORT_FAILED"

    def construct_query(self, period_start, period_end, alarms_0_1):
        """Constructs the SQL query for fetching alarm data."""
        # Convert list/Series to tuple for SQL IN clause
        alarm_codes_tuple = tuple(alarms_0_1.tolist())
        if not alarm_codes_tuple:
            alarm_codes_tuple = ("NULL",)  # Avoid SQL syntax error

        query = f"""
        SELECT [ID], [TimeOn], [TimeOff], [StationNr], [Alarmcode], [Parameter]
        FROM [WpsHistory].[dbo].[tblAlarmLog]
        WHERE ([Alarmcode] <> 50100)
        AND (
            ([TimeOff] BETWEEN '{period_start}' AND '{period_end}')
            OR ([TimeOn] BETWEEN '{period_start}' AND '{period_end}')
            OR ([TimeOn] <= '{period_start}' AND [TimeOff] >= '{period_end}')
            OR ([TimeOff] IS NULL AND [Alarmcode] IN {alarm_codes_tuple})
        )
        ORDER BY TimeOn, StationNr, Alarmcode -- Add ordering
        """
        return query

    def export_table_data(self, table_name, period, output_path, update_mode="append"):
        """Exports data for a specific table and period."""
        try:
            # Calculate period start and end dates
            period_dt = datetime.strptime(period, "%Y-%m")
            period_start = period_dt.strftime("%Y-%m-%d %H:%M:%S")
            # End date is the start of the next month
            next_month = period_dt.replace(day=1) + relativedelta(months=1)
            period_end = next_month.strftime("%Y-%m-%d %H:%M:%S")

            logger.info(
                f"Exporting {table_name} for period {period} ({period_start} to {period_end}) with mode '{update_mode}'"
            )

            # 1. Check current DB state
            db_count, db_checksum = self.check_data_state(
                table_name, period_start, period_end
            )
            if db_count is None:
                logger.error(
                    f"Failed to get DB state for {table_name}. Skipping export."
                )
                return False  # Indicate failure

            # 2. Read existing metadata
            metadata_path = self._get_metadata_path(output_path)
            meta_count, meta_checksum = self._read_metadata(metadata_path)

            # 3. Decide whether to export based on mode and state
            needs_export = False
            if update_mode == "force-overwrite":
                needs_export = True
            elif update_mode == "check":
                # Check mode handled within _reconcile_and_export, just call it
                result = self._reconcile_and_export(
                    table_name,
                    period_start,
                    period_end,
                    output_path,
                    db_count,
                    db_checksum,
                    update_mode,
                )
                return result in [
                    "NO_CHANGE",
                    "CHANGE_DETECTED",
                    "METADATA_MISSING",
                ]  # Success if check ran
            elif update_mode == "append":
                # Always reconcile in append mode if file exists or DB has data
                needs_export = True
                # Check if data is identical based on metadata to potentially skip
                if meta_count is not None and meta_checksum is not None:
                    if db_count == meta_count and db_checksum == meta_checksum:
                        logger.info(
                            f"Data for {table_name} appears unchanged based on metadata. Skipping reconciliation export."
                        )
                        return True  # Success, no change needed
                    else:
                        logger.info(
                            f"Metadata indicates change for {table_name}. Proceeding with reconciliation."
                        )
                else:
                    logger.info(
                        f"No valid metadata for {table_name}. Proceeding with reconciliation/export."
                    )

            else:  # Invalid mode
                logger.error(f"Invalid update_mode '{update_mode}'. Cannot export.")
                return False

            # 4. Perform reconciliation and export if needed
            if needs_export:
                result = self._reconcile_and_export(
                    table_name,
                    period_start,
                    period_end,
                    output_path,
                    db_count,
                    db_checksum,
                    update_mode,
                )
                return result == "EXPORT_DONE"
            else:
                # This case should ideally be handled above (e.g., append mode with no change)
                return True  # Indicate success (no action needed)

        except Exception as e:
            logger.exception(
                f"Failed to export data for {table_name} for period {period}: {e}"
            )
            return False

    def _get_columns_for_table(self, table_name):
        """Returns the SELECT column list string for a given table using TABLE_COLUMNS."""
        columns = TABLE_COLUMNS.get(table_name)
        if columns:
            return ", ".join(columns)
        else:
            logger.warning(
                f"Column definition not found in TABLE_COLUMNS for table: {table_name}. Selecting all columns (*)."
            )
            return "*"

# --- Main Export Function ---


def export_table_to_csv(period, file_types=None, update_mode="append"):
    """
    Exports data for specified file types and period directly to CSV.
    Args:
        period (str): Period in YYYY-MM format.
        file_types (list, optional): List of file types ('sum', 'met', etc.)
                                     to export. Defaults to all types in TABLE_MAPPINGS.
        update_mode (str): Strategy for handling existing files.
                           'append': Reconcile DB changes with existing CSV, preserving deletions.
                           'force-overwrite': Export fresh data from DB, overwriting existing CSV.
                           'check': Compare DB state with metadata, report changes, no export.

    Returns:
        dict: A dictionary with file types as keys and boolean success status as values.
    """
    if file_types is None:
        file_types = list(TABLE_MAPPINGS.keys())

    results = {}
    connection_pool = ConnectionPool()  # Create pool for this export run
    exporter = DBExporter(connection_pool)

    try:
        for file_type in file_types:
            if file_type not in TABLE_MAPPINGS:
                logger.warning(f"Unknown file type '{file_type}'. Skipping.")
                results[file_type] = False
                continue

            table_name = TABLE_MAPPINGS[file_type]
            file_type_upper = file_type.upper()

            # Define paths using the unified data directory
            csv_dir = os.path.join(BASE_DATA_PATH, file_type_upper)
            csv_filename = f"{period}.{FILE_EXTENSION}"
            csv_path = os.path.join(csv_dir, csv_filename)

            os.makedirs(csv_dir, exist_ok=True)

            logger.info(f"--- Processing {file_type_upper} for {period} ---")

            # Export data (includes reconciliation based on update_mode)
            export_success = exporter.export_table_data(
                table_name, period, csv_path, update_mode
            )

            # Archiving step removed
            results[file_type] = (
                export_success  # Success depends only on export_table_data result
            )

            # Optionally remove metadata file if export failed? Or keep it? Keeping it for now.

    except Exception as e:  # Keep the main exception handling
        logger.exception(f"An unexpected error occurred during export process: {e}")
        # Mark all requested types as failed if a major error occurs
        for ft in file_types:
            results[ft] = False
    finally:
        connection_pool.close_all()  # Ensure connections are closed

    return results


# --- Orchestration Functions  ---


def ensure_directories():
    """Ensure all necessary base and type-specific directories exist in the unified structure"""
    # Uses the unified BASE_DATA_PATH constant
    os.makedirs(BASE_DATA_PATH, exist_ok=True)

    # Create type-specific directories within BASE_DATA_PATH
    for file_type in TABLE_MAPPINGS.keys():
        os.makedirs(os.path.join(BASE_DATA_PATH, file_type.upper()), exist_ok=True)



def export_data_for_period(
    period, file_types=None, update_mode="append"
):  # Added update_mode
    """
    Export data for the given period and file types using ThreadPoolExecutor
    """
    # Ensure directories exist before starting threads
    ensure_directories()

    results = {}
    # Use Rich progress bar
    with Progress(
        TextColumn("[bold blue]{task.description}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        TimeRemainingColumn(),
        transient=True,  # Clear progress on exit
    ) as progress:
        # Max workers can be tuned
        with ThreadPoolExecutor(max_workers=len(file_types) or 1) as executor:
            futures = {}
            for file_type in file_types:
                task_description = f"Exporting {file_type.upper()}"
                task_id = progress.add_task(
                    task_description, total=1
                )  # Total=1 step (export+archive)
                # Submit the main export function
                futures[task_id] = executor.submit(
                    export_table_to_csv,  # Use renamed function
                    period,
                    [file_type],  # Pass file_type as a list
                    update_mode=update_mode,
                )

            # Process results as they complete
            for task_id, future in futures.items():
                file_type = (
                    progress._tasks[task_id].description.split(" ")[1].lower()
                )  # Get file_type from description
                try:
                    result_dict = (
                        future.result()
                    )  # This is the dict returned by export_table_to_csv
                    success = result_dict.get(file_type, False)
                    results[file_type] = success
                    progress.update(
                        task_id,
                        completed=1,
                        description=f"Exporting {file_type.upper()} - {'OK' if success else 'FAIL'}",
                    )
                except Exception as e:
                    logger.error(f"Thread failed for {file_type}: {e}")
                    results[file_type] = False
                    progress.update(
                        task_id,
                        completed=1,
                        description=f"Exporting {file_type.upper()} - ERROR",
                    )
                finally:
                    # Ensure task is marked as completed even on error before result processing
                    if not progress._tasks[task_id].finished:
                        progress.update(task_id, completed=1)

    return results


def main_export_flow(
    period, file_types=None, update_mode="append"
):  # Renamed from main, added update_mode
    """
    Main function to orchestrate export of data and alarms for the given period
    """
    logger.info(
        f"--- Starting Data Export for Period: {period} (Mode: {update_mode}) ---"
    )

    # Ensure base directories exist first
    ensure_directories()

    # Define file types to process (handle None case)
    if file_types is None:
        all_types = list(TABLE_MAPPINGS.keys())
    else:
        all_types = file_types

    # All requested types will be handled in the parallel export step
    data_file_types = all_types # Use all requested types directly

    # Removed separate handling for 'sum' type

    # Export all requested data types in parallel
    data_results = {}
    if data_file_types: # Check if there are any types to export
        data_results = export_data_for_period(
            period, data_file_types, update_mode=update_mode
        )

    # Combine results and print summary
    # Results are directly from export_data_for_period now
    final_results = data_results

    logger.info(f"\n--- Export Summary for {period} ---")
    for ft, success in final_results.items():
        logger.info(f"{ft.upper()}: {'SUCCESS' if success else 'FAILED'}")
    logger.info("--- Export Process Finished ---")


# --- Standalone Execution ---

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Export data from database to CSV files"
    )
    parser.add_argument("period", help="Period in YYYY-MM format")
    parser.add_argument(
        "--types",
        nargs="+",
        help="Specific file types to export (e.g., met din sum). Exports all if omitted.",
    )
    parser.add_argument(
        "--update-mode",
        choices=["check", "append", "force-overwrite"],
        default="append",
        help="Mode for handling existing data exports: 'check' (report changes), 'append' (update/append preserving deletions), 'force-overwrite' (export fresh data).",
    )

    args = parser.parse_args()

    # Validate period format (basic check)
    try:
        datetime.strptime(args.period, "%Y-%m")
    except ValueError:
        logger.error("Invalid period format. Please use YYYY-MM.")
        exit(1)

    # Run the main export flow
    main_export_flow(args.period, args.types, args.update_mode)
