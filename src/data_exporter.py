"""
Data Exporter Module

This module handles exporting data from a SQL Server database to CSV files,
performing reconciliation. It integrates database interaction,
file handling, and progress reporting, exporting directly to CSV.
"""

import os
import argparse
import pandas as pd
import pyodbc
import queue
import json
import hashlib
from contextlib import contextmanager
from datetime import datetime
from dateutil.relativedelta import relativedelta
from rich.progress import (
    BarColumn,
    TextColumn,
    Progress,
    TimeRemainingColumn,
)
from sqlalchemy import create_engine

# Import centralized logging and configuration
from . import config
from . import logger_config

# Get a logger for this module
logger = logger_config.get_logger(__name__)

# --- Configuration Constants (Merged from db_export.py) ---

# Database connection parameters from environment variables
DB_CONFIG = config.DB_CONFIG

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
    "tblSCMet": ["[TimeStamp]", "[StationId]", "[met_WindSpeedRot_mean]", "[met_WinddirectionRot_mean]", "[met_Pressure_mean]", "[met_TemperatureTen_mean]"],
    "tblSCTurbine": ["[TimeStamp]", "[StationId]", "[wtc_AcWindSp_mean]", "[wtc_AcWindSp_stddev]", "[wtc_ActualWindDirection_mean]", "[wtc_ActualWindDirection_stddev]"],
    "tblSCTurGrid": ["[TimeStamp]", "[StationId]", "[wtc_ActPower_min]", "[wtc_ActPower_max]", "[wtc_ActPower_mean]"],
    "tblSCTurCount": ["[TimeStamp]", "[StationId]", "[wtc_kWG1Tot_accum]", "[wtc_kWG1TotE_accum]", "[wtc_kWG1TotI_accum]", "[wtc_BoostKWh_endvalue]", "[wtc_BostkWhS_endvalue]"],
    "tblSCTurDigiIn": ["[TimeStamp]", "[StationId]", "[wtc_PowerRed_timeon]"]
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
BASE_DATA_PATH = config.BASE_DATA_PATH  # Unified data directory from environment
METADATA_EXTENSION = ".meta.json"

# Path to manual adjustments file
MANUAL_ADJUSTMENTS_FILE = config.MANUAL_ADJUSTMENTS_FILE

# --- Logging is now handled by logger_config module ---

# --- Database Connection Pool (from db_export.py) ---


class ConnectionPool:
    """A simple connection pool for reusing database connections"""

    def __init__(self, max_connections=5):
        """Initialize the connection pool"""
        self.pool = queue.Queue(max_connections)
        self.size = max_connections
        self.engine = self._create_sqlalchemy_engine()
        self._create_connections()

    def _create_sqlalchemy_engine(self):
        """Create a SQLAlchemy engine for database connections"""
        connection_string = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']}"
        )
        try:
            # Create SQLAlchemy engine with pyodbc connection
            connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"
            engine = create_engine(connection_url, fast_executemany=True)
            logger.debug("SQLAlchemy engine created successfully.")
            return engine
        except Exception as e:
            logger.error(f"Failed to create SQLAlchemy engine: {str(e)}")
            # Still return None to allow fallback to direct pyodbc if needed
            return None

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
            # Try to use the engine's connection if available
            if self.engine is not None:
                conn = self.engine.connect().connection
                logger.debug("Database connection created from SQLAlchemy engine.")
                return conn
            else:
                # Fallback to direct pyodbc connection
                conn = pyodbc.connect(connection_string, timeout=10)  # Added timeout
                logger.debug("Database connection created directly with pyodbc.")
                return conn
        except Exception as e:
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
        # Lazy loading: only load alarm codes and manual adjustments when needed
        self.alarms_0_1 = None
        self.manual_adjustments = None
        self._alarm_data_loaded = False

    def _ensure_alarm_data_loaded(self):
        """Ensure alarm codes and manual adjustments are loaded (lazy loading)"""
        if not self._alarm_data_loaded:
            logger.info("Loading alarm codes and manual adjustments...")
            self._load_error_list()
            self.manual_adjustments = self._load_manual_adjustments()
            self._alarm_data_loaded = True

    def _load_error_list(self):
        """Loads and prepares the alarm error list from the Excel file."""
        try:
            excel_path = config.ALARMS_FILE_PATH
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

    def _ensure_manual_adjustments_loaded(self):
        """Ensures manual adjustments are loaded, loading them if necessary."""
        if not hasattr(self, 'manual_adjustments') or self.manual_adjustments is None:
            self.manual_adjustments = self._load_manual_adjustments()

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
            # Prepare the query based on table type
            if table_name == "tblAlarmLog":
                # Ensure alarm data is loaded for tblAlarmLog operations
                self._ensure_alarm_data_loaded()
                if self.alarms_0_1 is None or self.alarms_0_1.empty:
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

            logger.debug(f"Executing state check query for {table_name}")  #: {query}") # Hide query details

            # Use SQLAlchemy engine if available, otherwise fall back to connection pool
            if hasattr(self.connection_pool, 'engine') and self.connection_pool.engine is not None:
                # Use SQLAlchemy engine
                connection = self.connection_pool.engine.raw_connection()
                try:
                    cursor = connection.cursor()
                    cursor.execute(query)
                    result = cursor.fetchone()
                    cursor.close()
                    connection.close()

                    if result:
                        count = result[0]
                        checksum_agg = result[1] if result[1] is not None else 0
                        logger.debug(  # Changed to debug
                            f"DB state for {table_name} ({period_start} to {period_end}): Count={count}, Checksum={checksum_agg}"
                        )
                        return count, checksum_agg
                    else:
                        logger.warning(f"Could not retrieve state for {table_name} for period.")
                        return None, None
                except Exception as e:
                    logger.error(f"Error executing query with SQLAlchemy engine: {e}")
                    connection.close()
                    return None, None
            else:
                # Fall back to connection pool
                with self.connection_pool.get_connection() as conn:
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
                        logger.warning(f"Could not retrieve state for {table_name} for period.")
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
            # Use SQLAlchemy engine if available, otherwise fall back to connection pool
            if hasattr(self.connection_pool, 'engine') and self.connection_pool.engine is not None:
                # Use SQLAlchemy engine directly with pandas
                if table_name == "tblAlarmLog":
                    # Ensure alarm data is loaded for tblAlarmLog operations
                    self._ensure_alarm_data_loaded()
                    if self.alarms_0_1 is None or self.alarms_0_1.empty:
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
                # Use SQLAlchemy engine directly with pandas
                df = pd.read_sql(query, self.connection_pool.engine)
                logger.info(f"Fetched {len(df)} rows from DB for {table_name} using SQLAlchemy engine")
            else:
                # Fall back to using the connection pool if engine is not available
                with self.connection_pool.get_connection() as conn:
                    if table_name == "tblAlarmLog":
                        # Ensure alarm data is loaded for tblAlarmLog operations
                        self._ensure_alarm_data_loaded()
                        if self.alarms_0_1 is None or self.alarms_0_1.empty:
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
                    logger.warning("Using direct pyodbc connection as fallback (SQLAlchemy engine not available)")
                    df = pd.read_sql(query, conn)
                    logger.info(f"Fetched {len(df)} rows from DB for {table_name} using pyodbc connection")

            # Standardize TimeStamp columns (for both SQLAlchemy and pyodbc paths)
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
                adjustment_count = 0
                try:
                    # Apply time_off adjustment if present
                    if "time_off" in adjustment and adjustment["time_off"]:
                        time_off = pd.to_datetime(adjustment["time_off"], errors="coerce")
                        if pd.notna(time_off):  # Check if conversion was successful
                            df_adjusted.loc[mask, "TimeOff"] = time_off
                            adjustment_count += 1
                        else:
                            logger.warning(
                                f"Invalid time_off format in adjustment for ID {adjustment['id']}: {adjustment['time_off']}"
                            )

                    # Apply time_on adjustment if present
                    if "time_on" in adjustment and adjustment["time_on"]:
                        time_on = pd.to_datetime(adjustment["time_on"], errors="coerce")
                        if pd.notna(time_on):  # Check if conversion was successful
                            df_adjusted.loc[mask, "TimeOn"] = time_on
                            adjustment_count += 1
                        else:
                            logger.warning(
                                f"Invalid time_on format in adjustment for ID {adjustment['id']}: {adjustment['time_on']}"
                            )

                    if adjustment_count > 0:
                        adjustments_applied += 1
                        logger.debug(
                            f"Applied adjustment for alarm ID {adjustment['id']}: "
                            f"{'time_on' if 'time_on' in adjustment and adjustment['time_on'] else ''}"
                            f"{' and ' if 'time_on' in adjustment and adjustment['time_on'] and 'time_off' in adjustment and adjustment['time_off'] else ''}"
                            f"{'time_off' if 'time_off' in adjustment and adjustment['time_off'] else ''}"
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
                # Note: We could compare counts and checksums here, but it's more reliable to use metadata
                # Recalculating checksum on existing_df might be needed for accurate check
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
                    # Preserve the original column order from the existing file
                    original_column_order = existing_df.columns.tolist()
                    
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
                        # Add logging before merge
                        logger.debug(f"[_reconcile] Pre-merge db_df columns: {list(db_df.columns)}")
                        logger.debug(f"[_reconcile] Pre-merge db_df dtypes:\n{db_df.dtypes.to_string()}")
                        logger.debug(f"[_reconcile] Pre-merge existing_df columns: {list(existing_df.columns)}")
                        logger.debug(f"[_reconcile] Pre-merge existing_df dtypes:\n{existing_df.dtypes.to_string()}")

                        # Merge based on unique keys
                        merged_df = pd.merge(
                            db_df.add_suffix("_db"),
                            existing_df.add_suffix("_ex"),
                            left_on=[k + "_db" for k in unique_keys],
                            right_on=[k + "_ex" for k in unique_keys],
                            how="outer",
                            indicator=True,
                        )
                        logger.debug(f"[_reconcile] Post-merge merged_df columns: {list(merged_df.columns)}")

                        # Identify new, deleted, and potentially updated rows
                        new_rows = merged_df[merged_df["_merge"] == "left_only"].copy()
                        deleted_rows = merged_df[
                            merged_df["_merge"] == "right_only"
                        ].copy()
                        common_rows = merged_df[merged_df["_merge"] == "both"].copy()

                        # Select only the DB columns and restore original names for new rows
                        db_cols_new = [c for c in new_rows.columns if c.endswith("_db")]
                        new_rows = new_rows[db_cols_new].copy() # Select only _db columns
                        new_rows.columns = [
                            c.replace("_db", "") for c in new_rows.columns # Rename
                        ]

                        # Select only the existing columns and restore original names for deleted rows
                        ex_cols_deleted = [c for c in deleted_rows.columns if c.endswith("_ex")]
                        deleted_rows = deleted_rows[ex_cols_deleted].copy() # Select only _ex columns
                        deleted_rows.columns = [
                            c.replace("_ex", "") for c in deleted_rows.columns # Rename
                        ]

                        # For common rows, check for updates (simple check: keep DB version)
                        # A more robust check would compare non-key columns
                        # Select only the columns from the DB version (_db suffix)
                        db_cols = [c for c in common_rows.columns if c.endswith("_db")]
                        common_rows_corrected = common_rows[db_cols].copy()

                        # Rename columns by removing the suffix
                        common_rows_corrected.columns = [
                            c.replace("_db", "") for c in common_rows_corrected.columns
                        ]
                        logger.debug(f"[_reconcile] Pre-concat common_rows_corrected columns: {list(common_rows_corrected.columns)}")
                        logger.debug(f"[_reconcile] Pre-concat new_rows columns: {list(new_rows.columns)}")
                        logger.debug(f"[_reconcile] Pre-concat deleted_rows columns: {list(deleted_rows.columns)}")

                        # Combine results
                        final_df = pd.concat(
                            [common_rows_corrected, new_rows, deleted_rows],
                            ignore_index=True,
                        )
                        logger.debug(f"[_reconcile] Post-concat final_df columns: {list(final_df.columns)}")
                        # Drop the merge indicator if it exists
                        if "_merge" in final_df.columns:
                            final_df.drop(columns=["_merge"], inplace=True)

                        # Ensure consistent data types and column order to match original file
                        # First, align data types with the original file where possible
                        for col in original_column_order:
                            if col in final_df.columns and col in existing_df.columns:
                                # Preserve the original data type from existing file when possible
                                orig_dtype = existing_df[col].dtype
                                curr_dtype = final_df[col].dtype
                                
                                # Only convert if the data types are compatible and different
                                if orig_dtype != curr_dtype:
                                    # Handle numeric types - preserve original types and format
                                    if pd.api.types.is_numeric_dtype(orig_dtype) and pd.api.types.is_numeric_dtype(curr_dtype):
                                        try:
                                            final_df[col] = final_df[col].astype(orig_dtype)
                                        except (ValueError, TypeError):
                                            # If conversion fails, keep the current type but ensure no decimal points for integers
                                            if 'int' in str(orig_dtype):
                                                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').astype(orig_dtype if orig_dtype in [int, 'int64', 'int32'] else 'float64')
                                    # Handle datetime types
                                    elif pd.api.types.is_datetime64_any_dtype(orig_dtype):
                                        final_df[col] = pd.to_datetime(final_df[col], errors='coerce')
                        
                        # Reorder columns to match the original file order
                        # Only include columns that exist in the final DataFrame
                        existing_cols = [col for col in original_column_order if col in final_df.columns]
                        new_cols = [col for col in final_df.columns if col not in original_column_order]
                        final_column_order = existing_cols + new_cols
                        
                        final_df = final_df.reindex(columns=final_column_order)

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
            # Handle process-existing mode: skip DB/export, process existing file
            if update_mode == "process-existing":
                if os.path.exists(output_path):
                    logger.info(f"[process-existing] Processing existing file: {output_path}")
                    try:
                        df = pd.read_csv(output_path)
                        logger.info(f"[process-existing] {len(df)} rows found in {output_path}")

                        # Apply manual adjustments if this is the alarm table
                        if table_name == "tblAlarmLog" and not df.empty:
                            # Ensure manual adjustments are loaded
                            self._ensure_manual_adjustments_loaded()

                            # Convert time columns to datetime for proper adjustment application
                            for col in ["TimeOn", "TimeOff"]:
                                if col in df.columns:
                                    df[col] = pd.to_datetime(df[col], errors="coerce")

                            # Apply manual adjustments
                            df_adjusted = self._apply_manual_adjustments(df)

                            # Save the updated file with adjustments
                            if not df_adjusted.equals(df):
                                logger.info(f"[process-existing] Saving updated file with manual adjustments: {output_path}")
                                df_adjusted.to_csv(output_path, index=False)
                                logger.debug("[process-existing] Manual adjustments applied, but metadata unchanged (represents DB state)")
                            else:
                                logger.info("[process-existing] No new adjustments to apply")

                        return True
                    except Exception as e:
                        logger.error(f"[process-existing] Failed to read {output_path}: {e}")
                        return False
                else:
                    logger.warning(f"[process-existing] File does not exist: {output_path}")
                    return False

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
                           'process-existing': Skip DB/export, process existing files only.

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
            csv_filename = f"{period}-{file_type}.{FILE_EXTENSION}"
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
    Export data for the given period and file types sequentially
    """
    # Ensure directories exist before starting export
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
        # Process each file type sequentially
        for file_type in file_types:
            task_description = f"Exporting {file_type.upper()}"
            task_id = progress.add_task(
                task_description, total=1
            )  # Total=1 step (export+archive)

            try:
                # Call the export function directly
                result_dict = export_table_to_csv(
                    period,
                    [file_type],  # Pass file_type as a list
                    update_mode=update_mode,
                )
                success = result_dict.get(file_type, False)
                results[file_type] = success
                progress.update(
                    task_id,
                    completed=1,
                    description=f"Exporting {file_type.upper()} - {'OK' if success else 'FAIL'}",
                )
            except Exception as e:
                logger.error(f"Export failed for {file_type}: {e}")
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


# --- Helper Functions ---

def generate_period_range(start_period, end_period=None):
    """
    Generate a list of periods (YYYY-MM) from start_period to end_period (inclusive).
    If end_period is None, returns a list with only start_period.

    Args:
        start_period (str): Start period in YYYY-MM format
        end_period (str, optional): End period in YYYY-MM format. Defaults to None.

    Returns:
        list: List of periods in YYYY-MM format
    """
    # If no end period, return just the start period
    if not end_period:
        return [start_period]

    # Parse start and end dates
    start_date = datetime.strptime(start_period, "%Y-%m")
    end_date = datetime.strptime(end_period, "%Y-%m")

    # Ensure start date is before or equal to end date
    if start_date > end_date:
        logger.error(f"Start period {start_period} is after end period {end_period}")
        return []

    # Generate list of periods
    periods = []
    current_date = start_date

    while current_date <= end_date:
        periods.append(current_date.strftime("%Y-%m"))
        # Move to next month
        current_date = current_date.replace(day=1) + relativedelta(months=1)

    return periods

# --- Standalone Execution ---

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Export data from database to CSV files"
    )
    parser.add_argument(
        "--period-start",
        required=True,
        help="Start period in YYYY-MM format (e.g., 2020-01)"
    )
    parser.add_argument(
        "--period-end",
        help="End period in YYYY-MM format (e.g., 2024-12). If provided, exports all periods from start to end inclusive.",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        help="Specific file types to export (e.g., met din sum). Exports all types if omitted.",
    )
    parser.add_argument(
        "--update-mode",
        choices=["check", "append", "force-overwrite", "process-existing"],
        default="append",
        help="Mode for handling existing data exports: 'check' (report changes), 'append' (update/append preserving deletions), 'force-overwrite' (export fresh data), 'process-existing' (skip export/check, process existing files only).",
    )

    args = parser.parse_args()

    # Validate period format (basic check)
    try:
        datetime.strptime(args.period_start, "%Y-%m")
        if args.period_end:
            datetime.strptime(args.period_end, "%Y-%m")
    except ValueError:
        logger.error("Invalid period format. Please use YYYY-MM format (e.g., 2023-01).")
        exit(1)

    # Generate list of periods to process
    periods = generate_period_range(args.period_start, args.period_end)
    if not periods:
        logger.error("No valid periods to process.")
        exit(1)

    # Process each period
    for period in periods:
        logger.info(f"\n=== Processing period: {period} ===")
        # Run the main export flow for this period
        main_export_flow(period, args.types, args.update_mode)
