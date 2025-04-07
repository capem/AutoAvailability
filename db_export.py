import os
import pyodbc
import queue
import logging
import zipfile
import json  # Added for metadata handling
import hashlib  # Added for row hashing during reconciliation
from contextlib import contextmanager
from datetime import datetime
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("db_export")

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

# File extension to use for exports
FILE_EXTENSION = "csv"

# Output directories
BASE_EXPORT_PATH = "./monthly_data/exports"
BASE_UPLOAD_PATH = "./monthly_data/uploads"
METADATA_EXTENSION = ".meta.json"  # Added for metadata files


class ConnectionPool:
    """A simple connection pool for reusing database connections"""

    def __init__(self, max_connections=5):
        """Initialize the connection pool

        Args:
            max_connections: Maximum number of connections in the pool
        """
        self.pool = queue.Queue(max_connections)
        self.size = max_connections
        self._create_connections()

    def _create_connections(self):
        """Create initial connections and add them to the pool"""
        for _ in range(self.size):
            conn = self._create_connection()
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
            conn = pyodbc.connect(connection_string)
            return conn
        except pyodbc.Error as e:
            logger.error(f"Failed to create database connection: {str(e)}")
            raise

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool

        Yields:
            A database connection that will be returned to the pool
        """
        conn = None
        try:
            conn = self.pool.get(block=True, timeout=30)
            yield conn
        finally:
            if conn:
                try:
                    # Make sure connection is still alive
                    conn.execute("SELECT 1")
                    self.pool.put(conn)
                except pyodbc.Error:
                    # If connection is broken, create a new one
                    logger.warning("Connection is broken, replacing with a new one")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    self.pool.put(self._create_connection())

    def close_all(self):
        """Close all connections in the pool"""
        for _ in range(self.size):
            try:
                conn = self.pool.get(block=False)
                conn.close()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")


class DBExporter:
    """Handles exporting data from SQL Server to CSV files"""

    def __init__(self, connection_pool):
        """Initialize the exporter

        Args:
            connection_pool: Database connection pool
        """
        self.connection_pool = connection_pool
        self._load_error_list()  # Load error list once during initialization

    def _load_error_list(self):
        """Loads and prepares the alarm error list from the Excel file."""
        try:
            # Use raw string for path to avoid backslash issues
            excel_path = r"Alarmes List Norme RDS-PP_Tarec.xlsx"
            if not os.path.exists(excel_path):
                logger.error(f"Error list file not found at: {excel_path}")
                self.alarms_0_1 = pd.Series(dtype=int)  # Empty series if file not found
                return

            error_list = pd.read_excel(excel_path)
            error_list.Number = error_list.Number.astype(int)
            error_list.drop_duplicates(subset=["Number"], inplace=True)
            error_list.rename(columns={"Number": "Alarmcode"}, inplace=True)
            self.alarms_0_1 = error_list.loc[
                error_list["Error Type"].isin([1, 0])
            ].Alarmcode
            logger.info(
                f"Loaded {len(self.alarms_0_1)} alarm codes for type 0/1 from {excel_path}"
            )
        except Exception as e:
            logger.error(f"Failed to load or process error list from Excel: {e}")
            self.alarms_0_1 = pd.Series(dtype=int)  # Ensure it's defined even on error

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
            logger.info(f"Metadata written to {metadata_path}")
        except IOError as e:
            logger.error(f"Could not write metadata file {metadata_path}: {e}")

    def _get_unique_keys(self, table_name):
        """Returns the list of unique key columns for a table."""
        if table_name == "tblAlarmLog":
            return ["ID"]
        else:
            # Assuming TimeStamp and StationId for others based on prior info
            return ["TimeStamp", "StationId"]

    def _get_checksum_columns(self, table_name):
        """Returns the list of columns to include in the checksum calculation."""
        # Exclude unique keys if they are simple IDs, include them if they are part of the data
        if table_name == "tblAlarmLog":
            # Exclude ID as it's just an identifier, checksum the actual data
            return "[TimeOn], [TimeOff], [StationNr], [Alarmcode], [Parameter]"
        else:
            # For other tables, include all selected columns in checksum
            # Use the same columns as selected in _get_columns_for_table
            return self._get_columns_for_table(table_name)

    def check_data_state(self, table_name, period_start, period_end):
        """
        Checks the current state (count and checksum) of data in the DB for the period.
        """
        query = ""
        checksum_cols = self._get_checksum_columns(table_name)

        try:
            with self.connection_pool.get_connection() as conn:
                if table_name == "tblAlarmLog":
                    # Use the specific alarm query structure for count and checksum
                    # Ensure alarms_0_1 is loaded
                    if not hasattr(self, "alarms_0_1") or self.alarms_0_1.empty:
                        logger.warning(
                            "alarms_0_1 not loaded or empty, cannot perform accurate check for tblAlarmLog."
                        )
                        # Fallback or error handling needed? For now, return None to indicate check failure.
                        # Alternatively, could attempt load here, but might be slow.
                        # Let's try loading if not present.
                        self._load_error_list()
                        if self.alarms_0_1.empty:
                            logger.error(
                                "Failed to load alarms_0_1 for tblAlarmLog check."
                            )
                            return None, None  # Indicate check failure

                    # Convert list/Series to tuple for SQL IN clause
                    alarm_codes_tuple = tuple(self.alarms_0_1.tolist())
                    if not alarm_codes_tuple:  # Handle empty tuple case
                        alarm_codes_tuple = (
                            "NULL",
                        )  # Avoid SQL syntax error, though likely no matches

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
                        CHECKSUM_AGG(CAST(BINARY_CHECKSUM({checksum_cols}) AS BIGINT))
                    FROM [WpsHistory].[dbo].[tblAlarmLog]
                    {where_clause}
                    """
                else:
                    # Standard query for other tables
                    where_clause = f"WHERE TimeStamp >= '{period_start}' AND TimeStamp < '{period_end}'"
                    query = f"""
                    SET NOCOUNT ON;
                    SELECT
                        COUNT_BIG(*),
                        CHECKSUM_AGG(CAST(BINARY_CHECKSUM({checksum_cols}) AS BIGINT))
                    FROM {table_name}
                    {where_clause}
                    """

                logger.debug(f"Executing state check query for {table_name}: {query}")
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()

                if result:
                    count = result[0]
                    checksum_agg = (
                        result[1] if result[1] is not None else 0
                    )  # Handle NULL checksum for 0 rows
                    logger.info(
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
                    # Ensure alarms_0_1 is loaded
                    if not hasattr(self, "alarms_0_1") or self.alarms_0_1.empty:
                        self._load_error_list()
                        if self.alarms_0_1.empty:
                            logger.error(
                                "Failed to load alarms_0_1 for tblAlarmLog fetch."
                            )
                            return pd.DataFrame()  # Return empty DataFrame on failure
                    query = self.construct_query(
                        period_start, period_end, self.alarms_0_1
                    )  # Use existing method
                else:
                    columns = self._get_columns_for_table(table_name)
                    query = f"""
                    SELECT {columns} FROM {table_name}
                    WHERE TimeStamp >= '{period_start}' AND TimeStamp < '{period_end}'
                    """
                logger.info(
                    f"Fetching data for {table_name} ({period_start} to {period_end})"
                )
                df = pd.read_sql(query, conn)
                logger.info(f"Fetched {len(df)} rows from DB for {table_name}")

                # Standardize TimeStamp column if exists
                if "TimeStamp" in df.columns:
                    df["TimeStamp"] = pd.to_datetime(df["TimeStamp"])
                if "TimeOn" in df.columns:
                    df["TimeOn"] = pd.to_datetime(df["TimeOn"])
                if "TimeOff" in df.columns:
                    df["TimeOff"] = pd.to_datetime(df["TimeOff"])

                return df
        except Exception as e:
            logger.error(f"Failed to fetch data for {table_name}: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error

    def _hash_row(self, row, columns_to_hash):
        """Creates a hash for a pandas Series based on specified columns."""
        hash_input = "".join(
            str(row[col]) for col in columns_to_hash if col in row.index
        )
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _reconcile_and_export(
        self, table_name, period_start, period_end, output_path, db_count, db_checksum
    ):
        """Performs data reconciliation and exports the final CSV."""
        try:
            # 1. Fetch current data from DB
            db_df = self._fetch_db_data(table_name, period_start, period_end)
            if db_df.empty and db_count > 0:
                logger.warning(
                    f"DB fetch returned empty but count was {db_count}. Proceeding with empty DB data."
                )
                # This might indicate an issue, but we proceed based on the fetch result.

            # 2. Read existing CSV
            existing_df = pd.DataFrame()
            if os.path.exists(output_path):
                try:
                    existing_df = pd.read_csv(output_path)
                    logger.info(
                        f"Read {len(existing_df)} rows from existing file {output_path}"
                    )
                    # Convert timestamp columns in existing data
                    if "TimeStamp" in existing_df.columns:
                        existing_df["TimeStamp"] = pd.to_datetime(
                            existing_df["TimeStamp"]
                        )
                    if "TimeOn" in existing_df.columns:
                        existing_df["TimeOn"] = pd.to_datetime(existing_df["TimeOn"])
                    if "TimeOff" in existing_df.columns:
                        existing_df["TimeOff"] = pd.to_datetime(existing_df["TimeOff"])

                except Exception as e:
                    logger.error(
                        f"Failed to read or parse existing CSV {output_path}: {e}. Treating as empty."
                    )
                    existing_df = pd.DataFrame()  # Ensure it's an empty DF on error

            # Ensure consistent columns if one is empty
            if existing_df.empty and not db_df.empty:
                existing_df = pd.DataFrame(columns=db_df.columns)
            elif db_df.empty and not existing_df.empty:
                db_df = pd.DataFrame(columns=existing_df.columns)
            elif db_df.empty and existing_df.empty:
                logger.warning(
                    f"Both DB and existing file are empty for {table_name} {period_start}-{period_end}. Writing empty file."
                )
                # Create an empty file with headers if possible (need headers)
                # We might not have headers if both are truly empty. Let's try getting headers from DB schema if needed,
                # but for now, just write an empty file. If db_df had columns, use those.
                final_df = (
                    db_df if not db_df.empty else existing_df
                )  # Use whichever has columns defined
                final_df.to_csv(output_path, index=False)
                self._write_metadata(
                    self._get_metadata_path(output_path), db_count, db_checksum
                )
                return "EXPORT_DONE"  # Technically done, wrote empty file

            # 3. Determine unique keys and data columns
            unique_keys = self._get_unique_keys(table_name)
            data_columns = [col for col in db_df.columns if col not in unique_keys]

            # Ensure key columns exist in both dataframes for merging
            if not all(key in db_df.columns for key in unique_keys) or not all(
                key in existing_df.columns for key in unique_keys
            ):
                logger.error(
                    f"Unique key columns missing in DB or existing data for {table_name}. Cannot reconcile."
                )
                # Fallback: Overwrite with DB data? Or fail? Let's overwrite for now.
                logger.warning(
                    f"Overwriting {output_path} with current DB data due to key mismatch."
                )
                db_df.to_csv(output_path, index=False)
                self._write_metadata(
                    self._get_metadata_path(output_path), db_count, db_checksum
                )
                return "EXPORT_DONE"

            # 4. Merge DataFrames
            # Add prefixes to distinguish columns after merge
            db_df_prefixed = db_df.add_prefix("db_")
            existing_df_prefixed = existing_df.add_prefix("ex_")

            # Rename key columns for merging
            db_merge_keys = {f"db_{k}": k for k in unique_keys}
            ex_merge_keys = {f"ex_{k}": k for k in unique_keys}
            db_df_prefixed.rename(columns=db_merge_keys, inplace=True)
            existing_df_prefixed.rename(columns=ex_merge_keys, inplace=True)

            merged_df = pd.merge(
                db_df_prefixed,
                existing_df_prefixed,
                on=unique_keys,  # Merge on the original key names
                how="outer",
                indicator=True,
            )

            # 5. Identify Changes
            deleted_in_db_mask = merged_df["_merge"] == "right_only"
            new_in_db_mask = merged_df["_merge"] == "left_only"
            potentially_modified_mask = merged_df["_merge"] == "both"

            # Extract rows based on masks
            deleted_rows = merged_df[deleted_in_db_mask]
            new_rows = merged_df[new_in_db_mask]
            potentially_modified_rows = merged_df[
                potentially_modified_mask
            ].copy()  # Use copy to avoid SettingWithCopyWarning

            # 6. Identify Actual Modifications
            modified_rows_list = []
            unchanged_rows_list = []
            if not potentially_modified_rows.empty:
                # Define columns to hash (non-key columns)
                db_data_cols_prefixed = [f"db_{col}" for col in data_columns]
                ex_data_cols_prefixed = [f"ex_{col}" for col in data_columns]

                # Calculate hashes - handle missing columns gracefully
                potentially_modified_rows["db_hash"] = potentially_modified_rows.apply(
                    lambda row: self._hash_row(row, db_data_cols_prefixed), axis=1
                )
                potentially_modified_rows["ex_hash"] = potentially_modified_rows.apply(
                    lambda row: self._hash_row(row, ex_data_cols_prefixed), axis=1
                )

                modified_mask = (
                    potentially_modified_rows["db_hash"]
                    != potentially_modified_rows["ex_hash"]
                )
                modified_rows_list.append(potentially_modified_rows[modified_mask])
                unchanged_rows_list.append(potentially_modified_rows[~modified_mask])

            modified_rows = (
                pd.concat(modified_rows_list) if modified_rows_list else pd.DataFrame()
            )
            unchanged_rows = (
                pd.concat(unchanged_rows_list)
                if unchanged_rows_list
                else pd.DataFrame()
            )

            # 7. Construct Final DataFrame
            final_dfs = []

            # Add deleted rows (from existing data)
            if not deleted_rows.empty:
                deleted_final = deleted_rows[
                    unique_keys + [f"ex_{col}" for col in data_columns]
                ]
                deleted_final.columns = (
                    unique_keys + data_columns
                )  # Rename columns back
                final_dfs.append(deleted_final)
                logger.info(f"Preserving {len(deleted_final)} rows deleted from DB.")

            # Add new rows (from DB data)
            if not new_rows.empty:
                new_final = new_rows[
                    unique_keys + [f"db_{col}" for col in data_columns]
                ]
                new_final.columns = unique_keys + data_columns  # Rename columns back
                final_dfs.append(new_final)
                logger.info(f"Adding {len(new_final)} new rows from DB.")

            # Add modified rows (use DB version)
            if not modified_rows.empty:
                modified_final = modified_rows[
                    unique_keys + [f"db_{col}" for col in data_columns]
                ]
                modified_final.columns = (
                    unique_keys + data_columns
                )  # Rename columns back
                final_dfs.append(modified_final)
                logger.info(f"Updating {len(modified_final)} modified rows from DB.")

            # Add unchanged rows (use DB version - could use existing too)
            if not unchanged_rows.empty:
                unchanged_final = unchanged_rows[
                    unique_keys + [f"db_{col}" for col in data_columns]
                ]
                unchanged_final.columns = (
                    unique_keys + data_columns
                )  # Rename columns back
                final_dfs.append(unchanged_final)
                # logger.info(f"Keeping {len(unchanged_final)} unchanged rows.")

            # Concatenate all parts
            if not final_dfs:
                logger.warning(
                    f"No rows identified for final output for {table_name}. Writing empty file."
                )
                final_df = pd.DataFrame(
                    columns=db_df.columns
                )  # Use original DB columns if possible
            else:
                final_df = pd.concat(final_dfs, ignore_index=True)

            # Ensure correct column order (using original db_df columns as reference)
            final_df = final_df[db_df.columns]

            # 8. Write Final CSV
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            final_df.to_csv(output_path, index=False)
            logger.info(
                f"Successfully reconciled and exported {len(final_df)} rows to {output_path}"
            )

            # 9. Write Metadata
            metadata_path = self._get_metadata_path(output_path)
            self._write_metadata(metadata_path, db_count, db_checksum)

            return "EXPORT_DONE"

        except Exception as e:
            logger.exception(
                f"Error during reconciliation for {table_name}: {e}"
            )  # Use exception for stack trace
            return "EXPORT_FAILED"

    def construct_query(self, period_start, period_end, alarms_0_1):
        # Constructing the IN clause for the SQL query
        query = f"""
        SET NOCOUNT ON;
        SELECT [TimeOn]
        ,[TimeOff]
        ,[StationNr]
        ,[Alarmcode]
        ,[Parameter]
        ,[ID]
    FROM [WpsHistory].[dbo].[tblAlarmLog]
    WHERE ([Alarmcode] <> 50100)
    AND (
        ([TimeOff] BETWEEN '{period_start}' AND '{period_end}')
        OR ([TimeOn] BETWEEN '{period_start}' AND '{period_end}')
        OR ([TimeOn] <= '{period_start}' AND [TimeOff] >= '{period_end}')
        OR ([TimeOff] IS NULL AND [Alarmcode] IN {tuple(alarms_0_1.tolist())}))"""

        return query

    def export_table_data(self, table_name, period, output_path, update_mode="append"):
        """
        Export data from a SQL Server table to a CSV file, with change detection.

        Args:
            table_name: Name of the SQL Server table.
            period: Period in YYYY-MM format.
            output_path: Path where the CSV file should be created.
            update_mode: 'check', 'append', or 'force-overwrite'.

        Returns:
            Status string: 'NO_CHANGE', 'CHANGES_DETECTED', 'EXPORT_DONE', 'EXPORT_FAILED'.
        """
        logger.info(
            f"Starting export for {table_name}, period {period}, mode '{update_mode}'"
        )
        metadata_path = self._get_metadata_path(output_path)

        # Calculate period start/end
        try:
            period_dt = datetime.strptime(period, "%Y-%m")
            next_month_dt = datetime(
                period_dt.year + (1 if period_dt.month == 12 else 0),
                (period_dt.month % 12) + 1,
                1,
            )
            period_start = f"{period}-01 00:00:00.000"
            period_end = f"{next_month_dt.strftime('%Y-%m')}-01 00:00:00.000"
        except ValueError:
            logger.error(f"Invalid period format: {period}. Expected YYYY-MM.")
            return "EXPORT_FAILED"

        # --- State Check ---
        db_count, db_checksum = None, None
        if update_mode != "force-overwrite":
            db_count, db_checksum = self.check_data_state(
                table_name, period_start, period_end
            )
            if db_count is None:  # Check failed
                logger.error(
                    f"Failed to get DB state for {table_name}. Cannot proceed with check/append."
                )
                # Decide fallback: maybe force-overwrite? Or just fail? Let's fail for safety.
                return "EXPORT_FAILED"

            stored_count, stored_checksum = self._read_metadata(metadata_path)

            # Check if state matches stored metadata
            if (
                stored_count is not None
                and stored_checksum is not None
                and db_count == stored_count
                and db_checksum == stored_checksum
            ):
                logger.info(
                    f"No changes detected for {table_name} period {period} based on count and checksum. Skipping export."
                )
                return "NO_CHANGE"
            else:
                if stored_count is None:
                    logger.info(
                        f"No previous metadata found for {table_name} period {period}. Proceeding with export/reconciliation."
                    )
                else:
                    logger.info(
                        f"Change detected for {table_name} period {period}. DB(count={db_count}, chk={db_checksum}) vs Stored(count={stored_count}, chk={stored_checksum})."
                    )

        # --- Action based on mode ---
        if update_mode == "check":
            # We already detected changes above if we reached here
            logger.info(
                f"Update mode is 'check'. Reporting changes detected for {table_name} period {period}."
            )
            return "CHANGES_DETECTED"

        elif update_mode == "append" or update_mode == "force-overwrite":
            if update_mode == "force-overwrite":
                logger.info(
                    f"Update mode is 'force-overwrite'. Forcing reconciliation/export for {table_name} period {period}."
                )
                # We might not have checked the state if forcing, get it now if needed for metadata writing later
                if db_count is None:
                    db_count, db_checksum = self.check_data_state(
                        table_name, period_start, period_end
                    )
                    if db_count is None:
                        logger.error(
                            "Failed to get DB state even for force-overwrite metadata. Failing export."
                        )
                        return "EXPORT_FAILED"

            # Perform reconciliation (which includes fetching DB data and writing CSV/metadata)
            return self._reconcile_and_export(
                table_name, period_start, period_end, output_path, db_count, db_checksum
            )

        else:
            logger.error(f"Invalid update_mode: {update_mode}")
            return "EXPORT_FAILED"

    # Removed _export_alarms - logic is now integrated into _fetch_db_data and _reconcile_and_export

    def _get_columns_for_table(self, table_name):
        """
        Get the specific columns to query for each table type

        Args:
            table_name: Name of the SQL Server table

        Returns:
            String of column names for the SQL query
        """
        if table_name == "tblSCMet":
            return """TimeStamp, StationId, met_WindSpeedRot_mean, met_WinddirectionRot_mean, met_Pressure_mean, met_TemperatureRot_mean"""

        elif table_name == "tblSCTurbine":
            return """TimeStamp, StationId, wtc_AcWindSp_mean, wtc_AcWindSp_stddev,
                   wtc_ActualWindDirection_mean, wtc_ActualWindDirection_stddev"""

        elif table_name == "tblSCTurGrid":
            return """TimeStamp, StationId, wtc_ActPower_min, wtc_ActPower_max, wtc_ActPower_mean"""

        elif table_name == "tblSCTurCount":
            return """TimeStamp, StationId, wtc_kWG1Tot_accum, wtc_kWG1TotE_accum, wtc_kWG1TotI_accum, wtc_BoostKWh_endvalue, wtc_BostkWhS_endvalue"""

        elif table_name == "tblSCTurDigiIn":
            return """TimeStamp, StationId, wtc_PowerRed_timeon"""

        # Default case, return all columns
        return "*"

    # Removed _export_regular_table - logic is now integrated into _fetch_db_data and _reconcile_and_export


class ArchiveCreator:
    """Handles creating ZIP archives from CSV files"""

    @staticmethod
    def create_archive(source_path, output_path):
        """
        Create a ZIP archive containing a CSV file

        Args:
            source_path: Path to the CSV file
            output_path: Path where the ZIP file should be created

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Create ZIP archive
            with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                # Add CSV file to archive
                zipf.write(source_path, os.path.basename(source_path))

            logger.info(f"Created archive {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to create archive: {e}")
            return False


def export_and_archive_tables(period, file_types=None, update_mode="append"):
    """
    Export tables to CSV files (with change detection) and create ZIP archives.

    Args:
        period (str): Period in YYYY-MM format.
        file_types (list, optional): List of file types ('sum', 'met', etc.) to export. Defaults to all.
        update_mode (str): 'check', 'append', or 'force-overwrite'. Defaults to 'append'.

    Returns:
        dict: Results for each file type, indicating status ('SUCCESS', 'FAILED', 'NO_CHANGE', 'CHECK_DIFF').
    """
    if file_types is None:
        file_types = TABLE_MAPPINGS.keys()

    # Create connection pool
    pool = ConnectionPool()

    # Create exporter
    exporter = DBExporter(pool)

    results = {}

    try:
        for file_type in file_types:
            logger.info(f"Processing {file_type} for period {period}")

            # Get table name
            table_name = TABLE_MAPPINGS.get(file_type)
            if not table_name:
                logger.error(f"No table mapping found for {file_type}")
                results[file_type] = False
                continue

            # Define paths
            export_dir = os.path.join(BASE_EXPORT_PATH, file_type.upper())
            os.makedirs(export_dir, exist_ok=True)

            csv_path = os.path.join(
                export_dir, f"{period}-{file_type.lower()}.{FILE_EXTENSION}"
            )
            zip_path = os.path.join(
                BASE_UPLOAD_PATH, file_type.upper(), f"{period}-{file_type.lower()}.zip"
            )

            # Export data to CSV
            logger.info(f"Exporting {table_name} to {csv_path}")
            export_status = exporter.export_table_data(
                table_name, period, csv_path, update_mode
            )

            # Determine overall result for this file type
            if export_status == "EXPORT_DONE":
                # Create ZIP archive only if export was done
                logger.info(f"Creating archive {zip_path} for {file_type}")
                archive_success = ArchiveCreator.create_archive(csv_path, zip_path)
                results[file_type] = "SUCCESS" if archive_success else "ARCHIVE_FAILED"
            elif export_status == "NO_CHANGE":
                results[file_type] = "NO_CHANGE"
            elif export_status == "CHANGES_DETECTED":
                # This status only happens in 'check' mode
                results[file_type] = "CHECK_DIFF"
            else:  # EXPORT_FAILED or other errors
                results[file_type] = "FAILED"
    finally:
        # Close all connections
        pool.close_all()

    return results


if __name__ == "__main__":
    # Example usage

    import argparse

    parser = argparse.ArgumentParser(
        description="Export data from SQL Server with change detection."
    )
    parser.add_argument("period", help="Period in YYYY-MM format.")
    parser.add_argument(
        "file_types",
        nargs="*",
        help="Optional list of file types (e.g., sum met tur). Exports all if omitted.",
    )
    parser.add_argument(
        "--update-mode",
        choices=["check", "append", "force-overwrite"],
        default="append",
        help="Mode for handling existing data: 'check' (report changes), 'append' (update/append preserving deletions - default), 'force-overwrite' (export fresh data).",
    )
    args = parser.parse_args()

    period = args.period
    file_types = args.file_types if args.file_types else None
    update_mode = args.update_mode

    results = export_and_archive_tables(period, file_types, update_mode)

    # Print results
    print("\nExport Results:")
    for file_type, status in results.items():
        print(f"{file_type}: {status}")
