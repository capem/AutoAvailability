import sys
import os
import pyodbc
import queue
import logging
import zipfile
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
                    except:
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

    def construct_query(period_start, period_end, alarms_0_1):
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

    def export_table_data(self, table_name, period, output_path):
        """
        Export data from a SQL Server table to a CSV file

        Args:
            table_name: Name of the SQL Server table
            period: Period in YYYY-MM format
            output_path: Path where the CSV file should be created

        Returns:
            True if successful, False otherwise
        """
        # Create period date range for filtering
        period_dt = datetime.strptime(period, "%Y-%m")
        next_month = datetime(
            period_dt.year + (1 if period_dt.month == 12 else 0),
            (period_dt.month % 12) + 1,
            1,
        )
        period_start = f"{period}-01 00:00:00.000"
        period_end = f"{next_month.strftime('%Y-%m')}-01 00:00:00.000"

        # For tblAlarmLog (sum), use the same query logic as in sql_alarms.py
        if table_name == "tblAlarmLog":
            return self._export_alarms(period_start, period_end, output_path)

        # For other tables, export data for the given period
        return self._export_regular_table(
            table_name, period_start, period_end, output_path
        )

    def _export_alarms(self, period_start, period_end, output_path):
        """Export alarm data"""
        try:
            # Read error list to get alarm codes
            error_list = pd.read_excel(r"Alarmes List Norme RDS-PP_Tarec.xlsx")
            error_list.Number = error_list.Number.astype(int)
            error_list.drop_duplicates(subset=["Number"], inplace=True)
            error_list.rename(columns={"Number": "Alarmcode"}, inplace=True)
            alarms_0_1 = error_list.loc[error_list["Error Type"].isin([1, 0])].Alarmcode

            # Construct query as in sql_alarms.py
            query = self.construct_query(period_start, period_end, alarms_0_1)

            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Execute query and save results to CSV
            with self.connection_pool.get_connection() as conn:
                df = pd.read_sql(query, conn)

                if df.empty:
                    logger.warning("No alarm data found for the specified period")
                    # Create an empty file with headers
                    df.to_csv(output_path, index=False)
                    return True

                # Save to CSV
                df.to_csv(output_path, index=False)
                logger.info(f"Successfully exported alarm data to {output_path}")
                return True

        except Exception as e:
            logger.error(f"Failed to export alarm data: {e}")
            return False

    def _get_columns_for_table(self, table_name):
        """
        Get the specific columns to query for each table type

        Args:
            table_name: Name of the SQL Server table

        Returns:
            String of column names for the SQL query
        """
        if table_name == "tblSCMet":
            return """TimeStamp, StationId, met_WindSpeedRot_mean, met_WinddirectionRot_mean"""

        elif table_name == "tblSCTurbine":
            return """TimeStamp, StationId, wtc_AcWindSp_mean, wtc_AcWindSp_stddev,
                   wtc_ActualWindDirection_mean, wtc_ActualWindDirection_stddev"""

        elif table_name == "tblSCTurGrid":
            return """TimeStamp, StationId, wtc_ActPower_min, wtc_ActPower_max, wtc_ActPower_mean"""

        elif table_name == "tblSCTurCount":
            return """TimeStamp, StationId, wtc_kWG1Tot_accum, wtc_kWG1TotE_accum, wtc_kWG1TotI_accum"""

        elif table_name == "tblSCTurDigiIn":
            return """TimeStamp, StationId, wtc_PowerRed_timeon"""

        # Default case, return all columns
        return "*"

    def _export_regular_table(self, table_name, period_start, period_end, output_path):
        """Export regular table data for the given period to CSV"""
        try:
            # Get specific columns for this table
            columns = self._get_columns_for_table(table_name)

            # Build query based on table type with specific columns
            query = f"""
            SELECT {columns} FROM {table_name}
            WHERE TimeStamp >= '{period_start}' AND TimeStamp < '{period_end}'
            """

            # Execute query
            with self.connection_pool.get_connection() as conn:
                logger.info(f"Executing query: {query}")
                df = pd.read_sql(query, conn)

                if df.empty:
                    logger.warning(
                        f"No data found for {table_name} in period {period_start} to {period_end}"
                    )
                    # Create an empty file with headers
                    df.to_csv(output_path, index=False)
                    return True

                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                # Save to CSV
                df.to_csv(output_path, index=False)

                logger.info(f"Successfully exported {len(df)} rows to {output_path}")
                return True

        except Exception as e:
            logger.error(f"Failed to export {table_name} data: {e}")
            return False


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


def export_and_archive_tables(period, file_types=None):
    """
    Export tables to CSV files and create ZIP archives

    Args:
        period: Period in YYYY-MM format
        file_types: List of file types to export, defaults to all

    Returns:
        Dict with results for each file type
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
            export_success = exporter.export_table_data(table_name, period, csv_path)

            if export_success:
                # Create ZIP archive
                logger.info(f"Creating archive {zip_path}")
                archive_success = ArchiveCreator.create_archive(csv_path, zip_path)
                results[file_type] = archive_success
            else:
                results[file_type] = False
    finally:
        # Close all connections
        pool.close_all()

    return results


if __name__ == "__main__":
    # Example usage

    if len(sys.argv) < 2:
        print("Usage: python db_export.py YYYY-MM [file_type1 file_type2 ...]")
        sys.exit(1)

    period = sys.argv[1]
    file_types = sys.argv[2:] if len(sys.argv) > 2 else None

    results = export_and_archive_tables(period, file_types)

    # Print results
    print("\nExport Results:")
    for file_type, success in results.items():
        print(f"{file_type}: {'SUCCESS' if success else 'FAILED'}")
