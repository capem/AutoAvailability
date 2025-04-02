# --- Add time import ---
import time
import pyodbc
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import csv
import os
import sys
from tqdm import tqdm
from sqlalchemy import create_engine
import urllib

# --- Constants ---
DEBUG_PERFORMANCE = True  # Set to False to disable performance timing/reporting

DB_SERVER = "10.173.224.101"
DB_DATABASE = "WpsHistory"
DB_USERNAME = "odbc_user"
DB_PASSWORD_QUOTED = urllib.parse.quote_plus("0dbc@1cust")

DB_DRIVER_NAME = "ODBC Driver 11 for SQL Server"
DB_DRIVER_QUOTED = urllib.parse.quote_plus(DB_DRIVER_NAME)  # Quote the plain name

DB_TIMEOUT = 15

OUTPUT_DIR = "./monthly_data/uploads/SUM/"
EXCEL_FILE_NAME = "Alarmes List Norme RDS-PP_Tarec.xlsx"

START_DATE = dt(2023, 8, 1)
END_DATE = dt(2025, 1, 1)
# -----------------


# --- Helper function for PyInstaller data files ---
def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller"""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.abspath(__file__))
        if not base_path:
            base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


# ----------------------------------------------------


def read_alarm_list(excel_file_path):
    """Reads the Excel file and returns a Series of Alarmcodes for type 0 and 1."""
    t_start = time.perf_counter() if DEBUG_PERFORMANCE else 0
    tqdm.write(f"Reading alarm list from: {excel_file_path}")
    alarms = None  # Initialize
    try:
        if not os.path.exists(excel_file_path):
            raise FileNotFoundError(f"Excel file not found at {excel_file_path}")

        df_errors = pd.read_excel(excel_file_path)

        required_cols = ["Number", "Error Type"]
        if not all(col in df_errors.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df_errors.columns]
            raise ValueError(f"Missing required columns in Excel: {', '.join(missing)}")

        df_errors["Number"] = pd.to_numeric(df_errors["Number"], errors="coerce")
        df_errors.dropna(subset=["Number"], inplace=True)
        df_errors["Number"] = df_errors["Number"].astype(int)
        df_errors.drop_duplicates(subset=["Number"], inplace=True)
        df_errors.rename(columns={"Number": "Alarmcode"}, inplace=True)

        alarms = df_errors.loc[df_errors["Error Type"].isin([0, 1]), "Alarmcode"]

        if alarms.empty:
            tqdm.write(
                f"Warning: No alarms of type 0 or 1 found in {os.path.basename(excel_file_path)}."
            )
        else:
            tqdm.write(f"Found {len(alarms)} alarms of type 0 or 1.")

        return alarms

    except Exception as e:
        print(f"\nFATAL ERROR reading/processing Excel file: {e}")
        print("Cannot continue without the alarm list.")
        return None
    finally:
        # --- Record Excel Read Time ---
        if DEBUG_PERFORMANCE:
            t_end = time.perf_counter()
            # Store duration on the function object (simple way for one-off timing)
            read_alarm_list.duration = t_end - t_start


def construct_query(period_start_str, period_end_str, alarms_0_1_series):
    """
    Constructs the SQL query string with formatted dates and IN clause.
    Handles empty alarm list. Returns query string and construction duration.
    """
    t_start = time.perf_counter() if DEBUG_PERFORMANCE else 0

    alarm_list = alarms_0_1_series.tolist()

    if not alarm_list:
        in_clause_tuple_str = "(-1)"
    elif len(alarm_list) == 1:
        in_clause_tuple_str = f"({alarm_list[0]})"
    else:
        in_clause_tuple_str = str(tuple(alarm_list))

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
        ([TimeOn] < '{period_end_str}' AND [TimeOff] >= '{period_start_str}')
        OR ([TimeOn] >= '{period_start_str}' AND [TimeOn] < '{period_end_str}')
        OR ([TimeOn] < '{period_start_str}' AND [TimeOff] IS NULL AND [Alarmcode] IN {in_clause_tuple_str})
    )
    ORDER BY [TimeOn];
    """
    duration = (time.perf_counter() - t_start) if DEBUG_PERFORMANCE else 0
    return query, duration


def process_and_export_period(
    period_str, period_start_str, period_end_str, alarms_0_1, engine, output_dir
):
    """
    Fetches data using Pandas via SQLAlchemy engine and exports to RPT for a single period.
    Returns a dictionary with timing information if DEBUG_PERFORMANCE is True.
    """
    timing_info = {"period": period_str} if DEBUG_PERFORMANCE else None
    t_period_start = time.perf_counter() if DEBUG_PERFORMANCE else 0

    output_file = os.path.join(output_dir, f"{period_str}-sum.rpt")

    query, construct_duration = construct_query(
        period_start_str, period_end_str, alarms_0_1
    )
    if DEBUG_PERFORMANCE:
        timing_info["1_construct_query_s"] = construct_duration

    df = pd.DataFrame()
    db_fetch_duration = 0
    csv_write_duration = 0
    rows_fetched = 0
    write_successful = False

    try:
        t_db_start = time.perf_counter() if DEBUG_PERFORMANCE else 0
        tqdm.write(f"[{period_str}] Connecting via engine & executing query...")
        df = pd.read_sql_query(query, engine)
        rows_fetched = len(df)
        if DEBUG_PERFORMANCE:
            t_db_end = time.perf_counter()
            db_fetch_duration = t_db_end - t_db_start
            timing_info["2_db_fetch_s"] = db_fetch_duration
            timing_info["rows_fetched"] = rows_fetched
        tqdm.write(f"[{period_str}] Fetched {rows_fetched} rows.")

        t_csv_start = time.perf_counter() if DEBUG_PERFORMANCE else 0
        if df.empty:
            if not df.columns.empty:
                tqdm.write(
                    f"[{period_str}] Query returned 0 data rows. Writing header only to {output_file}."
                )
                df.head(0).to_csv(
                    output_file,
                    sep="|",
                    encoding="utf-8",
                    index=False,
                    header=True,
                    na_rep="",
                    quoting=csv.QUOTE_MINIMAL,
                )
                write_successful = True
            else:
                tqdm.write(
                    f"[{period_str}] Warning: Query returned no columns. Skipping file write."
                )
        else:
            tqdm.write(
                f"[{period_str}] Writing {rows_fetched} rows to {output_file}..."
            )
            df.to_csv(
                output_file,
                sep="|",
                encoding="utf-8",
                index=False,
                header=True,
                na_rep="",
                quoting=csv.QUOTE_MINIMAL,
            )
            write_successful = True

        if DEBUG_PERFORMANCE and write_successful:
            t_csv_end = time.perf_counter()
            csv_write_duration = t_csv_end - t_csv_start
            timing_info["3_csv_write_s"] = csv_write_duration

    except (pd.errors.DatabaseError, Exception) as e:
        dbapi_exception = getattr(e, "orig", None)
        if dbapi_exception and isinstance(dbapi_exception, pyodbc.Error):
            sqlstate = dbapi_exception.args[0]
            db_msg = str(dbapi_exception).split("\n")[-1]
            tqdm.write(
                f"[{period_str}] Database error: SQLSTATE {sqlstate}. Message: {db_msg}. Skipping period."
            )
        else:
            tqdm.write(
                f"[{period_str}] Error during processing/export: {type(e).__name__} - {e}. Skipping period."
            )
        if DEBUG_PERFORMANCE:
            timing_info["error"] = True
    except IOError as e:
        tqdm.write(
            f"[{period_str}] File writing error for {output_file}: {e}. Skipping period."
        )
        if DEBUG_PERFORMANCE:
            timing_info["error"] = True
    finally:
        if DEBUG_PERFORMANCE:
            t_period_end = time.perf_counter()
            timing_info["4_total_period_s"] = t_period_end - t_period_start
            return timing_info
        else:
            return None


# --- Main Execution Logic ---
if __name__ == "__main__":
    script_start_time = time.perf_counter() if DEBUG_PERFORMANCE else 0
    period_timing_data = []

    print("Script starting...")

    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"Output directory: '{os.path.abspath(OUTPUT_DIR)}'")
    except OSError as e:
        print(
            f"FATAL ERROR: Could not create output directory '{OUTPUT_DIR}'. Error: {e}"
        )
        input("Press Enter to exit...")
        sys.exit(1)

    excel_read_duration = 0
    active_alarms_type_0_1 = None
    try:
        excel_path = resource_path(EXCEL_FILE_NAME)
        active_alarms_type_0_1 = read_alarm_list(excel_path)
        if active_alarms_type_0_1 is None:
            input("Press Enter to exit...")
            sys.exit(1)
        if DEBUG_PERFORMANCE and hasattr(read_alarm_list, "duration"):
            excel_read_duration = read_alarm_list.duration
    except Exception as e:
        print(f"FATAL ERROR: Could not determine path or read Excel file. Error: {e}")
        input("Press Enter to exit...")
        sys.exit(1)

    periods_to_process = []
    current_date = START_DATE
    while current_date < END_DATE:
        periods_to_process.append(current_date.strftime("%Y-%m"))
        current_date += relativedelta(months=1)

    engine = None
    engine_creation_duration = 0
    if not periods_to_process:
        print("No periods selected for processing.")
    else:
        print(
            f"Starting processing for {len(periods_to_process)} periods from {periods_to_process[0]} to {periods_to_process[-1]}..."
        )
        t_engine_start = time.perf_counter() if DEBUG_PERFORMANCE else 0
        try:
            # Construct the connection URL using the FIX applied to DB_DRIVER_QUOTED
            connection_url = (
                f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD_QUOTED}@{DB_SERVER}/{DB_DATABASE}?"
                f"driver={DB_DRIVER_QUOTED}&connect_timeout={DB_TIMEOUT}"  # Correct driver format here
            )
            engine = create_engine(connection_url, echo=False)
            with engine.connect() as test_conn:
                print("Database engine created and connection tested successfully.")
            if DEBUG_PERFORMANCE:
                t_engine_end = time.perf_counter()
                engine_creation_duration = t_engine_end - t_engine_start
        except Exception as e:
            # Catch engine creation error specifically
            print(f"\nFATAL ERROR: Failed to create SQLAlchemy engine. Error: {e}")
            print(
                "Please check database credentials, server address, driver name, and ensure SQLAlchemy and pyodbc are installed correctly."
            )
            # Add the background info link from the original error
            if isinstance(e, Exception) and "https://sqlalche.me/e/20/dbapi" in str(e):
                print("Background on this error type: https://sqlalche.me/e/20/dbapi")
            input("Press Enter to exit...")
            sys.exit(1)  # Exit if engine creation fails

        # --- Loop through periods ---
        for period_str in tqdm(
            periods_to_process, desc="Exporting Alarms", unit="month", ncols=100
        ):
            period_result = None
            try:
                period_dt = dt.strptime(period_str, "%Y-%m")
                next_period_dt = period_dt + relativedelta(months=+1)
                period_start_str = period_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                period_end_str = next_period_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                period_result = process_and_export_period(
                    period_str,
                    period_start_str,
                    period_end_str,
                    active_alarms_type_0_1,
                    engine,
                    OUTPUT_DIR,
                )

            except ValueError:
                tqdm.write(
                    f"Error: Invalid period format generated '{period_str}'. Skipping."
                )
                if DEBUG_PERFORMANCE:
                    period_result = {"period": period_str, "error": True}
            except Exception as loop_err:
                tqdm.write(
                    f"Error setting up processing for period {period_str}: {loop_err}. Skipping."
                )
                if DEBUG_PERFORMANCE:
                    period_result = {"period": period_str, "error": True}
            finally:
                if DEBUG_PERFORMANCE and period_result:
                    period_timing_data.append(period_result)
        # ---------------------------

        print(
            f"\n--- Processing complete. Output files are in '{os.path.abspath(OUTPUT_DIR)}' ---"
        )

    if engine:
        engine.dispose()
        print("Database engine disposed.")

    if DEBUG_PERFORMANCE:
        script_end_time = time.perf_counter()
        total_script_duration = script_end_time - script_start_time
        print("\n--- Performance Summary ---")
        print(f"Total Script Execution Time: {total_script_duration:.3f} seconds")
        print(f"  - Excel Read Time:         {excel_read_duration:.3f} seconds")
        print(
            f"  - Engine Creation Time:    {engine_creation_duration:.3f} seconds"
        )  # Now includes successful timing

        if period_timing_data:
            valid_periods = [p for p in period_timing_data if not p.get("error")]
            num_valid_periods = len(valid_periods)

            if num_valid_periods > 0:
                avg_total = (
                    sum(p.get("4_total_period_s", 0) for p in valid_periods)
                    / num_valid_periods
                )
                avg_construct = (
                    sum(p.get("1_construct_query_s", 0) for p in valid_periods)
                    / num_valid_periods
                )
                avg_db_fetch = (
                    sum(p.get("2_db_fetch_s", 0) for p in valid_periods)
                    / num_valid_periods
                )
                avg_csv_write = (
                    sum(p.get("3_csv_write_s", 0) for p in valid_periods)
                    / num_valid_periods
                )
                avg_rows = (
                    sum(p.get("rows_fetched", 0) for p in valid_periods)
                    / num_valid_periods
                )

                print(f"\nProcessing Averages ({num_valid_periods} valid periods):")
                print(f"  - Avg Total Time per Period: {avg_total:.3f} seconds")
                print(f"    - Avg Query Construct:     {avg_construct:.4f} seconds")
                print(f"    - Avg DB Fetch:            {avg_db_fetch:.3f} seconds")
                print(f"    - Avg CSV Write:           {avg_csv_write:.3f} seconds")
                print(f"  - Avg Rows Fetched per Period: {avg_rows:.1f}")

            num_error_periods = len(period_timing_data) - num_valid_periods
            if num_error_periods > 0:
                print(
                    f"\nNote: {num_error_periods} period(s) encountered errors during processing."
                )
        else:
            print("\nNo period timing data collected (or no periods processed).")
        print("---------------------------\n")

    print("\nPress Enter to exit...")
    input()
