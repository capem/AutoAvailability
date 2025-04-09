# pyinstaller AutoHebdo.py -y
import argparse
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import time
import threading

import data_exporter
import calculation
import hebdo_calc
import email_send
from rich.logging import RichHandler
import logging
from logging.handlers import RotatingFileHandler
import os


def setup_logging(log_directory, log_filename):
    # Create log directory if it doesn't exist
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_file_path = os.path.join(log_directory, log_filename)

    # Create a logger at the root level
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Root logger captures INFO and above

    # File handler for WARNING and above
    file_handler = RotatingFileHandler(log_file_path, maxBytes=1e7, backupCount=5)
    file_handler.setLevel(logging.WARNING)  # Set to WARNING level
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="[%X]"
        )
    )

    # Console handler for INFO and above
    console_handler = RichHandler(
        rich_tracebacks=True, show_time=False, show_path=False
    )
    console_handler.setLevel(logging.INFO)  # Set to INFO level
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="[%X]"
        )
    )

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def user_input(stop_event):
    input()  # Wait for Enter key press
    stop_event.set()


def countdown_timer(duration, stop_event):
    for i in range(duration, 0, -1):
        if stop_event.is_set():
            print(f"\rExiting due to user input.{' ' * 30}", end="", flush=True)
            return
        print(f"\rExiting in {i} seconds... (Press Enter to exit)", end="", flush=True)
        time.sleep(1)
    print(f"\rExiting due to timeout...{' ' * 30}", end="", flush=True)


def try_forever(func, *args, **kwargs):
    start_time = dt.now()
    attempts = 0
    max_attempts = 120
    max_duration = 3600  # 1 hour in seconds

    while attempts < max_attempts:
        try:
            return func(*args, **kwargs)
        except Exception:
            attempts += 1
            current_time = dt.now()
            elapsed_time = (current_time - start_time).total_seconds()

            if elapsed_time > max_duration:
                logging.error("Maximum duration exceeded. Stopping retries.")
                break

            logging.exception(
                f"Attempt {attempts}: An error occurred. Retrying in 30 seconds..."
            )
            time.sleep(30)

    # Send an email after failing
    send_failure_email()  # Define this function to send a notification email


def send_failure_email():
    # You can define this function to send an email about the failure
    subject = "Script Failure Notification"
    message = "The script failed after 1 hour or 120 attempts."
    # Use your email sending function here
    email_send.send_email(
        df=pd.DataFrame(),  # An empty DataFrame or relevant information
        receiver_email="s.atmani@tarec.ma",
        subject=subject,
        body=message,
    )


if __name__ == "__main__":
    setup_logging("./logs", "hebdo.log")
    # Define and parse the command-line argument
    parser = argparse.ArgumentParser(
        description="Process weekly data, exporting and calculating."
    )
    parser.add_argument(
        "-y",
        "--yesterday",
        type=str,
        help='Optional date in YYYY-MM-DD format that overwrites the "yesterday" variable.',
        default=None,
    )
    parser.add_argument(
        "--update-mode",
        choices=["check", "append", "force-overwrite"],
        default="append",  # Defaulting to 'append' as decided
        help="Mode for handling existing data exports: 'check' (report changes), 'append' (update/append preserving deletions), 'force-overwrite' (export fresh data).",
    )
    args = parser.parse_args()

    # Check if a date was provided and is valid
    if args.yesterday:
        try:
            # Parse the provided date string into a datetime object
            yesterday = dt.strptime(args.yesterday, "%Y-%m-%d")
        except ValueError:
            # If the date format is incorrect, raise an error and exit
            raise ValueError(
                "The provided date is not in the correct format (YYYY-MM-DD)."
            )
    else:
        # If no date is provided, set yesterday to the day before the current date
        yesterday = dt.today() - timedelta(days=1)

    period_month = yesterday.strftime("%Y-%m")

    period_start_dt = yesterday.replace(
        hour=00, minute=00, second=0, microsecond=0
    ) - timedelta(days=6)
    period_end_dt = yesterday.replace(hour=23, minute=50, second=0, microsecond=0)
    period_range = pd.period_range(start=period_start_dt, end=period_end_dt, freq="M")

    logging.warning("data_exporter")
    # Call data_exporter for each required month, passing the update mode
    for period in period_range:
        period_str = period.strftime("%Y-%m")
        logging.info(
            f"Running data export for period {period_str} with mode '{args.update_mode}'"
        )
        # Use lambda to pass extra arguments to the function called by try_forever
        try_forever(
            # Call the main export orchestration function from data_exporter
            lambda p, mode: data_exporter.main_export_flow(
                period=p, update_mode=mode
            ), # Pass period and mode explicitly
            period_str,  # First arg for the lambda (p)
            args.update_mode,  # Second arg for the lambda (mode)
        )

    # Alarm data is now exported by data_exporter

    logging.warning("calculation")
    # Loop over the period range and call the functions
    for period in period_range:
        period_month = period.strftime("%Y-%m")
        results = try_forever(calculation.full_calculation, period_month)
        try_forever(results.to_pickle, f"./monthly_data/results/{period_month}.pkl")

    # Group and round the results
    results_grouped = (
        results.groupby("StationId").sum(numeric_only=True).round(2).reset_index()
    )

    # Extract columns only once
    columns = [
        "wtc_kWG1TotE_accum",
        "EL",
        "ELX",
        "ELNX",
        "EL_2006",
        "EL_PowerRed",
        "EL_Misassigned",
        "EL_wind",
        "EL_wind_start",
        "EL_alarm_start",
    ]
    (
        Ep,
        EL,
        ELX,
        ELNX,
        EL_2006,
        EL_PowerRed,
        EL_Misassigned,
        EL_wind,
        EL_wind_start,
        EL_alarm_start,
    ) = [results_grouped[col] for col in columns]

    # Simplified calculations
    ELX_eq = ELX - EL_Misassigned
    ELNX_eq = ELNX + EL_2006 + EL_PowerRed + EL_Misassigned
    Epot_eq = Ep + ELX_eq + ELNX_eq

    # Calculate MAA_brut and MAA_brut_mis
    results_grouped["MAA_brut"] = (
        100 * (Ep + ELX) / (Ep + ELX + ELNX + EL_2006 + EL_PowerRed)
    )
    results_grouped["MAA_brut_mis"] = round(100 * (Ep + ELX_eq) / Epot_eq, 2)

    # Calculate MAA_indefni_adjusted
    total_EL_wind = EL_wind + EL_wind_start + EL_alarm_start
    results_grouped["MAA_indefni_adjusted"] = (
        100 * (Ep + ELX) / (Ep + EL - total_EL_wind)
    )

    # Adjust index and save to CSV
    results_grouped.index += 1
    csv_filename = f"./monthly_data/results/Grouped_Results/grouped_{period_month}-Availability.csv"
    results_grouped.to_csv(csv_filename, decimal=",", sep=",")
    # results = pd.read_pickle(f"./monthly_data/results/{period_month}.pkl")

    logging.warning("hebdo_calc")
    df_exploi = try_forever(
        hebdo_calc.main, period_range, period_start_dt, period_end_dt
    )
    df_Top15 = try_forever(
        hebdo_calc.Top15, period_range, period_start_dt, period_end_dt
    )

    title = f"From {period_start_dt.strftime('%Y_%m_%d')} To {period_end_dt.strftime('%Y_%m_%d')}"

    logging.warning("send_email")
    try_forever(
        email_send.send_email,
        df=df_exploi,
        receiver_email="s.atmani@tarec.ma",
        subject=f"Indisponibilité {title}",
        # cc_emails=["s.atmani@tarec.ma", "s.atmani@tarec.ma", "s.atmani@tarec.ma"],
    )

    try_forever(
        email_send.send_email,
        df=df_Top15,
        receiver_email="s.atmani@tarec.ma",
        subject=f"Top 15 Total Energy Lost(MWh){title}",
    )

    logging.warning("Done")
    stop_event = threading.Event()

    # Thread for user input
    input_thread = threading.Thread(target=user_input, args=(stop_event,))
    input_thread.daemon = True
    input_thread.start()

    # Countdown timer
    countdown_timer(30, stop_event)
