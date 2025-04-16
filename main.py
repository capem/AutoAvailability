# pyinstaller AutoHebdo.py -y
import argparse
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import time
# os is used by logger_config

from src import data_exporter
from src import calculation
from src import hebdo_calc
from src import email_send
from src import logger_config
from src import results_grouper

# Get a logger for this module
logger = logger_config.get_logger(__name__)


def simple_countdown(duration):
    """Simple countdown timer without threading"""
    logger.info(f"Process completed. Exiting in {duration} seconds...")
    print(f"Process completed. Press Enter to exit immediately or wait {duration} seconds.")

    # Use a non-blocking approach to check for input while counting down
    import msvcrt
    start_time = time.time()
    while time.time() - start_time < duration:
        # Check if a key is pressed
        if msvcrt.kbhit():
            key = msvcrt.getch()
            if key == b'\r':  # Enter key
                logger.info("Exiting due to user input.")
                return

        # Update countdown display
        remaining = duration - int(time.time() - start_time)
        print(f"\rExiting in {remaining} seconds... (Press Enter to exit)", end="", flush=True)
        time.sleep(0.1)

    logger.info("Exiting due to timeout.")


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
                logger.error("Maximum duration exceeded. Stopping retries.")
                break

            logger.exception(
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
    # Initialize logging
    logger_config.configure_logging()
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

    logger.info("Starting data export process")
    # Call data_exporter for each required month, passing the update mode
    for period in period_range:
        period_str = period.strftime("%Y-%m")
        logger.info(
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

    logger.info("Starting calculation process")
    # Loop over the period range and call the functions
    for period in period_range:
        period_month = period.strftime("%Y-%m")
        # Calculate results for this period
        results = try_forever(calculation.full_calculation, period_month)
        try_forever(results.to_pickle, f"./monthly_data/results/{period_month}.pkl")

        # Process and save grouped results for this period
        try_forever(results_grouper.process_grouped_results, results, period_month)

    logger.info("Starting weekly calculation process")
    df_exploi = try_forever(
        hebdo_calc.main, period_range, period_start_dt, period_end_dt
    )
    df_Top15 = try_forever(
        hebdo_calc.Top15, period_range, period_start_dt, period_end_dt
    )

    title = f"From {period_start_dt.strftime('%Y_%m_%d')} To {period_end_dt.strftime('%Y_%m_%d')}"

    logger.info("Starting email sending process")
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

    logger.info("All processes completed successfully")

    # Simple countdown without threading
    simple_countdown(30)
