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
from src import config

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
        receiver_email=config.EMAIL_CONFIG["failure_recipient"],
        subject=subject,
        body=message,
    )


if __name__ == "__main__":
    # Initialize logging
    logger_config.configure_logging()

    # Parser with rich help and examples
    epilog = (
        "Examples:\n"
        "  Single date (backward compatible):\n"
        "    uv run ./main.py -y 2025-01-31 --update-mode process-existing\n"
        "  Multiple dates via repeated -y:\n"
        "    uv run ./main.py -y 2025-01-31 -y 2025-02-28 --update-mode process-existing\n"
        "  Multiple dates via comma-separated list:\n"
        "    uv run ./main.py -y 2025-01-31,2025-02-28 --update-mode process-existing\n"
        "  Windows CMD variant:\n"
        "    uv run .\\main.py -y 2025-01-31 -y 2025-02-28 --update-mode process-existing\n"
    )
    parser = argparse.ArgumentParser(
        description="Process weekly data, exporting and calculating.",
        epilog=epilog,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Accept multiple dates via repeatable -y/--date and comma-separated values
    parser.add_argument(
        "-y",
        "--yesterday",
        dest="yesterdays",
        action="append",
        help=(
            "Date(s) in YYYY-MM-DD. May be specified multiple times or as a comma-separated list. "
            "If omitted, defaults to yesterday."
        ),
    )
    parser.add_argument(
        "--update-mode",
        choices=["check", "append", "force-overwrite", "process-existing"],
        default="append",
        help="Handling of existing data exports: check | append | force-overwrite | process-existing",
    )
    args = parser.parse_args()

    # Normalize date inputs to a list of datetime objects
    raw_dates = []
    if args.yesterdays:
        for entry in args.yesterdays:
            if entry is None:
                continue
            # Support comma-separated entries
            parts = [p.strip() for p in entry.split(",") if p.strip()]
            raw_dates.extend(parts)

    if not raw_dates:
        # Default to single date: yesterday (backward-compatible behavior)
        target_dates = [dt.today() - timedelta(days=1)]
    else:
        target_dates = []
        for s in raw_dates:
            try:
                target_dates.append(dt.strptime(s, "%Y-%m-%d"))
            except ValueError:
                raise SystemExit(
                    f"error: invalid -y/--yesterday value '{s}'. Expected YYYY-MM-DD."
                )

    # Process each requested date independently
    for run_date in target_dates:
        period_month = run_date.strftime("%Y-%m")

        period_start_dt = run_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=6)
        period_end_dt = run_date.replace(hour=23, minute=50, second=0, microsecond=0)
        period_range = pd.period_range(start=period_start_dt, end=period_end_dt, freq="M")

        logger.info(f"Starting data export process for date {run_date.date()}")
        # Call data_exporter for each required month, passing the update mode
        for period in period_range:
            period_str = period.strftime("%Y-%m")
            logger.info(
                f"Running data export for period {period_str} with mode '{args.update_mode}'"
            )
            # Use lambda to pass extra arguments to the function called by try_forever
            try_forever(
                lambda p, mode: data_exporter.main_export_flow(
                    period=p, update_mode=mode
                ),
                period_str,
                args.update_mode,
            )

        # Alarm data is now exported by data_exporter

        logger.info(f"Starting calculation process for date {run_date.date()}")
        # Loop over the period range and call the functions
        for period in period_range:
            period_month = period.strftime("%Y-%m")
            results = try_forever(calculation.full_calculation, period_month)
            try_forever(results.to_pickle, f"./monthly_data/results/{period_month}.pkl")
            try_forever(results_grouper.process_grouped_results, results, period_month)

        logger.info(f"Starting weekly calculation process for date {run_date.date()}")
        df_exploi = try_forever(
            hebdo_calc.main, period_range, period_start_dt, period_end_dt
        )
        df_Top15 = try_forever(
            hebdo_calc.Top15, period_range, period_start_dt, period_end_dt
        )

        title = f"From {period_start_dt.strftime('%Y_%m_%d')} To {period_end_dt.strftime('%Y_%m_%d')}"

        logger.info(f"Starting email sending process for date {run_date.date()}")
        try_forever(
            email_send.send_email,
            df=df_exploi,
            receiver_email=config.EMAIL_CONFIG["receiver_default"],
            subject=f"Indisponibilité {title}",
        )

        try_forever(
            email_send.send_email,
            df=df_Top15,
            receiver_email=config.EMAIL_CONFIG["receiver_default"],
            subject=f"Top 15 Total Energy Lost(MWh){title}",
        )

        logger.info(f"All processes for date {run_date.date()} completed successfully")

    # Final countdown after all dates are processed
    simple_countdown(30)
