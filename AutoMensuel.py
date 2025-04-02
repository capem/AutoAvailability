# AccessDatabaseEngine_x64.exe /passive
# --hidden-import=sqlalchemy.dialects.access
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import time
import threading
import openpyxl.cell._writer
import sqlalchemy_access.pyodbc

# import sqlalchemy.dialects.access

import download_wps_history

import sql_alarms
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
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="[%X]"))

    # Console handler for INFO and above
    console_handler = RichHandler(rich_tracebacks=True, show_time=False, show_path=False)
    console_handler.setLevel(logging.INFO)  # Set to INFO level
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="[%X]")
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
        except Exception as e:
            attempts += 1
            current_time = dt.now()
            elapsed_time = (current_time - start_time).total_seconds()

            if elapsed_time > max_duration:
                logging.error("Maximum duration exceeded. Stopping retries.")
                break

            logging.exception(f"Attempt {attempts}: An error occurred. Retrying in 30 seconds...")
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
    yesterday = dt.today() - timedelta(days=1)
    period_month = yesterday.strftime("%Y-%m")

    period_start_dt = yesterday.replace(hour=23, minute=50, second=0, microsecond=0) - timedelta(days=6)
    period_end_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    period_range = pd.period_range(start=period_start_dt, end=period_end_dt, freq="M")

    logging.warning("download_wps_history")
    try_forever(download_wps_history.main, period_month)

    logging.warning("sql_alarms")
    try_forever(sql_alarms.main, period_month)

    logging.warning("calculation")
    results = try_forever(calculation.full_calculation, period_month)
    try_forever(results.to_pickle, f"./monthly_data/results/{period_month}.pkl")
    # results = pd.read_pickle(f"./monthly_data/results/{period_month}.pkl")

    logging.warning("hebdo_calc")
    df_exploi = try_forever(hebdo_calc.main, results, period_range, period_start_dt, period_end_dt)
    df_Top15 = try_forever(hebdo_calc.Top15, results, period_start_dt, period_end_dt)

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
