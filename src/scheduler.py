"""
Scheduler Module for Wind Farm Data Processing

Provides background job scheduling using APScheduler.
Runs weekly processing jobs and sends failure alerts.
Jobs run in a separate process to allow clean shutdown.
"""

import json
import os
import multiprocessing
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent

from src import logger_config
from src import config

logger = logger_config.get_logger(__name__)

# Track the running job process for clean shutdown
_job_process: Optional[multiprocessing.Process] = None

# Config file path for persisting scheduler settings
SCHEDULER_CONFIG_FILE = Path(__file__).parent.parent / "config" / "scheduler_config.json"

# Global scheduler instance
_scheduler: Optional[BackgroundScheduler] = None
_scheduler_config: Dict[str, Any] = {
    "enabled": False,
    "day_of_week": "mon",  # monday
    "hour": 6,
    "minute": 0,
    "last_run": None,
    "last_status": None,
    "last_error": None,
}


def _load_config() -> Dict[str, Any]:
    """Load scheduler configuration from file."""
    global _scheduler_config
    if SCHEDULER_CONFIG_FILE.exists():
        try:
            with open(SCHEDULER_CONFIG_FILE, "r") as f:
                saved = json.load(f)
                _scheduler_config.update(saved)
        except Exception as e:
            logger.error(f"Failed to load scheduler config: {e}")
    return _scheduler_config


def _save_config() -> None:
    """Save scheduler configuration to file."""
    try:
        SCHEDULER_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SCHEDULER_CONFIG_FILE, "w") as f:
            json.dump(_scheduler_config, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"Failed to save scheduler config: {e}")


def _send_failure_alert(error_message: str, job_time: datetime) -> None:
    """Send email alert when scheduled job fails."""
    try:
        from src import email_send
        
        subject = f"⚠️ AutoAvailability Scheduled Processing Failed - {job_time.strftime('%Y-%m-%d %H:%M')}"
        
        # Create a simple DataFrame for the email
        import pandas as pd
        error_df = pd.DataFrame({
            "Field": ["Scheduled Time", "Error", "Server Time"],
            "Value": [
                job_time.strftime("%Y-%m-%d %H:%M:%S"),
                str(error_message)[:500],  # Truncate long errors
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ]
        })
        
        email_send.send_email(
            df=error_df,
            receiver_email=config.EMAIL_CONFIG["receiver_default"],
            subject=subject
        )
        logger.info("Failure alert email sent successfully")
    except Exception as e:
        logger.error(f"Failed to send failure alert email: {e}")


def _processing_worker() -> None:
    """
    Worker function that runs in a separate process.
    Processes yesterday's data with all standard steps.
    """
    import pandas as pd
    from src import data_exporter
    from src import calculation
    from src import hebdo_calc
    from src import email_send
    from src import validation_runner
    from src import results_grouper
    
    job_time = datetime.now()
    target_date = datetime.now() - timedelta(days=1)
    date_str = target_date.strftime("%Y-%m-%d")
    
    logger.info(f"[SCHEDULER] Starting weekly processing for {date_str}")
    
    try:
        # Step 1: Export
        period_start_dt = target_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=6)
        period_end_dt = target_date.replace(hour=23, minute=50, second=0, microsecond=0)
        period_range = pd.period_range(start=period_start_dt, end=period_end_dt, freq="M")
        
        for period in period_range:
            period_str = period.strftime("%Y-%m")
            logger.info(f"[SCHEDULER] Exporting data for {period_str}")
            data_exporter.main_export_flow(period=period_str, update_mode="append")
        
        # Step 2: Calculations
        for period in period_range:
            period_month = period.strftime("%Y-%m")
            logger.info(f"[SCHEDULER] Running calculations for {period_month}")
            results = calculation.full_calculation(period_month)
            results.to_pickle(f"./monthly_data/results/{period_month}.pkl")
            results_grouper.process_grouped_results(results, period_month)
        
        # Step 3: Weekly reports
        logger.info("[SCHEDULER] Generating weekly reports")
        df_exploi = hebdo_calc.main(period_range, period_start_dt, period_end_dt)
        df_Top15 = hebdo_calc.Top15(period_range, period_start_dt, period_end_dt)
        
        # Step 4: Emails
        logger.info("[SCHEDULER] Sending email reports")
        title = f"From {period_start_dt.strftime('%Y_%m_%d')} To {period_end_dt.strftime('%Y_%m_%d')}"
        
        email_send.send_email(
            df=df_exploi,
            receiver_email=config.EMAIL_CONFIG["receiver_default"],
            subject=f"Indisponibilité {title}"
        )
        
        email_send.send_email(
            df=df_Top15,
            receiver_email=config.EMAIL_CONFIG["receiver_default"],
            subject=f"Top 15 Total Energy Lost(MWh){title}"
        )
        
        # Step 5: Validation
        logger.info("[SCHEDULER] Running data validation")
        validation_periods = list(set([p.strftime("%Y-%m") for p in period_range]))
        validation_runner.run_validation_scan(
            target_periods=validation_periods,
            override_end_date=date_str
        )
        
        # Update status - reload config in case it changed
        _load_config()
        _scheduler_config["last_run"] = job_time.isoformat()
        _scheduler_config["last_status"] = "success"
        _scheduler_config["last_error"] = None
        _save_config()
        
        logger.info(f"[SCHEDULER] Weekly processing completed successfully for {date_str}")
        
    except Exception as e:
        error_msg = str(e)
        logger.exception(f"[SCHEDULER] Processing failed: {error_msg}")
        
        # Update status
        _load_config()
        _scheduler_config["last_run"] = job_time.isoformat()
        _scheduler_config["last_status"] = "error"
        _scheduler_config["last_error"] = error_msg
        _save_config()
        
        # Send failure alert
        _send_failure_alert(error_msg, job_time)


def _run_scheduled_processing() -> None:
    """
    Wrapper that launches the processing job in a separate process.
    This allows the server to be shut down cleanly with Ctrl+C.
    """
    global _job_process
    
    # If a job is already running, skip
    if _job_process is not None and _job_process.is_alive():
        logger.warning("[SCHEDULER] Job already running, skipping this trigger")
        return
    
    # Start the worker in a separate process
    _job_process = multiprocessing.Process(target=_processing_worker)
    _job_process.start()
    logger.info(f"[SCHEDULER] Started job in process {_job_process.pid}")


def _on_job_event(event: JobExecutionEvent) -> None:
    """Handle APScheduler job events for logging."""
    if event.exception:
        logger.error(f"[SCHEDULER] Job {event.job_id} failed with exception")
    else:
        logger.info(f"[SCHEDULER] Job {event.job_id} executed successfully")


def get_scheduler_status() -> Dict[str, Any]:
    """Get current scheduler status and configuration."""
    global _scheduler, _scheduler_config
    
    _load_config()
    
    next_run = None
    if _scheduler and _scheduler.running:
        jobs = _scheduler.get_jobs()
        if jobs:
            next_run_time = jobs[0].next_run_time
            if next_run_time:
                next_run = next_run_time.isoformat()
    
    return {
        "enabled": _scheduler_config.get("enabled", False),
        "day_of_week": _scheduler_config.get("day_of_week", "mon"),
        "hour": _scheduler_config.get("hour", 6),
        "minute": _scheduler_config.get("minute", 0),
        "next_run": next_run,
        "last_run": _scheduler_config.get("last_run"),
        "last_status": _scheduler_config.get("last_status"),
        "last_error": _scheduler_config.get("last_error"),
        "is_running": _scheduler is not None and _scheduler.running,
    }


def configure_scheduler(
    enabled: bool,
    day_of_week: str = "mon",
    hour: int = 6,
    minute: int = 0,
) -> Dict[str, Any]:
    """
    Configure the scheduler with new settings.
    
    Args:
        enabled: Whether to enable the scheduler
        day_of_week: Day of week (mon, tue, wed, thu, fri, sat, sun)
        hour: Hour to run (0-23)
        minute: Minute to run (0-59)
    
    Returns:
        Updated scheduler status
    """
    global _scheduler, _scheduler_config
    
    # Validate inputs
    valid_days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    if day_of_week.lower() not in valid_days:
        raise ValueError(f"Invalid day_of_week. Must be one of: {valid_days}")
    if not (0 <= hour <= 23):
        raise ValueError("Hour must be between 0 and 23")
    if not (0 <= minute <= 59):
        raise ValueError("Minute must be between 0 and 59")
    
    # Update config
    _scheduler_config["enabled"] = enabled
    _scheduler_config["day_of_week"] = day_of_week.lower()
    _scheduler_config["hour"] = hour
    _scheduler_config["minute"] = minute
    _save_config()
    
    # Apply changes to scheduler
    if enabled:
        start_scheduler()
    else:
        stop_scheduler()
    
    return get_scheduler_status()


def start_scheduler() -> None:
    """Start or restart the scheduler with current configuration."""
    global _scheduler, _scheduler_config
    
    _load_config()
    
    # Stop existing scheduler if running
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    
    # Create new scheduler
    _scheduler = BackgroundScheduler(timezone="Europe/Paris")
    _scheduler.add_listener(_on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    
    # Add the weekly job
    trigger = CronTrigger(
        day_of_week=_scheduler_config.get("day_of_week", "mon"),
        hour=_scheduler_config.get("hour", 6),
        minute=_scheduler_config.get("minute", 0),
    )
    
    _scheduler.add_job(
        _run_scheduled_processing,
        trigger=trigger,
        id="weekly_processing",
        name="Weekly Data Processing",
        replace_existing=True,
    )
    
    _scheduler.start()
    logger.info(f"[SCHEDULER] Started with schedule: {_scheduler_config['day_of_week']} at {_scheduler_config['hour']:02d}:{_scheduler_config['minute']:02d}")


def stop_scheduler() -> None:
    """Stop the scheduler and any running job process."""
    global _scheduler, _job_process
    
    # Terminate running job process if any
    if _job_process is not None and _job_process.is_alive():
        logger.info(f"[SCHEDULER] Terminating running job process {_job_process.pid}")
        _job_process.terminate()
        _job_process.join(timeout=2)
        if _job_process.is_alive():
            _job_process.kill()
            _job_process.join(timeout=1)
    _job_process = None
    
    # Shutdown scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("[SCHEDULER] Stopped")
    _scheduler = None


def init_scheduler() -> None:
    """Initialize scheduler on application startup if enabled."""
    _load_config()
    if _scheduler_config.get("enabled", False):
        start_scheduler()
        logger.info("[SCHEDULER] Auto-started on application startup")
    else:
        logger.info("[SCHEDULER] Disabled - not starting")


def shutdown_scheduler() -> None:
    """Cleanup scheduler on application shutdown."""
    stop_scheduler()


def trigger_now() -> Dict[str, str]:
    """Manually trigger the scheduled job immediately (for testing)."""
    global _scheduler, _job_process
    
    # Check if a job is already running
    if _job_process is not None and _job_process.is_alive():
        return {"status": "running", "message": "A job is already running"}
    
    if _scheduler and _scheduler.running:
        job = _scheduler.get_job("weekly_processing")
        if job:
            _scheduler.modify_job("weekly_processing", next_run_time=datetime.now())
            return {"status": "triggered", "message": "Job will run shortly"}
    
    # If scheduler not running, run directly via multiprocessing
    _run_scheduled_processing()
    return {"status": "triggered", "message": "Job started in background"}

