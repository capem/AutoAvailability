"""
API Routes for Wind Farm Data Processing

Exposes endpoints for data processing, alarm management, logs, and system status.
"""

import os
import multiprocessing
import json
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
import pathlib
import mimetypes
import zipfile
import io

# Import src modules - adjust path since we're in backend/
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config
from src import adjust_alarms
from src import logger_config
from src import scheduler as app_scheduler

logger = logger_config.get_logger(__name__)

router = APIRouter()

# --- Configuration for File Manager ---
# Resolves to absolute path of 'monthly_data' relative to the root of the repo
# backend/api.py -> backend/ -> root -> monthly_data
BASE_DATA_DIR = pathlib.Path(__file__).resolve().parent.parent / "monthly_data"


# --- Pydantic Models ---

class ProcessRequest(BaseModel):
    """Request model for data processing."""
    dates: List[str] = Field(..., description="List of dates in YYYY-MM-DD format")
    update_mode: str = Field(default="append", description="Update mode: append, check, force-overwrite, process-existing, process-existing-except-alarms")


class AlarmAdjustment(BaseModel):
    """Model for alarm adjustment."""
    id: int
    alarm_code: int
    station_nr: int
    time_on: Optional[str] = None
    time_off: Optional[str] = None
    notes: Optional[str] = None


class AlarmUpdateRequest(BaseModel):
    """Request model for updating an alarm adjustment."""
    time_on: Optional[str] = None
    time_off: Optional[str] = None
    notes: Optional[str] = None


class BulkDeleteRequest(BaseModel):
    ids: List[int]


class BulkUpdateRequest(BaseModel):
    ids: List[int]
    data: AlarmUpdateRequest


class ProcessingStatus(BaseModel):
    """Model for processing status response."""
    status: str
    message: str
    date: Optional[str] = None
    step: Optional[str] = None


# --- Multiprocessing-based task management ---

# Shared state using multiprocessing Manager
_manager = None
_processing_status = None
_current_process: Optional[multiprocessing.Process] = None


def get_manager():
    """Get or create the multiprocessing manager."""
    global _manager, _processing_status
    if _manager is None:
        _manager = multiprocessing.Manager()
        _processing_status = _manager.dict({
            "status": "idle",
            "message": "No processing in progress",
            "date": None,
            "step": None
        })
    return _manager, _processing_status


def cleanup_resources():
    """Cleanup multiprocessing resources on shutdown."""
    global _manager, _current_process
    
    logger.info("[API] Cleaning up resources...")
    
    if _current_process is not None and _current_process.is_alive():
        logger.info("[API] Terminating running process...")
        try:
            _current_process.terminate()
            _current_process.join(timeout=2)
            if _current_process.is_alive():
                _current_process.kill()
        except Exception as e:
            logger.error(f"[API] Error terminating process: {e}")
            
    if _manager is not None:
        logger.info("[API] Shutting down multiprocessing manager...")
        try:
            _manager.shutdown()
        except Exception as e:
            logger.error(f"[API] Error shutting down manager: {e}")
            
    logger.info("[API] Cleanup complete.")


def run_processing_worker(status_dict, dates: List[str], update_mode: str):
    """Worker function that runs in a separate process."""
    try:
        import pandas as pd
        from src import data_exporter
        from src import calculation
        from src import hebdo_calc
        from src import email_send
        
        for date_str in dates:
            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            
            status_dict["status"] = "running"
            status_dict["message"] = f"Processing {date_str}"
            status_dict["date"] = date_str
            status_dict["step"] = "Exporting data"
            
            # Step 1: Data Export
            period_start_dt = target_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=6)
            period_end_dt = target_date.replace(hour=23, minute=50, second=0, microsecond=0)
            period_range = pd.period_range(start=period_start_dt, end=period_end_dt, freq="M")
            
            for period in period_range:
                period_str = period.strftime("%Y-%m")
                data_exporter.main_export_flow(period=period_str, update_mode=update_mode)
            
            # Step 2: Calculations
            status_dict["step"] = "Running calculations"
            for period in period_range:
                period_month = period.strftime("%Y-%m")
                results = calculation.full_calculation(period_month)
                results.to_pickle(f"./monthly_data/results/{period_month}.pkl")
                
                from src import results_grouper
                results_grouper.process_grouped_results(results, period_month)
            
            # Step 3: Weekly calculations
            status_dict["step"] = "Generating weekly reports"
            df_exploi = hebdo_calc.main(period_range, period_start_dt, period_end_dt)
            df_Top15 = hebdo_calc.Top15(period_range, period_start_dt, period_end_dt)

            # Step 4: Email reports
            status_dict["step"] = "Sending email reports"
            try:
                title = f"From {period_start_dt.strftime('%Y_%m_%d')} To {period_end_dt.strftime('%Y_%m_%d')}"
                
                logger.info("[API] Sending availability report email...")
                email_send.send_email(
                    df=df_exploi,
                    receiver_email=config.EMAIL_CONFIG["receiver_default"],
                    subject=f"Indisponibilité {title}"
                )
                
                logger.info("[API] Sending Top 15 report email...")
                email_send.send_email(
                    df=df_Top15,
                    receiver_email=config.EMAIL_CONFIG["receiver_default"],
                    subject=f"Top 15 Total Energy Lost(MWh){title}"
                )
            except Exception as e:
                logger.error(f"[API] Failed to send email reports: {e}")
                # We don't stop the process if email fails, just log it
            
            # Step 5: Data Integrity Validation
            status_dict["step"] = "Running Data Validation"
            try:
                from src import validation_runner
                validation_periods = [p.strftime("%Y-%m") for p in period_range]
                # Filter unique periods just in case
                validation_periods = list(set(validation_periods))
                
                logger.info(f"[API] Running scoped validation for periods: {validation_periods} up to {date_str}")
                validation_runner.run_validation_scan(
                    target_periods=validation_periods, 
                    override_end_date=date_str
                )
            except Exception as e:
                logger.error(f"[API] Data validation failed: {e}")
        
        status_dict["status"] = "completed"
        status_dict["message"] = f"Successfully processed {len(dates)} date(s)"
        status_dict["date"] = None
        status_dict["step"] = None
        
    except Exception as e:
        status_dict["status"] = "error"
        status_dict["message"] = str(e)
        status_dict["date"] = None
        status_dict["step"] = None


# --- Endpoints ---

class ValidationRequest(BaseModel):
    dates: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    stuck_intervals: Optional[int] = None
    exclude_zero: Optional[bool] = False

def run_validation_worker(status_dict, dates=None, start_date=None, end_date=None, stuck_intervals=None, exclude_zero=False):
    """Worker function for running validation scan."""
    try:
            from src import validation_runner
            import time
            # Short sleep to let status sync
            time.sleep(1)
            
            status_dict["step"] = "Scanning MET Data"
            validation_runner.run_validation_scan(
                target_periods=dates, 
                override_start_date=start_date,
                override_end_date=end_date,
                stuck_intervals=stuck_intervals,
                exclude_zero=exclude_zero
            )
            
            status_dict["status"] = "completed"
            status_dict["message"] = "Validation complete"
            status_dict["step"] = None
    except Exception as e:
            status_dict["status"] = "error"
            status_dict["message"] = str(e)
            status_dict["step"] = None

@router.get("/integrity/rules")
async def get_integrity_rules():
    """Get current data integrity validation rules and thresholds."""
    return {
        "ranges": {
            "WindSpeed": config.MET_WINDSPEED_RANGE,
            "WindDirection": config.MET_WINDDIRECTION_RANGE,
            "Pressure": config.MET_PRESSURE_RANGE,
            "Temperature": config.MET_TEMPERATURE_RANGE,
        },
        "defaults": {
            "stuck_intervals": config.MET_STUCK_INTERVALS
        }
    }

@router.post("/integrity/run")
async def run_integrity_check(request: ValidationRequest = ValidationRequest()):
    """Trigger a data integrity scan."""
    global _current_process
    _, status_dict = get_manager()
    
    if status_dict.get("status") == "running":
        raise HTTPException(status_code=409, detail="Processing already in progress")
        
    status_dict["status"] = "running"
    status_dict["message"] = "Running Data Validation..."
    status_dict["step"] = "Initializing"
    status_dict["date"] = None

    _current_process = multiprocessing.Process(
        target=run_validation_worker, 
        args=(status_dict, request.dates, request.start_date, request.end_date, request.stuck_intervals, request.exclude_zero)
    )
    _current_process.start()
    return {"message": "Validation started"}


@router.get("/integrity/report")
async def get_integrity_report():
    """Get the latest validation report."""
    report_file = BASE_DATA_DIR / "validation_report.json"
    if not report_file.exists():
        # Return empty structure if no report exists
        return {"last_run": None, "summary": {}, "details": []}
    
    try:
        with open(report_file, "r") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read report: {e}")


@router.post("/process")
async def process_dates(request: ProcessRequest):
    """Trigger data processing for specified dates."""
    global _current_process
    _, status_dict = get_manager()
    
    if status_dict.get("status") == "running":
        raise HTTPException(status_code=409, detail="Processing already in progress")
    
    # Parse dates
    try:
        for d in request.dates:
            datetime.strptime(d, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    # Validate update mode
    valid_modes = ["append", "check", "force-overwrite", "process-existing", "process-existing-except-alarms"]
    if request.update_mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"Invalid update mode. Must be one of: {valid_modes}")
    
    # Update status
    status_dict["status"] = "starting"
    status_dict["message"] = "Initializing processing..."
    status_dict["date"] = None
    status_dict["step"] = None
    
    # Start worker process
    _current_process = multiprocessing.Process(
        target=run_processing_worker,
        args=(status_dict, request.dates, request.update_mode)
    )
    _current_process.start()
    
    return {"message": "Processing started", "dates": request.dates, "mode": request.update_mode}


@router.post("/process/abort")
async def abort_processing():
    """Abort the current processing task."""
    global _current_process
    _, status_dict = get_manager()
    
    if status_dict.get("status") not in ["running", "starting"]:
        raise HTTPException(status_code=400, detail="No processing in progress to abort")
    
    if _current_process is not None and _current_process.is_alive():
        _current_process.terminate()
        _current_process.join(timeout=5)
        
        if _current_process.is_alive():
            _current_process.kill()
            _current_process.join(timeout=2)
        
        _current_process = None
    
    status_dict["status"] = "aborted"
    status_dict["message"] = "Processing was aborted by user"
    status_dict["date"] = None
    status_dict["step"] = None
    
    return {"message": "Processing aborted successfully"}


@router.get("/process/status")
async def get_processing_status():
    """Get current processing status."""
    global _current_process
    _, status_dict = get_manager()
    
    # Check if process finished unexpectedly
    if _current_process is not None and not _current_process.is_alive():
        if status_dict.get("status") == "running":
            status_dict["status"] = "error"
            status_dict["message"] = "Process terminated unexpectedly"
        _current_process = None
    
    return dict(status_dict)


@router.get("/alarms")
async def list_alarms(
    page: int = 1,
    page_size: int = 10,
    alarm_code: Optional[int] = None,
    station_nr: Optional[int] = None,
    sort_by: Optional[str] = None,
    sort_order: str = "asc"
):
    """List all alarm adjustments with filtering, sorting, and pagination."""
    data = adjust_alarms.load_adjustments()
    all_adjustments = data.get("adjustments", [])
    
    # Filtering
    if alarm_code is not None:
        all_adjustments = [a for a in all_adjustments if a.get("alarm_code") == alarm_code]
    
    if station_nr is not None:
        all_adjustments = [a for a in all_adjustments if a.get("station_nr") == station_nr]
    
    # Sorting
    if sort_by:
        reverse = sort_order.lower() == "desc"
        def get_sort_key(item):
            val = item.get(sort_by)
            
            # String fields: Force string conversion
            if sort_by in ["time_on", "time_off", "notes", "last_updated"]:
                if val is None:
                    return ""
                return str(val)
                
            # Numeric fields: Force int conversion
            if val is None:
                return 0
            try:
                return float(val) # Handle int or float
            except (ValueError, TypeError):
                return 0 # Default to 0 if not a number
            
        all_adjustments.sort(key=get_sort_key, reverse=reverse)
    else:
        # Default sort by ID desc (newest first)
        all_adjustments.sort(key=lambda x: x.get("id", 0), reverse=True)

    total = len(all_adjustments)
    start = (page - 1) * page_size
    end = start + page_size
    
    paginated_adjustments = all_adjustments[start:end]
    
    return {
        "adjustments": paginated_adjustments,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 1
    }


@router.get("/alarms/ids")
async def list_alarm_ids(
    alarm_code: Optional[int] = None,
    station_nr: Optional[int] = None
):
    """Get all alarm IDs matching the filters."""
    data = adjust_alarms.load_adjustments()
    all_adjustments = data.get("adjustments", [])
    
    # Filtering
    if alarm_code is not None:
        all_adjustments = [a for a in all_adjustments if a.get("alarm_code") == alarm_code]
    
    if station_nr is not None:
        all_adjustments = [a for a in all_adjustments if a.get("station_nr") == station_nr]
    
    return [a.get("id") for a in all_adjustments if a.get("id") is not None]


@router.post("/alarms")
async def add_alarm(adjustment: AlarmAdjustment):
    """Add a new alarm adjustment."""
    if not adjustment.time_on and not adjustment.time_off:
        raise HTTPException(status_code=400, detail="At least one of time_on or time_off must be provided")
    
    # Create mock args object for adjust_alarms functions
    class MockArgs:
        def __init__(self):
            self.id = adjustment.id
            self.alarm_code = adjustment.alarm_code
            self.station_nr = adjustment.station_nr
            self.time_on = adjustment.time_on or ""
            self.time_off = adjustment.time_off or ""
            self.notes = adjustment.notes or ""
    
    success = adjust_alarms.add_adjustment(MockArgs())
    if success:
        return {"message": "Adjustment added successfully", "id": adjustment.id}
    else:
        raise HTTPException(status_code=500, detail="Failed to add adjustment")


@router.post("/alarms/bulk/delete")
async def bulk_delete_alarms(request: BulkDeleteRequest):
    """Delete multiple alarm adjustments."""
    success = adjust_alarms.remove_adjustments_batch(request.ids)
    if success:
        return {"message": f"Successfully deleted {len(request.ids)} adjustments"}
    else:
        raise HTTPException(status_code=404, detail="No adjustments found to delete")


@router.put("/alarms/bulk/update")
async def bulk_update_alarms(request: BulkUpdateRequest):
    """Update multiple alarm adjustments."""
    logger.info(f"[API] Bulk update request: ids={request.ids}, data={request.data}")
    
    # Convert Pydantic model to dict, excluding None values
    update_data = {k: v for k, v in request.data.dict().items() if v is not None}
    
    if not update_data:
         raise HTTPException(status_code=400, detail="No update data provided")

    success = adjust_alarms.update_adjustments_batch(request.ids, update_data)
    if success:
        return {"message": f"Successfully updated {len(request.ids)} adjustments"}
    else:
         raise HTTPException(status_code=404, detail="No adjustments found to update")


@router.put("/alarms/{alarm_id}")
async def update_alarm(alarm_id: int, request: AlarmUpdateRequest):
    """Update an existing alarm adjustment."""
    class MockArgs:
        def __init__(self):
            self.id = alarm_id
            self.time_on = request.time_on
            self.time_off = request.time_off
            self.notes = request.notes
    
    success = adjust_alarms.update_adjustment(MockArgs())
    if success:
        return {"message": "Adjustment updated successfully", "id": alarm_id}
    else:
        raise HTTPException(status_code=404, detail=f"Adjustment with ID {alarm_id} not found")


@router.delete("/alarms/{alarm_id}")
async def delete_alarm(alarm_id: int):
    """Delete an alarm adjustment."""
    class MockArgs:
        def __init__(self):
            self.id = alarm_id
    
    success = adjust_alarms.remove_adjustment(MockArgs())
    if success:
        return {"message": "Adjustment deleted successfully", "id": alarm_id}
    else:
        raise HTTPException(status_code=404, detail=f"Adjustment with ID {alarm_id} not found")


@router.get("/logs")
async def get_logs(lines: int = 50):
    """Get recent application logs."""
    log_file = "./logs/application.log"
    
    if not os.path.exists(log_file):
        return {"logs": [], "total_lines": 0, "message": "No log file found"}
    
    try:
        with open(log_file, "r") as f:
            all_lines = f.readlines()
        
        recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        return {
            "logs": [line.strip() for line in recent_lines],
            "total_lines": len(all_lines),
            "displayed_lines": len(recent_lines)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")


@router.get("/status")
async def system_status():
    """Get system health status."""
    _, status_dict = get_manager()
    status_items = []
    
    # Check database config
    try:
        db_config = config.DB_CONFIG
        status_items.append({
            "component": "Database Config",
            "status": "ok",
            "details": f"Server: {db_config['server']}, Database: {db_config['database']}"
        })
    except Exception as e:
        status_items.append({
            "component": "Database Config",
            "status": "error",
            "details": str(e)
        })
    
    # Check email config
    try:
        email_config = config.EMAIL_CONFIG
        status_items.append({
            "component": "Email Config",
            "status": "ok",
            "details": f"SMTP: {email_config['smtp_host']}:{email_config['smtp_port']}"
        })
    except Exception as e:
        status_items.append({
            "component": "Email Config",
            "status": "error",
            "details": str(e)
        })
    
    # Check data directory
    data_path = config.BASE_DATA_PATH
    if os.path.exists(data_path):
        status_items.append({
            "component": "Data Directory",
            "status": "ok",
            "details": f"Path: {data_path}"
        })
    else:
        status_items.append({
            "component": "Data Directory",
            "status": "warning",
            "details": f"Missing: {data_path}"
        })
    
    # Check log directory
    log_path = "./logs"
    if os.path.exists(log_path):
        status_items.append({
            "component": "Log Directory",
            "status": "ok",
            "details": f"Path: {log_path}"
        })
    else:
        status_items.append({
            "component": "Log Directory",
            "status": "warning",
            "details": f"Missing: {log_path}"
        })
    
    return {
        "overall": "healthy" if all(s["status"] == "ok" for s in status_items) else "degraded",
        "components": status_items,
        "processing": dict(status_dict)
    }


@router.post("/test/database")
async def test_database_connection():
    """Test database connection by attempting to connect and run a simple query."""
    try:
        import pyodbc
        
        db_config = config.DB_CONFIG
        
        # Build connection string
        conn_str = (
            f"DRIVER={db_config['driver']};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']};"
            f"TrustServerCertificate=yes;"
        )
        
        # Attempt connection with timeout
        connection = pyodbc.connect(conn_str, timeout=10)
        cursor = connection.cursor()
        
        # Run a simple test query
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        return {
            "success": True,
            "message": "Database connection successful",
            "details": {
                "server": db_config['server'],
                "database": db_config['database'],
                "username": db_config['username'],
            }
        }
        
    except Exception as e:
        logger.error(f"[API] Database connection test failed: {e}")
        return {
            "success": False,
            "message": f"Database connection failed: {str(e)}",
            "details": {
                "server": config.DB_CONFIG.get('server', 'N/A'),
                "database": config.DB_CONFIG.get('database', 'N/A'),
                "username": config.DB_CONFIG.get('username', 'N/A'),
            }
        }


@router.post("/test/email")
async def test_email_configuration():
    """Test email configuration by checking SMTP connection (without sending email)."""
    try:
        import smtplib
        
        email_config = config.EMAIL_CONFIG
        
        # Create SMTP connection
        server = smtplib.SMTP(email_config['smtp_host'], email_config['smtp_port'], timeout=10)
        server.ehlo()
        server.starttls()
        server.ehlo()
        
        # Attempt login
        server.login(email_config['sender_email'], email_config['password'])
        
        server.quit()
        
        return {
            "success": True,
            "message": "Email configuration valid - SMTP connection successful",
            "details": {
                "sender": email_config['sender_email'],
                "smtp_host": email_config['smtp_host'],
                "smtp_port": email_config['smtp_port'],
                "default_recipient": email_config['receiver_default'],
            }
        }
        
    except Exception as e:
        logger.error(f"[API] Email configuration test failed: {e}")
        return {
            "success": False,
            "message": f"Email configuration test failed: {str(e)}",
            "details": {
                "sender": config.EMAIL_CONFIG.get('sender_email', 'N/A'),
                "smtp_host": config.EMAIL_CONFIG.get('smtp_host', 'N/A'),
                "smtp_port": config.EMAIL_CONFIG.get('smtp_port', 'N/A'),
                "default_recipient": config.EMAIL_CONFIG.get('receiver_default', 'N/A'),
            }
        }


# --- File Manager Endpoints ---

@router.get("/fs/list")
async def list_files(path: str = ""):
    """
    List files and directories in the given path relative to BASE_DATA_DIR.
    Secured against path traversal.
    """
    try:
        # Securely resolve the target path
        target_path = (BASE_DATA_DIR / path).resolve()
        
        # Verify that the target path is inside BASE_DATA_DIR
        if not str(target_path).startswith(str(BASE_DATA_DIR)):
             raise HTTPException(status_code=403, detail="Access denied: Path outside allowed directory")
        
        if not target_path.exists():
            raise HTTPException(status_code=404, detail="Path not found")
            
        if not target_path.is_dir():
             raise HTTPException(status_code=400, detail="Not a directory")

        items = []
        # Use os.scandir for performance
        with os.scandir(target_path) as entries:
            for entry in entries:
                # Exclude metadata/json files
                if entry.name.endswith('.json') or entry.name.endswith('.meta'):
                    continue

                # Basic info
                stat = entry.stat()
                item_type = "directory" if entry.is_dir() else "file"
                
                # Determine mime type for files
                mime_type = None
                if item_type == "file":
                     mime_type, _ = mimetypes.guess_type(entry.name)
                
                items.append({
                    "name": entry.name,
                    "type": item_type,
                    "size": stat.st_size if item_type == "file" else 0, # Folder size calculation is expensive, skip
                    "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "mime_type": mime_type,
                    "path": str(pathlib.Path(path) / entry.name).replace("\\", "/") # Relative path for frontend
                })
        
        # Sort items: Directories first, then alphabetical
        items.sort(key=lambda x: (0 if x["type"] == "directory" else 1, x["name"].lower()))
        
        return items

    except PermissionError:
         raise HTTPException(status_code=403, detail="Permission denied")
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        logger.error(f"[API] Error listing files at {path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fs/download")
async def download_file(path: str):
    """
    Download a file from the given path relative to BASE_DATA_DIR.
    Secured against path traversal.
    """
    try:
        # Securely resolve the target path
        target_path = (BASE_DATA_DIR / path).resolve()
        
        # Verify that the target path is inside BASE_DATA_DIR
        if not str(target_path).startswith(str(BASE_DATA_DIR)):
             raise HTTPException(status_code=403, detail="Access denied: Path outside allowed directory")
             
        if not target_path.exists() or not target_path.is_file():
             raise HTTPException(status_code=404, detail="File not found")
             
        # Return file as attachment
        return FileResponse(
            path=target_path,
            filename=target_path.name,
            media_type='application/octet-stream' 
        )

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        logger.error(f"[API] Error downloading file at {path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class BulkDownloadRequest(BaseModel):
    paths: List[str]


@router.post("/fs/download-zip")
async def download_zip(request: BulkDownloadRequest):
    """
    Download multiple files as a ZIP archive.
    """
    try:
        # Create in-memory zip
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for path in request.paths:
                # Securely resolve path
                target_path = (BASE_DATA_DIR / path).resolve()
                
                # Check security
                if not str(target_path).startswith(str(BASE_DATA_DIR)):
                    logger.warning(f"[API] Skipping security violation path: {path}")
                    continue
                    
                if target_path.exists() and target_path.is_file():
                    # Add to zip with a clean arcname (relative to BASE_DATA_DIR usually, or just filename)
                    # Here we use the filename to keep it flat or relative structure if needed
                    # Let's preserve the structure relative to the requested path from the request
                    
                    # Calculate relative path for archive name
                    try:
                        arcname = target_path.relative_to(BASE_DATA_DIR)
                    except ValueError:
                         arcname = target_path.name

                    zip_file.write(target_path, arcname=str(arcname))
        
        # Rewind buffer
        zip_buffer.seek(0)
        
        # Generator for streaming
        def iterfile():
            yield from zip_buffer
            
        return StreamingResponse(
            iterfile(),
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=files.zip"}
        )

    except Exception as e:
        logger.error(f"[API] Error creating zip: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fs/search")
async def search_files(
    query: Optional[str] = None,
    months: Optional[List[str]] = Query(None),
    types: Optional[List[str]] = Query(None)
):
    """
    Recursively search for files in BASE_DATA_DIR matching filters.
    """
    try:
        items = []
        
        # We need to walk the entire directory structure
        # pathlib.rglob('*') is useful here
        for path in BASE_DATA_DIR.rglob('*'):
            if not path.is_file():
                continue
            
            # Exclude metadata/json files
            if path.name.endswith('.json') or path.name.endswith('.meta'):
                continue
                
            # Apply filters
            
            # 1. Type Filter (Folder Name)
            # If types matches ANY selected type
            if types:
                rel_path = path.relative_to(BASE_DATA_DIR)
                # Check if any parent folder matches any of the selected types
                # Using set intersection for efficiency
                path_parts = set(p.lower() for p in rel_path.parts[:-1])
                selected_types = set(t.lower() for t in types)
                if not path_parts.intersection(selected_types):
                    continue
            
            # 2. Month Filter (YYYY-MM in filename)
            # If months matches ANY selected month
            if months:
                # Check if any of the selected month strings are in the filename
                if not any(m in path.name for m in months):
                    continue
            
            # 3. Query Filter (Filename substring)
            if query:
                if query.lower() not in path.name.lower():
                    continue
            
            # If we get here, it's a match
            stat = path.stat()
            mime_type, _ = mimetypes.guess_type(path.name)
            
            # Calculate relative path string for frontend
            rel_path_str = str(path.relative_to(BASE_DATA_DIR)).replace("\\", "/")
            
            items.append({
                "name": path.name,
                "type": "file", # Search only returns files for now allows flat list
                "size": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "mime_type": mime_type,
                "path": rel_path_str
            })
            
        # Sort by date modified desc (newest first usually most relevant)
        items.sort(key=lambda x: x["mtime"], reverse=True)
        
        return items

    except Exception as e:
        logger.error(f"[API] Error searching files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Scheduler Endpoints ---

class SchedulerConfigRequest(BaseModel):
    """Request model for scheduler configuration."""
    enabled: bool
    day_of_week: str = "mon"
    hour: int = 6
    minute: int = 0


@router.get("/scheduler/status")
async def get_scheduler_status():
    """Get current scheduler status and configuration."""
    return app_scheduler.get_scheduler_status()


@router.post("/scheduler/configure")
async def configure_scheduler(request: SchedulerConfigRequest):
    """Configure the scheduler with new settings."""
    try:
        return app_scheduler.configure_scheduler(
            enabled=request.enabled,
            day_of_week=request.day_of_week,
            hour=request.hour,
            minute=request.minute,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/scheduler/trigger")
async def trigger_scheduler():
    """Manually trigger the scheduled job (for testing)."""
    result = app_scheduler.trigger_now()
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result


# --- Application Settings Endpoints ---

class AppSettings(BaseModel):
    email_enabled: bool
    default_update_mode: str

@router.get("/settings")
async def get_app_settings():
    """Get current application settings."""
    from src import settings_manager
    return settings_manager.load_settings()

@router.post("/settings")
async def update_app_settings(settings: AppSettings):
    """Update application settings."""
    from src import settings_manager
    
    # Validate update mode
    valid_modes = ["append", "check", "force-overwrite", "process-existing", "process-existing-except-alarms"]
    if settings.default_update_mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"Invalid update mode. Must be one of: {valid_modes}")
        
    try:
        settings_manager.save_settings(settings.dict())
        return settings.dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
