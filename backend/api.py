"""
API Routes for Wind Farm Data Processing

Exposes endpoints for data processing, alarm management, logs, and system status.
"""

import os
import multiprocessing
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

# Import src modules - adjust path since we're in backend/
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config
from src import adjust_alarms
from src import logger_config

logger = logger_config.get_logger(__name__)

router = APIRouter()


# --- Pydantic Models ---

class ProcessRequest(BaseModel):
    """Request model for data processing."""
    dates: List[str] = Field(..., description="List of dates in YYYY-MM-DD format")
    update_mode: str = Field(default="append", description="Update mode: append, check, force-overwrite, process-existing")


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


def run_processing_worker(status_dict, dates: List[str], update_mode: str):
    """Worker function that runs in a separate process."""
    try:
        import pandas as pd
        from src import data_exporter
        from src import calculation
        from src import hebdo_calc
        
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
            hebdo_calc.main(period_range, period_start_dt, period_end_dt)
        
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
    valid_modes = ["append", "check", "force-overwrite", "process-existing"]
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
async def list_alarms():
    """List all alarm adjustments."""
    adjustments = adjust_alarms.load_adjustments()
    return adjustments


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
