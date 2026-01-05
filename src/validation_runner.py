import pandas as pd
import pathlib
import json
import traceback
from datetime import datetime, date
from . import integrity
from . import logger_config

logger = logger_config.get_logger(__name__)

# Correct path assuming src/validation_runner.py -> src -> parent -> monthly_data
BASE_DATA_DIR = pathlib.Path(__file__).resolve().parent.parent / "monthly_data"
REPORT_FILE = BASE_DATA_DIR / "validation_report.json"

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON Encoder for standardizing complex types."""
    def default(self, obj):
        if isinstance(obj, (datetime, date, pd.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, pd.Timedelta):
            return str(obj)
        # Handle numpy types
        if hasattr(obj, 'item'):
             return obj.item()
        if pd.isna(obj): # Handle NaN
             return None
        return super().default(obj)
        
def run_validation_scan(target_periods=None, override_end_date=None):
    """
    Scans MET data files and generates a validation report.
    Args:
        target_periods (list): Optional list of 'YYYY-MM' strings to filter files.
        override_end_date (str): Optional 'YYYY-MM-DD' date to cap the check period.
    """
    logger.info("Starting full data validation scan...")
    
    report = {
        "last_run": datetime.now().isoformat(),
        "summary": {
            "total_files_scanned": 0,
            "total_issues": 0,
            "files_with_issues": 0,
            "stuck_values_count": 0,
            "out_of_range_count": 0,
            "completeness_issues_count": 0,
            "system_issues_count": 0
        },
        "details": []
    }
    
    met_dir = BASE_DATA_DIR / "data" / "MET"
    if not met_dir.exists():
        logger.error(f"MET data directory not found: {met_dir}")
        return report

    all_files = sorted(list(met_dir.glob("*-met.csv")))
    
    # Filter by target periods if provided
    files = []
    if target_periods:
        for f in all_files:
            # Check if filename starts with any of the target periods
            if any(f.name.startswith(p) for p in target_periods):
                files.append(f)
    else:
        files = all_files

    report["summary"]["total_files_scanned"] = len(files)
    
    files_with_issues = 0
    
    # Parse override date once
    parsed_override_end = None
    if override_end_date:
        try:
            parsed_override_end = pd.to_datetime(override_end_date)
            # Make it end of day
            parsed_override_end = parsed_override_end.replace(hour=23, minute=59, second=59)
        except:
             logger.warning(f"Invalid override end date: {override_end_date}")

    for file_path in files:
        try:
            logger.info(f"Scanning {file_path.name}...")
            
            # Extract period from filename (YYYY-MM-met.csv)
            try:
                period_str = file_path.name[:7] # YYYY-MM
                period_start = datetime.strptime(period_str, "%Y-%m")
                # End of month is start of next month - 1 second
                next_month = period_start.replace(day=1) + pd.DateOffset(months=1)
                month_end = next_month - pd.Timedelta(seconds=1)
                
                # Determine effective end date
                # Start with month end
                limit_date = month_end
                
                # If user provided override, clamp to that
                if parsed_override_end:
                    limit_date = min(limit_date, parsed_override_end)
                    
                # Always cap at now() to avoid future
                period_end = min(limit_date, datetime.now())

            except ValueError:
                logger.warning(f"Could not parse period from filename {file_path.name}. Skipping completeness check.")
                period_start = None
                period_end = None

            df = pd.read_csv(file_path)
            
            # Simple timestamp conversion if needed for sorting/reporting dates
            if 'TimeStamp' in df.columns:
                df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')

            # Run integrity checks
            issues = integrity.scan_met_integrity(df, period_start, period_end)
            
            if issues:
                files_with_issues += 1
                report["summary"]["total_issues"] += len(issues)
                
                file_report = {
                    "file": file_path.name,
                    "issues": []
                }
                
                for issue in issues:
                    # Remove 'indices' and 'mask' to keep JSON small and avoiding duplicates
                    clean_issue = {k: v for k, v in issue.items() if k not in ['indices', 'mask']}
                    file_report["issues"].append(clean_issue)
                    
                    if issue['type'] == 'stuck_value':
                        report["summary"]["stuck_values_count"] += 1
                    elif issue['type'] == 'out_of_range':
                        report["summary"]["out_of_range_count"] += 1
                    elif issue['type'] == 'completeness':
                        report["summary"]["completeness_issues_count"] += 1
                    elif issue['type'] == 'system_completeness':
                        report["summary"]["system_issues_count"] += 1
                        
                report["details"].append(file_report)
                
        except Exception as e:
            logger.error(f"Error processing file {file_path.name}: {e}")
            logger.debug(traceback.format_exc())
            report["details"].append({
                "file": file_path.name,
                "error": str(e)
            })

    report["summary"]["files_with_issues"] = files_with_issues
    
    # Save report
    try:
        with open(REPORT_FILE, "w") as f:
            json.dump(report, f, cls=CustomJSONEncoder, indent=4)
        logger.info(f"Validation report saved to {REPORT_FILE}")
    except Exception as e:
         logger.error(f"Failed to save validation report: {e}")

    return report
