"""
Data Exporter Module

This module replaces the HTTP download functionality in download_wps_history.py
with local database export functionality. It exports data from SQL Server to
MDB files and creates ZIP archives for consumption by calculation.py.
"""

import os
import argparse
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    TextColumn,
    Progress,
    TimeRemainingColumn,
)
from db_export import export_and_archive_tables, TABLE_MAPPINGS


# Setup logging
def setup_default_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )

logger = logging.getLogger("rich")
BASE_FILE_PATH = "./monthly_data/uploads"

def ensure_directories():
    """Ensure all necessary directories exist"""
    # Create base directories
    os.makedirs("./monthly_data/exports", exist_ok=True)
    os.makedirs(BASE_FILE_PATH, exist_ok=True)
    
    # Create type-specific directories
    for file_type in TABLE_MAPPINGS.keys():
        os.makedirs(f"{BASE_FILE_PATH}/{file_type.upper()}", exist_ok=True)
        os.makedirs(f"./monthly_data/exports/{file_type.upper()}", exist_ok=True)

def export_alarms(period):
    """
    Export alarm data directly from the database for the given period
    
    Args:
        period: Period in YYYY-MM format
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Exporting alarm data for period {period}")
    
    # Use the db_export module to export alarms data
    # This is handled by the 'sum' file type (see TABLE_MAPPINGS)
    results = export_and_archive_tables(period, ['sum'])
    
    return results.get('sum', False)

def export_data_for_period(period, file_types=None):
    """
    Export data for the given period and file types
    
    Args:
        period: Period in YYYY-MM format
        file_types: List of file types to export, defaults to all
        
    Returns:
        Dict with results for each file type
    """
    if file_types is None:
        # Use all types except 'sum' which is handled separately
        file_types = [ft for ft in TABLE_MAPPINGS.keys() if ft != 'sum']
    
    with Progress(
        TextColumn("[bold blue]{task.description}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        TimeRemainingColumn(),
    ) as progress:
        tasks = {}
        
        # Create a task for each file type
        for file_type in file_types:
            task_description = f"Exporting {file_type.upper()}"
            task_id = progress.add_task(task_description, total=100)
            tasks[file_type] = task_id
        
        # Process each file type
        results = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {}
            for file_type in file_types:
                # Update progress to indicate start
                progress.update(tasks[file_type], completed=10, description=f"Exporting {file_type.upper()} - started")
                
                # Submit export task to executor
                futures[file_type] = executor.submit(
                    export_and_archive_tables, 
                    period, 
                    [file_type]
                )
            
            # Process results as they complete
            for file_type, future in futures.items():
                try:
                    # Update progress for database query
                    progress.update(tasks[file_type], completed=30, description=f"Exporting {file_type.upper()} - querying DB")
                    
                    # Get result from future
                    result = future.result()
                    success = result.get(file_type, False)
                    
                    results[file_type] = success
                    progress.update(tasks[file_type], 
                                    completed=100 if success else 0, 
                                    description=f"Exporting {file_type.upper()} - {'completed' if success else 'failed'}")
                
                except Exception as e:
                    logger.error(f"Failed to export {file_type}: {e}")
                    results[file_type] = False
                    progress.update(tasks[file_type], completed=0, description=f"Exporting {file_type.upper()} - failed")
    
    return results

def main(period):
    """
    Main function to export data and alarms for the given period
    
    Args:
        period: Period in YYYY-MM format
    """
    logger.info(f"Starting data export for period {period}")
    
    # Define file types, excluding 'sum' which is handled separately by export_alarms
    file_types = ["met", "din", "grd", "cnt", "tur"]
    
    # Ensure all necessary directories exist
    ensure_directories()
    
    # Generate date range for the period
    periods = pd.date_range(start=period, end=period, freq="MS").strftime("%Y-%m").tolist()
    
    # Export data for each period
    for p in periods:
        logger.info(f"Processing period {p}")
        
        # First, export the alarm data
        logger.info(f"Exporting alarm data for period {p}")
        alarm_result = export_alarms(p)
        if alarm_result:
            logger.info(f"Successfully exported alarm data for period {p}")
        else:
            logger.warning(f"Failed to export alarm data for period {p}")
        results = export_data_for_period(p, file_types)
        
        # Print summary
        logger.info(f"\nExport summary for {p}:")
        for file_type, success in results.items():
            logger.info(f"{file_type.upper()}: {'SUCCESS' if success else 'FAILED'}")

if __name__ == "__main__":
    setup_default_logger()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Export data from database to CSV files and create ZIP archives")
    parser.add_argument("period", help="Period in YYYY-MM format")
    parser.add_argument("--types", nargs="+", help="File types to export (optional)")
    
    args = parser.parse_args()
    
    period = args.period
    file_types = args.types
    
    main(period)