# AutoTasks - Wind Farm Data Processing and Analysis

AutoTasks is a comprehensive data processing and analysis system for wind farm operations. It automates the extraction, processing, and reporting of operational data from wind turbines, providing insights into performance, availability, and energy production.

## Features

- **Automated Data Export**: Extracts data from SQL Server databases and archives it for processing
- **Data Preservation**: Maintains deleted database records in exported files
- **Availability Calculation**: Computes various availability metrics for wind turbines
- **Energy Loss Analysis**: Calculates and categorizes energy losses
- **Weekly Reporting**: Generates weekly reports on turbine performance and availability
- **Email Notifications**: Automatically sends reports and notifications via email
- **Cross-Month Processing**: Handles 7-day periods that span across two months


## Usage

### Basic Usage

Run the main script to process data for the previous day and the preceding 6 days:

```
python main.py
```

### Command Line Arguments

- `-y, --yesterday YYYY-MM-DD`: Specify a custom date instead of using yesterday
- `--update-mode {check,append,force-overwrite}`: Control how existing data is handled
  - `check`: Report changes without modifying existing data
  - `append`: Update/append while preserving deleted records (default)
  - `force-overwrite`: Export fresh data, overwriting existing files

### Examples

Process data for a specific date:
```
python main.py -y 2023-12-31
```

Force a complete refresh of data:
```
python main.py --update-mode force-overwrite
```

## Project Structure

- `main.py`: Entry point for the application
- `data_exporter.py`: Handles data export from SQL Server
- `db_export.py`: Low-level database export functionality
- `calculation.py`: Performs calculations on exported data
- `hebdo_calc.py`: Generates weekly calculations and reports
- `email_send.py`: Handles email notifications
- `monthly_data/`: Directory for storing exported and processed data
  - `exports/`: Raw exports from the database
  - `uploads/`: Processed data files ready for analysis
  - `results/`: Calculation results and reports
- `logs/`: Application logs

## Configuration

Database connection parameters are defined in `db_export.py`:
Email configuration is defined in `email_send.py`.

## Dependencies

- pandas: Data manipulation and analysis
- numpy: Numerical computing
- pyodbc: Database connectivity
- scipy: Scientific computing
- rich: Terminal formatting and logging
- smtplib: Email functionality
