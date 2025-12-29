# AutoTasks - Wind Farm Data Processing and Analysis

AutoTasks is a comprehensive data processing and analysis system for wind farm operations. It automates the extraction, processing, and reporting of operational data from wind turbines, providing insights into performance, availability, and energy production.

## Features

- **Automated Data Export**: Extracts data from SQL Server databases and archives it for processing
- **Data Preservation**: Maintains deleted database records in exported files
- **Manual Alarm Adjustments**: Allows manual setting of timeoff values for alarms
- **Availability Calculation**: Computes various availability metrics for wind turbines
- **Energy Loss Analysis**: Calculates and categorizes energy losses
- **Weekly Reporting**: Generates weekly reports on turbine performance and availability
- **Email Notifications**: Automatically sends reports and notifications via email
- **Cross-Month Processing**: Handles 7-day periods that span across two months


## Usage

### TUI Interface (Recommended)

Launch the new Terminal User Interface for an intuitive graphical experience:

```bash
uv run python tui_main.py
```

The TUI provides:
- Interactive menus and progress bars
- Real-time monitoring and logging
- Manual alarm adjustment interface
- System status and health checks
- Settings and configuration management

See [Quick Start Guide](QUICK_START_TUI.md) for immediate usage or [TUI Documentation](TUI_README.md) for comprehensive details.

### Command Line Interface (Legacy)

Run the original CLI to process data for the previous day and the preceding 6 days:

```bash
# Via TUI launcher
uv run python tui_main.py --cli

# Or directly
uv run python main.py
```

### Command Line Arguments (CLI Mode)

- `-y, --yesterday YYYY-MM-DD`: Specify a custom date instead of using yesterday
- `--update-mode {check,append,force-overwrite,process-existing}`: Control how existing data is handled
  - `check`: Report changes without modifying existing data
  - `append`: Update/append while preserving deleted records (default)
  - `force-overwrite`: Export fresh data, overwriting existing files
  - `process-existing`: Skip DB/export, process existing files only

### Examples

**TUI Examples (Recommended):**
```bash
# Launch TUI interface
uv run python tui_main.py

# Then use interactive menus:
# - Option 2: Run Yesterday
# - Option 3: Custom Date → Enter 2023-12-31
# - Option 4: Manage Alarms
```

**CLI Examples:**
```bash
# Process data for a specific date
uv run python tui_main.py --cli -y 2023-12-31

# Force a complete refresh of data
uv run python tui_main.py --cli --update-mode force-overwrite

# Process multiple dates
uv run python tui_main.py --cli -y 2023-12-31,2024-01-01 --update-mode append
```

### Manual Alarm Adjustments

The system allows you to manually set timeoff values for alarms that have no timeoff in the database. This is useful for correcting data issues or handling special cases.

**TUI Method (Recommended):**
```bash
uv run python tui_main.py
# Select option 4 (Manage Alarms)
# Use interactive interface to add, edit, or delete adjustments
```

**CLI Method:**
```bash
# List all current manual adjustments
uv run python -m src.adjust_alarms list

# Add a new manual adjustment
uv run python -m src.adjust_alarms add 12345 1005 2307405 "2023-01-15 08:30:00" "2023-01-15 14:45:00" --notes "Manually adjusted due to missing timeoff"

# Update an existing adjustment
uv run python -m src.adjust_alarms update 12345 --time_off "2023-01-15 16:00:00" --notes "Updated notes"

# Remove an adjustment
uv run python -m src.adjust_alarms remove 12345
```

## Project Structure

### Entry Points
- `tui_main.py`: **Main entry point** - Launches TUI or CLI interface
- `main.py`: Legacy CLI entry point
- `test_tui.py`: TUI testing and validation script

### TUI Components
- `src/simple_tui.py`: Simple terminal-compatible TUI (default)
- `src/tui_app.py`: Advanced Textual-based TUI (fallback)

### Core Modules
- `src/data_exporter.py`: Handles data export from SQL Server
- `src/calculation.py`: Performs calculations on exported data
- `src/hebdo_calc.py`: Generates weekly calculations and reports
- `src/email_send.py`: Handles email notifications
- `src/adjust_alarms.py`: Tool for managing manual alarm adjustments
- `src/config.py`: Configuration management
- `src/logger_config.py`: Logging configuration

### Data and Configuration
- `config/manual_adjustments.json`: Stores manual alarm timeoff adjustments
- `config/`: Configuration files (Excel files, etc.)
- `monthly_data/`: Directory for storing exported and processed data
  - `data/`: Raw exports from the database (organized by type)
  - `results/`: Calculation results and reports
- `logs/`: Application logs

### Documentation
- `TUI_README.md`: Comprehensive TUI documentation
- `QUICK_START_TUI.md`: Quick start guide for TUI
- `README.md`: Main project documentation

## Configuration

### Environment Variables Setup

This application uses environment variables for secure configuration management. All sensitive information like database credentials and email passwords are stored in a `.env` file.

#### Initial Setup

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Edit the `.env` file with your actual configuration values:
   ```bash
   # Database Configuration
   DB_SERVER=your_database_server_ip
   DB_DATABASE=your_database_name
   DB_USERNAME=your_database_username
   DB_PASSWORD=your_database_password

   # Email Configuration
   EMAIL_SENDER=your_email@domain.com
   EMAIL_PASSWORD=your_email_app_password
   EMAIL_RECEIVER_DEFAULT=recipient@domain.com
   EMAIL_FAILURE_RECIPIENT=admin@domain.com
   ```

#### Security Notes

- **Never commit the `.env` file to version control** - it contains sensitive credentials
- Use app-specific passwords for email authentication (not your regular password)
- Ensure database user has only the necessary permissions
- The `.env` file is already added to `.gitignore` to prevent accidental commits

#### Required Environment Variables

- `DB_SERVER`: Database server IP address
- `DB_DATABASE`: Database name
- `DB_USERNAME`: Database username
- `DB_PASSWORD`: Database password
- `EMAIL_SENDER`: Sender email address
- `EMAIL_PASSWORD`: Email app password
- `EMAIL_RECEIVER_DEFAULT`: Default recipient email
- `EMAIL_FAILURE_RECIPIENT`: Email for failure notifications

All configuration is now centralized in the `src/config.py` module, which loads values from environment variables with proper validation.

## Dependencies

- pandas: Data manipulation and analysis
- numpy: Numerical computing
- pyodbc: Database connectivity
- scipy: Scientific computing
- rich: Terminal formatting and logging
- smtplib: Email functionality
- python-dotenv: Environment variable management
