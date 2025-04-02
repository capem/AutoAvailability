# Data Export and Processing System

This project handles the export, archiving, and processing of wind turbine data from SQL Server databases.

## Architecture

The system follows a modular approach with these key components:

1. **Database Export Module** (`db_export.py`):
   - Connection pool management
   - Table schema extraction
   - MDB file creation
   - Data export from SQL Server to MDB
   - ZIP archive creation

2. **Data Export CLI** (`data_exporter.py`):
   - Command-line interface for data export
   - Parallel processing of export tasks
   - Progress reporting
   - Replaces the previous HTTP download functionality

3. **Calculation Module** (`calculation.py`):
   - Processes exported data
   - Handles ZIP extraction
   - Performs calculations and analysis

## Using the System

### Exporting Data

To export data for a specific period:

```bash
python data_exporter.py 2023-12
```

To export only specific data types:

```bash
python data_exporter.py 2023-12 --types met grd tur
```

### Running Calculations

The calculation system automatically uses the exported data:

```bash
python calculation.py
```

## Migration Guide

This system replaces the previous HTTP download approach with direct database exports:

1. **Transition Period**:
   - Both systems can run in parallel during testing
   - Generated MDB files are fully compatible with the existing calculation pipeline

2. **Full Migration**:
   - After validation, remove the `download_wps_history.py` script
   - Update any automation scripts to use `data_exporter.py` instead

3. **Validation Process**:
   - Export data for a known period using both the old and new methods
   - Compare MD5 checksums of the generated MDB files
   - Run calculations on both datasets and compare results

## Technical Details

### Database Connection

The system uses connection pooling to efficiently manage database connections:
- Reuses existing connections when possible
- Automatically recovers from broken connections
- Limits the number of concurrent connections

### File Structure

```
monthly_data/
├── exports/           # Generated MDB files
│   ├── MET/
│   ├── TUR/
│   └── ...
├── uploads/           # ZIP archives for calculation.py
│   ├── MET/
│   ├── TUR/
│   └── ...
└── results/           # Calculation results
```

### Data Types and Tables

| Type | SQL Server Table | Description |
|------|------------------|-------------|
| met  | tblSCMet         | Meteorological data |
| tur  | tblSCTurbine     | Turbine data |
| grd  | tblSCTurGrid     | Grid data |
| cnt  | tblSCTurCount    | Counter data |
| din  | tblSCTurDigiIn   | Digital input data |
| sum  | tblAlarmLog      | Alarm data |

## Error Handling

The system includes comprehensive error handling:
- Logging for all database operations
- Automatic retry for transient errors
- Graceful degradation when data is unavailable

## Dependencies

- pyodbc
- pandas
- rich (for progress display)