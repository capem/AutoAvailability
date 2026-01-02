# Wind Farm Data Processing System

A modern web application for processing, analyzing, and reporting wind farm availability data. Built with **FastAPI** (Backend) and **React + Mantine** (Frontend).

## 🚀 Features

-   **Interactive Dashboard**: Select dates, choose advanced update modes, and trigger data processing tasks.
-   **Data Processing Modes**: Multiple strategies for data handling, including incremental updates and refreshing specific data types like alarms.
-   **Real-time Status**: Monitor processing progress with live step-by-step updates and progress tracking.
-   **Process Control**: Ability to abort/cancel running processing tasks safely through the UI.
-   **Granular Alarm Management**: Advanced UI for managing manual alarm adjustments with filtering, sorting, and **Bulk Actions** (Bulk Edit/Delete).
-   **System Health & Integrity**: Real-time status checks and integrated data completeness monitoring during export.
-   **Log Viewer**: Preview and download application logs directly in the browser.
-   **File Manager**: Built-in file explorer to browse processed results. Features **Multi-Select Filtering** (by Category and Period) and **Bulk Zip Downloads** for efficient data retrieval.

## 📊 Data Processing Modes

The system provides several strategies for handling data during the export process:

-   **Append (Default)**: Reconciles changes from the database while preserving records that may have been manually removed or adjusted in the local files.
-   **Check**: Performs a state check (count and checksum) between the database and local metadata to report changes without performing an export.
-   **Force Overwrite**: Performs a fresh export from the database, completely replacing existing local files.
-   **Process Existing**: Skips the database export entirely and runs calculations using the data already available in the local files.
-   **Process Existing except Alarms**: A hybrid mode that refreshes only the alarm data from the database while using existing local files for all other data types (Met, Turbine, etc.).

## 🛠️ Technology Stack

-   **Backend**: Python 3.13+, FastAPI, Multiprocessing, Pandas, PyODBC
-   **Frontend**: React 18, TypeScript, Vite, Mantine UI, React Query
-   **Database**: SQL Server (Source Data), File-based Storage (Processed Results)

## 📋 Prerequisites

-   **Python 3.13+** (Managed via `uv`)
-   **Node.js 20+** (v24.12.0 used in development)
-   **ODBC Driver 17 for SQL Server**

## ⚙️ Installation

### 1. Backend Setup

Ensure you have `uv` installed.

```powershell
# Install Python dependencies
uv sync
```

### 2. Frontend Setup

```powershell
cd frontend
# Install Node dependencies
npm install
```

> **Environment Troubleshooting**:
> If you encounter issues with `npm` not being recognized or platform checks failing (especially on Windows Server), you may need to manually add Node to your PATH and bypass platform checks in your PowerShell session:
> ```powershell
> # Add Node.js to PATH (adjust version path as needed)
> $env:PATH = "$PWD\..\node-v24.12.0-win-x64;$env:PATH"
>
> # Bypass platform check for older Windows versions
> $env:NODE_SKIP_PLATFORM_CHECK = 1
> ```

### 3. Configuration

Ensure your `.env` file is configured in the root directory:

```ini
DB_SERVER=192.168.x.x
DB_DATABASE=Scada_Data
DB_USERNAME=user
DB_PASSWORD=pass
EMAIL_SENDER=...
```

### 4. SSL Certificate Generation

Generate self-signed certificates (valid for 10 years) for HTTPS support:

```powershell
# Install cryptography library
uv pip install cryptography

# Generate cert.pem and key.pem
uv run python generate_cert.py
```

## 🏃‍♂️ Running the Application

### Option A: Production Mode (Recommended)

Run the backend and frontend from a single server.

1.  **Build the Frontend**:
    ```powershell
    cd frontend
    npm run build
    ```

2.  **Run the Server**:
    Go back to the root directory and start the FastAPI server.
    ```powershell
    cd ..
    
    # Run the Server (HTTPS - Port 443)
    # Note: Requires Administrator privileges
    uv run uvicorn backend.main:app --host 0.0.0.0 --port 443 --ssl-keyfile key.pem --ssl-certfile cert.pem
    ```
    
    > **Firewall Configuration**:
    > To access from other computers on the LAN, you must allow port 443 through the Windows Firewall:
    > ```powershell
    > New-NetFirewallRule -DisplayName "AutoAvailability HTTPS" -Direction Inbound -LocalPort 443 -Protocol TCP -Action Allow
    > ```

3.  **Access the App**:
    -   **Local**: [https://localhost](https://localhost)
    -   **LAN**: `https://192.168.0.208`
    *(Accept the self-signed cert warning)*

---

### Option B: Development Mode

Run backend and frontend separately for hot-reloading.

**Terminal 1 (Backend):**
```powershell
uv run uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload
```

**Terminal 2 (Frontend):**
```powershell
cd frontend
npm run dev
```

-   Frontend: [http://localhost:5173](http://localhost:5173)
-   Backend API Docs: [http://localhost:8000/docs](http://localhost:8000/docs)

## 📁 Project Structure

```
AutoAvailability/
├── backend/            # FastAPI Backend
│   ├── main.py         # App entry point (Static Files + CORS)
│   └── api.py          # API Endpoints (Processing, Alarms, Logs)
├── frontend/           # React Frontend
│   ├── src/
│   │   ├── components/ # Reusable UI components
│   │   ├── pages/      # Dashboard, Alarms, Settings views
│   │   └── api.ts      # Typed API client
│   └── dist/           # Built static files
├── src/                # Core Business Logic (Legacy/Shared)
│   ├── data_exporter.py
│   ├── integrity.py
│   └── ...
├── config/             # Configuration files
└── logs/               # Application logs
```

## 🔄 Core Logic

The application wraps the existing Python business logic located in `src/`. The backend uses **multiprocessing** to run heavy data processing tasks (export, calculation, reporting) without blocking the API server, allowing for real-time status updates and cancellation.
