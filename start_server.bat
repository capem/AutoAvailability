@echo off
uv run uvicorn backend.main:app --host 0.0.0.0 --port 443 --ssl-keyfile key.pem --ssl-certfile cert.pem --timeout-graceful-shutdown 1
pause