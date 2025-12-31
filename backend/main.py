"""
FastAPI Backend for Wind Farm Data Processing

Main entry point with CORS setup and API router registration.
"""

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .api import router

from contextlib import asynccontextmanager
from .api import router, cleanup_resources

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    yield
    # Shutdown
    cleanup_resources()

app = FastAPI(
    title="Wind Farm Data Processing API",
    description="Backend API for Wind Farm Data Processing System",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS configuration - allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],  # Vite dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


# Serve built frontend static files
FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"

if FRONTEND_DIST.exists():
    # Serve static assets (JS, CSS, etc.)
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="assets")
    
    # Catch-all route to serve index.html for client-side routing
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the SPA index.html for all non-API routes."""
        # Don't intercept API routes
        # Check both relative and absolute path scenarios
        path_to_check = full_path.lstrip('/')
        if path_to_check.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        
        index_file = FRONTEND_DIST / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
        return {"error": "Frontend not built"}


def _patch_asyncio_windows():
    """
    Patches asyncio's ProactorBasePipeTransport to suppress ConnectionResetError
    on Windows during shutdown/connection loss.
    """
    import sys
    if sys.platform == "win32":
        try:
            import asyncio.proactor_events
            
            # Access the class directly
            _ProactorBasePipeTransport = asyncio.proactor_events._ProactorBasePipeTransport
            
            # Save original method preventing double patch
            if not hasattr(_ProactorBasePipeTransport, "_original_call_connection_lost"):
                _ProactorBasePipeTransport._original_call_connection_lost = _ProactorBasePipeTransport._call_connection_lost
            
            original_method = _ProactorBasePipeTransport._original_call_connection_lost

            def patched_call_connection_lost(self, exc):
                try:
                    original_method(self, exc)
                except ConnectionResetError:
                    # Suppress WinError 10054
                    pass
            
            _ProactorBasePipeTransport._call_connection_lost = patched_call_connection_lost
        except ImportError:
            # Should not happen on Windows but safe guard
            pass

# Apply the patch immediately on import
_patch_asyncio_windows()

