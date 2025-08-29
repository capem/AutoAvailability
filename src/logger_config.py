"""
Centralized Logging Configuration

This module provides a consistent logging configuration for the entire application.
It sets up both file and console logging with appropriate formatting and levels.
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from rich.logging import RichHandler

# Constants
LOG_DIRECTORY = "./logs"
LOG_FILENAME = "application.log"
DEFAULT_LOG_LEVEL = logging.INFO
FILE_LOG_LEVEL = logging.INFO
CONSOLE_LOG_LEVEL = logging.INFO

# Ensure log directory exists
if not os.path.exists(LOG_DIRECTORY):
    os.makedirs(LOG_DIRECTORY)

# Log file path
LOG_FILE_PATH = os.path.join(LOG_DIRECTORY, LOG_FILENAME)

# Configure root logger only once
_is_configured = False

def configure_logging():
    """Configure the root logger with file and console handlers."""
    global _is_configured
    
    if _is_configured:
        return
    
    # Create a logger at the root level
    root_logger = logging.getLogger()
    root_logger.setLevel(DEFAULT_LOG_LEVEL)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # File handler for WARNING and above
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(FILE_LOG_LEVEL)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    )
    
    # Console handler with Rich formatting for INFO and above
    console_handler = RichHandler(
        rich_tracebacks=True,
        show_time=False,
        show_path=False
    )
    console_handler.setLevel(CONSOLE_LOG_LEVEL)
    console_handler.setFormatter(
        logging.Formatter("%(message)s")
    )
    
    # Add handlers to the root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    _is_configured = True


def get_logger(name):
    """
    Get a logger with the specified name.
    
    Args:
        name: The name of the logger, typically the module name.
        
    Returns:
        A configured logger instance.
    """
    # Ensure logging is configured
    configure_logging()
    
    # Return a logger with the specified name
    return logging.getLogger(name)
