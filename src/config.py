"""
Environment Configuration Module

This module loads and manages environment variables from .env file
with proper defaults and validation for the AutoAvailability application.
"""

import os

from dotenv import load_dotenv

from . import logger_config

# Get a logger for this module
logger = logger_config.get_logger(__name__)

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class that loads environment variables with defaults and validation."""

    def __init__(self):
        """Initialize configuration by loading environment variables."""
        self._load_config()
        self._validate_config()

    def _load_config(self):
        """Load all configuration values from environment variables."""

        # Database Configuration
        self.DB_SERVER = os.getenv("DB_SERVER", "10.173.224.101")
        self.DB_DATABASE = os.getenv("DB_DATABASE", "WpsHistory")
        self.DB_USERNAME = os.getenv("DB_USERNAME", "odbc_user")
        self.DB_PASSWORD = os.getenv("DB_PASSWORD")
        self.DB_DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 11 for SQL Server}")

        # Email Configuration
        self.EMAIL_SENDER = os.getenv("EMAIL_SENDER")
        self.EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
        self.EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
        self.EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))

        # Default Email Recipients
        self.EMAIL_RECEIVER_DEFAULT = os.getenv("EMAIL_RECEIVER_DEFAULT")
        self.EMAIL_FAILURE_RECIPIENT = os.getenv("EMAIL_FAILURE_RECIPIENT")

        # Application Configuration
        self.CONFIG_ALARMS_FILE = os.getenv(
            "CONFIG_ALARMS_FILE", "./config/Alarmes List Norme RDS-PP_Tarec.xlsx"
        )
        self.CONFIG_MANUAL_ADJUSTMENTS_FILE = os.getenv(
            "CONFIG_MANUAL_ADJUSTMENTS_FILE", "./config/manual_adjustments.json"
        )
        self.BASE_DATA_PATH = os.getenv("BASE_DATA_PATH", "./monthly_data/data")

    def _validate_config(self):
        """Validate that required configuration values are present."""
        required_vars = [
            ("DB_PASSWORD", self.DB_PASSWORD),
            ("EMAIL_SENDER", self.EMAIL_SENDER),
            ("EMAIL_PASSWORD", self.EMAIL_PASSWORD),
            ("EMAIL_RECEIVER_DEFAULT", self.EMAIL_RECEIVER_DEFAULT),
            ("EMAIL_FAILURE_RECIPIENT", self.EMAIL_FAILURE_RECIPIENT),
        ]

        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)

        if missing_vars:
            error_msg = (
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info("Configuration loaded successfully from environment variables")

    def get_db_config(self):
        """Get database configuration as a dictionary."""
        return {
            "server": self.DB_SERVER,
            "database": self.DB_DATABASE,
            "username": self.DB_USERNAME,
            "password": self.DB_PASSWORD,
            "driver": self.DB_DRIVER,
        }

    def get_email_config(self):
        """Get email configuration as a dictionary."""
        return {
            "sender_email": self.EMAIL_SENDER,
            "password": self.EMAIL_PASSWORD,
            "smtp_host": self.EMAIL_SMTP_HOST,
            "smtp_port": self.EMAIL_SMTP_PORT,
            "receiver_default": self.EMAIL_RECEIVER_DEFAULT,
            "failure_recipient": self.EMAIL_FAILURE_RECIPIENT,
        }


# Create a global configuration instance
config = Config()

# Export commonly used configurations
DB_CONFIG = config.get_db_config()
EMAIL_CONFIG = config.get_email_config()

# Export file paths
ALARMS_FILE_PATH = config.CONFIG_ALARMS_FILE
MANUAL_ADJUSTMENTS_FILE = config.CONFIG_MANUAL_ADJUSTMENTS_FILE
BASE_DATA_PATH = config.BASE_DATA_PATH
