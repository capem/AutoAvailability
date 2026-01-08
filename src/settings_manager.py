import json
from pathlib import Path
from typing import Dict, Any
from src import logger_config

logger = logger_config.get_logger(__name__)

# Config file path
SETTINGS_FILE = Path(__file__).parent.parent / "config" / "app_settings.json"

# Default settings
DEFAULT_SETTINGS: Dict[str, Any] = {
    "email_enabled": True,
    "default_update_mode": "append",
    "calculation_source": "energy"
}

_current_settings: Dict[str, Any] = DEFAULT_SETTINGS.copy()

def load_settings() -> Dict[str, Any]:
    """Load application settings from file."""
    global _current_settings
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE, "r") as f:
                saved = json.load(f)
                _current_settings.update(saved)
        except Exception as e:
            logger.error(f"[SETTINGS] Failed to load settings: {e}")
    return _current_settings

def save_settings(new_settings: Dict[str, Any]) -> None:
    """Save application settings to file."""
    global _current_settings
    
    # Update current settings with new values
    _current_settings.update(new_settings)
    
    try:
        SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SETTINGS_FILE, "w") as f:
            json.dump(_current_settings, f, indent=2)
        logger.info("[SETTINGS] Configuration saved successfully")
    except Exception as e:
        logger.error(f"[SETTINGS] Failed to save settings: {e}")
        raise

def get_setting(key: str, default: Any = None) -> Any:
    """Get a single setting value."""
    _current_settings = load_settings()
    return _current_settings.get(key, default)

# Load on module import
load_settings()
