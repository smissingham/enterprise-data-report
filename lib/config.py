import json
from pathlib import Path

from enum import Enum


class Config(Enum):
    DIR_DATA_SOURCES = "DIR_DATA_SOURCES"
    DIR_DATA_STAGING = "DIR_DATA_STAGING"
    DIR_DATA_OUTPUT = "DIR_DATA_OUTPUT"

_cached_config = None


def _get_config_path(config_file: str) -> Path:
    """Find the project root and return the config file path."""
    current_dir = Path(__file__).parent
    while current_dir != current_dir.parent:
        if (current_dir / "pyproject.toml").exists():
            break
        current_dir = current_dir.parent
    
    return current_dir / config_file


def load_config(config_file: str = "config.json") -> dict[str, object]:
    """Load configuration settings from JSON file."""
    config_path = _get_config_path(config_file)
    
    with open(config_path, "r") as f:
        config = json.load(f)
        return config


def get_setting(setting: Config) -> str:
    """Get a specific configuration setting value."""
    global _cached_config
    if _cached_config is None:
        _cached_config = load_config()
    value = _cached_config[setting.value].__str__()
    return str(Path(value).expanduser())


def save_config(
    settings: dict[str, object], config_file: str = "config.json"
) -> None:
    """Save configuration settings to JSON file."""
    global _cached_config
    
    config_path = _get_config_path(config_file)

    with open(config_path, "w") as f:
        json.dump(settings, f, indent=2)

    _cached_config = settings
