import json
from pathlib import Path

from enum import Enum


class Setting(Enum):
    DIR_DATA_SOURCES = "DIR_DATA_SOURCES"
    DIR_DATA_STAGING = "DIR_DATA_STAGING"
    DIR_DATA_OUTPUT = "DIR_DATA_OUTPUT"


_DEFAULTS: dict[str, object] = {
    Setting.DIR_DATA_SOURCES.value: "../../datasources",
    Setting.DIR_DATA_STAGING.value: "../../datastaging",
    Setting.DIR_DATA_OUTPUT.value: "../../dataoutput",
}

_cached_settings = None


def load_settings(config_file: str = "config.json") -> dict[str, object]:
    # Find the project root by looking for pyproject.toml
    current_dir = Path(__file__).parent
    while current_dir != current_dir.parent:
        if (current_dir / "pyproject.toml").exists():
            break
        current_dir = current_dir.parent
    
    config_path = current_dir / config_file
    
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            return {**_DEFAULTS, **config}
    except FileNotFoundError:
        return _DEFAULTS


def get_setting(setting: Setting) -> str:
    global _cached_settings
    if _cached_settings is None:
        _cached_settings = load_settings()
    return _cached_settings[setting.value].__str__()


def save_settings(
    settings: dict[str, object], config_file: str = "config.json"
) -> None:
    global _cached_settings

    # Find the project root by looking for pyproject.toml
    current_dir = Path(__file__).parent
    while current_dir != current_dir.parent:
        if (current_dir / "pyproject.toml").exists():
            break
        current_dir = current_dir.parent
    
    config_path = current_dir / config_file

    # Filter out default values
    filtered_settings = {
        k: v for k, v in settings.items() if k not in _DEFAULTS or v != _DEFAULTS[k]
    }

    with open(config_path, "w") as f:
        json.dump(filtered_settings, f, indent=2)

    # Update cache
    _cached_settings = {**_DEFAULTS, **filtered_settings}
