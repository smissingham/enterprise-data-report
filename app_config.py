"""
Configuration manager with type-safe enum keys and configlib backend.

Usage:
    # In app.py (initialize once)
    import app_config
    app_config.init_config("app_config.yaml")
    
    # In lib files (use anywhere)
    from app_config import ConfigKeys, get_str, get_int, get_bool, set_str, save
    host = get_str(ConfigKeys.APP_DEBUG, 'localhost')
    set_str(ConfigKeys.APP_DEBUG, 'new-host')
    save()  # Persist changes back to file
"""

import yaml
from enum import Enum
from typing import Any, Optional, Union


class ConfigKeys(Enum):
    """Type-safe configuration keys enum."""
    DIR_DATA_INPUTS = "directories.data.inputs"
    DIR_DATA_STAGING= "directories.data.staging"
    DIR_DATA_OUTPUTS= "directories.data.outputs"


# Global config instance and file path
_config: Optional[dict] = None
_config_path: Optional[str] = None


def init_config(config_path: str = "app_config.yaml") -> Any:
    """
    Initialize the configuration from a YAML file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        The loaded configuration object
        
    Raises:
        FileNotFoundError: If the config file doesn't exist
        yaml.YAMLError: If the YAML file is malformed
    """
    global _config, _config_path
    if _config is None:
        with open(config_path, 'r') as f:
            _config = yaml.safe_load(f)
        _config_path = config_path
    return _config


def _ensure_initialized() -> None:
    """Ensure config is initialized before use."""
    if _config is None:
        raise RuntimeError(
            "Config not initialized. Call init_config() first from your main application."
        )


def _get(key: Union[ConfigKeys, str], default: Any = None) -> Any:
    """
    Private base getter with error handling.
    
    Args:
        key: Configuration key (enum or string)
        default: Default value if key not found
        
    Returns:
        Configuration value or default
        
    Raises:
        KeyError: If key is missing and no default provided
    """
    _ensure_initialized()
    key_str = key.value if isinstance(key, ConfigKeys) else key
    
    # Handle nested keys with dot notation
    keys = key_str.split('.')
    value = _config
    
    try:
        for k in keys:
            if value is None:
                break
            value = value[k]
    except (KeyError, TypeError):
        value = None
    
    if value is None:
        if default is None:
            # Pretty error message showing available keys for troubleshooting
            available_keys = list(_config.keys())[:10] if isinstance(_config, dict) else []
            
            error_msg = f"Configuration key '{key_str}' not found in config file."
            if available_keys:
                error_msg += f"\nAvailable top-level keys: {', '.join(available_keys)}"
            if isinstance(key, ConfigKeys):
                error_msg += f"\nEnum used: {key.name}"
            
            raise KeyError(error_msg)
        
        return default
    
    return value


def _set_nested_value(data: dict, key_path: str, value: Any) -> None:
    """
    Set a nested value in a dictionary using dot notation.
    
    Args:
        data: Dictionary to modify
        key_path: Dot-separated key path (e.g., "database.host")
        value: Value to set
    """
    keys = key_path.split('.')
    current = data
    
    # Navigate to the parent of the target key
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        elif not isinstance(current[key], dict):
            # Convert non-dict values to dict if needed
            current[key] = {}
        current = current[key]
    
    # Set the final value
    current[keys[-1]] = value


def _get_config_data() -> dict:
    """Get the underlying config data as a dictionary."""
    _ensure_initialized()
    
    # Config is already a dictionary from YAML loading
    if isinstance(_config, dict):
        return _config
    else:
        raise RuntimeError("Config is not a dictionary - invalid state")


def _set(key: Union[ConfigKeys, str], value: Any) -> None:
    """
    Private setter for configuration values.
    
    Args:
        key: Configuration key (enum or string)
        value: Value to set
    """
    _ensure_initialized()
    key_str = key.value if isinstance(key, ConfigKeys) else key
    
    data = _get_config_data()
    _set_nested_value(data, key_str, value)


def get_str(key: Union[ConfigKeys, str], default: str = "") -> str:
    """
    Get a string configuration value.
    
    Args:
        key: Configuration key (enum or string)
        default: Default string value
        
    Returns:
        String configuration value
    """
    return str(_get(key, default))


def get_int(key: Union[ConfigKeys, str], default: int = 0) -> int:
    """
    Get an integer configuration value.
    
    Args:
        key: Configuration key (enum or string)
        default: Default integer value
        
    Returns:
        Integer configuration value
    """
    value = _get(key, default)
    try:
        return int(value)
    except (ValueError, TypeError) as e:
        key_str = key.value if isinstance(key, ConfigKeys) else key
        raise ValueError(f"Configuration key '{key_str}' has value '{value}' which cannot be converted to int: {e}")


def get_float(key: Union[ConfigKeys, str], default: float = 0.0) -> float:
    """
    Get a float configuration value.
    
    Args:
        key: Configuration key (enum or string)
        default: Default float value
        
    Returns:
        Float configuration value
    """
    value = _get(key, default)
    try:
        return float(value)
    except (ValueError, TypeError) as e:
        key_str = key.value if isinstance(key, ConfigKeys) else key
        raise ValueError(f"Configuration key '{key_str}' has value '{value}' which cannot be converted to float: {e}")


def get_bool(key: Union[ConfigKeys, str], default: bool = False) -> bool:
    """
    Get a boolean configuration value.
    
    Args:
        key: Configuration key (enum or string)
        default: Default boolean value
        
    Returns:
        Boolean configuration value
    """
    value = _get(key, default)
    if isinstance(value, bool):
        return value
    elif isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on', 'enabled')
    else:
        try:
            return bool(value)
        except (ValueError, TypeError) as e:
            key_str = key.value if isinstance(key, ConfigKeys) else key
            raise ValueError(f"Configuration key '{key_str}' has value '{value}' which cannot be converted to bool: {e}")


def get_list(key: Union[ConfigKeys, str], default: Optional[list] = None) -> list:
    """
    Get a list configuration value.
    
    Args:
        key: Configuration key (enum or string)
        default: Default list value
        
    Returns:
        List configuration value
        
    Raises:
        KeyError: If key is missing and no default provided
        ValueError: If value is not a list
    """
    if default is None:
        default = []
    
    value = _get(key, default)
    if not isinstance(value, list):
        key_str = key.value if isinstance(key, ConfigKeys) else key
        raise ValueError(f"Configuration key '{key_str}' has value '{value}' which is not a list")
    return value


def set_str(key: Union[ConfigKeys, str], value: str) -> None:
    """
    Set a string configuration value.
    
    Args:
        key: Configuration key (enum or string)
        value: String value to set
    """
    _set(key, str(value))


def set_int(key: Union[ConfigKeys, str], value: int) -> None:
    """
    Set an integer configuration value.
    
    Args:
        key: Configuration key (enum or string)
        value: Integer value to set
    """
    _set(key, int(value))


def set_float(key: Union[ConfigKeys, str], value: float) -> None:
    """
    Set a float configuration value.
    
    Args:
        key: Configuration key (enum or string)
        value: Float value to set
    """
    _set(key, float(value))


def set_bool(key: Union[ConfigKeys, str], value: bool) -> None:
    """
    Set a boolean configuration value.
    
    Args:
        key: Configuration key (enum or string)
        value: Boolean value to set
    """
    _set(key, bool(value))


def set_list(key: Union[ConfigKeys, str], value: list) -> None:
    """
    Set a list configuration value.
    
    Args:
        key: Configuration key (enum or string)
        value: List value to set
    """
    if not isinstance(value, list):
        raise ValueError(f"Value must be a list, got {type(value)}")
    _set(key, value)


def reload_config() -> None:
    """
    Reload configuration from file.
    """
    global _config
    if _config_path is not None:
        with open(_config_path, 'r') as f:
            _config = yaml.safe_load(f)


def save() -> None:
    """
    Save the current configuration back to the YAML file.
    
    Raises:
        RuntimeError: If config not initialized or no config path available
        IOError: If unable to write to the config file
    """
    _ensure_initialized()
    
    if _config_path is None:
        raise RuntimeError("No config file path available for saving")
    
    try:
        data = _get_config_data()
        with open(_config_path, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    except Exception as e:
        raise IOError(f"Failed to save config to '{_config_path}': {e}")
