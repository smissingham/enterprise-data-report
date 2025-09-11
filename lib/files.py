import polars as pl
from pathlib import Path
from typing import Optional
from lib.settings import get_setting, Setting


def list_readable_files(
    directory: str, extensions: list[str] | None = None
) -> list[str]:
    """List all readable files in a directory with optional extension filtering."""
    if extensions is None:
        extensions = [".parquet", ".csv", ".xlsx", ".json"]

    try:
        path = Path(directory)
        if not path.exists():
            return []

        files = []
        for file in path.iterdir():
            if file.is_file() and file.suffix.lower() in extensions:
                files.append(file.name)

        return sorted(files)
    except Exception:
        return []


def get_source_files() -> list[str]:
    """Get list of readable files in the sources directory."""
    staging_dir = get_setting(Setting.DIR_DATA_SOURCES)
    return list_readable_files(staging_dir)


def get_staging_files() -> list[str]:
    """Get list of readable files in the staging directory."""
    staging_dir = get_setting(Setting.DIR_DATA_STAGING)
    return list_readable_files(staging_dir)


def get_output_files() -> list[str]:
    """Get list of readable files in the output directory."""
    output_dir = get_setting(Setting.DIR_DATA_OUTPUT)
    return list_readable_files(output_dir)


def read_dataframe(file_type: str, filename: str) -> Optional[pl.DataFrame]:
    """Read a dataframe from the specified type directory and filename."""
    try:
        if file_type == "Staging":
            directory = get_setting(Setting.DIR_DATA_STAGING)
        elif file_type == "Output":
            directory = get_setting(Setting.DIR_DATA_OUTPUT)
        else:
            return None

        file_path = Path(directory) / filename

        if not file_path.exists():
            return None

        # Read based on file extension
        extension = file_path.suffix.lower()
        strpath = str(file_path)

        print("reading dataframe " + strpath)

        match extension:
            case ".parquet":
                return pl.read_parquet(strpath)
            case ".csv":
                return pl.read_csv(strpath)
            case ".xlsx" | ".xls":
                return pl.read_excel(strpath)
            case _:
                return None

    except Exception:
        return None
