import os
import polars as pl
from pathlib import Path
from typing import List, Optional
from lib.settings import get_setting, Setting


def redetermine_types(df: pl.DataFrame) -> pl.DataFrame:
    """Intelligently redetermine better data types for columns in dataframe"""
    return df.with_columns(
        [
            pl.col(col).cast(pl.Categorical)
            if df[col].dtype == pl.Utf8 and df[col].n_unique() / df.height < 0.5
            else pl.col(col)
            for col in df.columns
        ]
    )


def sanitise_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans a dataframe of common problems in column names, data types, null values etc."""
    # sanitise column names
    df = df.rename(
        {
            col: col.strip().replace(" ", "_").replace("-", "_").lower()
            for col in df.columns
        }
    )

    # Replace common null value representations with actual nulls
    null_values = ["*", "N/A", "N.A.", "???", "NULL", "null"]
    string_columns = [col for col in df.columns if df[col].dtype == pl.Utf8]
    if string_columns:
        df = df.with_columns(
            [
                pl.col(col)
                .str.strip_chars()  # Remove leading/trailing whitespace
                .map_elements(
                    lambda x: None if x and x in null_values else x,
                    return_dtype=pl.Utf8,
                )
                for col in string_columns
            ]
        )

    # Drop rows where ALL columns are null (completely empty rows)
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))

    # Drop columns where ALL values are null (completely empty columns)
    df = df.select([col for col in df.columns if not df[col].is_null().all()])

    # Now that it's clean, see if we can re-determine data types
    df = redetermine_types(df)

    return df


def list_readable_files(directory: str, extensions: List[str] = None) -> List[str]:
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


def get_source_files() -> List[str]:
    """Get list of readable files in the sources directory."""
    staging_dir = get_setting(Setting.DIR_DATA_SOURCES)
    return list_readable_files(staging_dir)


def get_staging_files() -> List[str]:
    """Get list of readable files in the staging directory."""
    staging_dir = get_setting(Setting.DIR_DATA_STAGING)
    return list_readable_files(staging_dir)


def get_output_files() -> List[str]:
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
