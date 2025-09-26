import os
import re
import glob
import shutil
from pathlib import Path
import polars as pl

import app_config
from lib.files import extract_dataframes

def write_staging(df: pl.DataFrame, filename: str):
    staging_dir = app_config.get_str(app_config.ConfigKeys.DIR_DATA_STAGING)
    filepath = os.path.join(staging_dir, f"{filename}.parquet")
    df.write_parquet(filepath)

# Delete & recreate the staging directory
def staging_reset() -> None:
    staging_dir = app_config.get_str(app_config.ConfigKeys.DIR_DATA_STAGING)
    shutil.rmtree(staging_dir, ignore_errors=True)
    Path(staging_dir).mkdir(parents=True, exist_ok=True)

def stagefiles_refresh() -> None:
    staging_reset()

    data_sources_dir = app_config.get_str(app_config.ConfigKeys.DIR_DATA_INPUTS)
    for filepath in glob.glob(os.path.join(data_sources_dir, "*")):
        dataframes = extract_dataframes(filepath)
        
        for df, identifier in dataframes:
            filename = re.sub(r"[\s\$\.]", "", identifier)
            write_staging(df, filename)

def stagefiles_ensure() -> None:
    staging_dir = app_config.get_str(app_config.ConfigKeys.DIR_DATA_STAGING)
    if not any(glob.glob(os.path.join(staging_dir, "*"))):
        stagefiles_refresh()
