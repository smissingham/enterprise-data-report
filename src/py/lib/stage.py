import os
import glob
import shutil
from pathlib import Path
import openpyxl
import polars as pl

from lib.settings import get_setting, Setting
from lib.data import sanitise_dataframe


def write_staging(df: pl.DataFrame, filename: str):
    staging_dir = get_setting(Setting.DIR_DATA_STAGING)
    filepath = os.path.join(staging_dir, f"{filename}.parquet")
    df.write_parquet(filepath)


# Delete & recreate the staging directory
def staging_reset() -> None:
    staging_dir = get_setting(Setting.DIR_DATA_STAGING)
    shutil.rmtree(staging_dir, ignore_errors=True)
    Path(staging_dir).mkdir(parents=True, exist_ok=True)


def stagefiles_refresh() -> None:
    # reset staging area contents
    staging_reset()

    # stage from sources to staging depending on filetype
    data_sources_dir = get_setting(Setting.DIR_DATA_SOURCES)
    for filepath in glob.glob(os.path.join(data_sources_dir, "*")):
        filename = os.path.basename(filepath)

        # ignore open-file cache files
        if filepath.lower().endswith((".xlsx", ".xls")) and not filename.startswith(
            "~$"
        ):
            print("Parsing excel file: " + filename)

            sheets = []
            try:
                workbook = openpyxl.load_workbook(filepath, read_only=True)
                sheets = workbook.sheetnames
            except Exception as e:
                print(f"Failed reading excel file {filename}: " + e.__str__())

            for sheet in sheets:
                try:
                    df = sanitise_dataframe(
                        pl.read_excel(filepath, sheet_name=sheet, raise_if_empty=False)
                    )

                    # skip empty sheets
                    if df.shape[1] == 0:
                        print(f"Skipping empty sheet {sheet}")
                        continue

                    # write dataframe to staging area
                    write_staging(df, f"{filename}-{sheet}")
                    print("Sheet parsed " + sheet)
                except Exception as e:
                    print(f"Failed parsing sheet {sheet}: {str(e)}")
        else:
            print("Skipping unknown file: " + filepath)


# Ensure that stagefiles are present if possible
def stagefiles_ensure() -> None:
    staging_dir = get_setting(Setting.DIR_DATA_STAGING)
    if not any(glob.glob(os.path.join(staging_dir, "*"))):
        stagefiles_refresh()
