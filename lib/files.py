import polars as pl
from pathlib import Path
from typing import Optional
import openpyxl
from lib.config import get_setting, Config
from lib.cleaning import df_clean_all

def extract_dataframes(filepath: str) -> list[tuple[pl.DataFrame, str]]:
    filename = Path(filepath).name
    results: list[tuple[pl.DataFrame, str]] = []
    
    if filename.startswith("~$"):
        return results
    
    extension = Path(filepath).suffix.lower()
    
    if extension in [".xlsx", ".xls"]:
        print("Parsing excel file: " + filename)
        
        try:
            workbook = openpyxl.load_workbook(filepath, read_only=True)
            sheets = workbook.sheetnames
        except Exception as e:
            print(f"Failed opening excel file {filename}: " + str(e))
            return results
        for sheet in sheets:
            try:
                df = pl.read_excel(filepath, sheet_name=sheet, raise_if_empty=False)
                sheet_identifier = f"{sheet}"
                results.append((df, sheet_identifier))
                print("Sheet extracted " + sheet)
                
            except Exception as e:
                print(f"Failed extracting sheet {sheet}: {str(e)}")
                
    elif extension == ".csv":
        try:
            df = pl.read_csv(filepath)
            results.append((df, filename))
            print(f"CSV extracted {filename}")
        except Exception as e:
            print(f"Failed reading CSV file {filename}: {str(e)}")
            
    elif extension == ".parquet":
        try:
            df = pl.read_parquet(filepath)
            results.append((df, filename))
            print(f"Parquet extracted {filename}")
        except Exception as e:
            print(f"Failed reading Parquet file {filename}: {str(e)}")
            
    else:
        print(f"Skipping unknown file type: {filepath}")

    # Filter and clean results before return
    results = [
        (df_clean_all(df), name) 
        for df, name in results 
        if df is not None
        and df.shape[1] > 0
    ]

    print(f"Writing {len(results)} Tables: \n{str.join("\n", [name for df, name in results])}")
    return results


def list_readable_files(directory: str, extensions: list[str] | None = None) -> list[str]:
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
    staging_dir = get_setting(Config.DIR_DATA_SOURCES)
    return list_readable_files(staging_dir)


def get_staging_files() -> list[str]:
    staging_dir = get_setting(Config.DIR_DATA_STAGING)
    return list_readable_files(staging_dir)


def get_output_files() -> list[str]:
    output_dir = get_setting(Config.DIR_DATA_OUTPUT)
    return list_readable_files(output_dir)


def read_dataframe(file_type: str, filename: str) -> Optional[pl.DataFrame]:
    try:
        if file_type == "Staging":
            directory = get_setting(Config.DIR_DATA_STAGING)
        elif file_type == "Output":
            directory = get_setting(Config.DIR_DATA_OUTPUT)
        else:
            return None

        file_path = Path(directory) / filename

        if not file_path.exists():
            return None

        extension = file_path.suffix.lower()
        strpath = str(file_path)

        print("reading dataframe " + strpath)

        match extension:
            case ".parquet":
                return pl.read_parquet(strpath)
            case ".csv":
                return pl.read_csv(strpath)
            case _:
                return None

    except Exception:
        return None
