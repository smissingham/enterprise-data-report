from lib.config import load_config, get_setting, Config
from lib.stage import *
from lib.files import get_staging_files, read_dataframe
from pathlib import Path
import polars as pl
import json
import importlib.util
import sys
import shutil

def load_column_determination():
    spec = importlib.util.spec_from_file_location(
        "column_determination", 
        "lib/smarts/column_determination.py"
    )
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.find_ranked_composite_keys
    return None

def clear_output_directory():
    output_dir = Path(get_setting(Config.DIR_DATA_OUTPUT))
    if output_dir.exists():
        print(f"Clearing output directory: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created clean output directory: {output_dir}")

def process_staging_files():
    find_ranked_composite_keys = load_column_determination()
    if not find_ranked_composite_keys:
        print("Could not load column determination module")
        return
    
    staging_files = get_staging_files()
    output_dir = Path(get_setting(Config.DIR_DATA_OUTPUT))
    
    for filename in staging_files:
        df = read_dataframe("Staging", filename)
        if df is not None:
            print(f"Processing {filename}")
            
            # Write Excel file
            excel_filename = "Table_" + Path(filename).stem + ".xlsx"
            excel_path = output_dir / excel_filename
            print(f"  Writing Excel to {excel_path}")
            df.write_excel(str(excel_path))
            
            # Analyze composite keys
            print(f"  Analyzing composite keys")
            composite_keys = find_ranked_composite_keys(df)
            
            # Create report
            report = {
                "filename": filename,
                "total_rows": df.height,
                "total_columns": df.width,
                "composite_keys": composite_keys
            }
            
            # Write report JSON
            report_filename = "Report_" + Path(filename).stem + ".json"
            report_path = output_dir / report_filename
            print(f"  Writing report to {report_path}")
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)

def main():
    stagefiles_refresh()
    clear_output_directory()
    process_staging_files()
    
if __name__ == "__main__":
    main()
