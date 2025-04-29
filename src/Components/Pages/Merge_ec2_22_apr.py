import pandas as pd
import openpyxl
import os
import s3fs
import logging
import time

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
S3_OUTPUT_BASE_PATH = "s3://my-bucket-sun-test/output/intermediate_parquet/"  # Base path for Glue output
FINAL_EXCEL_S3_PATH = "s3://my-bucket-sun-test/final_output/merged_data.xlsx"
AWS_REGION = "us-east-1"

def merge_parquet_to_excel():
    s3 = s3fs.S3FileSystem(region_name=AWS_REGION)
    workbook_dirs = [d for d in s3.ls(S3_OUTPUT_BASE_PATH) if d.endswith('/') and 'workbook_id=' in d]
    all_data = {}

    logger.info(f"Found workbook directories: {workbook_dirs}")

    for workbook_dir in workbook_dirs:
        try:
            workbook_id = workbook_dir.split('=')[-1].rstrip('/')
            parquet_files = s3.glob(f"{workbook_dir}/*.parquet")
            logger.info(f"Processing workbook ID: {workbook_id}, found {len(parquet_files)} Parquet files.")

            workbook_data = pd.DataFrame()
            for file in parquet_files:
                try:
                    df = pd.read_parquet(f"s3://{file}")
                    workbook_data = pd.concat([workbook_data, df], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error reading Parquet file {file}: {e}")

            if not workbook_data.empty:
                # Pivot the data for Excel structure
                try:
                    pivot_df = workbook_data.pivot_table(
                        index=['sheet_name', 'row_id'],
                        columns='col_id',
                        values='number',
                        aggfunc='first'
                    )
                    # Store data, using sheet_name as the key
                    for sheet in workbook_data['sheet_name'].unique():
                        sheet_df = pivot_df.loc[(sheet,)]
                        if sheet not in all_data:
                            all_data[sheet] = sheet_df
                        else:
                            # Handle potential duplicate sheet names if needed (e.g., append)
                            logger.warning(f"Duplicate sheet name '{sheet}' found. Appending data.")
                            all_data[sheet] = pd.concat([all_data[sheet], sheet_df], ignore_index=True)

                    logger.info(f"Processed workbook ID: {workbook_id}, shape: {workbook_data.shape}")
                except Exception as e:
                    logger.error(f"Error pivoting data for workbook {workbook_id}: {e}")
        except Exception as e:
            logger.error(f"Error processing workbook directory {workbook_dir}: {e}")

    # Write to Excel
    try:
        with pd.ExcelWriter(f"/tmp/merged_data.xlsx", engine='openpyxl') as writer:
            for sheet_name, df in all_data.items():
                try:
                    df.to_excel(writer, sheet_name=sheet_name, header=False, index=False)
                except Exception as e:
                    logger.error(f"Error writing sheet '{sheet_name}' to Excel: {e}")
        logger.info(f"Successfully created merged Excel file at /tmp/merged_data.xlsx")
        s3.upload_file("/tmp/merged_data.xlsx", FINAL_EXCEL_S3_PATH)
        logger.info(f"Uploaded merged Excel file to {FINAL_EXCEL_S3_PATH}")
    except Exception as e:
        logger.error(f"Error writing to Excel file: {e}")
    finally:
        if os.path.exists("/tmp/merged_data.xlsx"):
            os.remove("/tmp/merged_data.xlsx")

if __name__ == "__main__":
    start_time = time.time()
    merge_parquet_to_excel()
    end_time = time.time()
    logger.info(f"Merging process completed in {end_time - start_time:.2f} seconds.")