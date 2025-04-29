import asyncio
import aiobotocore.session
import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
import time
import gc
import psutil
import xlsxwriter
from aiobotocore.config import AioConfig
from concurrent.futures import ThreadPoolExecutor
from functools import partial

def log_memory_usage():
    """Log current memory usage in GiB."""
    process = psutil.Process()
    mem_info = process.memory_info()
    print(f"Memory usage: {mem_info.rss / 1024**3:.2f} GiB")
    print(f"Available memory: {psutil.virtual_memory().available / 1024**3:.2f} GiB")

async def upload_to_s3(s3_client, local_path: str, bucket: str, key: str):
    """
    Asynchronously uploads a file to S3 using put_object.
    """
    try:
        with open(local_path, 'rb') as f:
            data = f.read()
        await s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        print(f"Successfully uploaded to s3://{bucket}/{key}")
    except Exception as e:
        print(f"Error uploading to s3://{bucket}/{key}: {e}")
        raise

def process_parquet_file(parquet_path: str) -> pd.DataFrame:
    """
    Processing a single Parquet file, reading required columns and cleaning data.
    """
    try:
        start_time = time.time()
        ddf = dd.read_parquet(parquet_path, engine='pyarrow', columns=['row_id', 'col_id', 'sheet_name', 'number'])
        ddf['number'] = ddf['number'].astype(str).replace({None: '', 'None': '', pd.NA: ''})
        ddf['sheet_name'] = ddf['sheet_name'].fillna('z1')
        df = ddf.compute()
        print(f"Computed {len(df)} rows for {parquet_path} in {time.time() - start_time:.2f} seconds")
        return df
    except Exception as e:
        print(f"Error processing {parquet_path}: {e}")
        return pd.DataFrame()

def prepare_sheet(sheet_name: str, sheet_data: pd.DataFrame) -> tuple:
    """
    Prepare a single 1000x1000 sheet from sheet_data using vectorized operations.
    """
    start_time = time.time()
    sheet_df = pd.DataFrame('', index=range(1000), columns=range(1000))
    if not sheet_data.empty:
        # Ensure indices are integers and within bounds
        row_indices = sheet_data['row_id'].astype(int) % 1000
        col_indices = sheet_data['col_id'].astype(int) % 1000
        numbers = sheet_data['number']
        # Use vectorized assignment
        sheet_df.values[row_indices, col_indices] = numbers
    print(f"Prepared sheet {sheet_name} in {time.time() - start_time:.2f} seconds")
    return sheet_name, sheet_df

async def merge_parquet_to_excel_s3(s3_input_bucket: str, s3_input_prefix: str, s3_output_bucket: str, s3_output_prefix: str = "final_output"):
    """
    Reads row_id, col_id, sheet_name, and number from Parquet files in S3, maps rows directly to 1000x1000 grids
    across sheets z1-z100 using provided metadata. Processes all rows without dropping duplicates, using the last
    number for each cell. Parallelizes file reading (16 workers) and sheet preparation (8 workers). Triggers workbooks
    at 100 million rows.
    """
    print(f"Downloading Parquet files from s3://{s3_input_bucket}/{s3_input_prefix} and saving to s3://{s3_output_bucket}/{s3_output_prefix}")
    log_memory_usage()

    session = aiobotocore.session.get_session()
    async with session.create_client('s3', config=AioConfig(connect_timeout=30, read_timeout=30)) as s3_client:
        sheet_names = [f'z{i}' for i in range(1, 101)]  # z1 to z100 for 100M rows
        rows_per_workbook = 100_000_000  # 100M rows (100 sheets)
        batch_size = 50_000_000  # ~50M rows per batch
        file_workers = 16  # 16 vCPUs for file reading
        sheet_workers = 8  # 8 workers for sheet preparation
        total_rows_processed = 0
        workbook_count = 0
        accumulated_dfs = []
        accumulated_rows = 0

        # List workbook_id directories with pagination
        workbook_prefixes = []
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            async for page in paginator.paginate(Bucket=s3_input_bucket, Prefix=s3_input_prefix, Delimiter='/'):
                workbook_prefixes.extend([prefix['Prefix'] for prefix in page.get('CommonPrefixes', [])])
            print(f"Found {len(workbook_prefixes)} workbook prefixes: {workbook_prefixes}")
        except s3_client.exceptions.ClientError as e:
            print(f"Error listing workbook directories in S3: {e}")
            return

        if not workbook_prefixes:
            print(f"No workbook directories found in s3://{s3_input_bucket}/{s3_input_prefix}")
            return

        async def write_workbook(combined_df, workbook_count, local_output_filename, local_output_path):
            print(f"  Writing Excel file to {local_output_filename}")
            try:
                with xlsxwriter.Workbook(local_output_filename) as workbook:
                    # Group by sheet_name and process in parallel
                    with ThreadPoolExecutor(max_workers=sheet_workers) as executor:
                        sheet_tasks = []
                        for sheet_name in sheet_names:
                            sheet_data = combined_df[combined_df['sheet_name'] == sheet_name]
                            if not sheet_data.empty:
                                sheet_tasks.append(executor.submit(prepare_sheet, sheet_name, sheet_data))

                        # Collect results and write sheets
                        for future in sheet_tasks:
                            sheet_name, sheet_df = future.result()
                            print(f"  Writing sheet {sheet_name} to Excel")
                            worksheet = workbook.add_worksheet(sheet_name)
                            # Write rows in bulk to reduce I/O overhead
                            for row_idx in range(1000):
                                worksheet.write_row(row_idx, 0, sheet_df.iloc[row_idx].values)
                            print(f"  Wrote sheet {sheet_name}")
                            del sheet_df
                            del sheet_data
                            gc.collect()
                            log_memory_usage()

                print(f"  Successfully created merged Excel file locally at {local_output_path}")
                s3_output_key = f"{s3_output_prefix}/merged_data_workbook_{workbook_count}.xlsx"
                print(f"  Uploading to s3://{s3_output_bucket}/{s3_output_key}")
                await upload_to_s3(s3_client, local_output_filename, s3_output_bucket, s3_output_key)

            except Exception as e:
                print(f"  Error writing or uploading Excel file: {e}")
            finally:
                if os.path.exists(local_output_filename):
                    os.remove(local_output_filename)
                    print(f"  Cleaned up local file {local_output_filename}")

        for workbook_prefix in workbook_prefixes:
            print(f"\nProcessing workbook: {workbook_prefix}")
            try:
                # List Parquet files in the workbook prefix
                response = await s3_client.list_objects_v2(Bucket=s3_input_bucket, Prefix=workbook_prefix)
                parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
                print(f"Found {len(parquet_files)} Parquet files for {workbook_prefix}")

                # Process Parquet files in parallel
                with ThreadPoolExecutor(max_workers=file_workers) as executor:
                    parquet_paths = [f"s3://{s3_input_bucket}/{parquet_file}" for parquet_file in parquet_files]
                    chunk_dfs = list(executor.map(process_parquet_file, parquet_paths))
                    chunk_dfs = [df for df in chunk_dfs if not df.empty]

                for chunk_df in chunk_dfs:
                    accumulated_dfs.append(chunk_df)
                    accumulated_rows += len(chunk_df)
                    total_rows_processed += len(chunk_df)
                    print(f"Current accumulated rows: {accumulated_rows}, Total processed: {total_rows_processed}")
                    log_memory_usage()

                    # Process batch if enough rows
                    if accumulated_rows >= batch_size:
                        print(f"Processing batch of {accumulated_rows} rows")
                        combined_df = pd.concat(accumulated_dfs, ignore_index=True)
                        print(f"Combined batch with {len(combined_df)} rows")
                        log_memory_usage()

                        # Check if enough for a workbook
                        if total_rows_processed >= rows_per_workbook * (workbook_count + 1):
                            workbook_count += 1
                            local_output_filename = f"merged_data_workbook_{workbook_count}.xlsx"
                            local_output_path = os.path.abspath(local_output_filename)
                            print(f"\nCreating workbook {workbook_count} - {local_output_filename}")

                            await write_workbook(combined_df, workbook_count, local_output_filename, local_output_path)
                            accumulated_dfs = []
                            accumulated_rows = 0
                            del combined_df
                            gc.collect()
                        else:
                            accumulated_dfs = [combined_df]
                            accumulated_rows = len(combined_df)
                            print(f"Keeping batch with {accumulated_rows} rows")
                            del combined_df
                            gc.collect()

                del chunk_dfs
                gc.collect()

            except Exception as e:
                print(f"Error processing workbook {workbook_prefix}: {e}")
                continue

        # Process any remaining data
        if accumulated_dfs and accumulated_rows > 0:
            workbook_count += 1
            local_output_filename = f"merged_data_workbook_{workbook_count}.xlsx"
            local_output_path = os.path.abspath(local_output_filename)
            print(f"\nCreating final workbook {workbook_count} - {local_output_filename}")

            combined_df = pd.concat(accumulated_dfs, ignore_index=True)
            print(f"Combined remaining rows with {len(combined_df)} rows")
            log_memory_usage()

            await write_workbook(combined_df, workbook_count, local_output_filename, local_output_path)
            del combined_df
            gc.collect()

def main():
    """
    Main function to run the asynchronous merging and upload process.
    """
    s3_input_bucket = "my-bucket-parquet-test"
    s3_input_prefix = "today/intermediate_parquet/"
    s3_output_bucket = "my-bucket-sun-test"
    s3_output_prefix = "final_output"

    start_time = time.time()
    asyncio.run(merge_parquet_to_excel_s3(s3_input_bucket, s3_input_prefix, s3_output_bucket, s3_output_prefix))
    end_time = time.time()

    print(f"Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()