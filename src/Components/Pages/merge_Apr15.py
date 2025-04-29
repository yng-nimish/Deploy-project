import asyncio
import aiobotocore.session
import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
import time
import gc
import psutil
from aiobotocore.config import AioConfig

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

async def merge_parquet_to_excel_s3(s3_input_bucket: str, s3_input_prefix: str, s3_output_bucket: str, s3_output_prefix: str = "final_output"):
    """
    Reads only the 'number' column from Parquet files in S3, maps all rows sequentially to 1000x1000 grids
    across sheets z1-z1000, and creates Excel files with 1 billion rows per workbook (1000 sheets x 1M cells).
    Writes a workbook as soon as 1 billion rows are accumulated, then continues for subsequent billions.
    Preserves all data without dropping duplicates. Processes data incrementally to minimize memory usage.
    """
    print(f"Downloading Parquet files from s3://{s3_input_bucket}/{s3_input_prefix} and saving to s3://{s3_output_bucket}/{s3_output_prefix}")
    log_memory_usage()

    session = aiobotocore.session.get_session()
    async with session.create_client('s3', config=AioConfig(connect_timeout=30, read_timeout=30)) as s3_client:
        sheet_names = [f'z{i}' for i in range(1, 1001)]  # z1 to z1000
        rows_per_workbook = 1_000_000_000  # 1 billion rows (1000 sheets x 1M cells)
        rows_per_sheet = 1_000_000  # 1000 x 1000
        workbook_count = 0
        total_rows_processed = 0

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

        # Estimate total rows
        total_estimated_rows = 0
        for workbook_prefix in workbook_prefixes:
            parquet_path = f"s3://{s3_input_bucket}/{workbook_prefix}*.parquet"
            ddf = dd.read_parquet(parquet_path, engine='pyarrow', columns=['number'])
            est_rows = len(ddf)
            total_estimated_rows += est_rows
            print(f"Estimated rows for {workbook_prefix}: {est_rows}")
        print(f"Total estimated rows across all workbooks: {total_estimated_rows}")

        accumulated_numbers = []
        accumulated_rows = 0

        for workbook_prefix in workbook_prefixes:
            print(f"\nProcessing workbook: {workbook_prefix}")
            try:
                # List Parquet files in the workbook prefix
                response = await s3_client.list_objects_v2(Bucket=s3_input_bucket, Prefix=workbook_prefix)
                parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
                print(f"Found {len(parquet_files)} Parquet files for {workbook_prefix}")

                # Process each Parquet file individually
                for parquet_file in parquet_files:
                    parquet_path = f"s3://{s3_input_bucket}/{parquet_file}"
                    print(f"Reading Parquet file: {parquet_file}")
                    try:
                        # Read only the 'number' column
                        ddf = dd.read_parquet(parquet_path, engine='pyarrow', columns=['number'])
                        print(f"Loaded Dask DataFrame with {len(ddf)} rows (estimated)")
                        print(f"Schema: {ddf.dtypes}")

                        # Clean data
                        ddf['number'] = ddf['number'].astype(str).replace({None: '', 'None': '', pd.NA: ''})

                        # Compute to Pandas Series
                        number_series = ddf['number'].compute()
                        print(f"Computed {len(number_series)} rows for {parquet_file}")
                        log_memory_usage()

                        accumulated_numbers.append(number_series)
                        accumulated_rows += len(number_series)
                        total_rows_processed += len(number_series)
                        print(f"Current accumulated rows: {accumulated_rows}, Total processed: {total_rows_processed}")

                        del number_series
                        del ddf
                        gc.collect()

                        # Check if 1 billion rows accumulated
                        if accumulated_rows >= rows_per_workbook:
                            workbook_count += 1
                            local_output_filename = f"merged_data_workbook_{workbook_count}.xlsx"
                            local_output_path = os.path.abspath(local_output_filename)
                            print(f"\nCreating workbook {workbook_count} - {local_output_filename}")

                            # Concatenate accumulated numbers
                            print(f"Memory before concat: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
                            combined_numbers = pd.concat(accumulated_numbers, ignore_index=True)
                            print(f"Combined accumulated numbers with {len(combined_numbers)} rows")
                            print(f"Memory after concat: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
                            log_memory_usage()

                            # Map numbers to 1000x1000 sheets
                            workbook_data = {}
                            rows_to_process = min(len(combined_numbers), rows_per_workbook)
                            for sheet_idx, sheet_name in enumerate(sheet_names):
                                print(f"  Preparing sheet {sheet_name}")
                                sheet_df = pd.DataFrame('', index=range(1000), columns=range(1000))
                                start_row = sheet_idx * rows_per_sheet
                                end_row = min(start_row + rows_per_sheet, rows_to_process)

                                if start_row >= rows_to_process:
                                    break

                                # Map numbers to 1000x1000 grid
                                for i in range(start_row, end_row):
                                    row_idx = (i % rows_per_sheet) // 1000
                                    col_idx = (i % rows_per_sheet) % 1000
                                    sheet_df.iloc[row_idx, col_idx] = combined_numbers.iloc[i]

                                workbook_data[sheet_name] = sheet_df
                                print(f"  Prepared sheet {sheet_name} with data from index {start_row} to {end_row - 1}")
                                del sheet_df
                                gc.collect()
                                log_memory_usage()

                            # Write to Excel
                            try:
                                print(f"  Writing Excel file to {local_output_filename}")
                                with pd.ExcelWriter(local_output_filename, engine='xlsxwriter') as writer:
                                    for sheet_name, df in workbook_data.items():
                                        print(f"    Writing sheet {sheet_name}")
                                        df.to_excel(writer, sheet_name=sheet_name, header=False, index=False)
                                        print(f"    Wrote sheet {sheet_name}")
                                print(f"  Successfully created merged Excel file locally at {local_output_path}")

                                # Upload to S3
                                s3_output_key = f"{s3_output_prefix}/merged_data_workbook_{workbook_count}.xlsx"
                                print(f"  Uploading to s3://{s3_output_bucket}/{s3_output_key}")
                                await upload_to_s3(s3_client, local_output_filename, s3_output_bucket, s3_output_key)

                            except Exception as e:
                                print(f"  Error writing or uploading Excel file: {e}")
                            finally:
                                if os.path.exists(local_output_filename):
                                    os.remove(local_output_filename)
                                    print(f"  Cleaned up local file {local_output_filename}")
                                del combined_numbers
                                del workbook_data
                                gc.collect()
                                log_memory_usage()

                            # Update accumulated data
                            remaining_rows = len(combined_numbers) - rows_to_process if len(combined_numbers) > rows_to_process else 0
                            if remaining_rows > 0:
                                accumulated_numbers = [combined_numbers.iloc[rows_to_process:]]
                                accumulated_rows = remaining_rows
                                print(f"  Remaining rows after workbook {workbook_count}: {accumulated_rows}")
                            else:
                                accumulated_numbers = []
                                accumulated_rows = 0
                                print(f"  No remaining rows after workbook {workbook_count}")

                    except Exception as e:
                        print(f"Error processing {parquet_file}: {e}")
                        continue

            except Exception as e:
                print(f"Error processing workbook {workbook_prefix}: {e}")
                continue

        # Process any remaining data
        if accumulated_numbers and accumulated_rows > 0:
            workbook_count += 1
            local_output_filename = f"merged_data_workbook_{workbook_count}.xlsx"
            local_output_path = os.path.abspath(local_output_filename)
            print(f"\nCreating final workbook {workbook_count} - {local_output_filename}")

            print(f"Memory before concat: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
            combined_numbers = pd.concat(accumulated_numbers, ignore_index=True)
            print(f"Combined remaining numbers with {len(combined_numbers)} rows")
            print(f"Memory after concat: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
            log_memory_usage()

            workbook_data = {}
            rows_to_process = min(len(combined_numbers), rows_per_workbook)
            if rows_to_process > rows_per_sheet:
                rows_to_process = rows_per_sheet * (rows_to_process // rows_per_sheet)

            for sheet_idx, sheet_name in enumerate(sheet_names):
                print(f"  Preparing sheet {sheet_name}")
                sheet_df = pd.DataFrame('', index=range(1000), columns=range(1000))
                start_row = sheet_idx * rows_per_sheet
                end_row = min(start_row + rows_per_sheet, rows_to_process)

                if start_row >= rows_to_process:
                    break

                # Map numbers to 1000x1000 grid
                for i in range(start_row, end_row):
                    row_idx = (i % rows_per_sheet) // 1000
                    col_idx = (i % rows_per_sheet) % 1000
                    sheet_df.iloc[row_idx, col_idx] = combined_numbers.iloc[i]

                workbook_data[sheet_name] = sheet_df
                print(f"  Prepared sheet {sheet_name} with data from index {start_row} to {end_row - 1}")
                del sheet_df
                gc.collect()

            try:
                print(f"  Writing Excel file to {local_output_filename}")
                with pd.ExcelWriter(local_output_filename, engine='xlsxwriter') as writer:
                    for sheet_name, df in workbook_data.items():
                        print(f"    Writing sheet {sheet_name}")
                        df.to_excel(writer, sheet_name=sheet_name, header=False, index=False)
                        print(f"    Wrote sheet {sheet_name}")
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
                del combined_numbers
                del workbook_data
                gc.collect()
                log_memory_usage()

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