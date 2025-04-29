import asyncio
import aiobotocore.session
import dask.dataframe as dd
import pandas as pd
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
    Reads Parquet files from S3, accumulates all rows (including duplicates), and merges them into Excel files
    with 1000 sheets (z1-z1000), each with 1000 rows and 1000 columns. Uses last value for each row_id, col_id
    in the Excel grid. Processes data incrementally to avoid OOM.
    """
    print(f"Downloading Parquet files from s3://{s3_input_bucket}/{s3_input_prefix} and saving to s3://{s3_output_bucket}/{s3_output_prefix}")
    log_memory_usage()

    session = aiobotocore.session.get_session()
    async with session.create_client('s3', config=AioConfig(connect_timeout=30, read_timeout=30)) as s3_client:
        sheet_names = [f'z{i}' for i in range(1, 1001)]  # z1 to z1000
        rows_per_workbook = 1_000_000_000  # Reverted to 1 billion
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
            ddf = dd.read_parquet(parquet_path, engine='pyarrow')
            est_rows = len(ddf)
            total_estimated_rows += est_rows
            print(f"Estimated rows for {workbook_prefix}: {est_rows}")
        print(f"Total estimated rows across all workbooks: {total_estimated_rows}")

        accumulated_dfs = []
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
                        # Read using Dask for memory efficiency
                        ddf = dd.read_parquet(parquet_path, engine='pyarrow')
                        print(f"Loaded Dask DataFrame with {len(ddf)} rows (estimated)")
                        print(f"Schema: {ddf.dtypes}")

                        # Clean data
                        ddf['number'] = ddf['number'].astype(str).replace({None: '', 'None': '', pd.NA: ''})
                        ddf['sheet_name'] = ddf['sheet_name'].fillna('z1')

                        # No early aggregation to preserve all rows
                        chunk_df = ddf.compute()
                        print(f"Computed {len(chunk_df)} rows for {parquet_file}")
                        log_memory_usage()

                        accumulated_dfs.append(chunk_df)
                        accumulated_rows += len(chunk_df)
                        total_rows_processed += len(chunk_df)
                        print(f"Current accumulated rows: {accumulated_rows}, Total processed: {total_rows_processed}")

                        del chunk_df
                        del ddf
                        gc.collect()

                    except Exception as e:
                        print(f"Error processing {parquet_file}: {e}")
                        continue

                    # Check if enough data for a workbook
                    if accumulated_rows >= rows_per_workbook:
                        workbook_count += 1
                        local_output_filename = f"merged_data_workbook_{workbook_count}.xlsx"
                        local_output_path = os.path.abspath(local_output_filename)
                        print(f"\nCreating workbook {workbook_count} - {local_output_filename}")

                        # Combine accumulated chunks
                        print(f"Memory before concat: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
                        combined_df = pd.concat(accumulated_dfs, ignore_index=True)
                        print(f"Combined accumulated chunks with {len(combined_df)} rows")
                        print(f"Memory after concat: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
                        log_memory_usage()

                        # Use last value for each row_id, col_id
                        aggregated_df = combined_df.sort_values(by=['row_id', 'col_id']).drop_duplicates(subset=['row_id', 'col_id'], keep='last').reset_index(drop=True)
                        print(f"Processed to {len(aggregated_df)} rows (keeping last value per row_id, col_id)")
                        log_memory_usage()
                        del combined_df
                        gc.collect()

                        # Write workbook incrementally
                        workbook_data = {}
                        rows_to_process = min(len(aggregated_df), rows_per_workbook)
                        start_index = 0

                        for sheet_name in sheet_names:
                            print(f"  Preparing sheet {sheet_name}")
                            sheet_df = pd.DataFrame('', index=range(1000), columns=range(1000))
                            end_index = min(start_index + rows_per_sheet, rows_to_process)

                            if start_index >= rows_to_process:
                                break

                            sheet_data = aggregated_df.iloc[start_index:end_index]
                            for _, row in sheet_data.iterrows():
                                row_idx = int(row['row_id']) % 1000
                                col_idx = int(row['col_id']) % 1000
                                sheet_df.iloc[row_idx, col_idx] = row['number']

                            workbook_data[sheet_name] = sheet_df
                            print(f"  Prepared sheet {sheet_name} with data from index {start_index} to {end_index - 1}")
                            start_index += rows_per_sheet
                            del sheet_data
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
                            del aggregated_df
                            del workbook_data
                            gc.collect()
                            log_memory_usage()

                        # Update accumulated data
                        remaining_rows = len(aggregated_df) - rows_to_process if len(aggregated_df) > rows_to_process else 0
                        if remaining_rows > 0:
                            accumulated_dfs = [aggregated_df.iloc[rows_to_process:]]
                            accumulated_rows = remaining_rows
                            print(f"  Remaining rows after workbook {workbook_count}: {accumulated_rows}")
                        else:
                            accumulated_dfs = []
                            accumulated_rows = 0
                            print(f"  No remaining rows after workbook {workbook_count}")

            except Exception as e:
                print(f"Error processing workbook {workbook_prefix}: {e}")
                continue

        # Process any remaining data
        if accumulated_dfs and accumulated_rows > 0:
            workbook_count += 1
            local_output_filename = f"merged_data_workbook_{workbook_count}.xlsx"
            local_output_path = os.path.abspath(local_output_filename)
            print(f"\nCreating final workbook {workbook_count} - {local_output_filename}")

            print(f"Memory before concat: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
            combined_df = pd.concat(accumulated_dfs, ignore_index=True)
            print(f"Combined remaining chunks with {len(combined_df)} rows")
            print(f"Memory after concat: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
            log_memory_usage()

            aggregated_df = combined_df.sort_values(by=['row_id', 'col_id']).drop_duplicates(subset=['row_id', 'col_id'], keep='last').reset_index(drop=True)
            print(f"Processed to {len(aggregated_df)} rows (keeping last value per row_id, col_id)")
            log_memory_usage()
            del combined_df
            gc.collect()

            workbook_data = {}
            rows_to_process = min(len(aggregated_df), rows_per_workbook)
            if rows_to_process > rows_per_sheet:
                rows_to_process = rows_per_sheet * (rows_to_process // rows_per_sheet)
            start_index = 0

            for sheet_name in sheet_names:
                print(f"  Preparing sheet {sheet_name}")
                sheet_df = pd.DataFrame('', index=range(1000), columns=range(1000))
                end_index = min(start_index + rows_per_sheet, rows_to_process)

                if start_index >= rows_to_process:
                    break

                sheet_data = aggregated_df.iloc[start_index:end_index]
                for _, row in sheet_data.iterrows():
                    row_idx = int(row['row_id']) % 1000
                    col_idx = int(row['col_id']) % 1000
                    sheet_df.iloc[row_idx, col_idx] = row['number']

                workbook_data[sheet_name] = sheet_df
                print(f"  Prepared sheet {sheet_name} with data from index {start_index} to {end_index - 1}")
                start_index += rows_per_sheet
                del sheet_data
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
                del aggregated_df
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