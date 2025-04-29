import asyncio
import aiobotocore.session
import pandas as pd
import os
import time
import uuid
from io import BytesIO
from typing import List, Dict
from aiobotocore.config import AioConfig
def log_memory_usage():
    """Log current memory usage in GiB."""
    process = psutil.Process()
    mem_info = process.memory_info()
    print(f"Memory usage: {mem_info.rss / 1024**3:.2f} GiB")
    print(f"Available memory: {psutil.virtual_memory().available / 1024**3:.2f} GiB")
    
async def download_file_from_s3(s3_client, bucket_name: str, key: str) -> bytes:
    """
    Asynchronously downloads a file from S3 and returns the content as bytes.
    """
    try:
        obj = await s3_client.get_object(Bucket=bucket_name, Key=key)
        print(f"Successfully downloaded {key}")
        return await obj['Body'].read()
    except s3_client.exceptions.ClientError as e:
        print(f"Error downloading {key}: {e}")
        raise

async def process_parquet_file(s3_client, bucket_name: str, key: str) -> pd.DataFrame:
    """
    Asynchronously downloads and reads a Parquet file from S3 into a Pandas DataFrame.
    """
    try:
        parquet_data = await download_file_from_s3(s3_client, bucket_name, key)
        parquet_data_io = BytesIO(parquet_data)
        df = pd.read_parquet(parquet_data_io)
        print(f"Read Parquet file {key} with {len(df)} rows")
        df['number'] = df['number'].astype(str).replace({None: '', 'None': '', pd.NA: ''})
        df['sheet_name'] = df['sheet_name'].fillna('z1')
        print(f"Processed {key} successfully")
        return df
        log_memory_usage()

    except Exception as e:
        print(f"Error processing {key}: {e}")
        raise
        log_memory_usage()


async def merge_parquet_to_excel_s3(s3_input_bucket: str, s3_input_prefix: str, s3_output_bucket: str, s3_output_prefix: str = "final_output"):
    """
    Asynchronously downloads multiple Parquet files from an S3 bucket, merges them into multiple Excel files,
    each with 1000 sheets (z1-z1000), each with 1000 rows and 1000 columns filled with data (1 billion numbers total).
    Accumulates data across workbook_id prefixes until 1 billion numbers are reached, then creates and uploads an Excel file.
    """
    print(f"Downloading Parquet files from s3://{s3_input_bucket}/{s3_input_prefix} and saving to s3://{s3_output_bucket}/{s3_output_prefix}")
    log_memory_usage()

    session = aiobotocore.session.get_session()
    async with session.create_client('s3', config=AioConfig(connect_timeout=30, read_timeout=30)) as s3_client:
        sheet_names = [f'z{i}' for i in range(1, 1001)]  # z1 to z1000
        rows_per_workbook = 1_000_000_000  # 1 billion numbers
        rows_per_sheet = 1_000_000  # 1000 x 1000
        workbook_count = 0
        semaphore = asyncio.Semaphore(10)

        # List workbook_id directories with pagination
        workbook_prefixes = []
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            async for page in paginator.paginate(Bucket=s3_input_bucket, Prefix=s3_input_prefix, Delimiter='/'):
                workbook_prefixes.extend([prefix['Prefix'] for prefix in page.get('CommonPrefixes', [])])
            print(f"Found {len(workbook_prefixes)} workbook prefixes: {workbook_prefixes}")
            log_memory_usage()

        except s3_client.exceptions.ClientError as e:
            print(f"Error listing workbook directories in S3: {e}")
            return

        if not workbook_prefixes:
            print(f"No workbook directories found in s3://{s3_input_bucket}/{s3_input_prefix}")
            return

        accumulated_dfs = []
        total_rows = 0

        for workbook_prefix in workbook_prefixes:
            print(f"Processing workbook: {workbook_prefix}")
            try:
                # List Parquet files with pagination
                parquet_files = []
                async for page in paginator.paginate(Bucket=s3_input_bucket, Prefix=workbook_prefix):
                    parquet_files.extend([obj['Key'] for obj in page.get('Contents', []) if obj['Key'].endswith('.parquet')])
                print(f"Found {len(parquet_files)} Parquet files in {workbook_prefix}")
                log_memory_usage()


                if not parquet_files:
                    print(f"No Parquet files found in {workbook_prefix}")
                    continue

                async def process_with_semaphore(key):
                    async with semaphore:
                        return await process_parquet_file(s3_client, s3_input_bucket, key)

                tasks = [process_with_semaphore(file_key) for file_key in parquet_files]
                parquet_dfs = await asyncio.gather(*tasks, return_exceptions=True)

                valid_dfs = []
                for i, result in enumerate(parquet_dfs):
                    if isinstance(result, Exception):
                        print(f"Error in task {parquet_files[i]}: {result}")
                    else:
                        valid_dfs.append(result)

                if valid_dfs:
                    workbook_df = pd.concat(valid_dfs, ignore_index=True)
                    print(f"Concatenated {len(valid_dfs)} DataFrames with {len(workbook_df)} rows for {workbook_prefix}")
                    accumulated_dfs.append(workbook_df)
                    total_rows += len(workbook_df)
                    print(f"Current total rows accumulated: {total_rows}")
                else:
                    print(f"No valid DataFrames for {workbook_prefix}")
                    continue

                # Check if accumulated data is enough for a workbook
                while total_rows >= rows_per_workbook:
                    workbook_count += 1
                    local_output_filename = f"merged_data_workbook_{workbook_count}.xlsx"
                    local_output_path = os.path.abspath(local_output_filename)
                    print(f"\nCreating workbook {workbook_count} - {local_output_filename}")

                    # Combine accumulated DataFrames
                    combined_df = pd.concat(accumulated_dfs, ignore_index=True)
                    print(f"Combined all accumulated dataframes with total rows: {len(combined_df)}")

                    combined_df['number'] = combined_df['number'].replace({None: '', 'None': '', pd.NA: ''})

                    try:
                        # Aggregate to remove duplicates, taking first 'number' for each row_id, col_id
                        aggregated_df = combined_df.groupby(['row_id', 'col_id'])['number'].first().reset_index()
                        print(f"Aggregated to {len(aggregated_df)} rows")
                        if aggregated_df.empty:
                            print(f"Aggregated DataFrame is empty. Check input data: {combined_df.head()}")
                            accumulated_dfs = []
                            total_rows = 0
                            continue
                    except Exception as agg_error:
                        print(f"Error during aggregation: {agg_error}")
                        accumulated_dfs = []
                        total_rows = 0
                        continue

                    # Prepare workbook data
                    workbook_data: Dict[str, pd.DataFrame] = {}
                    rows_to_process = min(len(aggregated_df), rows_per_workbook)
                    start_index = 0

                    for sheet_index, sheet_name in enumerate(sheet_names):
                        print(f"  Preparing sheet {sheet_name}")
                        # Initialize empty 1000x1000 DataFrame
                        sheet_df = pd.DataFrame('', index=range(1000), columns=range(1000))
                        end_index = min(start_index + rows_per_sheet, rows_to_process)

                        if start_index >= rows_to_process:
                            break

                        # Get data for this sheet
                        sheet_data = aggregated_df.iloc[start_index:end_index]
                        for _, row in sheet_data.iterrows():
                            row_idx = int(row['row_id']) % 1000  # Map to 1000x1000 grid
                            col_idx = int(row['col_id']) % 1000
                            sheet_df.iloc[row_idx, col_idx] = row['number']

                        workbook_data[sheet_name] = sheet_df
                        print(f"  Prepared sheet {sheet_name} with data from index {start_index} to {end_index - 1}")
                        start_index += rows_per_sheet

                    # Write to local Excel file
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
                        with open(local_output_filename, 'rb') as f:
                            await s3_client.upload_fileobj(f, s3_output_bucket, s3_output_key)
                        print(f"  Successfully uploaded to s3://{s3_output_bucket}/{s3_output_key}")

                    except Exception as e:
                        print(f"  Error writing or uploading Excel file: {e}")
                    finally:
                        if os.path.exists(local_output_filename):
                            os.remove(local_output_filename)
                            print(f"  Cleaned up local file {local_output_filename}")

                    # Update accumulated data: keep remaining rows
                    remaining_rows = len(aggregated_df) - rows_to_process
                    if remaining_rows > 0:
                        accumulated_dfs = [aggregated_df.iloc[rows_to_process:]]
                        total_rows = remaining_rows
                        print(f"  Remaining rows after workbook {workbook_count}: {total_rows}")
                    else:
                        accumulated_dfs = []
                        total_rows = 0
                        print(f"  No remaining rows after workbook {workbook_count}")

            except Exception as e:
                print(f"Error processing workbook {workbook_prefix}: {e}")
                continue

        # Process any remaining data
        if accumulated_dfs and total_rows > 0:
            workbook_count += 1
            local_output_filename = f"merged_data_workbook_{workbook_count}.xlsx"
            local_output_path = os.path.abspath(local_output_filename)
            print(f"\nCreating final workbook {workbook_count} - {local_output_filename}")

            combined_df = pd.concat(accumulated_dfs, ignore_index=True)
            print(f"Combined remaining dataframes with total rows: {len(combined_df)}")

            combined_df['number'] = combined_df['number'].replace({None: '', 'None': '', pd.NA: ''})

            try:
                aggregated_df = combined_df.groupby(['row_id', 'col_id'])['number'].first().reset_index()
                print(f"Aggregated to {len(aggregated_df)} rows")
                if aggregated_df.empty:
                    print(f"Aggregated DataFrame is empty. Check input data: {combined_df.head()}")
                    return
            except Exception as agg_error:
                print(f"Error during aggregation: {agg_error}")
                return

            workbook_data: Dict[str, pd.DataFrame] = {}
            start_index = 0
            rows_to_process = min(len(aggregated_df), rows_per_workbook)

            for sheet_index, sheet_name in enumerate(sheet_names):
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
                with open(local_output_filename, 'rb') as f:
                    await s3_client.upload_fileobj(f, s3_output_bucket, s3_output_key)
                print(f"  Successfully uploaded to s3://{s3_output_bucket}/{s3_output_key}")

            except Exception as e:
                print(f"  Error writing or uploading Excel file: {e}")
            finally:
                if os.path.exists(local_output_filename):
                    os.remove(local_output_filename)
                    print(f"  Cleaned up local file {local_output_filename}")

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