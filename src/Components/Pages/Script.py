import pandas as pd
import boto3
from io import BytesIO

s3_client = boto3.client('s3')

def generate_excel(data, bucket_name, output_filename):
    # Define an in-memory Excel buffer I need this buffer to expand on the excel
    
    excel_buffer = BytesIO()

    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        sheet_index = 0
        rows_per_sheet = 1000
        cols_per_sheet = 1000

        # Process the data and write it in 1000x1000 grid per sheet
        for start_row in range(0, len(data), rows_per_sheet):
            end_row = min(start_row + rows_per_sheet, len(data))
            chunk = data.iloc[start_row:end_row, :cols_per_sheet]

      
            sheet_name = f"Z{sheet_index + 1}" # Sheet is named as Z for index
            chunk.to_excel(writer, sheet_name=sheet_name, index=False)

          
            sheet_index += 1

            # Stop if we reach 1000 sheets
            if sheet_index >= 1000:
                break

    # the Excel file will be saved to the buffer
    excel_data = excel_buffer.getvalue()

    s3_client.put_object(Body=excel_data, Bucket=bucket_name, Key=output_filename)

    return "Excel file generated and uploaded to S3."

