import pandas as pd
import numpy as np

def fill_excel_with_data(input_file, output_file):
    # Define constants
    MAX_ROWS = 1048576  # Max rows for Excel (Excel limit)
    MAX_COLUMNS = 16384  # Max columns for Excel (Excel limit)
    BATCH_SIZE = 1000000  # Number of numbers to process at once (1 million numbers)

    # Initialize variables
    sheet_index = 0
    current_chunk = []

    # Open a writer for Excel file with openpyxl engine (more efficient for large files)
    writer = pd.ExcelWriter(output_file, engine='openpyxl')

    # Read the numbers from the input file and process them in chunks
    with open(input_file, 'r') as file:
        for line in file:
            num = line.strip()
            if num.isdigit():  # Only consider valid numbers
                current_chunk.append(num)

            # Once the chunk is full, process it into a DataFrame
            if len(current_chunk) >= BATCH_SIZE:
                # Calculate how many rows are needed to fit the current chunk into columns of MAX_COLUMNS
                rows_needed = (len(current_chunk) + MAX_COLUMNS - 1) // MAX_COLUMNS  # Ceiling division

                # Check if the number of rows exceeds Excel's row limit (MAX_ROWS)
                if rows_needed > MAX_ROWS:
                    raise ValueError(f"Data exceeds Excel's row limit! Required rows: {rows_needed}, " f"Max allowed rows: {MAX_ROWS}")

                # Reshape data to fit within the Excel row/column limits
                reshaped_data = np.array(current_chunk).reshape(rows_needed, MAX_COLUMNS)

                # Create a DataFrame from the reshaped data
                df = pd.DataFrame(reshaped_data)

                # Ensure the data doesn't exceed the Excel limits
                if df.shape[0] > MAX_ROWS or df.shape[1] > MAX_COLUMNS:
                    raise ValueError(f"Sheet is too large! Your sheet size is: {df.shape}, " f"Max sheet size is: {MAX_ROWS}, {MAX_COLUMNS}")

                # Write the DataFrame to Excel in the corresponding sheet
                sheet_name = f'Sheet{sheet_index + 1}'
                df.to_excel(writer, index=False, header=False, sheet_name=sheet_name, startrow=0, startcol=0)

                # Reset chunk and prepare for the next set of data
                current_chunk = []
                sheet_index += 1  # Move to the next sheet

        # If there are any remaining numbers in the last chunk, write them as well
        if current_chunk:
            # Calculate the number of rows needed for the remaining data
            rows_needed = (len(current_chunk) + MAX_COLUMNS - 1) // MAX_COLUMNS  # Ceiling division
            reshaped_data = np.array(current_chunk).reshape(rows_needed, MAX_COLUMNS)
            df = pd.DataFrame(reshaped_data)
            sheet_name = f'Sheet{sheet_index + 1}'
            df.to_excel(writer, index=False, header=False, sheet_name=sheet_name, startrow=0, startcol=0)

    # Save the entire workbook
    writer.save()
    print(f"Data has been successfully written to {output_file}")

# Example usage
fill_excel_with_data('numbers.txt', 'output.xlsx')
