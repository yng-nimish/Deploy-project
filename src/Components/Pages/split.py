import pandas as pd

def split_excel_file(input_file, output_file, num_sheets=5):
    # Read the large file into a DataFrame
    df = pd.read_excel(input_file)
    
    # Calculate the number of rows per sheet
    num_rows = len(df)
    rows_per_sheet = num_rows // num_sheets + (num_rows % num_sheets > 0)
    
    # Create a new Excel writer
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        for i in range(num_sheets):
            start_row = i * rows_per_sheet
            end_row = start_row + rows_per_sheet
            df_subset = df.iloc[start_row:end_row]
            df_subset.to_excel(writer, sheet_name=f'Sheet{i+1}', index=False)
    
    print(f'File successfully split into {num_sheets} sheets: {output_file}')

# Example usage
split_excel_file('large_file.xlsx', 'split_output.xlsx')