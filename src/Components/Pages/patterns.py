import pandas as pd

excel_file = pd.ExcelFile('Patterns.xlsx')
sheet_names = excel_file.sheet_names
df_dict = {}
for sheet_name in sheet_names:
  df = pd.read_excel(excel_file, sheet_name=sheet_name)
  df_dict[sheet_name] = df
  
  for sheet_name, df in df_dict.items():
  rows, cols = df.shape
  print(f"Sheet: {sheet_name}, Rows: {rows}, Columns: {cols}")
  
  def iterate_and_compare_cells(df_dict, range_start=0, range_end=10):
  """Iterates through each sheet, row, and column combination within a specified range.

  Args:
    df_dict: A dictionary of pandas DataFrames, where keys are sheet names
        and values are DataFrames.
    range_start: The starting index for the range (inclusive).
    range_end: The ending index for the range (exclusive).
  """
  for sheet_name, df in df_dict.items():
    rows, cols = df.shape
    for row in range(range_start, min(rows - range_end + 1, rows)):
      for col in range(range_start, min(cols - range_end + 1, cols)):
        cell_set = df.iloc[row:row + range_end, col:col + range_end]
        # You would implement the comparison logic here using cell_set.
        print(f"Sheet: {sheet_name}, Row: {row}, Col: {col}")
        # print(cell_set)
        
        def iterate_and_compare_cells(df_dict, range_start=0, range_end=10):
  """Iterates through each sheet, row, and column combination within a specified range.

  Args:
    df_dict: A dictionary of pandas DataFrames, where keys are sheet names
        and values are DataFrames.
    range_start: The starting index for the range (inclusive).
    range_end: The ending index for the range (exclusive).
  """
  patterns = []
  for sheet_name, df in df_dict.items():
    rows, cols = df.shape
    for row in range(range_start, min(rows - range_end + 1, rows)):
      for col in range(range_start, min(cols - range_end + 1, cols)):
        cell_set = df.iloc[row:row + range_end, col:col + range_end].values.tolist()
        for other_sheet_name, other_df in df_dict.items():
          other_rows, other_cols = other_df.shape
          for other_row in range(range_start, min(other_rows - range_end + 1, other_rows)):
            for other_col in range(range_start, min(other_cols - range_end + 1, other_cols)):
              other_cell_set = other_df.iloc[other_row:other_row + range_end, other_col:other_col + range_end].values.tolist()
              if cell_set == other_cell_set and (sheet_name, row, col) != (other_sheet_name, other_row, other_col):
                patterns.append({
                    'sheet1': sheet_name,
                    'row1': row,
                    'col1': col,
                    'sheet2': other_sheet_name,
                    'row2': other_row,
                    'col2': other_col,
                    'values': cell_set
                })
                print(f"Pattern found: Sheet {sheet_name}, Row {row}, Col {col} matches Sheet {other_sheet_name}, Row {other_row}, Col {other_col}")
  if not patterns:
    print("No patterns found for the first set of 10 cells.")
  return patterns

pattern_results = iterate_and_compare_cells(df_dict)

