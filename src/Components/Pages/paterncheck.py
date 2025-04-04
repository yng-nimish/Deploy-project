import pandas as pd
import numpy as np
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

# Configure logging
logging.basicConfig(
    filename='pattern_check_across_workbook.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def setup_logging():
    """Initialize logging configuration"""
    logger = logging.getLogger('PatternChecker')
    if not logger.handlers:
        handler = logging.FileHandler('pattern_check_across_workbook.log')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
    return logger

def load_excel_sheet(file_path, sheet_num):
    """Load specific Excel sheet with error handling"""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_num, header=None, dtype=str)
        return df.fillna('000')  # Fill NA with '000' for consistency
    except Exception as e:
        logger = setup_logging()
        logger.error(f"Error loading sheet {sheet_num}: {str(e)}")
        raise

def get_reference_set(file_path):
    """Get the reference set of 10 cells from Sheet 0, Row 0, Col 0-9"""
    try:
        df = load_excel_sheet(file_path, 0)
        reference_set = df.iloc[0, 0:10].values  # Sheet 0, Row 0, Columns 0-9
        logger = setup_logging()
        logger.info(f"Reference set loaded: {reference_set}")
        print(f"Reference set: {reference_set}")
        return reference_set
    except Exception as e:
        logger = setup_logging()
        logger.error(f"Error loading reference set: {str(e)}")
        raise

def compare_patterns(reference_set, target_set):
    """Compare reference set with target set"""
    return np.array_equal(reference_set, target_set)

def process_sheet(args):
    """Process a single sheet, comparing reference set against all 10-cell sequences"""
    sheet_num, file_path, reference_set = args
    logger = setup_logging()
    
    try:
        df = load_excel_sheet(file_path, sheet_num)
        logger.info(f"Started processing Sheet {sheet_num}")
        print(f"Processing Sheet {sheet_num}")
        
        # Horizontal comparisons
        for row in range(df.shape[0]):
            for col_start in range(df.shape[1] - 9):  # Up to 991 for 10 cells
                target_set = df.iloc[row, col_start:col_start+10].values
                if compare_patterns(reference_set, target_set):
                    location = f"Sheet{sheet_num}:Row{row}:Col{col_start}-{col_start+9}"
                    logger.info(f"Match found at {location}")
                    print(f"Match found at {location}")
        
        # Vertical comparisons
        for col in range(df.shape[1]):
            for row_start in range(df.shape[0] - 9):  # Up to 991 for 10 cells
                target_set = df.iloc[row_start:row_start+10, col].values
                if compare_patterns(reference_set, target_set):
                    location = f"Sheet{sheet_num}:Col{col}:Row{row_start}-{row_start+9}"
                    logger.info(f"Match found at {location}")
                    print(f"Match found at {location}")
                
        logger.info(f"Completed Sheet {sheet_num}")
        print(f"Completed Sheet {sheet_num}")
        
    except Exception as e:
        logger.error(f"Error processing Sheet {sheet_num}: {str(e)}")

def main(file_path):
    """Main function to process all sheets against reference set"""
    start_time = time.time()
    logger = setup_logging()
    
    if not Path(file_path).exists():
        logger.error("Excel file not found")
        raise FileNotFoundError("Excel file not found")
    
    # Get the reference set
    reference_set = get_reference_set(file_path)
    
    # Process all sheets in parallel
    with ProcessPoolExecutor(max_workers=8) as executor:  # 8 cores on M2
        executor.map(process_sheet, [(i, file_path, reference_set) for i in range(1000)])
    
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Total execution time: {execution_time:.2f} seconds")
    print(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    try:
        excel_file = "your_spreadsheet.xlsx"  # Replace with your file path
        main(excel_file)
    except Exception as e:
        logger = setup_logging()
        logger.error(f"Program failed: {str(e)}")
        print(f"Program failed: {str(e)}")