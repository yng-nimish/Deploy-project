import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import logging
import json
from botocore.exceptions import ClientError
import sys
import io

# Configure logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger()

# AWS clients
s3_client = boto3.client('s3', region_name='us-east-1')
dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-1')

# Constants
BUCKET_NAME = 'my-bucket-parquet-test'
PRIMARY_FOLDER = 'April 24/Test2/'
EXPECTED_FILE_SIZE = 1.4 * 1024 * 1024  # 1.4 MB in bytes
SIZE_TOLERANCE = 0.8 * 1024 * 1024  # 800 KB tolerance
CHECKING_FIXTURES_TABLE = 'Checking_Fixtures'

# Hardcoded serialKey
SERIAL_KEY = [
    '631044756',
    '7421F0000',
    '677972183'
]

# Expected parquet schema
EXPECTED_SCHEMA = pa.schema([
    ('x_coordinate', pa.int32()),
    ('y_coordinate', pa.int32()),
    ('number', pa.string())
])

# Mapping for A-P coordinates
MAPPINGS = {
    'A': ('aei', lambda a, e, i: (a, e, i)),
    'B': ('adg', lambda a, d, g: (a, d, g)),
    'C': ('beh', lambda b, e, h: (b, e, h)),
    'D': ('cfi', lambda c, f, i: (c, '1000', i)),  # f replaced with 1000
    'E': ('ceg', lambda c, e, g: (c, e, g)),
    'F': ('abc', lambda a, b, c: (a, b, c)),
    'G': ('cba', lambda c, b, a: (c, b, a)),
    'H': ('def', lambda d, e, f: (d, e, '1000')),  # f replaced with 1000
    'I': ('fed', lambda f, e, d: ('1000', e, d)),  # f replaced with 1000
    'J': ('ghi', lambda g, h, i: (g, h, i)),
    'K': ('ihg', lambda i, h, g: (i, h, g)),
    'L': ('gec', lambda g, e, c: (g, e, c)),
    'M': ('gda', lambda g, d, a: (g, d, a)),
    'N': ('heb', lambda h, e, b: (h, e, b)),
    'O': ('ifc', lambda i, f, c: (i, '1000', c)),  # f replaced with 1000
    'P': ('iea', lambda i, e, a: (i, e, a)),
}

def parse_serial_key(serial_key):
    """Parse serialKey into a, b, c, d, e, f, g, h, i and extract serial number."""
    try:
        logger.info("Starting serialKey parsing")
        if len(serial_key) != 3:
            raise ValueError("serialKey must have exactly 3 rows")
        
        row1, row2, row3 = serial_key
        if len(row1) != 9 or len(row2) != 9 or len(row3) != 9:
            raise ValueError("Each serialKey row must be 9 characters")
        
        # First row: a, b, c
        a = row1[:3]
        b = row1[3:6]
        c = row1[6:9]
        
        # Second row: d, e, f, serial number
        if 'F' not in row2:
            raise ValueError("Second row must contain 'F'")
        d = row2[:3]
        e = row2[3:6].replace('F', '9')
        f = row2[6:9]
        serial_number = row2[row2.index('F'):row2.index('F')+5]
        if len(serial_number) != 5 or not serial_number.startswith('F'):
            raise ValueError("Serial number must be 5 characters starting with 'F'")
        
        # Third row: g, h, i
        g = row3[:3]
        h = row3[3:6]
        i = row3[6:9]
        
        # Replace '000' with '1000'
        mappings = {'a': a, 'b': b, 'c': c, 'd': d, 'e': e, 'f': f, 'g': g, 'h': h, 'i': i}
        for key, value in mappings.items():
            if value == '000':
                mappings[key] = '1000'
        
        logger.info("Completed serialKey parsing")
        return mappings, serial_number
    except Exception as e:
        logger.error(f"Error parsing serialKey {serial_key}: {str(e)}")
        raise

def get_required_folders(mappings):
    """Determine required z folders based on A-P mappings."""
    try:
        logger.info("Starting folder determination")
        required_folders = set()
        for key, (pattern, coord_func) in MAPPINGS.items():
            x, y, z = coord_func(
                mappings[pattern[0]],
                mappings[pattern[1]],
                mappings[pattern[2]]
            )
            required_folders.add(f'z{z}')
        logger.info("Completed folder determination")
        return required_folders
    except Exception as e:
        logger.error(f"Error determining folders: {str(e)}")
        raise

def validate_parquet_file(s3_key):
    """Validate if the parquet file has the expected schema."""
    try:
        logger.info(f"Validating parquet file: {s3_key}")
        # Download file to memory buffer
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        buffer = io.BytesIO(obj['Body'].read())
        parquet_file = pq.read_table(buffer)
        schema = parquet_file.schema
        if schema.equals(EXPECTED_SCHEMA):
            logger.info(f"Schema validated for {s3_key}")
            return True
        logger.error(f"Invalid schema for {s3_key}. Expected: {EXPECTED_SCHEMA}, Got: {schema}")
        return False
    except Exception as e:
        logger.error(f"Error validating parquet file {s3_key}: {str(e)}")
        return False

def find_parquet_file(folder_path):
    """Find the first parquet file in the given S3 folder based on size."""
    try:
        logger.info(f"Searching for parquet file in {folder_path}")
        # Try original folder path
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder_path)
        if 'Contents' in response:
            for obj in response['Contents']:
                size = obj['Size']
                if (EXPECTED_FILE_SIZE - SIZE_TOLERANCE) <= size <= (EXPECTED_FILE_SIZE + SIZE_TOLERANCE):
                    if validate_parquet_file(obj['Key']):
                        logger.info(f"Found valid parquet file: {obj['Key']}")
                        return obj['Key']
                    else:
                        logger.error(f"Skipping {obj['Key']} due to invalid schema")
        
        # If no files found, try stripping leading zeros from folder name (e.g., z044 -> z44)
        folder_name = folder_path.strip('/').split('/')[-1]  # Get folder name (e.g., z044)
        if folder_name.startswith('z') and folder_name[1:].lstrip('0'):
            stripped_folder_name = f"z{folder_name[1:].lstrip('0')}"
            if stripped_folder_name != folder_name:  # Avoid retrying the same folder
                stripped_folder_path = folder_path.replace(folder_name, stripped_folder_name)
                logger.info(f"No valid files in {folder_path}, trying {stripped_folder_path}")
                response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=stripped_folder_path)
                if 'Contents' in response:
                    for obj in response['Contents']:
                        size = obj['Size']
                        if (EXPECTED_FILE_SIZE - SIZE_TOLERANCE) <= size <= (EXPECTED_FILE_SIZE + SIZE_TOLERANCE):
                            if validate_parquet_file(obj['Key']):
                                logger.info(f"Found valid parquet file: {obj['Key']}")
                                return obj['Key']
                            else:
                                logger.error(f"Skipping {obj['Key']} due to invalid schema")
        
        logger.error(f"No parquet file matching size criteria in {folder_path} or its stripped version")
        return None
    except ClientError as e:
        logger.error(f"Error listing objects in {folder_path}: {str(e)}")
        return None

def read_parquet_value(s3_key, x_coord, y_coord):
    """Read the number value from the parquet file at given coordinates."""
    try:
        logger.info(f"Reading parquet value from {s3_key} at x={x_coord}, y={y_coord}")
        # Download file to memory
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        buffer = io.BytesIO(obj['Body'].read())
        parquet_file = pq.read_table(buffer)
        df = parquet_file.to_pandas()
        
        # Filter for exact coordinates
        result = df[(df['x_coordinate'] == int(x_coord)) & (df['y_coordinate'] == int(y_coord))]
        if result.empty:
            logger.error(f"No data found for x={x_coord}, y={y_coord} in {s3_key}")
            return None
        
        value = str(result.iloc[0]['number']).zfill(3)  # Ensure 3 digits with leading zeros
        logger.info(f"Read value {value} from {s3_key}")
        return value
    except pa.lib.ArrowInvalid as e:
        logger.error(f"Invalid parquet file {s3_key}: {str(e)}")
        return None
    except ClientError as e:
        logger.error(f"Error reading parquet file {s3_key}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing parquet file {s3_key}: {str(e)}")
        return None

def process_folder(folder, mappings):
    """Process a single z folder to extract required values."""
    try:
        logger.info(f"Processing folder {folder}")
        results = []
        folder_path = f"{PRIMARY_FOLDER}{folder}/"
        s3_key = find_parquet_file(folder_path)
        if not s3_key:
            logger.error(f"Skipping folder {folder} due to missing or invalid parquet file")
            return results
        
        # Find which A-P mappings require this folder
        for key, (pattern, coord_func) in MAPPINGS.items():
            x, y, z = coord_func(
                mappings[pattern[0]],
                mappings[pattern[1]],
                mappings[pattern[2]]
            )
            if f'z{z}' == folder:
                value = read_parquet_value(s3_key, x, y)
                if value:
                    # Format coordinates as X631 Y190 Z631
                    coordinates = f"X{x.zfill(3)} Y{y.zfill(3)} Z{z.zfill(3)}"
                    results.append({
                        'Mapping': key,
                        'Code': pattern,
                        'Coordinates': coordinates,
                        'Value': value
                    })
                else:
                    logger.error(f"Missing value for mapping {key} at x={x}, y={y}, z={z}")
        logger.info(f"Completed processing folder {folder}")
        return results
    except Exception as e:
        logger.error(f"Error in process_folder for {folder}: {str(e)}")
        return []

def save_to_dynamodb(serial_number, fixture_data):
    """Save the checking fixture to DynamoDB."""
    try:
        logger.info(f"Saving to DynamoDB for serial number {serial_number}")
        # Sort fixture_data by Mapping (A to P) and create tabular format
        sorted_data = sorted(fixture_data, key=lambda x: x['Mapping'])
        table = dynamodb_resource.Table(CHECKING_FIXTURES_TABLE)
        item = {
            'Serial Number': serial_number,
            'FolderName': 'Test2',
            'Checking_Fixture': json.dumps(sorted_data, ensure_ascii=False)
        }
        table.put_item(Item=item)
        logger.info(f"Successfully saved fixture for serial number {serial_number}")
    except ClientError as e:
        logger.error(f"Error saving to DynamoDB for serial number {serial_number}: {str(e)}")
        raise

def main():
    """Main function to process serialKey and generate checking fixture."""
    try:
        # Parse hardcoded serialKey
        mappings, serial_number = parse_serial_key(SERIAL_KEY)
        logger.info(f"Parsed serialKey, serial number: {serial_number}")

        # Get required z folders
        required_folders = get_required_folders(mappings)
        logger.info(f"Required folders: {required_folders}")

        # Process folders sequentially
        fixture_data = []
        logger.info("Starting folder processing")
        for folder in required_folders:
            logger.info(f"Submitting folder {folder} for processing")
            results = process_folder(folder, mappings)
            fixture_data.extend(results)
            logger.info(f"Completed processing folder {folder}")

        # Check if all A-P mappings are present
        missing_mappings = [key for key in MAPPINGS if key not in [item['Mapping'] for item in fixture_data]]
        if missing_mappings:
            logger.error(f"Fixture incomplete due to missing mappings: {missing_mappings}")
            logger.error("Skipping DynamoDB save due to incomplete fixture")
            return

        # Save to DynamoDB if complete
        save_to_dynamodb(serial_number, fixture_data)

    except Exception as e:
        logger.error(f"Main process failed: {str(e)}")
        raise

if __name__ == '__main__':
    main()