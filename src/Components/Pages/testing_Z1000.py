import boto3
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import logging
import os
from botocore.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Constants
BUCKET = 'my-bucket-founder-series-sun'
PRIMARY_FOLDER = 'Batch 1/May 2'
TEST_FOLDERS = [f'F {str(i).zfill(4)}' for i in range(0, 125)]  # F 0001 to F 0125
Z_FOLDERS = [f'Z{str(i).zfill(3)}' for i in range(1, 1001)]  # Generates Z001, Z002, ..., Z1000
OUTPUT_PREFIX = 'Batch 1/Testing_results'
EXPECTED_FILE_SIZE = 1.4 * 1024 * 1024  # 1.4 MB in bytes
SIZE_TOLERANCE = 0.8 * 1024 * 1024  # 800 KB tolerance
MAX_WORKERS = 10  # Reduced to avoid connection pool issues
RETRY_CONFIG = Config(retries={'max_attempts': 10, 'mode': 'adaptive'})