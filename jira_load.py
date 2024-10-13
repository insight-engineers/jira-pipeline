import os
from datetime import datetime
from dotenv import load_dotenv
from utils.google_sheets_api import upload_parquet_to_sheets
import time

time.sleep(3)
load_dotenv()

output_dir = os.getenv('OUTPUT_DIR', os.getcwd())

parquet_file_name = f'test_{datetime.now().strftime("%Y-%m-%d")}.parquet'
parquet_file_path = os.path.join(output_dir, parquet_file_name)

spreadsheet_url = os.getenv('SPREADSHEET_URL')
api_credentials_json = os.getenv('API_CREDENTIALS_JSON')

upload_parquet_to_sheets(parquet_file_path, spreadsheet_url, api_credentials_json)
