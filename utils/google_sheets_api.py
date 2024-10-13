import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime


def authenticate_google_sheets(api_credentials_json):
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(api_credentials_json, scope)
    client = gspread.authorize(creds)
    return client


def upload_parquet_to_sheets(parquet_file_path, spreadsheet_url, api_credentials_json):
    df = pd.read_parquet(parquet_file_path)
    client = authenticate_google_sheets(api_credentials_json)

    try:
        sheet = client.open_by_url(spreadsheet_url)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Bảng tính không tìm thấy với URL: {spreadsheet_url}")
        return

    worksheet = sheet.get_worksheet(0)
    worksheet.clear()


    worksheet.append_row(df.columns.tolist())

    for row in df.values.tolist():
        worksheet.append_row(row)