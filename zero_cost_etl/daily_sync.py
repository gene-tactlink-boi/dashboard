import os
import json
import time
import gzip
import io
import csv
import datetime
import requests
import jwt # PyJWT
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
from google.api_core.exceptions import Forbidden

# --- CONFIGURATION (Load from Environment Variables for Security) ---
# GOOGLE PLAY
GCP_JSON = json.loads(os.environ.get('GCP_JSON_KEY')) # Service Account Key
GCP_BUCKET_ID = os.environ.get('GCP_BUCKET_ID', '').strip()       # e.g., pubsite_prod_rev_...
PACKAGE_NAME = os.environ.get('ANDROID_PACKAGE_NAME', '').strip() # e.g., com.example.app

# APP STORE CONNECT
APPLE_KEY_ID = os.environ.get('APPLE_KEY_ID', '').strip()
APPLE_ISSUER_ID = os.environ.get('APPLE_ISSUER_ID', '').strip()
APPLE_PRIVATE_KEY = os.environ.get('APPLE_PRIVATE_KEY') # Content of .p8 file (don't strip, might break key format)
APPLE_VENDOR_ID = os.environ.get('APPLE_VENDOR_ID', '').strip()     # Starts with 8...

# GOOGLE SHEETS
SHEET_ID = os.environ.get('SHEET_ID', '').strip() # ID from the URL of your Google Sheet
SHEET_TAB_NAME = 'Data'

def get_google_play_data(target_date):
    """
    Fetches the monthly CSV from GCS, parses it, and finds the row for target_date.
    """
    print("--- Fetching Google Play Data ---")
    
    # Authenticate
    client = storage.Client.from_service_account_info(GCP_JSON)
    
    # Use client.bucket() instead of get_bucket() to avoid making an API call 
    # that requires storage.buckets.get permission.
    bucket = client.bucket(GCP_BUCKET_ID)

    # Construct file path: stats/installs/installs_PACKAGE_YYYYMM_overview.csv
    # Note: Google updates the SAME monthly file every day.
    year_month = target_date.strftime('%Y%m')
    blob_name = f"stats/installs/installs_{PACKAGE_NAME}_{year_month}_overview.csv"
    
    blob = bucket.blob(blob_name)
    
    try:
        if not blob.exists():
            print(f"File {blob_name} not found (maybe start of month?).")
            return []

        content = blob.download_as_text(encoding='utf-16') # Google CSVs are often UTF-16
    except Forbidden:
        print(f"ERROR: Permission denied accessing {blob_name}.")
        print(f"Please ensure the service account {client.service_account_email} has 'Storage Object Viewer' permission on bucket {GCP_BUCKET_ID}.")
        return []
    except Exception as e:
        print(f"ERROR: Failed to download file: {e}")
        return []
    
    # Parse CSV
    data = []
    reader = csv.DictReader(io.StringIO(content))
    
    for row in reader:
        # CSV Date format is usually 'YYYY-MM-DD'
        if row['Date'] == target_date.strftime('%Y-%m-%d'):
            # "Daily User Installs" or "Daily Device Installs"
            installs = int(row.get('Daily User Installs', 0))
            country = row.get('Country', 'Unknown')
            
            if installs > 0:
                data.append([
                    row['Date'],
                    country,
                    'Android',
                    installs
                ])
    
    print(f"Found {len(data)} rows for Android.")
    return data

def get_apple_data(target_date):
    """
    Generates JWT, hits App Store API, downloads GZIP report, parses it.
    """
    print("--- Fetching App Store Data ---")
    
    # 1. Generate JWT Token
    expiration = int(time.time()) + 20 * 60 # 20 mins
    headers = {
        "alg": "ES256",
        "kid": APPLE_KEY_ID,
        "typ": "JWT"
    }
    payload = {
        "iss": APPLE_ISSUER_ID,
        "exp": expiration,
        "aud": "appstoreconnect-v1"
    }
    
    token = jwt.encode(payload, APPLE_PRIVATE_KEY, algorithm="ES256", headers=headers)
    
    # 2. Request Report
    date_str = target_date.strftime('%Y-%m-%d')
    url = "https://api.appstoreconnect.apple.com/v1/salesReports"
    params = {
        "filter[frequency]": "DAILY",
        "filter[reportSubType]": "SUMMARY",
        "filter[reportType]": "SALES",
        "filter[reportDate]": date_str,
        "filter[vendorNumber]": APPLE_VENDOR_ID
    }
    
    response = requests.get(url, params=params, headers={'Authorization': f'Bearer {token}'})
    
    if response.status_code != 200:
        print(f"Apple API Error: {response.text}")
        return []
    
    # 3. Decompress and Parse
    # Apple returns a GZIP file containing a Tab-Separated Values (TSV) text
    try:
        content = gzip.decompress(response.content).decode('utf-8')
    except:
        print("Failed to decompress Apple response.")
        return []

    data = []
    # Apple TSV Headers usually: Provider, Provider Country, SKU, Developer, ... Country Code, ... Units
    lines = content.strip().split('\n')
    header = lines[0].split('\t')
    
    # Find indices
    try:
        idx_units = header.index('Units')
        idx_country = header.index('Country Code')
    except ValueError:
        print("Could not find 'Units' or 'Country Code' in Apple report.")
        return []

    for line in lines[1:]:
        cols = line.split('\t')
        units = int(cols[idx_units])
        country = cols[idx_country]
        
        if units > 0:
            data.append([
                date_str,
                country,
                'iOS',
                units
            ])
            
    print(f"Found {len(data)} rows for iOS.")
    return data

def update_sheet(new_rows):
    """
    Appends new rows to Google Sheet.
    """
    if not new_rows:
        print("No new data to upload.")
        return

    print(f"--- Uploading {len(new_rows)} rows to Google Sheets ---")
    
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(GCP_JSON, scope)
    client = gspread.authorize(creds)
    
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_TAB_NAME)
    sheet.append_rows(new_rows)
    print("Success!")

if __name__ == "__main__":
    # TARGET DATE: usually "Yesterday" or "2 days ago" due to reporting lags
    # Apple is usually ready next day; Google can lag 2 days.
    # Safe bet: 2 days ago.
    target_date = datetime.datetime.now() - datetime.timedelta(days=2)
    print(f"Running ETL for date: {target_date.strftime('%Y-%m-%d')}")

    android_data = get_google_play_data(target_date)
    ios_data = get_apple_data(target_date)
    
    all_data = android_data + ios_data
    update_sheet(all_data)
