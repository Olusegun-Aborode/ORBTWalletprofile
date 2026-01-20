import pandas as pd
import requests
import os
import time
import io

# Try to load env vars
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DUNE_API_KEY = os.getenv("DUNE_API_KEY")
if not DUNE_API_KEY:
    raise ValueError("Please set DUNE_API_KEY in .env file")

# CONFIG
DUNE_NAMESPACE = "orbt_official"
DUNE_TABLE_NAME = "orbt_wallet_final_v2"
INPUT_FILE = "final_wallet_data_delta.csv" # Consolidated delta file

def upload_to_dune(csv_path):
    print(f"Reading {csv_path}...")
    try:
        df_input = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: {csv_path} not found. Did you run create_consolidated_table_delta.py?")
        return

    # Define the exact schema columns required by the Dune table
    required_columns = [
        'wallet_address',
        'tx_count',
        'wallet_age_days',
        'first_seen_date',
        'gas_fees_usd',
        'total_dex_volume_usd',
        'total_cex_volume_usd',
        'total_lending_volume_usd',
        'alchemy_current_wallet_value',
        'sim_current_wallet_value',
        'sim_ath_wallet_value',
        'top_tokens_held'
    ]

    # Prepare the DataFrame
    df_upload = df_input.copy()
    
    # Ensure wallet_address column exists (handle mapping if needed)
    if 'wallet_address' not in df_upload.columns and 'wallet' in df_upload.columns:
        df_upload['wallet_address'] = df_upload['wallet']
    
    # Fill missing columns with defaults (if any)
    for col in required_columns:
        if col not in df_upload.columns:
            if 'volume' in col or 'value' in col or 'fees' in col:
                df_upload[col] = 0.0
            elif 'count' in col or 'days' in col:
                df_upload[col] = 0
            else:
                df_upload[col] = ""

    # Reorder to match schema exactly
    df_upload = df_upload[required_columns]

    print(f"Prepared {len(df_upload)} records for upload.")
    
    # API Endpoint
    url = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{DUNE_TABLE_NAME}/insert"
    headers = {
        "X-Dune-Api-Key": DUNE_API_KEY,
        "Content-Type": "text/csv" # CRITICAL: Use CSV content type
    }
    
    # Batch upload
    BATCH_SIZE = 20000 # Dune supports larger CSV batches than JSON
    
    for i in range(0, len(df_upload), BATCH_SIZE):
        batch_df = df_upload.iloc[i:i+BATCH_SIZE]
        print(f"Uploading batch {i} to {i+len(batch_df)}...")
        
        # Convert batch to CSV string
        csv_buffer = io.StringIO()
        batch_df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        response = requests.post(url, headers=headers, data=csv_data)
        
        if response.status_code == 200:
            print("  -> Success")
        else:
            print(f"  -> Error {response.status_code}: {response.text}")
        
        time.sleep(1) # Rate limit safety

if __name__ == "__main__":
    upload_to_dune(INPUT_FILE)
