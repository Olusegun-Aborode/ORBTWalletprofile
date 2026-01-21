import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

# Load env vars
load_dotenv()
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
if not DUNE_API_KEY:
    raise ValueError("Please set DUNE_API_KEY in .env file")

DUNE_NAMESPACE = "orbt_official"
DUNE_TABLE_NAME = "dataset_wallet_portfolio_ath"
INPUT_FILE = "data/intermediate/wallet_portfolio_ath_backup.csv"
UPLOAD_BATCH_SIZE = 5000

headers_dune = {"X-Dune-Api-Key": DUNE_API_KEY}

def clear_and_create_table():
    print(f"🗑️  Checking/Deleting old table {DUNE_NAMESPACE}.{DUNE_TABLE_NAME}...")
    # Delete (ignore 404 if not exists)
    requests.delete(f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{DUNE_TABLE_NAME}", headers=headers_dune)
    time.sleep(2)
    
    print("🆕 Creating new table...")
    schema = [
        {"name": "wallet", "type": "varchar"},
        {"name": "present_value_usd", "type": "double"},
        {"name": "ath_value_usd", "type": "double"},
        {"name": "token_count", "type": "integer"},
        {"name": "top_tokens", "type": "varchar"}
    ]
    
    resp = requests.post(
        "https://api.dune.com/api/v1/table/create",
        headers=headers_dune,
        json={
            "namespace": DUNE_NAMESPACE,
            "table_name": DUNE_TABLE_NAME,
            "is_private": False,
            "schema": schema
        }
    )
    if resp.status_code in [200, 201]:
        print("✅ Table created.")
        return True
    else:
        print(f"❌ Failed to create table: {resp.text}")
        return False

def upload_chunk(df_chunk, chunk_index, total_chunks):
    csv_data = df_chunk.to_csv(index=False)
    url = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{DUNE_TABLE_NAME}/insert"
    
    for attempt in range(3):
        try:
            response = requests.post(url, headers={**headers_dune, "Content-Type": "text/csv"}, data=csv_data)
            if response.status_code == 200:
                print(f"✅ Uploaded chunk {chunk_index + 1}/{total_chunks}")
                return True
            elif response.status_code == 402:
                print("❌ Rate limit exceeded (402).")
                return False
            else:
                print(f"⚠️ Error {response.status_code}, retrying...")
                time.sleep(2)
        except Exception as e:
            print(f"⚠️ Exception: {e}")
            time.sleep(2)
    
    print(f"❌ Failed chunk {chunk_index + 1}")
    return False

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ {INPUT_FILE} not found.")
        return

    print(f"📖 Reading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    print(f"📊 Total records to upload: {len(df)}")
    
    if clear_and_create_table():
        chunks = [df[i:i + UPLOAD_BATCH_SIZE] for i in range(0, len(df), UPLOAD_BATCH_SIZE)]
        print(f"🚀 Uploading in {len(chunks)} batches...")
        
        for i, chunk in enumerate(chunks):
            if not upload_chunk(chunk, i, len(chunks)):
                print("❌ Aborting upload due to error.")
                break
        
        print(f"🎉 Done! Table: {DUNE_NAMESPACE}.{DUNE_TABLE_NAME}")

if __name__ == "__main__":
    main()
