import pandas as pd
import requests
import time
import os

# Try to load env vars
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

# CONFIG
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
if not DUNE_API_KEY:
    raise ValueError("Please set DUNE_API_KEY in .env file")

DUNE_NAMESPACE = "orbt_official"
DUNE_TABLE_NAME = "dataset_alchemy_balances"
INPUT_FILE = "alchemy_eth_balances.csv"
UPLOAD_BATCH_SIZE = 10000

headers_dune = {"X-Dune-Api-Key": DUNE_API_KEY}

def clear_dune_table():
    print(f"🗑️  Deleting old table {DUNE_TABLE_NAME}...")
    requests.delete(f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{DUNE_TABLE_NAME}", headers=headers_dune)
    time.sleep(2)
    
    print("🆕 Creating new table...")
    schema = [
        {"name": "wallet_address", "type": "varchar"},
        {"name": "alchemy_eth_balance", "type": "double"}
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
            else:
                time.sleep(2)
        except:
            time.sleep(2)
    print(f"❌ Failed chunk {chunk_index + 1}")
    return False

def main():
    print("📖 Loading Alchemy data...")
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File {INPUT_FILE} not found.")
        return

    df = pd.read_csv(INPUT_FILE)
    
    # Normalize
    df['wallet'] = df['wallet'].astype(str).str.lower().str.strip()
    
    # Deduplicate
    print(f"🧹 Deduplicating inputs... (Original: {len(df)})")
    df = df.drop_duplicates(subset=['wallet'])
    print(f"✅ Unique rows: {len(df)}")
    
    # Rename columns to match schema
    df.columns = ['wallet_address', 'alchemy_eth_balance']
    
    if clear_dune_table():
        chunks = [df[i:i + UPLOAD_BATCH_SIZE] for i in range(0, len(df), UPLOAD_BATCH_SIZE)]
        print(f"🚀 Uploading in {len(chunks)} batches...")
        
        for i, chunk in enumerate(chunks):
            upload_chunk(chunk, i, len(chunks))

        print(f"🎉 Done! Table: {DUNE_TABLE_NAME}")

if __name__ == "__main__":
    main()
