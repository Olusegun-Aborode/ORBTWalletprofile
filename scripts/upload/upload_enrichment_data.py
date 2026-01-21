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
UPLOAD_BATCH_SIZE = 5000
headers_dune = {"X-Dune-Api-Key": DUNE_API_KEY}

def upload_table(df, table_name, schema):
    if df.empty:
        print(f"⚠️  Skipping {table_name}: No data.")
        return False
        
    print(f"\n🚀 Processing {table_name} ({len(df)} rows)...")
    
    # 1. Delete Old
    print(f"🗑️  Deleting old table {table_name}...")
    requests.delete(f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{table_name}", headers=headers_dune)
    time.sleep(2)
    
    # 2. Create New
    print("🆕 Creating new table...")
    resp = requests.post(
        "https://api.dune.com/api/v1/table/create",
        headers=headers_dune,
        json={
            "namespace": DUNE_NAMESPACE,
            "table_name": table_name,
            "is_private": False,
            "schema": schema
        }
    )
    if resp.status_code not in [200, 201]:
        print(f"❌ Failed to create table: {resp.text}")
        # Try to continue if table exists? No, schema might change.
        # If it failed because it exists (and delete failed?), we might have issues.
        # But usually delete works.
    else:
        print("✅ Table created.")
        
    # 3. Upload Chunks
    print("📤 Uploading data...")
    chunks = [df[i:i + UPLOAD_BATCH_SIZE] for i in range(0, len(df), UPLOAD_BATCH_SIZE)]
    url = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{table_name}/insert"
    
    for i, chunk in enumerate(chunks):
        csv_data = chunk.to_csv(index=False)
        for attempt in range(3):
            try:
                response = requests.post(url, headers={**headers_dune, "Content-Type": "text/csv"}, data=csv_data)
                if response.status_code == 200:
                    print(f"   ✅ Uploaded chunk {i + 1}/{len(chunks)}")
                    break
                else:
                    time.sleep(2)
            except:
                time.sleep(2)
        else:
            print(f"   ❌ Failed chunk {i + 1}")
            
    print(f"🎉 Done with {table_name}!")
    return True

def main():
    print("🧹 Starting Enrichment Upload Process...")
    
    # 1. Wallet Ages
    if os.path.exists("data/intermediate/wallet_ages.csv"):
        df_ages = pd.read_csv("data/intermediate/wallet_ages.csv")
        schema_ages = [
            {"name": "wallet", "type": "varchar"},
            {"name": "first_tx_timestamp", "type": "varchar"}, # Keep as string for safety or timestamp
            {"name": "wallet_age_formatted", "type": "varchar"},
            {"name": "wallet_age_days", "type": "integer"}
        ]
        # Normalize
        df_ages['wallet'] = df_ages['wallet'].astype(str).str.lower().str.strip()
        upload_table(df_ages, "dataset_wallet_ages", schema_ages)
    else:
        print("⚠️  wallet_ages.csv not found.")

    # 2. Volumes (DEX, CEX, Lending)
    if os.path.exists("data/intermediate/wallet_volumes.csv"):
        df_vol = pd.read_csv("data/intermediate/wallet_volumes.csv")
        df_vol['wallet'] = df_vol['wallet'].astype(str).str.lower().str.strip()
        
        # DEX Table
        cols_dex = ['wallet', 'total_dex_volume_usd', 'interacted_dexs', 'most_used_dex', 'trade_count']
        schema_dex = [
            {"name": "wallet", "type": "varchar"},
            {"name": "total_dex_volume_usd", "type": "double"},
            {"name": "interacted_dexs", "type": "varchar"},
            {"name": "most_used_dex", "type": "varchar"},
            {"name": "trade_count", "type": "integer"}
        ]
        upload_table(df_vol[cols_dex], "dataset_dex_volume", schema_dex)
        
        # CEX Table
        cols_cex = ['wallet', 'total_cex_volume_usd', 'interacted_cexs', 'most_used_cex']
        schema_cex = [
            {"name": "wallet", "type": "varchar"},
            {"name": "total_cex_volume_usd", "type": "double"},
            {"name": "interacted_cexs", "type": "varchar"},
            {"name": "most_used_cex", "type": "varchar"}
        ]
        upload_table(df_vol[cols_cex], "dataset_cex_volume", schema_cex)
        
        # Lending Table
        cols_lending = ['wallet', 'total_lending_volume_usd', 'most_used_protocol']
        schema_lending = [
            {"name": "wallet", "type": "varchar"},
            {"name": "total_lending_volume_usd", "type": "double"},
            {"name": "most_used_protocol", "type": "varchar"}
        ]
        upload_table(df_vol[cols_lending], "dataset_lending_volume", schema_lending)
    else:
        print("⚠️  wallet_volumes.csv not found.")

    # 3. Gas Fees
    if os.path.exists("data/intermediate/wallet_gas_fees.csv"):
        df_gas = pd.read_csv("data/intermediate/wallet_gas_fees.csv")
        schema_gas = [
            {"name": "wallet", "type": "varchar"},
            {"name": "gas_fees_usd", "type": "double"},
            {"name": "total_transactions_analyzed", "type": "integer"}
        ]
        df_gas['wallet'] = df_gas['wallet'].astype(str).str.lower().str.strip()
        upload_table(df_gas, "dataset_gas_fees", schema_gas)
    else:
        print("⚠️  wallet_gas_fees.csv not found.")

if __name__ == "__main__":
    main()
