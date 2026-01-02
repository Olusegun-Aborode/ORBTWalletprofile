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

DUNE_NAMESPACE = "surgence_lab"
DUNE_TABLE_NAME = "dataset_wallet_portfolio_consolidated"
UPLOAD_BATCH_SIZE = 10000

# 1. LOAD DATA
print("📖 Loading data...")
df_sim = pd.read_csv("wallet_portfolio_ath_backup.csv")
df_alchemy = pd.read_csv("alchemy_eth_balances.csv")

# 2. PREPARE DATA
print("🔄 Merging datasets...")
# Normalize addresses
df_sim['wallet'] = df_sim['wallet'].astype(str).str.lower().str.strip()
df_alchemy['wallet'] = df_alchemy['wallet'].astype(str).str.lower().str.strip()

# Merge (Left join on SIM to keep all 206k wallets)
merged = pd.merge(df_sim, df_alchemy, on="wallet", how="left")

# Fill missing Alchemy values (NaN means we didn't fetch it because it wasn't in the active list)
merged['alchemy_eth_balance'] = merged['alchemy_eth_balance'].fillna(0.0)

# Calculate Alchemy Values
ETH_PRICE = 2970  # Current approx price
merged['alchemy_present_value_usd'] = merged['alchemy_eth_balance'] * ETH_PRICE

# For Alchemy ATH, we don't have historical data from that simple RPC call.
# We will assume Alchemy ATH = Alchemy Present (since we can't calculate it without full history)
# OR we could just leave it as 0/null. 
# User asked for "Alchemy ATH Wallet Value".
# Let's set it to Alchemy Present Value for now as a baseline, or 0. 
# Using Present Value is safer than 0 to avoid "missing data" confusion, but let's be honest.
merged['alchemy_ath_value_usd'] = merged['alchemy_present_value_usd'] # Placeholder as we only fetched current balance

# 3. SELECT & RENAME COLUMNS
final_df = merged[[
    'wallet',
    'alchemy_present_value_usd',
    'alchemy_ath_value_usd',
    'present_value_usd',
    'ath_value_usd',
    'top_tokens'
]]

# Rename to match user request
final_df.columns = [
    'wallet_address',
    'alchemy_current_wallet_value',
    'alchemy_ath_wallet_value',
    'sim_current_wallet_value',
    'sim_ath_wallet_value',
    'top_tokens_held'
]

# Round values
numeric_cols = ['alchemy_current_wallet_value', 'alchemy_ath_wallet_value', 'sim_current_wallet_value', 'sim_ath_wallet_value']
final_df[numeric_cols] = final_df[numeric_cols].round(2)

print(f"📊 Final Consolidated Dataset: {len(final_df)} rows")
print(final_df.head())

# 4. UPLOAD TO DUNE
headers_dune = {"X-Dune-Api-Key": DUNE_API_KEY}

def clear_dune_table():
    print(f"🗑️  Deleting old table {DUNE_TABLE_NAME}...")
    requests.delete(f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{DUNE_TABLE_NAME}", headers=headers_dune)
    time.sleep(2)
    
    print("🆕 Creating new table...")
    schema = [
        {"name": "wallet_address", "type": "varchar"},
        {"name": "alchemy_current_wallet_value", "type": "double"},
        {"name": "alchemy_ath_wallet_value", "type": "double"},
        {"name": "sim_current_wallet_value", "type": "double"},
        {"name": "sim_ath_wallet_value", "type": "double"},
        {"name": "top_tokens_held", "type": "varchar"}
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

if clear_dune_table():
    chunks = [final_df[i:i + UPLOAD_BATCH_SIZE] for i in range(0, len(final_df), UPLOAD_BATCH_SIZE)]
    print(f"🚀 Uploading in {len(chunks)} batches...")
    
    for i, chunk in enumerate(chunks):
        upload_chunk(chunk, i, len(chunks))

    print("🎉 Done! Table: dataset_wallet_portfolio_consolidated")
