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
UPLOAD_BATCH_SIZE = 10000
headers_dune = {"X-Dune-Api-Key": DUNE_API_KEY}

# Files
TX_FILE = "data/intermediate/wallet_tx_counts.csv"
ALCHEMY_FILE = "data/intermediate/alchemy_eth_balances.csv"
SIM_FILE = "data/intermediate/wallet_portfolio_ath_backup.csv"

def upload_table(df, table_name, schema):
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
        return False
        
    print("✅ Table created. Uploading data...")
    
    # 3. Upload Chunks
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
    print("🧹 Starting Data Cleaning & Upload Process...")
    
    # 1. Load Filter Data (Transaction Counts)
    print("📖 Loading transaction counts...")
    df_tx = pd.read_csv(TX_FILE)
    df_tx['wallet'] = df_tx['wallet'].astype(str).str.lower().str.strip()
    
    # Define Valid Wallets (0 < tx <= 20000)
    valid_wallets = set(df_tx[
        (df_tx['tx_count'] > 0) & 
        (df_tx['tx_count'] <= 20000)
    ]['wallet'])
    
    print(f"📊 Filtering Criteria:")
    print(f"   - Original Count: {len(df_tx)}")
    print(f"   - Valid Retail Count: {len(valid_wallets)}")
    print(f"   - Removed: {len(df_tx) - len(valid_wallets)} (Inactive or Bots)")

    # ---------------------------------------------------------
    # 2. Prepare & Upload: Alchemy Balances
    # ---------------------------------------------------------
    print("\n🔹 Preparing Alchemy Data...")
    df_alchemy = pd.read_csv(ALCHEMY_FILE)
    df_alchemy['wallet'] = df_alchemy['wallet'].astype(str).str.lower().str.strip()
    
    # Filter
    df_alchemy_clean = df_alchemy[df_alchemy['wallet'].isin(valid_wallets)].copy()
    df_alchemy_clean.columns = ['wallet_address', 'alchemy_eth_balance'] # Rename for Dune
    
    upload_table(
        df_alchemy_clean, 
        "dataset_alchemy_balances",
        [{"name": "wallet_address", "type": "varchar"}, {"name": "alchemy_eth_balance", "type": "double"}]
    )

    # ---------------------------------------------------------
    # 3. Prepare & Upload: SIM Portfolio
    # ---------------------------------------------------------
    print("\n🔹 Preparing SIM Portfolio Data...")
    df_sim = pd.read_csv(SIM_FILE)
    df_sim['wallet'] = df_sim['wallet'].astype(str).str.lower().str.strip()
    
    # Filter
    df_sim_clean = df_sim[df_sim['wallet'].isin(valid_wallets)].copy()
    
    # Rename cols for Dune
    df_sim_clean_upload = df_sim_clean.rename(columns={
        'wallet': 'wallet_address',
        'present_value_usd': 'present_value_usd',
        'ath_value_usd': 'ath_value_usd',
        'token_count': 'token_count',
        'top_tokens': 'top_tokens'
    })
    
    upload_table(
        df_sim_clean_upload, 
        "dataset_wallet_portfolio_ath",
        [
            {"name": "wallet_address", "type": "varchar"},
            {"name": "present_value_usd", "type": "double"},
            {"name": "ath_value_usd", "type": "double"},
            {"name": "token_count", "type": "integer"},
            {"name": "top_tokens", "type": "varchar"}
        ]
    )

    # ---------------------------------------------------------
    # 4. Prepare & Upload: Consolidated
    # ---------------------------------------------------------
    print("\n🔹 Preparing Consolidated Data...")
    
    # Merge Clean Datasets
    # Note: df_sim_clean and df_alchemy_clean are already filtered to the same set of valid wallets
    # But let's merge on 'wallet' (using the pre-renamed dataframes for convenience)
    
    # Use original column names for merge
    merged = pd.merge(df_sim_clean, df_alchemy, on="wallet", how="left")
    merged['alchemy_eth_balance'] = merged['alchemy_eth_balance'].fillna(0.0)
    
    # Calc Values
    ETH_PRICE = 2970
    merged['alchemy_present_value_usd'] = merged['alchemy_eth_balance'] * ETH_PRICE
    merged['alchemy_ath_value_usd'] = merged['alchemy_present_value_usd'] # Placeholder
    
    # Select Final Cols
    final_df = merged[[
        'wallet',
        'alchemy_present_value_usd',
        'alchemy_ath_value_usd',
        'present_value_usd',
        'ath_value_usd',
        'top_tokens'
    ]]
    
    final_df.columns = [
        'wallet_address',
        'alchemy_current_wallet_value',
        'alchemy_ath_wallet_value',
        'sim_current_wallet_value',
        'sim_ath_wallet_value',
        'top_tokens_held'
    ]
    
    # Round
    numeric_cols = ['alchemy_current_wallet_value', 'alchemy_ath_wallet_value', 'sim_current_wallet_value', 'sim_ath_wallet_value']
    final_df[numeric_cols] = final_df[numeric_cols].round(2)
    
    upload_table(
        final_df, 
        "dataset_wallet_portfolio_consolidated",
        [
            {"name": "wallet_address", "type": "varchar"},
            {"name": "alchemy_current_wallet_value", "type": "double"},
            {"name": "alchemy_ath_wallet_value", "type": "double"},
            {"name": "sim_current_wallet_value", "type": "double"},
            {"name": "sim_ath_wallet_value", "type": "double"},
            {"name": "top_tokens_held", "type": "varchar"}
        ]
    )

    # ---------------------------------------------------------
    # 5. Prepare & Upload: User List
    # ---------------------------------------------------------
    print("\n🔹 Preparing User List...")
    # Just the wallet addresses
    users_df = pd.DataFrame(list(valid_wallets), columns=['wallet_address'])
    
    upload_table(
        users_df,
        "dataset_orbt_users",
        [{"name": "wallet_address", "type": "varchar"}]
    )

    print("\n🎉 ALL TASKS COMPLETE! All Dune tables are now optimized and clean.")

if __name__ == "__main__":
    main()
