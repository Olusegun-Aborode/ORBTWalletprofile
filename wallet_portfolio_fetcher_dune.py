import requests
import pandas as pd
import time
import concurrent.futures
from threading import Lock
import os

# ============================================
# CONFIGURATION
# ============================================
SIM_API_KEY = "sim_EO9GHAnKw4OOQz1GGR6JbIIPlqS3nX1a"
DUNE_API_KEY = "iZZZp3h425CcCf6XYmkaur8B5zrfQt5g"
DUNE_NAMESPACE = "surgence_lab"
DUNE_TABLE_NAME = "dataset_wallet_portfolio_live"

INPUT_FILE = "all_wallets.csv"
BACKUP_FILE = "wallet_portfolio_backup.csv"

SIM_API_URL = "https://api.sim.dune.com/v1/evm/balances"
CHAIN_IDS = "1,42161,137,10,8453"

MAX_WORKERS = 10
UPLOAD_BATCH_SIZE = 1000

headers_sim = {"X-Sim-Api-Key": SIM_API_KEY}
headers_dune = {"X-Dune-Api-Key": DUNE_API_KEY}

results = []
results_lock = Lock()
processed_count = 0
count_lock = Lock()
uploaded_count = 0

def get_wallet_portfolio(wallet_address):
    """Fetch portfolio from SIM API"""
    try:
        wallet_str = str(wallet_address).strip()
        # Skip header if present
        if wallet_str.lower() == "wallet":
            return None
            
        if not wallet_str.startswith("0x"):
            wallet_str = "0x" + wallet_str
        
        response = requests.get(
            f"{SIM_API_URL}/{wallet_str}",
            headers=headers_sim,
            params={"chain_ids": CHAIN_IDS, "exclude_spam_tokens": "true"},
            timeout=30
        )
        
        if response.status_code == 429:
            time.sleep(2)
            return get_wallet_portfolio(wallet_address)
        
        if response.status_code != 200:
            return {"wallet": wallet_str, "present_value_usd": 0.0, "token_count": 0, "top_tokens": ""}
        
        data = response.json()
        if "balances" not in data:
            return {"wallet": wallet_str, "present_value_usd": 0.0, "token_count": 0, "top_tokens": ""}
        
        total_usd = 0.0
        token_count = 0
        top_tokens = []
        
        for b in data["balances"]:
            val = b.get("value_usd", 0)
            if b.get("low_liquidity", False) or not val:
                continue
            total_usd += val
            token_count += 1
            top_tokens.append({"s": b.get("symbol", "?"), "v": val})
        
        top_tokens.sort(key=lambda x: x["v"], reverse=True)
        top_3 = ", ".join([t["s"] for t in top_tokens[:3]])
        
        return {"wallet": wallet_str, "present_value_usd": round(total_usd, 2), "token_count": token_count, "top_tokens": top_3}
    except:
        return {"wallet": str(wallet_address), "present_value_usd": 0.0, "token_count": 0, "top_tokens": ""}

def upload_to_dune(data):
    global uploaded_count
    if not data:
        return True
        
    try:
        df = pd.DataFrame(data)
        csv_data = df.to_csv(index=False)
        
        url = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{DUNE_TABLE_NAME}/insert"
        response = requests.post(url, headers={**headers_dune, "Content-Type": "text/csv"}, data=csv_data)
        
        if response.status_code == 200:
            uploaded_count += len(data)
            print(f"✅ Uploaded {len(data)} to Dune (Total: {uploaded_count})")
            return True
        else:
            print(f"❌ Upload failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def process_wallet(wallet):
    global processed_count, results
    
    result = get_wallet_portfolio(wallet)
    
    if result is None:
        return None

    batch_ready = False
    batch = []

    with results_lock:
        results.append(result)
        if len(results) >= UPLOAD_BATCH_SIZE:
            batch = results.copy()
            results.clear()
            batch_ready = True
    
    with count_lock:
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"⏳ Processed {processed_count}...")
    
    if batch_ready:
        upload_to_dune(batch)
        # Append to backup file
        df_batch = pd.DataFrame(batch)
        # Write header only if file doesn't exist
        header = not os.path.exists(BACKUP_FILE)
        df_batch.to_csv(BACKUP_FILE, mode='a', header=header, index=False)
    
    time.sleep(0.02)
    return result

# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("🚀 Starting...")
    
    # Verify input file
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        exit(1)

    # Read wallets, skipping header if needed
    # Using header=0 to treat first row as header since we know it has one
    df = pd.read_csv(INPUT_FILE)
    wallets = df.iloc[:, 0].tolist()
    print(f"Found {len(wallets)} wallets")
    
    # Check for existing progress/backup to avoid reprocessing
    if os.path.exists(BACKUP_FILE):
        try:
            backup_df = pd.read_csv(BACKUP_FILE)
            processed_wallets = set(backup_df['wallet'].tolist())
            original_count = len(wallets)
            wallets = [w for w in wallets if str(w) not in processed_wallets and f"0x{w}" not in processed_wallets]
            print(f"Resuming from BACKUP... Skipping {original_count - len(wallets)} already processed wallets.")
        except Exception as e:
            print(f"Warning: Could not read backup file to resume: {e}")
    elif os.path.exists("progress.csv"):
        try:
            print("Found previous progress.csv. Migrating data...")
            progress_df = pd.read_csv("progress.csv")
            # Convert to list of dicts for upload
            records = progress_df.to_dict('records')
            
            # Upload to Dune
            print(f"Uploading {len(records)} existing records to Dune...")
            # Split into chunks of UPLOAD_BATCH_SIZE
            for i in range(0, len(records), UPLOAD_BATCH_SIZE):
                batch = records[i:i + UPLOAD_BATCH_SIZE]
                upload_to_dune(batch)
            
            # Save to new backup file
            progress_df.to_csv(BACKUP_FILE, index=False)
            
            processed_wallets = set(progress_df['wallet'].tolist())
            original_count = len(wallets)
            wallets = [w for w in wallets if str(w) not in processed_wallets and f"0x{w}" not in processed_wallets]
            print(f"Resumed from progress.csv. Skipping {original_count - len(wallets)} wallets.")
        except Exception as e:
            print(f"Warning: Could not migrate progress.csv: {e}")

    print(f"Starting with {MAX_WORKERS} parallel workers...")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            # We iterate to ensure exceptions are caught if any, though map handles it
            list(ex.map(process_wallet, wallets))
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted!")
    
    # Upload remaining results
    with results_lock:
        if results:
            print(f"Uploading remaining {len(results)} records...")
            upload_to_dune(results)
            df_batch = pd.DataFrame(results)
            header = not os.path.exists(BACKUP_FILE)
            df_batch.to_csv(BACKUP_FILE, mode='a', header=header, index=False)

    print(f"✅ Done! Processed: {processed_count}, Uploaded: {uploaded_count}")
