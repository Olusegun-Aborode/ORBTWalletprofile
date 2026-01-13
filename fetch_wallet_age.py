import requests
import pandas as pd
import time
import concurrent.futures
import os
from datetime import datetime, timezone
from threading import Lock

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
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
if not ALCHEMY_API_KEY:
    raise ValueError("Please set ALCHEMY_API_KEY in .env file")

ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
INPUT_FILE = "final_active_wallets.csv"
OUTPUT_FILE = "wallet_ages.csv"
MAX_WORKERS = 10

results = []
results_lock = Lock()
processed_count = 0

def get_wallet_age(wallet):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "fromAddress": wallet,
                "category": ["external", "erc20", "erc721", "erc1155"],
                "maxCount": "0x1",
                "order": "asc",
                "excludeZeroValue": False
            }
        ]
    }
    
    for attempt in range(3):
        try:
            response = requests.post(ALCHEMY_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if response.status_code == 200:
                data = response.json()
                transfers = data.get("result", {}).get("transfers", [])
                
                if not transfers:
                    # No transfers found (shouldn't happen for active wallets, but safe fallback)
                    return None
                
                # Get timestamp from the first transfer's block
                # Note: alchemy_getAssetTransfers returns 'metadata' with timestamp only if requested? 
                # Actually, standard response usually includes blockNum. 
                # To get timestamp, we might need 'withMetadata': True or fetch block.
                # Let's try to get it from metadata if available, or fetch block.
                # UPDATE: alchemy_getAssetTransfers supports 'withMetadata': True to get timestamps directly!
                
                # Let's retry with metadata if we missed it
                return transfers[0]
                
            elif response.status_code == 429:
                time.sleep(2 * (attempt + 1))
            else:
                print(f"❌ Error {response.status_code} for {wallet}: {response.text}")
                return None
        except Exception as e:
            print(f"❌ Exception for {wallet}: {e}")
            time.sleep(1)
            
    return None

def get_wallet_age_with_metadata(wallet):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "fromAddress": wallet,
                "category": ["external", "erc20", "erc721", "erc1155"],
                "maxCount": "0x1",
                "order": "asc",
                "withMetadata": True,
                "excludeZeroValue": False
            }
        ]
    }
    
    for attempt in range(3):
        try:
            response = requests.post(ALCHEMY_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if response.status_code == 200:
                data = response.json()
                transfers = data.get("result", {}).get("transfers", [])
                
                if transfers:
                    meta = transfers[0].get("metadata", {})
                    timestamp_str = meta.get("blockTimestamp") # Format: "2022-01-15T10:30:45.000Z"
                    return timestamp_str
                return "NA" # No transfers
            elif response.status_code == 429:
                time.sleep(2 * (attempt + 1))
        except Exception:
            time.sleep(1)
    return None

def process_wallet(wallet):
    ts_str = get_wallet_age_with_metadata(wallet)
    
    if not ts_str or ts_str == "NA":
        return None
        
    try:
        # Parse timestamp
        # Alchemy returns ISO format: 2021-06-23T10:23:45.000Z
        dt = datetime.strptime(ts_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        
        now = datetime.now(timezone.utc)
        age_days = (now - dt).days
        
        years = age_days // 365
        months = (age_days % 365) // 30
        
        fmt_age = f"{years} years {months} months"
        
        return {
            "wallet": wallet,
            "first_tx_timestamp": ts_str,
            "wallet_age_days": age_days,
            "wallet_age_formatted": fmt_age
        }
    except Exception as e:
        print(f"Error parsing date {ts_str} for {wallet}: {e}")
        return None

def main():
    print("🚀 Starting Wallet Age Fetcher...")
    
    # 0. Load Existing Results (Resume capability)
    processed_wallets = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            df_existing = pd.read_csv(OUTPUT_FILE)
            if 'wallet' in df_existing.columns:
                processed_wallets = set(df_existing['wallet'].astype(str).str.lower().str.strip())
                results.extend(df_existing.to_dict('records'))
                print(f"🔄 Resuming: Found {len(processed_wallets)} existing records in {OUTPUT_FILE}")
        except Exception as e:
            print(f"⚠️ Error reading existing output: {e}")

    # 1. Load Filtered List
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file {INPUT_FILE} not found.")
        return
        
    df = pd.read_csv(INPUT_FILE)
    df['wallet'] = df['wallet'].astype(str).str.lower().str.strip()
    
    # Filter for active retail (Removed per user request)
    print(f"Total wallets in file: {len(df)}")
    # if 'tx_count' in df.columns:
    #     df = df[(df['tx_count'] > 0) & (df['tx_count'] <= 20000)]
    
    # Exclude already processed
    df = df[~df['wallet'].isin(processed_wallets)]
    print(f"✅ Active Retail Wallets to process: {len(df)}")
    
    wallets = df['wallet'].tolist()
    
    # 2. Process
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_wallet, w): w for w in wallets}
        
        total = len(wallets)
        completed = 0
        
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            
            completed += 1
            if completed % 100 == 0:
                print(f"Progress: {completed}/{total} ({completed/total:.1%})")
                
            # Save intermediate
            if completed % 1000 == 0:
                pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
                print(f"💾 Saved {len(results)} rows to {OUTPUT_FILE}")

    # Final Save
    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"🎉 Done! Saved {len(results)} wallet ages to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
