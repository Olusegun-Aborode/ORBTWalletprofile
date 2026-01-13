import requests
import pandas as pd
import time
import concurrent.futures
import os
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

ALCHEMY_RPC_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

ORBT_FILE = "orbt_base_minters.csv"
TX_FILE = "wallet_tx_counts.csv"
FINAL_FILE = "final_active_wallets.csv"

MAX_WORKERS = 10
BATCH_SIZE = 50

results = []
results_lock = Lock()
processed_count = 0

def get_tx_counts_batch(wallets):
    payload = []
    for i, wallet in enumerate(wallets):
        payload.append({
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getTransactionCount",
            "params": [wallet, "latest"]
        })
    
    for attempt in range(3):
        try:
            response = requests.post(ALCHEMY_RPC_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                time.sleep(2 * (attempt + 1))
            else:
                return []
        except:
            time.sleep(1)
    else:
        return []

    try:
        data_json = response.json()
        if not isinstance(data_json, list):
            data_json = [data_json]
            
        parsed = []
        results_map = {r['id']: r for r in data_json if 'result' in r and 'id' in r}
        
        for i, wallet in enumerate(wallets):
            res = results_map.get(i)
            if res:
                val = int(res['result'], 16)
                parsed.append({"wallet": wallet, "tx_count": val})
            else:
                parsed.append({"wallet": wallet, "tx_count": 0})
        return parsed
    except:
        return []

def process_batch(batch_wallets):
    global processed_count
    data = get_tx_counts_batch(batch_wallets)
    with results_lock:
        results.extend(data)
    
    processed_count += len(batch_wallets)
    if processed_count % 1000 == 0:
        print(f"   Processed {processed_count} new wallets...")

def main():
    print("🚀 Starting Wallet Merge & Filter Process...")
    
    # 1. Load ORBT Minters
    if not os.path.exists(ORBT_FILE):
        print(f"❌ {ORBT_FILE} not found. Run fetch_orbt_holders_rpc.py first.")
        return
    
    df_orbt = pd.read_csv(ORBT_FILE)
    orbt_wallets = set(df_orbt['wallet'].astype(str).str.lower().str.strip())
    print(f"✅ Loaded {len(orbt_wallets)} ORBT Minters.")
    
    # 2. Load Existing TX Counts
    existing_wallets = set()
    if os.path.exists(TX_FILE):
        df_tx = pd.read_csv(TX_FILE)
        existing_wallets = set(df_tx['wallet'].astype(str).str.lower().str.strip())
        print(f"✅ Loaded {len(existing_wallets)} existing wallets with tx counts.")
    
    # 3. Identify New Wallets
    new_wallets = list(orbt_wallets - existing_wallets)
    print(f"📊 New Wallets to Process: {len(new_wallets)}")
    
    # 4. Fetch TX Counts for New Wallets
    if new_wallets:
        print("🔄 Fetching activity for new wallets...")
        batches = [new_wallets[i:i + BATCH_SIZE] for i in range(0, len(new_wallets), BATCH_SIZE)]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(process_batch, batches)
            
        # Append to TX File
        if results:
            df_new = pd.DataFrame(results)
            header = not os.path.exists(TX_FILE)
            df_new.to_csv(TX_FILE, mode='a', header=header, index=False)
            print(f"💾 Added {len(df_new)} new wallets to {TX_FILE}")
            
    # 5. Create Final Active List
    print("🧹 Creating Final Active List...")
    df_final = pd.read_csv(TX_FILE)
    df_final['wallet'] = df_final['wallet'].astype(str).str.lower().str.strip()
    
    # Filter: 0 < tx <= 20000
    df_active = df_final[(df_final['tx_count'] > 0) & (df_final['tx_count'] <= 20000)]
    
    print(f"   - Total Unique Wallets: {len(df_final)}")
    print(f"   - Active Retail Wallets: {len(df_active)}")
    
    df_active.to_csv(FINAL_FILE, index=False)
    print(f"🎉 Final List Saved: {FINAL_FILE}")
    
    # Update inputs for other scripts?
    # I should rename FINAL_FILE to 'wallet_tx_counts.csv' if I want to overwrite, 
    # but the scripts use 'wallet_tx_counts.csv' as input.
    # Actually, the scripts filter 'wallet_tx_counts.csv' themselves.
    # So updating 'wallet_tx_counts.csv' (which we did in step 4) is enough!
    # But saving a specific 'final_active_wallets.csv' is good for clarity.

if __name__ == "__main__":
    main()
