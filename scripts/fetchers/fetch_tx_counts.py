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

# ---------------------------------------------------------
# 🔑 CONFIGURATION
# ---------------------------------------------------------
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
if not ALCHEMY_API_KEY:
    raise ValueError("Please set ALCHEMY_API_KEY in .env file")

ALCHEMY_RPC_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

INPUT_FILE = "data/intermediate/wallet_portfolio_ath_backup.csv" # Using the clean list of 268k wallets
OUTPUT_FILE = "data/intermediate/wallet_tx_counts.csv"
MAX_WORKERS = 10
BATCH_SIZE = 50

results = []
results_lock = Lock()
processed_count = 0
count_lock = Lock()

def get_tx_counts_batch(wallets):
    payload = []
    for i, wallet in enumerate(wallets):
        payload.append({
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getTransactionCount",
            "params": [wallet, "latest"]
        })
    
    # Retry logic
    for attempt in range(3):
        try:
            response = requests.post(ALCHEMY_RPC_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                time.sleep(2 * (attempt + 1))
            else:
                print(f"❌ Error: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            print(f"❌ Exception: {e}")
            time.sleep(1)
    else:
        print("❌ Max retries exceeded for batch")
        return []

    try:
        data_json = response.json()
        
        # If single response (error or non-batch), wrap it
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
                # If error in individual result, assume 0 or handle
                # print(f"⚠️ No result for {wallet}")
                parsed.append({"wallet": wallet, "tx_count": 0})
        return parsed
    except Exception as e:
        print(f"❌ Parse Exception: {e}")
        return []

def process_batch(batch_wallets):
    global processed_count
    
    data = get_tx_counts_batch(batch_wallets)
    
    with results_lock:
        results.extend(data)
        
        # Save incrementally
        if len(results) >= 5000:
            df = pd.DataFrame(results)
            header = not os.path.exists(OUTPUT_FILE)
            df.to_csv(OUTPUT_FILE, mode='a', header=header, index=False)
            results.clear()
            
    with count_lock:
        processed_count += len(batch_wallets)
        if processed_count % 1000 == 0:
            print(f"⏳ Processed {processed_count} wallets...")

def main():
    print("🚀 Starting Transaction Count Fetcher...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file {INPUT_FILE} not found")
        return

    # Load wallets
    df = pd.read_csv(INPUT_FILE)
    # Ensure we have the wallet column
    if 'wallet' not in df.columns:
        # Fallback for old CSVs
        if 'wallet_address' in df.columns:
            wallets = df['wallet_address'].astype(str).tolist()
        else:
            wallets = df.iloc[:, 0].astype(str).tolist()
    else:
        wallets = df['wallet'].astype(str).tolist()

    wallets = [w.strip().lower() for w in wallets]
    
    # Filter processed
    processed_wallets = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            df_done = pd.read_csv(OUTPUT_FILE)
            processed_wallets = set(df_done['wallet'].astype(str).str.lower().str.strip())
            print(f"⏩ Found {len(processed_wallets)} already processed. Skipping...")
        except:
            pass
            
    remaining = [w for w in wallets if w not in processed_wallets]
    print(f"📊 Total to process: {len(remaining)}")
    
    if not remaining:
        print("✅ All done!")
        return

    # Create batches
    batches = [remaining[i:i + BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_batch, batches)

    # Flush final
    with results_lock:
        if results:
            df = pd.DataFrame(results)
            header = not os.path.exists(OUTPUT_FILE)
            df.to_csv(OUTPUT_FILE, mode='a', header=header, index=False)
            results.clear()

    print("🎉 Done fetching transaction counts!")

if __name__ == "__main__":
    main()
