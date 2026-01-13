import requests
import pandas as pd
import time
import concurrent.futures
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

# ---------------------------------------------------------
# 🔑 CONFIGURATION
# ---------------------------------------------------------
# Load from environment
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
if not ALCHEMY_API_KEY:
    raise ValueError("Please set ALCHEMY_API_KEY in .env file")

ALCHEMY_RPC_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

INPUT_FILE = "final_active_wallets.csv"
OUTPUT_FILE = "alchemy_eth_balances.csv"
MAX_WORKERS = 5
BATCH_SIZE = 50  # Alchemy supports batch requests

def get_eth_balances_batch(wallets):
    payload = []
    for i, wallet in enumerate(wallets):
        payload.append({
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getBalance",
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
        results = response.json()
        parsed = []
        # Sort results by ID to match input order (though JSON-RPC batch usually returns random order, we used IDs)
        # Map id to wallet
        results_map = {r['id']: r for r in results if 'result' in r}
        
        for i, wallet in enumerate(wallets):
            res = results_map.get(i)
            if res:
                # Convert hex to ETH
                val_wei = int(res['result'], 16)
                val_eth = val_wei / 1e18
                parsed.append({"wallet": wallet, "alchemy_eth_balance": val_eth})
            else:
                parsed.append({"wallet": wallet, "alchemy_eth_balance": 0.0})
        return parsed
    except Exception as e:
        print(f"❌ Exception: {e}")
        return []

def main():
    if "REPLACE" in ALCHEMY_API_KEY:
        print("⚠️  PLEASE UPDATE THE 'ALCHEMY_API_KEY' IN THE SCRIPT FIRST!")
        return

    print(f"📖 Reading {INPUT_FILE}...")
    try:
        # Read all wallets from CSV
        # We need to handle potential header 'wallet'
        df = pd.read_csv(INPUT_FILE)
        
        # Check if we have 'wallet' column
        if 'wallet' in df.columns:
             wallets = [str(x).strip() for x in df['wallet'].tolist()]
        else:
             # Fallback to first column
             wallets = [str(x).strip() for x in df.iloc[:, 0].tolist()]
             
    except Exception as e:
        print(f"❌ Error reading input file: {e}")
        return

    # Normalize wallets
    wallets = [str(w).strip().lower() for w in wallets]

    # Filter (Removed per user request)
    # if 'tx_count' in df.columns:
    #     df = df[(df['tx_count'] > 0) & (df['tx_count'] <= 20000)]

    # Load existing results to skip
    if os.path.exists(OUTPUT_FILE):
        try:
            df_existing = pd.read_csv(OUTPUT_FILE)
            # FORCE string type for all existing wallets
            existing_wallets = set(df_existing['wallet'].astype(str).str.lower().str.strip())
            
            # FORCE string type for input wallets
            wallets_set = set([str(w).lower().strip() for w in wallets])
            
            print(f"⏩ Found {len(existing_wallets)} already processed. Skipping...")
            
            # DEBUG: Print sample wallets from each to check formatting
            print(f"Sample Input: {list(wallets_set)[:3]}")
            print(f"Sample Existing: {list(existing_wallets)[:3]}")
 
            remaining_set = wallets_set - existing_wallets
            wallets = list(remaining_set)
            
            # Sanity Check
            print(f"DEBUG: Total unique input wallets: {len(wallets_set)}")
            print(f"DEBUG: Existing unique processed: {len(existing_wallets)}")
            print(f"DEBUG: Remaining to process: {len(wallets)}")
            
        except Exception as e:
            print(f"⚠️ Error reading existing output: {e}")

    if not wallets:
        print("✅ All wallets processed!")
        return

    print(f"🚀 Processing {len(wallets)} wallets in batches of {BATCH_SIZE}...")
    
    all_results = []

    # Process in chunks of BATCH_SIZE
    chunks = [wallets[i:i + BATCH_SIZE] for i in range(0, len(wallets), BATCH_SIZE)]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_eth_balances_batch, chunk): chunk for chunk in chunks}
        
        processed = 0
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            all_results.extend(res)
            processed += len(res)
            if processed % 1000 == 0:
                print(f"⏳ Processed {processed}/{len(wallets)}...")

    print(f"💾 Saving results to {OUTPUT_FILE}...")
    # Append if file exists, else write new
    mode = 'a' if os.path.exists(OUTPUT_FILE) else 'w'
    header = not os.path.exists(OUTPUT_FILE)
    pd.DataFrame(all_results).to_csv(OUTPUT_FILE, mode=mode, header=header, index=False)
    print(f"✅ Done! Added {len(all_results)} new records.")

if __name__ == "__main__":
    main()
