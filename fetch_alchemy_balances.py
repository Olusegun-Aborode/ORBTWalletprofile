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

INPUT_FILE = "wallets_active.csv"  # The 62k subset
OUTPUT_FILE = "alchemy_eth_balances.csv"
MAX_WORKERS = 2
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
        # Assuming headerless single column
        df = pd.read_csv(INPUT_FILE, header=None, names=['wallet'])
        wallets = df['wallet'].tolist()
    except:
        # Try reading the full backup if active csv doesn't exist
        print(f"⚠️  {INPUT_FILE} not found, reading full backup...")
        df = pd.read_csv("wallet_portfolio_ath_backup.csv")
        wallets = df[df['present_value_usd'] > 0]['wallet'].tolist()

    print(f"📊 Processing {len(wallets)} wallets...")
    
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

    print("💾 Saving results...")
    pd.DataFrame(all_results).to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Done! Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
