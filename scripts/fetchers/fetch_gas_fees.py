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

ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
INPUT_FILE = "data/input/final_active_wallets.csv"
OUTPUT_FILE = "data/intermediate/wallet_gas_fees.csv"
MAX_WORKERS = 5 # Lower workers to avoid rate limits with heavy batching

results = []
results_lock = Lock()

# Cache for ETH Price (simple static for now, or fetch once)
ETH_PRICE = 3300.0 

def get_recent_txs(wallet):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "fromAddress": wallet,
                "category": ["external", "erc20"],
                "maxCount": "0x64", # Hex for 100
                "order": "desc", # Recent first
                "excludeZeroValue": False
            }
        ]
    }
    
    try:
        response = requests.post(ALCHEMY_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
        if response.status_code == 200:
            data = response.json()
            return data.get("result", {}).get("transfers", [])
    except:
        pass
    return []

def get_gas_fees_batch(tx_hashes):
    if not tx_hashes:
        return 0.0
        
    payload = []
    for i, tx_hash in enumerate(tx_hashes):
        payload.append({
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash]
        })
        
    total_gas_eth = 0.0
    
    # Send batch
    try:
        response = requests.post(ALCHEMY_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
        if response.status_code == 200:
            results_batch = response.json()
            if not isinstance(results_batch, list):
                results_batch = [results_batch]
                
            for res in results_batch:
                if 'result' in res and res['result']:
                    receipt = res['result']
                    gas_used = int(receipt.get('gasUsed', '0x0'), 16)
                    effective_gas_price = int(receipt.get('effectiveGasPrice', '0x0'), 16)
                    
                    fee_wei = gas_used * effective_gas_price
                    fee_eth = fee_wei / 1e18
                    total_gas_eth += fee_eth
    except Exception as e:
        print(f"Batch Error: {e}")
        
    return total_gas_eth

def process_wallet(wallet):
    # 1. Get recent txs
    transfers = get_recent_txs(wallet)
    if not transfers:
        return None
        
    # 2. Extract hashes
    hashes = [t['hash'] for t in transfers if 'hash' in t]
    
    # 3. Get Gas Fees
    total_eth = get_gas_fees_batch(hashes)
    total_usd = total_eth * ETH_PRICE
    
    return {
        "wallet": wallet,
        "gas_fees_usd": round(total_usd, 2),
        "total_transactions_analyzed": len(hashes)
        # Note: 'total_transactions_3y' is just tx_count from our other file
    }

def main():
    print("🚀 Starting Gas Fee Calculator (Last 100 Txs)...")
    
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

    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file {INPUT_FILE} not found.")
        return
        
    df = pd.read_csv(INPUT_FILE)
    df['wallet'] = df['wallet'].astype(str).str.lower().str.strip()
    
    # Filter (Removed per user request)
    # if 'tx_count' in df.columns:
    #     df = df[(df['tx_count'] > 0) & (df['tx_count'] <= 20000)]
    
    # Exclude already processed
    df = df[~df['wallet'].isin(processed_wallets)]
    print(f"✅ Active Retail Wallets remaining: {len(df)}")
    
    wallets = df['wallet'].tolist()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_wallet, w): w for w in wallets}
        
        total = len(wallets)
        completed = 0
        
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            
            completed += 1
            if completed % 50 == 0: # Slower progress updates
                print(f"Progress: {completed}/{total} ({completed/total:.1%})")
                
            if completed % 200 == 0:
                pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
                print(f"💾 Saved {len(results)} rows")

    # Final Save
    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"🎉 Done! Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
