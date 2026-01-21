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
INPUT_FILE = "data/input/delta_wallets.csv"
OUTPUT_FILE = "data/intermediate/wallet_volumes_delta.csv"
MAX_WORKERS = 5

# ADDRESS DICTIONARIES (Lowercased)
DEX_ADDRESSES = {
    "0x68b3465833fb72b5a828cceda1ed448deca0d657": "Uniswap V3",
    "0x1111111254fb6c44bac0bed2854e76f90643097d": "1inch",
    "0xf0d4c12a5768d806021f80a262b4d39d26c58b8d": "Curve",
    "0xba12222222228d8ba445958a75a0704d566bf2c8": "Balancer",
    # Add more common routers if needed
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2", 
    "0xe592427a0aece92de3edee1f18e0157c05861564": "Uniswap V3 Router"
}

CEX_ADDRESSES = {
    "0x0548f59fee33adec2a8a7d361ba6c5476bb4ea3": "Binance",
    "0x742d35cc6634c0532925a3b844bc9e7595f42be": "Coinbase",
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": "Kraken",
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX"
}

LENDING_ADDRESSES = {
    "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9": "Aave V2",
    "0x87870bca3f3fd6335c3ef8743064d19e0420ed76": "Aave V3",
    "0xc00e94cb662c3520282e6f5717214febb0b260f3": "Compound",
    "0x1e0447b19bb6ecfdae1ab6cde1d2fbca2b268e59": "Yearn",
    "0xc1e6fc6c655703d3dd5140b48e6e4c4f453d1c56": "Moonwell"
}

# Pricing Constants (Approximation)
ETH_PRICE = 3300.0
STABLECOINS = ["USDC", "USDT", "DAI", "USDE", "PYUSD", "GUSD"]

# Token Prices for CIS Calculation (Snapshot)
TOKEN_PRICES = {
    "DOG": 0.000908,
    "VERSE": 0.000005,
    "PYME": 0.000001,
    "L3": 0.012888,
    "USDC": 1.0, "USDT": 1.0, "DAI": 1.0, "USDE": 1.0, "PYUSD": 1.0, "GUSD": 1.0,
    "WETH": ETH_PRICE, "ETH": ETH_PRICE
}

results = []
results_lock = Lock()

def get_transfers(wallet, direction="from"):
    """
    direction: 'from' (OUT) or 'to' (IN)
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "category": ["external", "erc20"],
                "withMetadata": False,
                "excludeZeroValue": True
            }
        ]
    }
    
    if direction == "from":
        payload["params"][0]["fromAddress"] = wallet
    else:
        payload["params"][0]["toAddress"] = wallet
        
    # Retry loop
    for attempt in range(3):
        try:
            response = requests.post(ALCHEMY_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if response.status_code == 200:
                data = response.json()
                return data.get("result", {}).get("transfers", [])
            elif response.status_code == 429:
                time.sleep(2 * (attempt + 1))
            else:
                return []
        except:
            time.sleep(1)
    return []

def calculate_volumes(wallet):
    # 1. Fetch OUTGOING (Spending/Trading)
    transfers_out = get_transfers(wallet, "from")
    
    # 2. Fetch INCOMING (For CIS Total Flow)
    # Optimization: Only fetch incoming if we really need it? Yes, for CIS.
    transfers_in = get_transfers(wallet, "to")
    
    dex_vol = 0.0
    cex_vol = 0.0
    lending_vol = 0.0
    
    interacted_dexs = set()
    interacted_cexs = set()
    interacted_lending = set()
    
    dex_counts = {}
    cex_counts = {}
    lending_counts = {}
    
    trade_count = 0
    
    # --- Process Outgoing for Category Metrics ---
    for tx in transfers_out:
        to_addr = str(tx.get("to")).lower()
        asset = tx.get("asset", "")
        raw_val = tx.get("value", 0)
        
        if raw_val is None: continue
            
        # Estimate USD Value (Standard Bluechip)
        val_usd = 0.0
        if asset == "ETH" or asset == "WETH":
            val_usd = raw_val * ETH_PRICE
        elif asset in STABLECOINS:
            val_usd = raw_val
        else:
            continue # Skip unknown for DEX metric
            
        # Check DEX
        if to_addr in DEX_ADDRESSES:
            dex_vol += val_usd
            name = DEX_ADDRESSES[to_addr]
            interacted_dexs.add(name)
            dex_counts[name] = dex_counts.get(name, 0) + 1
            trade_count += 1
        # Check CEX
        elif to_addr in CEX_ADDRESSES:
            cex_vol += val_usd
            name = CEX_ADDRESSES[to_addr]
            interacted_cexs.add(name)
            cex_counts[name] = cex_counts.get(name, 0) + 1
        # Check Lending
        elif to_addr in LENDING_ADDRESSES:
            lending_vol += val_usd
            name = LENDING_ADDRESSES[to_addr]
            interacted_lending.add(name)
            lending_counts[name] = lending_counts.get(name, 0) + 1
            
    # Find Most Used
    most_used_dex = max(dex_counts, key=dex_counts.get) if dex_counts else None
    most_used_cex = max(cex_counts, key=cex_counts.get) if cex_counts else None
    most_used_protocol = max(lending_counts, key=lending_counts.get) if lending_counts else None
    
    # --- Process ALL Transfers for CIS Metric (In + Out) ---
    total_cis_volume = 0.0
    
    # Combine lists
    all_transfers = transfers_out + transfers_in
    
    for tx in all_transfers:
        asset = tx.get("asset", "")
        raw_val = tx.get("value", 0)
        if raw_val is None: continue
        
        # Use extended price list
        if asset in TOKEN_PRICES:
            price = TOKEN_PRICES[asset]
            total_cis_volume += raw_val * price
        elif asset in STABLECOINS: # Fallback if not in dict but in list
             total_cis_volume += raw_val

    return {
        "wallet": wallet,
        "total_volume_usd_cis": round(total_cis_volume, 2), # NEW METRIC (In + Out, Extended Tokens)
        "total_dex_volume_usd": round(dex_vol, 2),
        "interacted_dexs": list(interacted_dexs),
        "most_used_dex": most_used_dex,
        "trade_count": trade_count,
        "total_cex_volume_usd": round(cex_vol, 2),
        "interacted_cexs": list(interacted_cexs),
        "most_used_cex": most_used_cex,
        "total_lending_volume_usd": round(lending_vol, 2),
        "most_used_protocol": most_used_protocol
    }

def main():
    print("🚀 Starting Combined Volume Fetcher...")
    
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
    print(f"Total wallets in file: {len(df)}")
    # if 'tx_count' in df.columns:
    #     df = df[(df['tx_count'] > 0) & (df['tx_count'] <= 20000)]
        
    # Exclude already processed
    df = df[~df['wallet'].isin(processed_wallets)]
    print(f"✅ Active Retail Wallets remaining: {len(df)}")
    
    wallets = df['wallet'].tolist()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(calculate_volumes, w): w for w in wallets}
        
        total = len(wallets)
        completed = 0
        
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            results.append(res)
            
            completed += 1
            if completed % 100 == 0:
                print(f"Progress: {completed}/{total} ({completed/total:.1%})")
                
            if completed % 1000 == 0:
                pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
                print(f"💾 Saved {len(results)} rows")

    # Final Save
    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"🎉 Done! Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
