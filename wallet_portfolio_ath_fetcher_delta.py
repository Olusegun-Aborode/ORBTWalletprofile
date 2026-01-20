import requests
import pandas as pd
import time
import concurrent.futures
import random
from threading import Lock
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
SIM_API_KEY = os.getenv("SIM_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")

if not SIM_API_KEY or not DUNE_API_KEY:
    raise ValueError("Please set SIM_API_KEY and DUNE_API_KEY in .env file")
DUNE_NAMESPACE = "orbt_official"
DUNE_TABLE_NAME = "dataset_wallet_portfolio_ath"

INPUT_FILE = "delta_wallets.csv"
OLD_BACKUP = "wallet_portfolio_backup_delta.csv"
NEW_BACKUP = "wallet_portfolio_ath_delta.csv"

SIM_API_URL = "https://api.sim.dune.com/v1/evm/balances"
CHAIN_IDS = "1"

MAX_WORKERS = 20
UPLOAD_BATCH_SIZE = 25000

headers_sim = {"X-Sim-Api-Key": SIM_API_KEY}
headers_dune = {"X-Dune-Api-Key": DUNE_API_KEY}

results = []
results_lock = Lock()
processed_count = 0
count_lock = Lock()
uploaded_count = 0

def get_wallet_portfolio(wallet_address):
    try:
        wallet_str = str(wallet_address).strip()
        if not wallet_str.startswith("0x"):
            wallet_str = "0x" + wallet_str
        response = requests.get(
            f"{SIM_API_URL}/{wallet_str}",
            headers=headers_sim,
            params={"chain_ids": CHAIN_IDS, "exclude_spam_tokens": "true", "historical_prices": "2160"},
            timeout=30
        )
        if response.status_code == 429:
            # print(f"⚠️ 429 Rate Limit for {wallet_address}. Retrying...")
            time.sleep(random.uniform(2, 5))
            return get_wallet_portfolio(wallet_address)
        if response.status_code != 200:
            return {"wallet": wallet_str, "present_value_usd": 0.0, "ath_value_usd": 0.0, "token_count": 0, "top_tokens": ""}
        data = response.json()
        if "balances" not in data:
            return {"wallet": wallet_str, "present_value_usd": 0.0, "ath_value_usd": 0.0, "token_count": 0, "top_tokens": ""}
        
        total_usd = 0.0
        total_90d = 0.0
        token_count = 0
        top_tokens = []
        
        for b in data["balances"]:
            if b.get("low_liquidity", False):
                continue
            
            # Strict Filtering Logic
            is_native = (b.get("address") == "native")
            pool_size = b.get("pool_size", 0) or 0
            val = b.get("value_usd", 0) or 0
            
            # Filter 1: Must be native OR have significant liquidity (> $50k)
            if not (is_native or pool_size > 50000):
                continue
                
            # Filter 2: Even if liquid, value shouldn't exceed pool size (anti-whale/scam check)
            # We skip this for native tokens as they don't always have a pool_size field or it represents a DEX pair
            if not is_native and val > pool_size:
                continue

            current_price = b.get("price_usd", 0) or 0
            total_usd += val
            if val > 0:
                token_count += 1
                top_tokens.append({"s": b.get("symbol", "?"), "v": val})
                # Calculate 90d value using ratio
                if current_price > 0:
                    best_90d = val # Default to current value
                    for h in b.get("historical_prices", []):
                        hp = h.get("price_usd", 0) or 0
                        if h.get("offset_hours") == 2160:
                            ratio = hp / current_price
                            # Sanity check: If ratio > 100 (99% drop), ignore it (likely scam wick)
                            # Also check if historical price is insanely high
                            if ratio > 100:
                                ratio = 1.0
                            best_90d = val * ratio
                    total_90d += best_90d
        
        top_tokens.sort(key=lambda x: x["v"], reverse=True)
        top_3 = ", ".join([t["s"] for t in top_tokens[:3]])
        ath = max(total_usd, total_90d)
        
        return {"wallet": wallet_str, "present_value_usd": round(total_usd, 2), "ath_value_usd": round(ath, 2), "token_count": token_count, "top_tokens": top_3}
    except:
        return {"wallet": str(wallet_address), "present_value_usd": 0.0, "ath_value_usd": 0.0, "token_count": 0, "top_tokens": ""}

def upload_to_dune(data):
    # Skipped for delta pipeline
    pass

def process_wallet(wallet):
    global processed_count, results
    result = get_wallet_portfolio(wallet)
    with results_lock:
        results.append(result)
        batch_ready = len(results) >= UPLOAD_BATCH_SIZE
        if batch_ready:
            batch = results.copy()
            results.clear()
    with count_lock:
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"⏳ Processed {processed_count}...")
    if batch_ready:
        # upload_to_dune(batch)
        pd.DataFrame(batch).to_csv(NEW_BACKUP, mode='a', header=not os.path.exists(NEW_BACKUP), index=False)
    time.sleep(0.02)
    return result

print("🚀 Starting ATH Portfolio Fetcher...")

df_all = pd.read_csv(INPUT_FILE)
# Ensure we get the 'wallet' column if it exists, otherwise assume first column if no header
if 'wallet' in df_all.columns:
    all_wallets = [str(w).strip().lower() for w in df_all['wallet'].tolist()]
else:
    all_wallets = [str(w).strip().lower() for w in df_all.iloc[:, 0].tolist()]

# Filter out header 'wallet' if present in data (just in case)
all_wallets = [w for w in all_wallets if w != 'wallet']
print(f"📊 Total unique input wallets: {len(set(all_wallets))}")

# Filter (Removed per user request)
# if 'tx_count' in df.columns:
#     df = df[(df['tx_count'] > 0) & (df['tx_count'] <= 20000)]

# Load already processed wallets from current run/backup
processed_wallets = set()
if os.path.exists(NEW_BACKUP):
    try:
        df_new = pd.read_csv(NEW_BACKUP)
        processed_wallets = set(str(w).strip().lower() for w in df_new['wallet'].tolist())
        print(f"⏩ Found {len(processed_wallets)} already processed in {NEW_BACKUP}. Skipping them.")
    except Exception as e:
        print(f"⚠️ Could not read existing backup: {e}")

original_old_wallets = set()

if os.path.exists(OLD_BACKUP):
    try:
        df_old = pd.read_csv(OLD_BACKUP)
        original_old_wallets = set(str(w).strip().lower() for w in df_old['wallet'].tolist())
        
        old_wallets = [w for w in original_old_wallets if w not in processed_wallets]
        
        print(f"📁 Found {len(old_wallets)} wallets in old backup (after skipping processed)")
        
        if old_wallets:
            print(f"\n🔄 Phase 1: Re-fetching {len(old_wallets)} wallets with ATH...")
            # Process old wallets
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                list(ex.map(process_wallet, old_wallets))
                
            # Flush remaining results from Phase 1
            with results_lock:
                if results:
                    # upload_to_dune(results)
                    pd.DataFrame(results).to_csv(NEW_BACKUP, mode='a', header=not os.path.exists(NEW_BACKUP), index=False)
                    results.clear()
            print(f"✅ Phase 1 complete!")
        else:
            print("✅ Phase 1 already complete (all wallets processed).")
            
    except Exception as e:
        print(f"⚠️ Error reading/processing old backup: {e}")
else:
    print("📁 No old backup found. Starting fresh...")

# Phase 2: Remaining
# Identify remaining wallets from ALL inputs that are not processed and not in old backup (already handled)
remaining = [w for w in all_wallets if w not in processed_wallets and w not in original_old_wallets]

print(f"\n🚀 Phase 2: Processing {len(remaining)} remaining wallets...")

processed_count = 0
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    list(ex.map(process_wallet, remaining))

# Flush final results
with results_lock:
    if results:
        # upload_to_dune(results)
        pd.DataFrame(results).to_csv(NEW_BACKUP, mode='a', header=not os.path.exists(NEW_BACKUP), index=False)

print(f"\n✅ DONE! Total uploaded: {uploaded_count}")
