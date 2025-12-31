import requests
import pandas as pd
import time
import concurrent.futures
from threading import Lock
import os

# CONFIG
SIM_API_KEY = "sim_EO9GHAnKw4OOQz1GGR6JbIIPlqS3nX1a"
DUNE_API_KEY = "iZZZp3h425CcCf6XYmkaur8B5zrfQt5g"
DUNE_NAMESPACE = "surgence_lab"
DUNE_TABLE_NAME = "dataset_wallet_portfolio_ath"

INPUT_FILE = "all_wallets.csv"
OLD_BACKUP = "wallet_portfolio_backup.csv"
NEW_BACKUP = "wallet_portfolio_ath_backup.csv"

SIM_API_URL = "https://api.sim.dune.com/v1/evm/balances"
CHAIN_IDS = "1"

MAX_WORKERS = 10
UPLOAD_BATCH_SIZE = 5000

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
            time.sleep(2)
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
    global uploaded_count
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
    with results_lock:
        results.append(result)
        batch_ready = len(results) >= UPLOAD_BATCH_SIZE
        if batch_ready:
            batch = results.copy()
            results.clear()
    with count_lock:
        processed_count += 1
        if processed_count % 500 == 0:
            print(f"⏳ Processed {processed_count}...")
    if batch_ready:
        upload_to_dune(batch)
        pd.DataFrame(batch).to_csv(NEW_BACKUP, mode='a', header=not os.path.exists(NEW_BACKUP), index=False)
    time.sleep(0.02)
    return result

print("🚀 Starting ATH Portfolio Fetcher...")

df_all = pd.read_csv(INPUT_FILE, header=None)
all_wallets = [str(w).strip() for w in df_all.iloc[:, 0].tolist()]
print(f"📊 Total wallets: {len(all_wallets)}")

# Load already processed wallets from current run/backup
processed_wallets = set()
if os.path.exists(NEW_BACKUP):
    try:
        df_new = pd.read_csv(NEW_BACKUP)
        processed_wallets = set(str(w).strip() for w in df_new['wallet'].tolist())
        print(f"⏩ Found {len(processed_wallets)} already processed in {NEW_BACKUP}. Skipping them.")
    except Exception as e:
        print(f"⚠️ Could not read existing backup: {e}")

try:
    df_old = pd.read_csv(OLD_BACKUP)
    old_wallets = [str(w).strip() for w in df_old['wallet'].tolist()]
    # Filter out already processed
    old_wallets = [w for w in old_wallets if w not in processed_wallets]
    
    print(f"📁 Found {len(old_wallets)} wallets in old backup (after skipping processed)")
    
    if old_wallets:
        print(f"\n🔄 Phase 1: Re-fetching {len(old_wallets)} wallets with ATH...")
        # Process old wallets
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            list(ex.map(process_wallet, old_wallets))
            
        # Flush remaining results from Phase 1
        with results_lock:
            if results:
                upload_to_dune(results)
                pd.DataFrame(results).to_csv(NEW_BACKUP, mode='a', header=not os.path.exists(NEW_BACKUP), index=False)
                results.clear()
        print(f"✅ Phase 1 complete!")
    else:
        print("✅ Phase 1 already complete (all wallets processed).")
    
    # Identify remaining wallets
    # Exclude wallets in OLD_BACKUP (original list) AND processed_wallets
    # Note: old_wallets variable is now filtered, so we need to reference df_old or just use processed_wallets check
    # But strictly speaking, we want to process everything in all_wallets that hasn't been processed.
    
    # Simpler logic for Phase 2:
    remaining = [w for w in all_wallets if w not in processed_wallets and w not in old_wallets]
    # Wait, if w was in old_wallets but NOT processed, it would have been handled in Phase 1.
    # If w was in old_wallets AND processed, it's done.
    # So we just need to ensure we don't double process.
    # Actually, let's just say remaining is everything in all_wallets that is NOT in processed_wallets AND NOT in the original old_wallets list (since we just handled those).
    
    original_old_wallets = set(str(w).strip() for w in df_old['wallet'].tolist())
    remaining = [w for w in all_wallets if w not in processed_wallets and w not in original_old_wallets]

    print(f"\n🚀 Phase 2: Processing {len(remaining)} remaining wallets...")
except FileNotFoundError:
    print("📁 No old backup found. Starting fresh...")
    remaining = [w for w in all_wallets if w not in processed_wallets]

processed_count = 0
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    list(ex.map(process_wallet, remaining))

# Flush final results
with results_lock:
    if results:
        upload_to_dune(results)
        pd.DataFrame(results).to_csv(NEW_BACKUP, mode='a', header=not os.path.exists(NEW_BACKUP), index=False)

print(f"\n✅ DONE! Total uploaded: {uploaded_count}")
