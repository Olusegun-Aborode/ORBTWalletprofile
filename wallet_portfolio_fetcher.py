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
INPUT_FILE = "all_wallets.csv"
OUTPUT_FILE = "wallet_portfolio_all.csv"
PROGRESS_FILE = "progress.csv"  # Save progress in case of interruption

SIM_API_URL = "https://api.sim.dune.com/v1/evm/balances"
CHAIN_IDS = "1,42161,137,10,8453"  # ETH, Arbitrum, Polygon, Optimism, Base

# Parallelization settings
MAX_WORKERS = 5  # Number of parallel requests (be careful with rate limits)
SAVE_EVERY = 500  # Save progress every N wallets

headers = {
    "X-Sim-Api-Key": SIM_API_KEY,
    "Content-Type": "application/json"
}

# Thread-safe results storage
results = []
results_lock = Lock()
processed_count = 0
count_lock = Lock()

def get_wallet_portfolio(wallet_address):
    """Fetch portfolio value from SIM API"""
    global processed_count
    
    try:
        wallet_str = str(wallet_address).strip()
        # Skip header if it was read
        if wallet_str.lower() == "wallet":
            return None

        if not wallet_str.startswith("0x"):
            wallet_str = "0x" + wallet_str
        
        response = requests.get(
            f"{SIM_API_URL}/{wallet_str}",
            headers=headers,
            params={
                "chain_ids": CHAIN_IDS,
                "exclude_spam_tokens": "true"
            },
            timeout=30
        )
        
        if response.status_code == 429:  # Rate limited
            time.sleep(5)
            return get_wallet_portfolio(wallet_address)  # Retry
        
        if response.status_code != 200:
            return {
                "wallet": wallet_str,
                "present_value_usd": 0,
                "token_count": 0,
                "top_tokens": ""
            }
        
        data = response.json()
        
        if "balances" not in data:
            return {
                "wallet": wallet_str,
                "present_value_usd": 0,
                "token_count": 0,
                "top_tokens": ""
            }
        
        total_usd = 0
        token_count = 0
        top_tokens = []
        
        for balance in data["balances"]:
            value_usd = balance.get("value_usd", 0)
            
            # Skip low liquidity tokens
            if balance.get("low_liquidity", False):
                continue
            
            if value_usd and value_usd > 0:
                total_usd += value_usd
                token_count += 1
                top_tokens.append({
                    "symbol": balance.get("symbol", "UNKNOWN"),
                    "value_usd": value_usd
                })
        
        top_tokens.sort(key=lambda x: x["value_usd"], reverse=True)
        top_3 = [t["symbol"] for t in top_tokens[:3]]
        
        return {
            "wallet": wallet_str,
            "present_value_usd": round(total_usd, 2),
            "token_count": token_count,
            "top_tokens": ", ".join(top_3) if top_3 else "" 
        }
        
    except Exception as e:
        return {
            "wallet": str(wallet_address),
            "present_value_usd": 0,
            "token_count": 0,
            "top_tokens": ""
        }

def process_wallet(wallet):
    """Process a single wallet and update progress"""
    global processed_count, results
    
    result = get_wallet_portfolio(wallet)
    
    if result is None:
        return None

    with results_lock:
        results.append(result)
    
    with count_lock:
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"Processed {processed_count} wallets...")
        
        # Save progress periodically
        if processed_count % SAVE_EVERY == 0:
            save_progress()
    
    time.sleep(0.05)  # Small delay between requests
    return result

def save_progress():
    """Save current results to file"""
    with results_lock:
        if results:
            df = pd.DataFrame(results)
            df.to_csv(PROGRESS_FILE, index=False)
            print(f"💾 Progress saved: {len(results)} wallets")

# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("Loading wallets...")
    # Check if file exists
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        exit(1)

    # Use header=0 since we know the file has a header
    df = pd.read_csv(INPUT_FILE)
    # Assuming the first column is the wallet address
    wallets = df.iloc[:, 0].tolist()
    print(f"Found {len(wallets)} wallets")

    # Check for existing progress
    try:
        existing = pd.read_csv(PROGRESS_FILE)
        processed_wallets = set(existing['wallet'].tolist())
        wallets = [w for w in wallets if str(w) not in processed_wallets and f"0x{w}" not in processed_wallets]
        results = existing.to_dict('records')
        print(f"Resuming from {len(processed_wallets)} already processed wallets")
        print(f"Remaining: {len(wallets)} wallets")
    except FileNotFoundError:
        print("Starting fresh...")

    # Process wallets in parallel
    print(f"\nStarting with {MAX_WORKERS} parallel workers...")
    print(f"Estimated time: {len(wallets) * 0.2 / MAX_WORKERS / 60:.1f} minutes")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(process_wallet, wallets)
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted! Saving progress...")
        save_progress()

    # Final save
    print("\nSaving final results...")
    if results:
        result_df = pd.DataFrame(results)
        result_df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Done! Saved to {OUTPUT_FILE}")
        print(f"Total wallets: {len(results)}")
        print(f"Wallets with value: {len([r for r in results if r['present_value_usd'] > 0])}")
        print(f"Total portfolio value: ${sum(r['present_value_usd'] for r in results):,.2f}")
    else:
        print("No results to save.")
