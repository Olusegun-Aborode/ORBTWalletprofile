import requests
import pandas as pd
import time
import os

# Public Base RPC
BASE_RPC_URL = "https://mainnet.base.org"
ORBT_CONTRACT = "0x48190e377ba663476c1ccd7100c0b49229319199"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC = "0x0000000000000000000000000000000000000000000000000000000000000000"

OUTPUT_FILE = "orbt_base_minters.csv"
BLOCK_RANGE = 2000 # Small range for public RPC

def hex_to_int(h):
    return int(h, 16)

def get_latest_block():
    try:
        resp = requests.post(BASE_RPC_URL, json={"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}, timeout=10)
        return int(resp.json()['result'], 16)
    except:
        return 0

def fetch_logs(from_block, to_block):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{
            "address": ORBT_CONTRACT,
            "topics": [TRANSFER_TOPIC, ZERO_TOPIC],
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block)
        }]
    }
    
    for attempt in range(3):
        try:
            resp = requests.post(BASE_RPC_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if 'error' in data:
                    # Rate limit or range too large
                    print(f"RPC Error: {data['error']}")
                    return None
                return data.get('result', [])
            elif resp.status_code == 429:
                time.sleep(2 * (attempt + 1))
        except:
            time.sleep(1)
    return None

def main():
    print("🚀 Fetching ORBT Minters from Base (Public RPC)...")
    
    # 1. Get Range
    # ORBT Deployed around Block 2,000,000? No, Base started later. 
    # Let's just go back enough or find deployment.
    # To be safe, we scan from 0? No, too slow.
    # Base Mainnet started Aug 2023. Block 0 is genesis.
    # We can try to binary search deployment or just fail fast.
    # Actually, simpler: Use `eth_getLogs` with "fromBlock": "earliest". 
    # Public RPCs often fail large ranges.
    # Let's try fetching latest block and go backwards.
    
    latest = get_latest_block()
    print(f"   Latest Base Block: {latest}")
    if latest == 0:
        print("❌ Failed to get block number")
        return

    minters = set()
    current_block = latest
    
    # We'll scan backwards until we hit empty results for many chunks? 
    # Or just scan a fixed large range if we knew deployment.
    # ORBT contract likely deployed recently?
    # Let's try to scan last 5M blocks (approx 6 months on Base? 2s block time = 1.2M blocks/month. So 7M blocks).
    # This might take too long with public RPC.
    
    # BETTER IDEA: Use the User's SQL Query on Dune?
    # No, I can't.
    
    # WAIT! Alchemy supports "multichain" keys usually. Maybe I just need to use a different key or url?
    # The error was "BASE_MAINNET is not enabled". This is a plan restriction.
    
    # Let's try the Public RPC with a smarter approach.
    # We can use a larger range like 10k and backoff.
    
    step = 10000
    start_block = 0 # We need to find this.
    
    # Let's try from block 10,000,000 (Base is at 20M+ now?)
    # Base current block is ~12M (as of Mar 2024). 
    # Let's just try scanning from 0 (or earliest available) if possible?
    # No, too slow.
    
    # Let's try a single request for the WHOLE history first. 
    # Some RPCs allow it if result set is small.
    # If it fails, we chunk.
    
    print("   Attempting full history scan (might fail)...")
    logs = fetch_logs(0, latest)
    
    if logs is not None:
        print(f"✅ Success! Got {len(logs)} logs in one go.")
        for log in logs:
            if len(log['topics']) >= 3:
                # topic[2] is the 'to' address (padded)
                to_hex = log['topics'][2]
                wallet = "0x" + to_hex[26:]
                minters.add(wallet)
    else:
        print("   Full scan failed. Chunking...")
        # Fallback to chunking from latest backwards
        # We'll scan last 10M blocks (approx 6 months)
        # 12M blocks total roughly.
        
        current = latest
        empty_streaks = 0
        
        while current > 0:
            start = max(0, current - step)
            logs = fetch_logs(start, current)
            
            if logs is None:
                # Reduce step
                step //= 2
                if step < 100:
                    print("❌ Step too small, skipping chunk.")
                    current -= 1000
                    step = 5000
                continue
            
            if logs:
                empty_streaks = 0
                for log in logs:
                    if len(log['topics']) >= 3:
                        to_hex = log['topics'][2]
                        wallet = "0x" + to_hex[26:]
                        minters.add(wallet)
                print(f"   Block {start}-{current}: Found {len(logs)} logs. Total: {len(minters)}")
            else:
                empty_streaks += 1
                # If we see no logs for 1M blocks, maybe we are before deployment?
                if empty_streaks > 100: # 100 * 10k = 1M blocks
                    print("   No logs for 1M blocks. Assuming before deployment.")
                    break
            
            current = start
            
    print(f"✅ Found {len(minters)} unique ORBT minters.")
    df = pd.DataFrame(list(minters), columns=["wallet"])
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
