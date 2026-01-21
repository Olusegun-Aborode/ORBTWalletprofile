import requests
import pandas as pd
import time
import os
from concurrent.futures import ThreadPoolExecutor

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

# USE BASE MAINNET
ALCHEMY_BASE_URL = f"https://base-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
ORBT_CONTRACT = "0x48190e377ba663476c1ccd7100c0b49229319199"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

OUTPUT_FILE = "orbt_base_minters.csv"

def fetch_orbt_minters():
    print("🚀 Fetching ORBT Minters from Base (Alchemy API)...")
    
    minters = set()
    page_key = None
    
    while True:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": "0x0",
                    "toBlock": "latest",
                    "fromAddress": ZERO_ADDRESS,
                    "contractAddresses": [ORBT_CONTRACT],
                    "category": ["erc20"],
                    "withMetadata": False,
                    "excludeZeroValue": True
                }
            ]
        }
        
        if page_key:
            payload["params"][0]["pageKey"] = page_key
            
        try:
            response = requests.post(ALCHEMY_BASE_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if response.status_code == 200:
                data = response.json()
                result = data.get("result", {})
                transfers = result.get("transfers", [])
                
                for tx in transfers:
                    to_addr = tx.get("to")
                    if to_addr:
                        minters.add(to_addr.lower().strip())
                
                page_key = result.get("pageKey")
                print(f"   Fetched {len(transfers)} transfers. Total Unique Minters: {len(minters)}")
                
                if not page_key:
                    break
            elif response.status_code == 429:
                print("   ⚠️ 429 Rate Limit. Sleeping 2s...")
                time.sleep(2)
            else:
                print(f"   ❌ Error: {response.status_code} - {response.text}")
                break
        except Exception as e:
            print(f"   ❌ Exception: {e}")
            time.sleep(2)
            
    return list(minters)

def main():
    # 1. Fetch Minters
    minters = fetch_orbt_minters()
    print(f"✅ Found {len(minters)} unique ORBT minters on Base.")
    
    # Save to file
    df = pd.DataFrame(minters, columns=["wallet"])
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
