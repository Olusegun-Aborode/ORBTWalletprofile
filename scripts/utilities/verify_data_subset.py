import pandas as pd
import requests
import os
from datetime import datetime, timezone

# Load env
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

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
SIM_API_KEY = os.getenv("SIM_API_KEY")
ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

def verify_wallet_age(wallet, expected_age_days, expected_first_seen):
    """Verify wallet age against Alchemy API"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [{
            "fromBlock": "0x0",
            "toBlock": "latest",
            "fromAddress": wallet,
            "category": ["external", "erc20", "erc721", "erc1155"],
            "maxCount": "0x1",
            "order": "asc",
            "withMetadata": True,
            "excludeZeroValue": False
        }]
    }
    
    try:
        response = requests.post(ALCHEMY_URL, json=payload, timeout=15)
        if response.status_code == 200:
            data = response.json()
            transfers = data.get("result", {}).get("transfers", [])
            
            if not transfers:
                return {
                    "wallet": wallet,
                    "status": "MATCH" if expected_age_days == 0 else "MISMATCH",
                    "expected_age": expected_age_days,
                    "actual_age": 0,
                    "expected_first_seen": expected_first_seen,
                    "actual_first_seen": "N/A",
                    "note": "No transfers found"
                }
            
            meta = transfers[0].get("metadata", {})
            timestamp_str = meta.get("blockTimestamp")
            
            if timestamp_str:
                dt = datetime.strptime(timestamp_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                actual_age_days = (now - dt).days
                
                age_diff = abs(actual_age_days - expected_age_days)
                status = "MATCH" if age_diff <= 1 else "MISMATCH"  # Allow 1 day tolerance
                
                return {
                    "wallet": wallet,
                    "status": status,
                    "expected_age": expected_age_days,
                    "actual_age": actual_age_days,
                    "age_diff": age_diff,
                    "expected_first_seen": expected_first_seen,
                    "actual_first_seen": timestamp_str,
                    "note": f"Age difference: {age_diff} days"
                }
    except Exception as e:
        return {
            "wallet": wallet,
            "status": "ERROR",
            "error": str(e)
        }
    
    return {"wallet": wallet, "status": "ERROR", "error": "Unknown error"}

def verify_tx_count(wallet, expected_count):
    """Verify transaction count"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getTransactionCount",
        "params": [wallet, "latest"]
    }
    
    try:
        response = requests.post(ALCHEMY_URL, json=payload, timeout=15)
        if response.status_code == 200:
            data = response.json()
            actual_count = int(data.get("result", "0x0"), 16)
            
            status = "MATCH" if actual_count == expected_count else "MISMATCH"
            
            return {
                "wallet": wallet,
                "status": status,
                "expected_count": expected_count,
                "actual_count": actual_count,
                "diff": abs(actual_count - expected_count)
            }
    except Exception as e:
        return {"wallet": wallet, "status": "ERROR", "error": str(e)}
    
    return {"wallet": wallet, "status": "ERROR", "error": "Unknown error"}

def verify_balance(wallet, expected_balance):
    """Verify ETH balance"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBalance",
        "params": [wallet, "latest"]
    }
    
    try:
        response = requests.post(ALCHEMY_URL, json=payload, timeout=15)
        if response.status_code == 200:
            data = response.json()
            balance_wei = int(data.get("result", "0x0"), 16)
            actual_balance = balance_wei / 1e18
            
            # Allow 0.001 ETH tolerance for small differences
            diff = abs(actual_balance - expected_balance)
            status = "MATCH" if diff < 0.001 else "MISMATCH"
            
            return {
                "wallet": wallet,
                "status": status,
                "expected_balance": expected_balance,
                "actual_balance": actual_balance,
                "diff": diff
            }
    except Exception as e:
        return {"wallet": wallet, "status": "ERROR", "error": str(e)}
    
    return {"wallet": wallet, "status": "ERROR", "error": "Unknown error"}

def main():
    print("🔍 Starting Data Verification...\n")
    
    # Load final data
    df = pd.read_csv("data/output/final_wallet_data.csv")
    
    # Select subset: wallets with actual data (non-zero tx_count)
    df_active = df[df['tx_count'] > 0].copy()
    
    if len(df_active) == 0:
        print("❌ No active wallets found in dataset!")
        return
    
    # Sample 10 random wallets
    sample_size = min(10, len(df_active))
    df_sample = df_active.sample(n=sample_size, random_state=42)
    
    print(f"📊 Testing {sample_size} random wallets with activity...\n")
    
    results = []
    
    for idx, row in df_sample.iterrows():
        wallet = row['wallet_address']
        print(f"Testing wallet: {wallet}")
        
        # Test 1: Wallet Age
        print("  ├─ Verifying age...", end=" ")
        age_result = verify_wallet_age(
            wallet, 
            row['wallet_age_days'], 
            row['first_seen_date']
        )
        print(f"{age_result['status']}")
        
        # Test 2: TX Count
        print("  ├─ Verifying tx count...", end=" ")
        tx_result = verify_tx_count(wallet, row['tx_count'])
        print(f"{tx_result['status']}")
        
        # Test 3: ETH Balance
        print("  └─ Verifying ETH balance...", end=" ")
        expected_eth = row['alchemy_current_wallet_value'] / 3300  # Reverse the USD conversion
        balance_result = verify_balance(wallet, expected_eth)
        print(f"{balance_result['status']}")
        
        results.append({
            "wallet": wallet,
            "age_status": age_result['status'],
            "age_diff": age_result.get('age_diff', 'N/A'),
            "tx_status": tx_result['status'],
            "tx_diff": tx_result.get('diff', 'N/A'),
            "balance_status": balance_result['status'],
            "balance_diff": balance_result.get('diff', 'N/A')
        })
        
        print()
    
    # Summary
    df_results = pd.DataFrame(results)
    df_results.to_csv("verification_results.csv", index=False)
    
    print("\n" + "="*60)
    print("📊 VERIFICATION SUMMARY")
    print("="*60)
    
    total = len(df_results)
    
    age_matches = len(df_results[df_results['age_status'] == 'MATCH'])
    tx_matches = len(df_results[df_results['tx_status'] == 'MATCH'])
    balance_matches = len(df_results[df_results['balance_status'] == 'MATCH'])
    
    print(f"\nWallet Age Verification:")
    print(f"  ✅ Matches: {age_matches}/{total} ({age_matches/total*100:.1f}%)")
    print(f"  ❌ Mismatches: {total - age_matches}/{total}")
    
    print(f"\nTransaction Count Verification:")
    print(f"  ✅ Matches: {tx_matches}/{total} ({tx_matches/total*100:.1f}%)")
    print(f"  ❌ Mismatches: {total - tx_matches}/{total}")
    
    print(f"\nETH Balance Verification:")
    print(f"  ✅ Matches: {balance_matches}/{total} ({balance_matches/total*100:.1f}%)")
    print(f"  ❌ Mismatches: {total - balance_matches}/{total}")
    
    overall_accuracy = (age_matches + tx_matches + balance_matches) / (total * 3) * 100
    print(f"\n🎯 Overall Accuracy: {overall_accuracy:.1f}%")
    
    print(f"\n💾 Detailed results saved to: verification_results.csv")
    
    if overall_accuracy >= 90:
        print("\n✅ Data quality is EXCELLENT!")
    elif overall_accuracy >= 75:
        print("\n⚠️  Data quality is GOOD but has some discrepancies")
    else:
        print("\n❌ Data quality needs attention")

if __name__ == "__main__":
    main()
