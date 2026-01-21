import subprocess
import pandas as pd
import sys
import os

def run_script(script_name):
    print(f"\n🚀 Running {script_name}...")
    try:
        subprocess.run(["python3", script_name], check=True)
        print(f"✅ {script_name} completed.")
    except subprocess.CalledProcessError as e:
        print(f"❌ {script_name} failed with error {e}")
        sys.exit(1)

def merge_tx_counts():
    print("\n🔄 Merging tx counts into delta_wallets.csv...")
    try:
        if not os.path.exists("data/intermediate/wallet_tx_counts_delta.csv"):
            print("⚠️ wallet_tx_counts_delta.csv not found, skipping merge.")
            return

        df_wallets = pd.read_csv("data/input/delta_wallets.csv")
        df_tx = pd.read_csv("data/intermediate/wallet_tx_counts_delta.csv")
        
        # Clean columns
        df_wallets['wallet'] = df_wallets['wallet'].astype(str).str.lower().str.strip()
        
        # Check tx columns
        if 'wallet' in df_tx.columns:
             df_tx['wallet'] = df_tx['wallet'].astype(str).str.lower().str.strip()
             merged = pd.merge(df_wallets, df_tx[['wallet', 'tx_count']], on='wallet', how='left')
        elif 'wallet_address' in df_tx.columns:
             df_tx['wallet_address'] = df_tx['wallet_address'].astype(str).str.lower().str.strip()
             merged = pd.merge(df_wallets, df_tx, left_on='wallet', right_on='wallet_address', how='left')
             if 'wallet_address' in merged.columns:
                 merged.drop(columns=['wallet_address'], inplace=True)
        else:
             print("Warning: wallet_tx_counts_delta.csv format unknown")
             return

        merged['tx_count'] = merged['tx_count'].fillna(0).astype(int)
        merged.to_csv("data/input/delta_wallets.csv", index=False)
        print(f"✅ delta_wallets.csv updated with tx_count. Total rows: {len(merged)}")
    except Exception as e:
        print(f"⚠️ Failed to merge tx counts: {e}")

if __name__ == "__main__":
    print("🌟 STARTING FULL DELTA PIPELINE 🌟")
    
    # 1. Fetch TX Counts first (needed for base file)
    run_script("fetch_tx_counts_delta.py")
    merge_tx_counts()
    
    # 2. Fetch other metrics
    run_script("fetch_wallet_age_delta.py")
    run_script("fetch_alchemy_balances_delta.py")
    run_script("fetch_volumes_delta.py")
    run_script("fetch_gas_fees_delta.py")
    run_script("wallet_portfolio_ath_fetcher_delta.py")
    
    # 3. Consolidate
    run_script("create_consolidated_table_delta.py")
    
    # 4. Upload
    run_script("upload_delta.py")
    
    print("\n🎉 PIPELINE COMPLETE! 🎉")
