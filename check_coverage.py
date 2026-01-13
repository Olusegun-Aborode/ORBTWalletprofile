import pandas as pd
import os

ALL_WALLETS_FILE = 'all_wallets.csv'
SIM_OUTPUT_FILE = 'wallet_portfolio_ath_backup.csv'

def normalize_wallets(df, col_idx=0):
    # Convert to string, strip, lower
    s = df.iloc[:, col_idx].astype(str).str.strip().str.lower()
    # Remove 'wallet' header if it exists as a value
    s = s[s != 'wallet']
    return set(s)

def main():
    print("🔍 Analyzing wallet coverage...")
    
    # 1. Load All Wallets
    try:
        df_all = pd.read_csv(ALL_WALLETS_FILE, header=None)
        all_wallets = normalize_wallets(df_all)
        print(f"✅ Total Unique Wallets to Process: {len(all_wallets)}")
    except Exception as e:
        print(f"❌ Error reading {ALL_WALLETS_FILE}: {e}")
        return

    # 2. Load SIM Processed Wallets
    processed_wallets = set()
    if os.path.exists(SIM_OUTPUT_FILE):
        try:
            df_sim = pd.read_csv(SIM_OUTPUT_FILE, header=None)
            # Assuming format: wallet, ... (wallet is 1st column)
            processed_wallets = normalize_wallets(df_sim)
            print(f"✅ Total Unique Processed (SIM): {len(processed_wallets)}")
            
            # Deduplicate the SIM file while we are at it
            if len(df_sim) > len(processed_wallets) + 1: # +1 for potential header
                 print("🧹 Deduplicating SIM output file...")
                 # We need to keep the data associated with the wallet.
                 # We'll drop duplicates based on the first column (normalized)
                 
                 # Create a temp column for normalized wallet
                 df_sim['norm_wallet'] = df_sim.iloc[:, 0].astype(str).str.strip().str.lower()
                 
                 # Drop duplicates keeping first
                 df_sim_clean = df_sim.drop_duplicates(subset=['norm_wallet'])
                 
                 # Remove the 'wallet' row if it exists in data
                 df_sim_clean = df_sim_clean[df_sim_clean['norm_wallet'] != 'wallet']
                 
                 # Remove temp col
                 df_sim_clean = df_sim_clean.drop(columns=['norm_wallet'])
                 
                 # Save back
                 # Note: The original file had no header (header=None), so we save without header
                 df_sim_clean.to_csv(SIM_OUTPUT_FILE, index=False, header=False)
                 print(f"💾 Cleaned SIM file saved. Rows: {len(df_sim_clean)}")
                 
        except Exception as e:
            print(f"⚠️ Error reading {SIM_OUTPUT_FILE}: {e}")
    else:
        print(f"⚠️ {SIM_OUTPUT_FILE} not found.")

    # 3. Compare
    missing = all_wallets - processed_wallets
    print(f"📊 Missing Wallets: {len(missing)}")
    
    if missing:
        print("Here are 3 sample missing wallets:")
        print(list(missing)[:3])
    else:
        print("🎉 COMPLETE! All wallets have been processed by SIM fetcher.")

if __name__ == "__main__":
    main()
