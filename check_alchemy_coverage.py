import pandas as pd
import os

ALL_WALLETS_FILE = 'all_wallets.csv'
ALCHEMY_OUTPUT_FILE = 'alchemy_eth_balances.csv'

def normalize_wallets(df, col_idx=0):
    # Convert to string, strip, lower
    s = df.iloc[:, col_idx].astype(str).str.strip().str.lower()
    # Remove 'wallet' header if it exists as a value
    s = s[s != 'wallet']
    return set(s)

def main():
    print("🔍 Analyzing Alchemy wallet coverage...")
    
    # 1. Load All Wallets
    try:
        df_all = pd.read_csv(ALL_WALLETS_FILE, header=None)
        all_wallets = normalize_wallets(df_all)
        print(f"✅ Total Unique Wallets to Process: {len(all_wallets)}")
    except Exception as e:
        print(f"❌ Error reading {ALL_WALLETS_FILE}: {e}")
        return

    # 2. Load Alchemy Processed Wallets
    processed_wallets = set()
    if os.path.exists(ALCHEMY_OUTPUT_FILE):
        try:
            df_alc = pd.read_csv(ALCHEMY_OUTPUT_FILE)
            processed_wallets = set(df_alc['wallet'].astype(str).str.strip().str.lower())
            print(f"✅ Total Unique Processed (Alchemy): {len(processed_wallets)}")
            
        except Exception as e:
            print(f"⚠️ Error reading {ALCHEMY_OUTPUT_FILE}: {e}")
    else:
        print(f"⚠️ {ALCHEMY_OUTPUT_FILE} not found.")

    # 3. Compare
    missing = all_wallets - processed_wallets
    print(f"📊 Missing Wallets: {len(missing)}")
    
    if missing:
        print("Here are 3 sample missing wallets:")
        print(list(missing)[:3])
        
        # Save missing to a file for inspection/processing
        with open("missing_alchemy.txt", "w") as f:
            for w in missing:
                f.write(f"{w}\n")
        print("💾 Missing wallets saved to missing_alchemy.txt")
    else:
        print("🎉 COMPLETE! All wallets have been processed by Alchemy fetcher.")

if __name__ == "__main__":
    main()
