import pandas as pd
import os

# Files to compare
EXTRACTED_FILE = "wallets_extracted.txt"
EXISTING_DB_FILE = "data/input/final_active_wallets.csv"

def verify_extraction_stats():
    print("🔍 Starting Verification of Wallet Extraction Stats...\n")
    
    # 1. Analyze Extracted File
    print(f"Reading {EXTRACTED_FILE}...")
    try:
        # Read text file, strip quotes and whitespace
        with open(EXTRACTED_FILE, 'r') as f:
            extracted_wallets = {line.strip().strip('"').lower() for line in f if line.strip()}
            
        count_extracted = len(extracted_wallets)
        print(f"✅ Loaded Extracted Wallets: {count_extracted}")
        
    except Exception as e:
        print(f"❌ Error reading extracted file: {e}")
        return

    # 2. Analyze Existing DB
    print(f"\nReading {EXISTING_DB_FILE}...")
    try:
        df_db = pd.read_csv(EXISTING_DB_FILE)
        # Handle column name variations
        col_name = 'wallet' if 'wallet' in df_db.columns else df_db.columns[0]
        existing_wallets = set(df_db[col_name].astype(str).str.lower().str.strip())
        
        count_existing = len(existing_wallets)
        print(f"✅ Loaded Existing DB Wallets: {count_existing}")
        
    except Exception as e:
        print(f"❌ Error reading DB file: {e}")
        return

    # 3. Compare
    print("\n--- COMPARISON RESULTS ---")
    
    common = extracted_wallets.intersection(existing_wallets)
    new_wallets = extracted_wallets - existing_wallets
    
    count_common = len(common)
    count_new = len(new_wallets)
    
    pct_duplicate = (count_common / count_extracted) * 100 if count_extracted > 0 else 0
    pct_new = (count_new / count_extracted) * 100 if count_extracted > 0 else 0
    
    print(f"Total Extracted (Unique): {count_extracted}")
    print(f"Common (Duplicate):       {count_common} ({pct_duplicate:.1f}%)")
    print(f"Truly NEW Wallets:        {count_new} ({pct_new:.1f}%)")
    
    # 4. Save New Wallets
    if count_new > 0:
        output_file = "new_wallets_only.txt"
        with open(output_file, 'w') as f:
            for w in new_wallets:
                f.write(f"{w}\n")
        print(f"\n✅ Saved {count_new} new wallets to {output_file}")
    else:
        print("\n⚠️ No new wallets found.")

if __name__ == "__main__":
    verify_extraction_stats()
