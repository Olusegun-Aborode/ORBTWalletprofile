import pandas as pd
import os

# Paths
CIS_EXPORT_FILE = "/Users/olusegunaborode/Downloads/cis-users-export.csv"
LOCAL_DB_FILE = "data/input/final_active_wallets.csv"

def check_duplicates():
    print("Checking for duplicates...")
    
    # 1. Load Local DB (Master List)
    print(f"Loading local DB from {LOCAL_DB_FILE}...")
    try:
        df_db = pd.read_csv(LOCAL_DB_FILE)
        # Assume column is 'wallet' or the first column
        if 'wallet' in df_db.columns:
            db_wallets = set(df_db['wallet'].astype(str).str.lower().str.strip())
        else:
            db_wallets = set(df_db.iloc[:, 0].astype(str).str.lower().str.strip())
        
        print(f"✅ Loaded {len(db_wallets)} unique wallets from local DB.")
    except Exception as e:
        print(f"❌ Error loading local DB: {e}")
        return

    # 2. Load CIS Export
    print(f"Loading export file from {CIS_EXPORT_FILE}...")
    try:
        df_cis = pd.read_csv(CIS_EXPORT_FILE)
        cis_wallets = set()
        
        if "Wallet Addresses" in df_cis.columns:
            for val in df_cis["Wallet Addresses"].dropna():
                # Split by semicolon and clean
                parts = str(val).split(';')
                for part in parts:
                    clean_w = part.strip().lower()
                    if clean_w.startswith('0x') and len(clean_w) == 42:
                        cis_wallets.add(clean_w)
        
        print(f"✅ Found {len(cis_wallets)} unique valid wallets in export file.")
        
    except Exception as e:
        print(f"❌ Error loading export file: {e}")
        return

    # 3. Check Intersection
    duplicates = cis_wallets.intersection(db_wallets)
    new_wallets = cis_wallets - db_wallets
    
    print("\n--- RESULTS ---")
    print(f"Total wallets in export: {len(cis_wallets)}")
    print(f"Already existing in DB:  {len(duplicates)}")
    print(f"Truly NEW wallets:       {len(new_wallets)}")
    
    if len(duplicates) > 0:
        print(f"\nExample duplicates: {list(duplicates)[:5]}")
    
    if len(duplicates) == len(cis_wallets):
        print("\n⚠️ CONCLUSION: ALL wallets in the export file ALREADY EXIST in the local DB.")
    elif len(duplicates) == 0:
        print("\n✅ CONCLUSION: NO duplicates found. All export wallets are new.")
    else:
        print("\n⚠️ CONCLUSION: Some wallets are duplicates, some are new.")

if __name__ == "__main__":
    check_duplicates()
