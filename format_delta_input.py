import pandas as pd
import os

INPUT_FILE = "new_wallets_only.txt"
OUTPUT_FILE = "data/input/delta_wallets.csv"

def format_delta_csv():
    print(f"Reading {INPUT_FILE}...")
    try:
        with open(INPUT_FILE, 'r') as f:
            wallets = [line.strip() for line in f if line.strip()]
            
        print(f"Found {len(wallets)} wallets.")
        
        df = pd.DataFrame(wallets, columns=['wallet'])
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Created formatted CSV at {OUTPUT_FILE} with header 'wallet'")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    format_delta_csv()
