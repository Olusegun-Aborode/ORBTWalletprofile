import pandas as pd

TX_FILE = "data/intermediate/wallet_tx_counts.csv"

def main():
    print("📊 Analyzing Wallet Transaction Counts...")
    df = pd.read_csv(TX_FILE)
    
    total = len(df)
    
    # 1. Zero Transactions (Inactive)
    zero_tx = df[df['tx_count'] == 0]
    count_zero = len(zero_tx)
    
    # 2. High Frequency (Bots/Whales) > 20,000
    high_tx = df[df['tx_count'] > 20000]
    count_high = len(high_tx)
    
    # 3. Valid Retail
    valid = df[(df['tx_count'] > 0) & (df['tx_count'] <= 20000)]
    count_valid = len(valid)
    
    print(f"Total Wallets: {total}")
    print(f"❌ Inactive (0 tx): {count_zero} ({count_zero/total:.1%})")
    print(f"❌ Bots/High-Freq (>20k tx): {count_high} ({count_high/total:.1%})")
    print(f"✅ Valid Retail: {count_valid} ({count_valid/total:.1%})")
    
    if count_high > 0:
        print("\nTop 5 High-Freq Wallets (Excluded):")
        print(high_tx.sort_values('tx_count', ascending=False).head(5))

if __name__ == "__main__":
    main()
