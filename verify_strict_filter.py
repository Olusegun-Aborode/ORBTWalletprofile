import requests

SIM_API_KEY = "sim_EO9GHAnKw4OOQz1GGR6JbIIPlqS3nX1a"

def check_wallet(wallet, name):
    print(f"\n--- Checking {name} ({wallet}) ---")
    response = requests.get(
        f"https://api.sim.dune.com/v1/evm/balances/{wallet}",
        headers={"X-Sim-Api-Key": SIM_API_KEY},
        params={"chain_ids": "1", "exclude_spam_tokens": "true"}
    )
    
    data = response.json()
    accepted = []
    rejected = []
    
    for b in data.get("balances", []):
        symbol = b.get("symbol", "?")
        val = b.get("value_usd", 0) or 0
        amount = b.get("amount", 0)
        price = b.get("price_usd", 0)
        decimals = b.get("decimals", 18)
        pool = b.get("pool_size", 0) or 0
        is_native = b.get("address") == "native"
        chain = b.get("chain_id")
        
        # REPLICATING THE STRICT FILTER
        # Filter 1: Must be native OR have significant liquidity (> $50k)
        if not (is_native or pool > 50000):
            rejected.append((symbol, val, pool, "Low Liquidity (<50k)"))
            continue
            
        # Filter 2: Value > Pool (Non-native only)
        if not is_native and val > pool:
            rejected.append((symbol, val, pool, "Value > Pool"))
            continue
            
        accepted.append((symbol, val, pool, chain, b.get("address"), amount, price, decimals))

    accepted.sort(key=lambda x: x[1], reverse=True)
    
    print(f"✅ ACCEPTED TOKENS (Total: ${sum(x[1] for x in accepted):.2f}):")
    print(f"{'Symbol':<10} | {'Amount':<15} | {'Price':<10} | {'Value':<12} | {'Pool Size':<15} | {'Address'}")
    for a in accepted:
        # a = (symbol, val, pool, chain, address, amount, price, decimals)
        formatted_amount = float(a[5]) / (10 ** a[7]) if a[7] else 0
        print(f"{a[0]:<10} | {formatted_amount:<15.4f} | ${a[6]:<9.2f} | ${a[1]:<11.2f} | ${a[2]:<15.2f} | {a[4]}")
        
    print(f"\n❌ REJECTED TOKENS (Top 5):")
    rejected.sort(key=lambda x: x[1], reverse=True)
    for r in rejected[:5]:
        print(f"{r[0]:<10} | ${r[1]:<11.2f} | Pool: ${r[2]:<10.2f} | Reason: {r[3]}")

check_wallet("0x788e6f9d17e60e47d6d05424cb8608613ff07de7", "Wallet A (The $28k one)")
check_wallet("0x8bd036ec5fce5341fdfd3a8c1f108a6820b55d92", "Wallet B (The $21k one)")
