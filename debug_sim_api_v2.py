import requests

SIM_API_KEY = "sim_EO9GHAnKw4OOQz1GGR6JbIIPlqS3nX1a"

response = requests.get(
    "https://api.sim.dune.com/v1/evm/balances/0x788e6f9d17e60e47d6d05424cb8608613ff07de7",
    headers={"X-Sim-Api-Key": SIM_API_KEY},
    params={"chain_ids": "1,42161,137,10,8453", "exclude_spam_tokens": "true"}
)

print(f"Status Code: {response.status_code}")
try:
    data = response.json()
    total_value = 0
    valid_tokens = []
    
    for b in data.get("balances", []):
        val = b.get("value_usd", 0) or 0
        pool = b.get("pool_size", 0) or 0
        if val > 0:
            valid_tokens.append(b)
            total_value += val
    
    # Sort by value descending
    valid_tokens.sort(key=lambda x: x.get("value_usd", 0), reverse=True)
    
    print(f"\nTotal Portfolio Value: ${total_value:.2f}")
    print("-" * 80)
    print(f"{'Symbol':<10} | {'Chain':<6} | {'Value USD':<15} | {'Pool Size':<15} | {'Low Liq':<10} | {'Address'}")
    print("-" * 90)
    
    for b in valid_tokens[:20]:
        print(f"{b.get('symbol', '?'):<10} | {str(b.get('chain_id', '?')):<6} | ${b.get('value_usd', 0):<14.2f} | ${b.get('pool_size', 0):<14.2f} | {str(b.get('low_liquidity')):<10} | {b.get('address')}")
        
except Exception as e:
    print(f"Error: {e}")
    print(response.text)
