import requests

SIM_API_KEY = "sim_EO9GHAnKw4OOQz1GGR6JbIIPlqS3nX1a"

response = requests.get(
    "https://api.sim.dune.com/v1/evm/balances/0x4638eed954ce851515cf8a1462dbcc69ca332ae5",
    headers={"X-Sim-Api-Key": SIM_API_KEY},
    params={"chain_ids": "1", "exclude_spam_tokens": "true"}
)

print(f"Status Code: {response.status_code}")
try:
    data = response.json()
    # print(data) # Debug: print full json if needed, but user asked for specific loop
    
    for b in data.get("balances", []):
        print(f"{b.get('symbol')}: ${b.get('value_usd', 0):.2f} | low_liq: {b.get('low_liquidity')} | pool: ${b.get('pool_size', 0):.2f}")
except Exception as e:
    print(f"Error parsing JSON: {e}")
    print(response.text)
