import requests
import json

def get_eth_balance(wallet):
    url = "https://eth.llamarpc.com"
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBalance",
        "params": [wallet, "latest"],
        "id": 1
    }
    
    try:
        response = requests.post(url, json=payload)
        data = response.json()
        wei = int(data["result"], 16)
        eth = wei / 10**18
        print(f"Wallet: {wallet}")
        print(f"RPC Balance: {eth:.6f} ETH")
        return eth
    except Exception as e:
        print(f"Error fetching balance for {wallet}: {e}")
        return 0

print("--- CHECKING REAL-TIME ON-CHAIN BALANCE ---")
bal1 = get_eth_balance("0x788e6f9d17e60e47d6d05424cb8608613ff07de7")
bal2 = get_eth_balance("0x8bd036ec5fce5341fdfd3a8c1f108a6820b55d92")
