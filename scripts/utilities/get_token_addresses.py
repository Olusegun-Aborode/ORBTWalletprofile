
import requests
import os

# Load Env
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

WALLET = "0xea3e579da60cdb97f3870f862af5db982c0ea08c" 

def get_transfers_with_contracts(wallet):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "fromAddress": wallet,
                "category": ["erc20"],
                "withMetadata": False,
                "excludeZeroValue": True
            }
        ]
    }
    resp = requests.post(ALCHEMY_URL, json=payload)
    return resp.json().get("result", {}).get("transfers", [])

transfers = get_transfers_with_contracts(WALLET)
token_map = {}

print("Scanning for token addresses...")
for tx in transfers:
    asset = tx.get("asset")
    contract = tx.get("rawContract", {}).get("address")
    if asset in ["L3", "DOG", "VERSE", "PYME", "WETH", "USDC", "USDT"]:
        token_map[asset] = contract

for name, addr in token_map.items():
    print(f"{name}: {addr}")
