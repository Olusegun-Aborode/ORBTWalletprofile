
import requests
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

SIM_API_KEY = os.getenv("SIM_API_KEY")
if not SIM_API_KEY:
    print("No SIM_API_KEY")
    exit()

TOKENS = {
    "DOG": "0xbaac2b4491727d78d2b78815144570b9f2fe8899",
    "VERSE": "0x249ca82617ec3dfb2589c4c17ab7ec9765350a18",
    "PYME": "0x3408636a7825e894ac5521ca55494f89f96df240",
    "L3": "0x88909d489678dd17aa6d9609f89b0419bf78fd9a"
}

headers = {"X-Sim-Api-Key": SIM_API_KEY}
SIM_URL = "https://api.sim.dune.com/v1/evm/token-info"

print("Fetching prices...")
for name, addr in TOKENS.items():
    try:
        r = requests.get(f"{SIM_URL}/{addr}", headers=headers, params={"chain_ids": "1"})
        data = r.json()
        tokens = data.get("tokens", [])
        if tokens:
            price = tokens[0].get("price_usd")
            print(f"{name}: {price}")
        else:
            print(f"{name}: No price found")
    except Exception as e:
        print(f"{name}: Error {e}")
