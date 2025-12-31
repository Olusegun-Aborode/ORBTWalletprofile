import requests

DUNE_API_KEY = "iZZZp3h425CcCf6XYmkaur8B5zrfQt5g"

# Delete old table
print("Deleting old table...")
response = requests.delete(
    "https://api.dune.com/api/v1/table/surgence_lab/dataset_wallet_portfolio_ath",
    headers={"X-Dune-Api-Key": DUNE_API_KEY}
)
print(f"Delete status: {response.status_code} {response.text}")

# Create new table
print("Creating new table...")
response = requests.post(
    "https://api.dune.com/api/v1/table/create",
    headers={"X-Dune-Api-Key": DUNE_API_KEY},
    json={
        "namespace": "surgence_lab",
        "table_name": "dataset_wallet_portfolio_ath",
        "is_private": False,
        "schema": [
            {"name": "wallet", "type": "varchar"},
            {"name": "present_value_usd", "type": "double"},
            {"name": "ath_value_usd", "type": "double"},
            {"name": "token_count", "type": "integer"},
            {"name": "top_tokens", "type": "varchar"}
        ]
    }
)
print(f"Create status: {response.status_code} {response.text}")
print("Done")
