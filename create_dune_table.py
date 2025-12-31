import requests

DUNE_API_KEY = "iZZZp3h425CcCf6XYmkaur8B5zrfQt5g"

# Using the new endpoint from docs: /v1/table/create
response = requests.post(
    "https://api.dune.com/api/v1/table/create",
    headers={"X-Dune-Api-Key": DUNE_API_KEY},
    json={
        "namespace": "surgence_lab",
        "table_name": "dataset_wallet_portfolio_live",
        "is_private": False,
        "schema": [
            {"name": "wallet", "type": "varchar"},
            {"name": "present_value_usd", "type": "double"},
            {"name": "token_count", "type": "integer"},
            {"name": "top_tokens", "type": "varchar"}
        ]
    }
)
print(response.status_code, response.text)
