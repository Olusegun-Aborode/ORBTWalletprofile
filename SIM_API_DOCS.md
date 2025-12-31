# Sim by Dune API Documentation

## Overview
The Sim API provides real-time access to EVM blockchain data, including balances, transactions, and token metadata.

**Base URL**: `https://api.sim.dune.com/v1`

## Authentication
All requests require an API key passed in the header.
- **Header**: `X-Sim-Api-Key: <your_api_key>`

---

## 1. Get Wallet Balances
Fetch token balances for a specific wallet address across supported chains.

**Endpoint**: `GET /evm/balances/{address}`

### Query Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `chain_ids` | string | No | Comma-separated list of chain IDs (e.g., `1,10,8453`). If omitted, returns all supported chains. |
| `exclude_spam_tokens` | boolean | No | If `true`, filters out tokens with <$100 liquidity. |
| `historical_prices` | string | No | Comma-separated list of hours for historical price lookups (e.g., `24,168` for 1 day and 1 week ago). |
| `limit` | integer | No | Max number of results per page. |
| `offset` | string | No | Cursor for pagination (from previous response `next_offset`). |

### Response Structure
```json
{
  "wallet_address": "0x...",
  "balances": [
    {
      "chain": "ethereum",
      "chain_id": 1,
      "address": "native", // or contract address
      "amount": "1000000000000000000",
      "symbol": "ETH",
      "decimals": 18,
      "price_usd": 3896.83,
      "value_usd": 3896.83,
      "pool_size": 500000000, // Liquidity pool size used for pricing
      "low_liquidity": false, // true if pool_size < $10k
      "historical_prices": [
        { "offset_hours": 24, "price_usd": 3800.00 }
      ]
    }
  ],
  "next_offset": "..."
}
```

### Example (Python)
```python
import requests

url = "https://api.sim.dune.com/v1/evm/balances/0x123..."
params = {
    "chain_ids": "1,10,42161",
    "exclude_spam_tokens": "true",
    "historical_prices": "24"
}
headers = {"X-Sim-Api-Key": "YOUR_KEY"}

response = requests.get(url, headers=headers, params=params)
data = response.json()
```

---

## 2. Get Token Info
Get metadata and real-time pricing for a specific token.

**Endpoint**: `GET /evm/token-info/{address}`
*Use `native` as address for native tokens (ETH, MATIC, etc.)*

### Query Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `chain_ids` | string | **Yes** | Single chain ID (e.g., `1`). |
| `historical_prices` | string | No | Historical price offsets in hours. |

### Response Structure
```json
{
  "contract_address": "0x...",
  "tokens": [
    {
      "chain_id": 1,
      "symbol": "USDC",
      "price_usd": 1.00,
      "pool_size": 450000000,
      "total_supply": "...",
      "fully_diluted_value": 1000000000
    }
  ]
}
```

---

## 3. Get Wallet Transactions
Fetch recent transactions for a wallet.

**Endpoint**: `GET /evm/transactions/{address}`

### Query Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `chain_ids` | string | No | Filter by chain(s). |
| `limit` | integer | No | Max transactions to return (default 100). |
| `decode` | boolean | No | If `true`, includes decoded function calls and logs. |

---

## Important Notes
- **Liquidity Filtering**: The `pool_size` field is crucial for filtering out scam tokens with fake prices. A common practice is to ignore tokens where `value_usd > pool_size` or enforce a minimum `pool_size` (e.g., $50,000).
- **Rate Limits**: Pay attention to Compute Unit (CU) usage. `chain_ids` count towards CU cost for Balances endpoint.

