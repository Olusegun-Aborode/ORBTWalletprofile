WITH consolidated AS (
    SELECT 
        wallet_address,
        sim_current_wallet_value,
        alchemy_current_wallet_value
    FROM dune.surgence_lab.dataset_wallet_portfolio_consolidated
),
dune_native AS (
    SELECT 
        address, 
        balance / 1e18 AS eth_balance,
        (balance / 1e18) * (SELECT price FROM prices.usd_latest WHERE symbol='WETH') AS eth_value_usd
    FROM ethereum.core.ez_current_balances
    WHERE address IN (SELECT wallet_address FROM consolidated)
),
dune_erc20 AS (
    SELECT 
        wallet_address,
        SUM(amount * price) AS token_value_usd
    FROM balances.ethereum.erc20_latest
    WHERE wallet_address IN (SELECT wallet_address FROM consolidated)
    GROUP BY 1
)
SELECT 
    c.wallet_address,
    c.sim_current_wallet_value,
    c.alchemy_current_wallet_value,
    COALESCE(n.eth_value_usd, 0) + COALESCE(e.token_value_usd, 0) AS dune_total_usd,
    COALESCE(n.eth_balance, 0) AS dune_eth_balance,
    -- Differences
    c.sim_current_wallet_value - (COALESCE(n.eth_value_usd, 0) + COALESCE(e.token_value_usd, 0)) AS diff_sim_dune,
    c.alchemy_current_wallet_value - (COALESCE(n.eth_value_usd, 0) + COALESCE(e.token_value_usd, 0)) AS diff_alchemy_dune
FROM consolidated c
LEFT JOIN dune_native n ON c.wallet_address = n.address
LEFT JOIN dune_erc20 e ON c.wallet_address = e.wallet_address
ORDER BY ABS(diff_sim_dune) DESC
LIMIT 1000;
