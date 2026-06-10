-- Create Account Behavioral Features
CREATE TABLE bfsi_demo.account_features AS
SELECT
    account_id,
    COUNT(*) AS txn_count,
    COUNT(DISTINCT merchant_id) AS distinct_merchants,
    COUNT(DISTINCT mcc) AS distinct_mccs,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    STDDEV_SAMP(amount) AS stddev_amount,
    -- Coefficient of variation: measures payment consistency
    -- Low CV (<0.1) = structuring (consistent $9,500 wires)
    -- High CV (>1.0) = normal varied spending
    ROUND(STDDEV_SAMP(amount) / NULLIF(AVG(amount), 0), 4) AS amount_cv,
    -- Merchant concentration: ratio of distinct merchants to transactions
    -- High ratio (close to 1.0) = card testing (every txn = new merchant)
    -- Low ratio (<0.1) = bust-out (few merchants, high spend)
    ROUND(COUNT(DISTINCT merchant_id)::numeric / NULLIF(COUNT(*), 0), 4) AS merchant_concentration
FROM bfsi_demo.transactions
WHERE ts > '2026-06-01'::timestamp
GROUP BY account_id
HAVING COUNT(*) >= 5  -- minimum activity threshold
DISTRIBUTED BY (account_id);

ANALYZE bfsi_demo.account_features;

-- Normalize Features (Z-Score Standardization)
CREATE TABLE bfsi_demo.account_features_norm AS
SELECT
    account_id,
    ARRAY[
        (txn_count - (SELECT AVG(txn_count) FROM bfsi_demo.account_features)) /
            NULLIF((SELECT STDDEV(txn_count) FROM bfsi_demo.account_features), 0),

        (distinct_merchants - (SELECT AVG(distinct_merchants) FROM bfsi_demo.account_features)) /
            NULLIF((SELECT STDDEV(distinct_merchants) FROM bfsi_demo.account_features), 0),

        (distinct_mccs - (SELECT AVG(distinct_mccs) FROM bfsi_demo.account_features)) /
            NULLIF((SELECT STDDEV(distinct_mccs) FROM bfsi_demo.account_features), 0),

        (total_amount - (SELECT AVG(total_amount) FROM bfsi_demo.account_features)) /
            NULLIF((SELECT STDDEV(total_amount) FROM bfsi_demo.account_features), 0),

        (merchant_concentration - (SELECT AVG(merchant_concentration) FROM bfsi_demo.account_features)) /
            NULLIF((SELECT STDDEV(merchant_concentration) FROM bfsi_demo.account_features), 0),

        (COALESCE(amount_cv, 0) - (SELECT AVG(COALESCE(amount_cv, 0)) FROM bfsi_demo.account_features)) /
            NULLIF((SELECT STDDEV(COALESCE(amount_cv, 0)) FROM bfsi_demo.account_features), 0)
    ]::double precision[] AS features
FROM bfsi_demo.account_features
DISTRIBUTED BY (account_id);

ANALYZE bfsi_demo.account_features_norm;

