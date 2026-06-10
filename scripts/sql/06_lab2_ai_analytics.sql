SET search_path TO bfsi_demo, public;

DROP TABLE IF EXISTS bfsi_demo.kmeans_assignments CASCADE;
DROP TABLE IF EXISTS bfsi_demo.account_features_norm CASCADE;
DROP TABLE IF EXISTS bfsi_demo.account_features CASCADE;
DROP TABLE IF EXISTS bfsi_demo.case_embeddings CASCADE;
DROP INDEX  IF EXISTS bfsi_demo.idx_case_embedding_hnsw CASCADE;

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE bfsi_demo.case_embeddings (
    note_id      BIGINT,
    account_id   BIGINT,
    card_bin     BIGINT,
    analyst      VARCHAR(64),
    queue        VARCHAR(32),
    severity     SMALLINT,
    narrative    TEXT,
    persona      TEXT,          -- 'normal' | 'card_testing' | 'bust_out' | 'structuring'
    embedding    vector(32)     -- 32-dimensional feature vector
) DISTRIBUTED BY (note_id);

INSERT INTO bfsi_demo.case_embeddings
    (note_id, account_id, card_bin, analyst, queue, severity, narrative, persona, embedding)
SELECT
    note_id, account_id, card_bin, analyst, queue, severity, narrative, persona, embedding
FROM (
    SELECT
        note_id,
        account_id,
        card_bin,
        analyst,
        queue,
        severity,
        LEFT(narrative, 500) AS narrative,
        -- Classify each narrative into a persona for lab validation
        CASE
            -- CARD-TESTING: penny auth keywords
            WHEN narrative ILIKE '%penny%' OR narrative ILIKE '%small auth%'
              OR narrative ILIKE '%test%' OR narrative ILIKE '%validate%'
              OR narrative ILIKE '%checking%' OR narrative ILIKE '%live%'
              OR narrative ILIKE '%micro%' OR narrative ILIKE '%sub-dollar%'
            THEN 'card_testing'
            -- BUST-OUT: credit abuse keywords
            WHEN narrative ILIKE '%limit%' OR narrative ILIKE '%ceiling%'
              OR narrative ILIKE '%ramp%' OR narrative ILIKE '%drain%'
              OR narrative ILIKE '%max%' OR narrative ILIKE '%electronics%'
              OR narrative ILIKE '%luxury%' OR narrative ILIKE '%consume%'
            THEN 'bust_out'
            -- STRUCTURING: AML evasion keywords
            WHEN narrative ILIKE '%structure%' OR narrative ILIKE '%below%'
              OR narrative ILIKE '%threshold%' OR narrative ILIKE '%wire%'
              OR narrative ILIKE '%offshore%' OR narrative ILIKE '%layer%'
              OR narrative ILIKE '%$9,%' OR narrative ILIKE '%similar-sized%'
            THEN 'structuring'
            ELSE 'normal'
        END AS persona,
        -- Build 32-dimensional feature vector
        ARRAY[
            severity::float / 5.0,  -- [0] normalized severity (0-1 scale)
            CASE WHEN queue = 'cards-fraud' THEN 1.0 ELSE 0.0 END,  -- [1] card fraud queue
            CASE WHEN queue = 'aml-tm' THEN 1.0 ELSE 0.0 END,  -- [2] AML queue
            CASE WHEN queue = 'disputes' THEN 1.0 ELSE 0.0 END,  -- [3] disputes queue
            CASE WHEN card_bin BETWEEN 41010100 AND 41010199 THEN 1.0 ELSE 0.0 END,  -- [4] card-testing BIN
            CASE WHEN card_bin BETWEEN 52020200 AND 52020299 THEN 1.0 ELSE 0.0 END,  -- [5] bust-out BIN
            CASE WHEN card_bin BETWEEN 53030300 AND 53030399 THEN 1.0 ELSE 0.0 END,  -- [6] structuring BIN
            -- Narrative content features [7-24] - keyword indicators
            CASE WHEN narrative ILIKE '%penny%' OR narrative ILIKE '%test%' OR narrative ILIKE '%validate%' THEN 1.0 ELSE 0.0 END,  -- [7] testing keywords
            CASE WHEN narrative ILIKE '%authorize%' OR narrative ILIKE '%auth%' OR narrative ILIKE '%approval%' THEN 1.0 ELSE 0.0 END,  -- [8] authorization
            CASE WHEN narrative ILIKE '%decline%' OR narrative ILIKE '%reject%' OR narrative ILIKE '%denied%' THEN 1.0 ELSE 0.0 END,  -- [9] declines
            CASE WHEN narrative ILIKE '%merchant%' OR narrative ILIKE '%store%' OR narrative ILIKE '%retailer%' THEN 1.0 ELSE 0.0 END,  -- [10] merchant references
            CASE WHEN narrative ILIKE '%limit%' OR narrative ILIKE '%ceiling%' OR narrative ILIKE '%maximum%' THEN 1.0 ELSE 0.0 END,  -- [11] credit limit
            CASE WHEN narrative ILIKE '%ramp%' OR narrative ILIKE '%drain%' OR narrative ILIKE '%consume%' THEN 1.0 ELSE 0.0 END,  -- [12] spend-up behavior
            CASE WHEN narrative ILIKE '%electronics%' OR narrative ILIKE '%luxury%' OR narrative ILIKE '%resaleable%' THEN 1.0 ELSE 0.0 END,  -- [13] high-value goods
            CASE WHEN narrative ILIKE '%wire%' OR narrative ILIKE '%transfer%' OR narrative ILIKE '%payment%' THEN 1.0 ELSE 0.0 END,  -- [14] wires
            CASE WHEN narrative ILIKE '%offshore%' OR narrative ILIKE '%overseas%' OR narrative ILIKE '%foreign%' THEN 1.0 ELSE 0.0 END,  -- [15] cross-border
            CASE WHEN narrative ILIKE '%structure%' OR narrative ILIKE '%smurfing%' OR narrative ILIKE '%layering%' THEN 1.0 ELSE 0.0 END,  -- [16] structuring terms
            CASE WHEN narrative ILIKE '%threshold%' OR narrative ILIKE '%$10,%' OR narrative ILIKE '%reporting%' THEN 1.0 ELSE 0.0 END,  -- [17] threshold avoidance
            CASE WHEN narrative ILIKE '%pattern%' OR narrative ILIKE '%suspicious%' OR narrative ILIKE '%anomaly%' THEN 1.0 ELSE 0.0 END,  -- [18] suspicious activity
            CASE WHEN narrative ILIKE '%velocity%' OR narrative ILIKE '%rapid%' OR narrative ILIKE '%burst%' THEN 1.0 ELSE 0.0 END,  -- [19] velocity indicators
            CASE WHEN narrative ILIKE '%dispute%' OR narrative ILIKE '%chargeback%' OR narrative ILIKE '%refund%' THEN 1.0 ELSE 0.0 END,  -- [20] customer service
            CASE WHEN narrative ILIKE '%travel%' OR narrative ILIKE '%geo%' OR narrative ILIKE '%location%' THEN 1.0 ELSE 0.0 END,  -- [21] geographic
            CASE WHEN narrative ILIKE '%card lost%' OR narrative ILIKE '%replacement%' OR narrative ILIKE '%reissue%' THEN 1.0 ELSE 0.0 END,  -- [22] card lifecycle
            CASE WHEN narrative ILIKE '%BIN%' OR narrative ILIKE '%scheme%' OR narrative ILIKE '%issuer%' THEN 1.0 ELSE 0.0 END,  -- [23] card metadata
            CASE WHEN narrative ILIKE '%Brazil%' OR narrative ILIKE '%Nigeria%' OR narrative ILIKE '%Russia%' THEN 1.0 ELSE 0.0 END,  -- [24] high-risk countries
            -- Severity features [25-27]
            CASE WHEN severity = 1 THEN 1.0 ELSE 0.0 END,  -- [25] critical
            CASE WHEN severity = 2 THEN 1.0 ELSE 0.0 END,  -- [26] high
            CASE WHEN severity = 3 THEN 1.0 ELSE 0.0 END,  -- [27] medium
            -- Random noise padding [28-31]
            random()*0.05, random()*0.05, random()*0.05, random()*0.05
        ]::vector(32) AS embedding
    FROM bfsi_demo.case_narratives
    WHERE ts > '2026-06-01'::timestamp
) sub
-- Sample: keep all fraud narratives, 10% of normal narratives
WHERE persona != 'normal' OR (persona = 'normal' AND note_id % 10 = 0)
LIMIT 600000;

ANALYZE bfsi_demo.case_embeddings;

\i /scripts/sql/08_add_diverse_narratives.sql

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

CREATE TABLE bfsi_demo.kmeans_assignments AS
WITH model AS (
    -- Train K-means++ model (MADlib built-in function)
    SELECT centroids
    FROM madlib.kmeanspp(
        'bfsi_demo.account_features_norm',  -- source table
        'features',                         -- feature column
        6,                                  -- k clusters (allows separation of fraud types)
        'madlib.squared_dist_norm2',        -- Euclidean distance
        'madlib.avg',                       -- centroid aggregate
        50,                                 -- max iterations
        0.001::double precision             -- convergence threshold
    )
),
-- Unpack the 2-D centroid array into one row per cluster
centroids AS (
    SELECT
        i - 1 AS cluster_id,
        ARRAY[
            m.centroids[i][1],  -- normalized txn_count
            m.centroids[i][2],  -- normalized distinct_merchants
            m.centroids[i][3],  -- normalized distinct_mccs
            m.centroids[i][4],  -- normalized total_amount
            m.centroids[i][5],  -- normalized merchant_concentration
            m.centroids[i][6]   -- normalized amount_cv
        ]::double precision[] AS centroid
    FROM model m, generate_series(1, 6) AS i
),
-- Assign each account to nearest cluster
assignments AS (
    SELECT
        n.account_id,
        c.cluster_id,
        ROW_NUMBER() OVER (
            PARTITION BY n.account_id
            ORDER BY madlib.dist_norm2(n.features, c.centroid)
        ) AS rn
    FROM bfsi_demo.account_features_norm n
    CROSS JOIN centroids c
)
SELECT account_id, cluster_id
FROM assignments
WHERE rn = 1
DISTRIBUTED BY (account_id);

ANALYZE bfsi_demo.kmeans_assignments;

WITH cluster_profile AS (
    -- Compute average characteristics for each cluster
    SELECT
        a.cluster_id,
        COUNT(*) AS accounts,
        ROUND(AVG(f.distinct_merchants), 1) AS avg_merchants,
        ROUND(AVG(f.total_amount), 0) AS avg_spend,
        ROUND(AVG(COALESCE(f.amount_cv, 0)), 3) AS avg_cv
    FROM bfsi_demo.kmeans_assignments a
    JOIN bfsi_demo.account_features f USING (account_id)
    GROUP BY a.cluster_id
)
SELECT
    cluster_id,
    accounts,
    avg_merchants,
    avg_spend,
    avg_cv,
    -- Interpret what each cluster represents
    CASE
        -- CARD-TESTING: Extreme merchant diversity (>1000 merchants)
        WHEN avg_merchants > 1000 THEN 'CARD-TESTING'

        -- STRUCTURING: Ultra-high spend + low CV (consistent amounts)
        WHEN avg_cv < 0.15 AND avg_spend > 10000000 THEN 'STRUCTURING'

        -- BUST-OUT: Ultra-high spend without low CV
        WHEN avg_spend > 10000000 THEN 'BUST-OUT'

        -- NORMAL: Everything else (baseline behavior)
        ELSE 'NORMAL'
    END AS inferred_label
FROM cluster_profile
ORDER BY cluster_id;

CREATE OR REPLACE VIEW bfsi_demo.kmeans_labeled AS
WITH cluster_profile AS (
    -- Compute average characteristics for each cluster
    SELECT
        a.cluster_id,
        AVG(f.distinct_merchants) AS avg_merchants,
        AVG(f.total_amount) AS avg_spend,
        AVG(COALESCE(f.amount_cv, 0)) AS avg_cv
    FROM bfsi_demo.kmeans_assignments a
    JOIN bfsi_demo.account_features f USING (account_id)
    GROUP BY a.cluster_id
),
cluster_labels AS (
    -- Assign labels based on behavioral signatures
    SELECT
        cluster_id,
        CASE
            WHEN avg_merchants > 1000 THEN 'CARD-TESTING'
            WHEN avg_cv < 0.15 AND avg_spend > 10000000 THEN 'STRUCTURING'
            WHEN avg_spend > 10000000 THEN 'BUST-OUT'
            ELSE 'NORMAL'
        END AS inferred_label
    FROM cluster_profile
)
SELECT
    a.account_id,
    a.cluster_id,
    cl.inferred_label
FROM bfsi_demo.kmeans_assignments a
JOIN cluster_labels cl USING (cluster_id);

-- CHECK FINAL RESULTS
SELECT inferred_label, COUNT(*) AS accounts
FROM bfsi_demo.kmeans_labeled
GROUP BY 1
ORDER BY
    CASE inferred_label
        WHEN 'CARD-TESTING' THEN 1
        WHEN 'BUST-OUT' THEN 2
        WHEN 'STRUCTURING' THEN 3
        WHEN 'NORMAL' THEN 4
    END;

-- View detailed cluster profiles
SELECT
    a.cluster_id,
    a.inferred_label,
    COUNT(*) AS member_count,
    ROUND(AVG(f.txn_count), 1) AS avg_txns,
    ROUND(AVG(f.distinct_merchants), 1) AS avg_merchants,
    ROUND(AVG(f.merchant_concentration), 3) AS avg_concentration,
    ROUND(AVG(f.total_amount), 0) AS avg_total_spend,
    ROUND(AVG(f.avg_amount), 2) AS avg_ticket,
    ROUND(AVG(COALESCE(f.amount_cv, 0)), 3) AS avg_amount_cv
FROM bfsi_demo.kmeans_labeled a
JOIN bfsi_demo.account_features f USING (account_id)
GROUP BY 1, 2
ORDER BY
    CASE a.inferred_label
        WHEN 'CARD-TESTING' THEN 1
        WHEN 'BUST-OUT' THEN 2
        WHEN 'STRUCTURING' THEN 3
        WHEN 'NORMAL' THEN 4
    END;
