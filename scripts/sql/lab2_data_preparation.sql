-- ═══════════════════════════════════════════════════════════════════════════════
-- Lab 2: AI Analytics Data Preparation (One-Step Setup)
-- ═══════════════════════════════════════════════════════════════════════════════
-- This script prepares all AI/ML tables needed for Lab 2 in a single execution:
--   1. pgvector embeddings for semantic fraud search
--   2. MADlib behavioral features for clustering
--   3. K-means fraud persona discovery
--   4. Labeled cluster views for querying
--
-- Estimated runtime: 2-3 minutes on 13M transactions
-- ═══════════════════════════════════════════════════════════════════════════════

SET search_path TO bfsi_demo, public;

\echo ''
\echo '═══════════════════════════════════════════════════════════════════════════════'
\echo 'Lab 2 Data Preparation - Building AI/ML Analytics Tables'
\echo '═══════════════════════════════════════════════════════════════════════════════'
\echo ''

-- ───────────────────────────────────────────────────────────────────────────────
-- Step 1: Clean Up & Enable pgvector
-- ───────────────────────────────────────────────────────────────────────────────
\echo '[1/5] Cleaning up existing tables and enabling pgvector...'

DROP TABLE IF EXISTS bfsi_demo.kmeans_assignments CASCADE;
DROP TABLE IF EXISTS bfsi_demo.account_features_norm CASCADE;
DROP TABLE IF EXISTS bfsi_demo.account_features CASCADE;
DROP TABLE IF EXISTS bfsi_demo.case_embeddings CASCADE;
DROP INDEX  IF EXISTS bfsi_demo.idx_case_embedding_hnsw CASCADE;

CREATE EXTENSION IF NOT EXISTS vector;

\echo '      ✓ Cleanup complete, pgvector enabled'
\echo ''

-- ───────────────────────────────────────────────────────────────────────────────
-- Step 2: Build pgvector Embeddings (Semantic Search)
-- ───────────────────────────────────────────────────────────────────────────────
\echo '[2/5] Building pgvector embeddings for semantic fraud search...'
\echo '      Converting ~200K case narratives to 32-dimensional vectors'
\echo '      (estimated time: 60-90 seconds)'

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

\echo '      ✓ Embeddings created'
\echo ''

-- Add diverse sample narratives for better demo quality
\echo '      Adding curated diverse fraud narratives...'
\i /scripts/sql/08_add_diverse_narratives.sql
\echo '      ✓ Diverse samples added'
\echo ''

-- ───────────────────────────────────────────────────────────────────────────────
-- Step 3: Extract MADlib Behavioral Features
-- ───────────────────────────────────────────────────────────────────────────────
\echo '[3/5] Extracting behavioral features for MADlib clustering...'
\echo '      Analyzing 50K accounts across 13M transactions'
\echo '      (estimated time: 15-20 seconds)'

CREATE TABLE bfsi_demo.account_features AS
SELECT
    account_id,
    COUNT(*) AS txn_count,
    COUNT(DISTINCT merchant_id) AS distinct_merchants,
    COUNT(DISTINCT mcc) AS distinct_mccs,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    STDDEV_SAMP(amount) AS stddev_amount,
    ROUND(STDDEV_SAMP(amount) / NULLIF(AVG(amount), 0), 4) AS amount_cv,
    ROUND(COUNT(DISTINCT merchant_id)::numeric / NULLIF(COUNT(*), 0), 4) AS merchant_concentration
FROM bfsi_demo.transactions
WHERE ts > '2026-06-01'::timestamp
GROUP BY account_id
HAVING COUNT(*) >= 5  -- minimum activity threshold
DISTRIBUTED BY (account_id);

ANALYZE bfsi_demo.account_features;

\echo '      ✓ Behavioral features extracted'
\echo ''

-- ───────────────────────────────────────────────────────────────────────────────
-- Step 4: Normalize Features & Run MADlib K-Means Clustering
-- ───────────────────────────────────────────────────────────────────────────────
\echo '[4/5] Running MADlib K-means clustering to discover fraud personas...'
\echo '      Normalizing features and training K-means++ (k=6)'
\echo '      (estimated time: 20-30 seconds)'

-- Normalize features (z-score standardization)
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

-- Run K-means++ clustering
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

\echo '      ✓ K-means clustering complete'
\echo ''

-- ───────────────────────────────────────────────────────────────────────────────
-- Step 5: Create Labeled Cluster View
-- ───────────────────────────────────────────────────────────────────────────────
\echo '[5/5] Creating labeled cluster view with fraud persona assignments...'

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

\echo '      ✓ Labeled view created'
\echo ''

-- ───────────────────────────────────────────────────────────────────────────────
-- Summary: Show What ML Discovered
-- ───────────────────────────────────────────────────────────────────────────────
\echo '═══════════════════════════════════════════════════════════════════════════════'
\echo 'Setup Complete! Here is what ML discovered:'
\echo '═══════════════════════════════════════════════════════════════════════════════'
\echo ''

-- Show cluster distribution
\echo 'Fraud Persona Distribution:'
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

\echo ''
\echo 'Detailed Cluster Profiles:'
-- Show detailed cluster profiles
SELECT
    a.cluster_id,
    a.inferred_label,
    COUNT(*) AS accounts,
    ROUND(AVG(f.distinct_merchants), 1) AS avg_merchants,
    ROUND(AVG(f.total_amount), 0) AS avg_total_spend,
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

\echo ''
\echo '═══════════════════════════════════════════════════════════════════════════════'
\echo '✓ AI models ready! You can now:'
\echo '  1. Visualize clusters: python3.9 /scripts/apps/dashboard.py'
\echo '  2. Query results: python3.9 /scripts/apps/app2.py'
\echo '═══════════════════════════════════════════════════════════════════════════════'
\echo ''
