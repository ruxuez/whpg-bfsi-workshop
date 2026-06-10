# Lab 2: AI Analytics & The AI Factory (BFSI Edition)

## Overview
In this lab, you will use **pgvector** for semantic fraud narrative search and **Apache MADlib** for behavioral clustering to discover card fraud patterns. The goal is to demonstrate how **in-database machine learning** outperforms traditional keyword searches and external Python/Spark workflows.

**Key Learning Objectives:**
1. Understand why **keyword search is fragile** (misses variations in fraud descriptions)
2. See how **pgvector semantic search** finds fraud by **meaning**, not exact keywords
3. Watch **MADlib unsupervised clustering** automatically discover **4 fraud personas**
4. Experience the **AI Factory** - both systems validating each other in one SQL query

## ⚠️ DO NOT HIT "Next" button without instructions ! ⚠️
---

## The Three-Part Journey

###  Part A: Demonstrating pgvector Value
**The Problem:** Traditional keyword search is brittle - it only finds exact matches.

| Query | What It Shows |
| :--- | :--- |
| **A1: The Keyword Search Problem** | `LIKE '%card testing%'` finds 320K logs, but `LIKE '%bust out%'` finds ZERO. Same fraud type, different words! This demonstrates keyword search fragility. |
| **A2: pgvector Finds Fraud by MEANING** | Uses vector similarity to find narratives semantically related to fraud - discovers "penny authorizations", "limit ramped", "sub-$10k wires", "structuring" WITHOUT exact keyword matches. |

**Key Insight:** Semantic search understands **meaning** and catches variations keyword search misses.

---

### Part B: Demonstrating MADlib Value
**The Power:** Unsupervised learning discovers patterns automatically - no labels, no rules, just math.

| Query | What It Shows |
| :--- | :--- |
| **B1: MADlib Discovered 4 Fraud Personas** | K-Means clustering found 4 behavioral clusters: 🃏 CARD-TESTING (high merchants, tiny amounts), 💳 BUST-OUT (massive spend), 💰 STRUCTURING (sub-$10k wires), ✅ NORMAL. |
| **B2: The Dramatic Differences** | CARD-TESTING cluster: **3,456 avg merchants** (2,841× normal). BUST-OUT cluster: **$48,725 average** (97× normal). STRUCTURING: **amount_cv < 0.1** (zero variance). |

**Key Insight:** MADlib discovers fraud **automatically** with dramatic statistical evidence.

---

### Part C: The AI Factory (Validation Through Agreement)
**The Proof:** When two independent AI systems find the same threats, confidence is high.

| Query | What It Shows |
| :--- | :--- |
| **C1: Fraud Pattern Correlation** | Shows how MADlib (behavioral) and pgvector (semantic) **independently discovered the same 3 fraud types**: CARD-TESTING, BUST-OUT, STRUCTURING. Both systems agree in ONE SQL query - no Python export! |
| **C2: Why This Matters** | Traditional warehouse: export → train → join (hours). WarehousePG: one SQL query (5 seconds on 13M transactions). In-database ML eliminates the "data movement tax". |

**Key Insight:** MADlib provides behavioral evidence ("3,456 merchants, $0.50 average"), pgvector provides semantic evidence ("logs say 'penny authorization testing'"). When both agree = high confidence.

---

### Part D: ML → Watchlist (Closing the Loop)
**The Power:** MADlib doesn't just discover fraud—it predicts who should be on your watchlist **before** manual investigation.

| Query | What It Shows |
| :--- | :--- |
| **D1: ML Fraud Gap** | Shows transactions from accounts MADlib flagged as fraud but are NOT yet in your fraud_watchlists. These are **predictive alerts** - the ML found them, but your operational system hasn't caught up yet. |
| **D2: Extended Watchlist Query** | After running "Refresh Watchlist", Query 1A from Lab 1 now includes **both** traditional BIN-range matches AND ML-discovered accounts. One query, two fraud detection methods working together. |

**Key Insight:** Traditional fraud watchlists are **reactive** - you add a BIN after it's compromised. MADlib clustering is **predictive** - it flags accounts exhibiting fraud-like behavior patterns before they hit your watchlist. The gap between D1 (before refresh) and D2 (after refresh) shows ML discovering fraud your existing rules missed.

**Why This Matters:** In production, you'd run MADlib clustering nightly on the previous day's transactions. By morning, your fraud analysts see a refreshed watchlist that includes behavioral anomalies detected by ML - often catching fraud rings 2-3 weeks before traditional rule-based systems. No Python export, no external ML platform, no data movement - just SQL writing directly back to your operational fraud_watchlists table.

---

## Lab Setup

### Prerequisites
Data was already loaded in Lab 1. This lab extracts AI/ML features from that existing data.

**Embedded Fraud Personas:**
The data contains 4 behavioral patterns:
- **Normal (70%)**: Baseline card activity
- **Card-Testing (12%)**: Penny auths - hundreds of merchants, tiny amounts ($0.40-$4)
- **Bust-Out (8%)**: Credit abuse - massive spend ($800-$5000), few merchants
- **Structuring (10%)**: AML evasion - sub-$10k wires, consistent amounts, high-risk countries

> [!NOTE]
> Execute following from the [button label="⚠️WarehousePG Tab"](tab-0).

---

## Step 1: Build pgvector Embeddings for Semantic Search

### 1.1 Open psql and Clean Up

```run
psql demo
```

Prepares a clean slate and enables pgvector for vector operations.

```run
SET search_path TO bfsi_demo, public;

DROP TABLE IF EXISTS bfsi_demo.kmeans_assignments CASCADE;
DROP TABLE IF EXISTS bfsi_demo.account_features_norm CASCADE;
DROP TABLE IF EXISTS bfsi_demo.account_features CASCADE;
DROP TABLE IF EXISTS bfsi_demo.case_embeddings CASCADE;
DROP INDEX  IF EXISTS bfsi_demo.idx_case_embedding_hnsw CASCADE;

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;
````

### 1.2 Create Narrative Embeddings Table

Creates a table where each fraud narrative will have a 32-dimensional vector representation.

```run
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
```
> [!NOTE]
> **Why vectors?** In production, you'd use a sentence-transformer model (e.g., all-MiniLM-L6-v2) to convert text to embeddings. Here we create feature-based vectors from message characteristics (program type, keywords, severity).

---

### 1.3 Generate Embeddings from Case Narratives

- Converts ~200K fraud case narratives into 32-dimensional vectors
- Each vector dimension represents a feature (queue, BIN, keywords, severity)
- Enables semantic similarity search: "find narratives similar to this fraud pattern"

```run,wrap
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
LIMIT 200000;

ANALYZE bfsi_demo.case_embeddings;
```
**How it works:** When you search for narratives similar to "Card testing with penny authorizations", pgvector computes cosine distance between vectors and returns the closest matches - even if they use different phrases like "sub-dollar auth validation" or "micro-charge sweeping".

---

### 1.4 Add Diverse Sample Narratives for Demo Quality

Adds 20+ manually crafted messages with diverse phrasings to ensure Query A2 shows clear variety (not just one repeated message).

Run the diversity enhancement script:
```run
\i /scripts/sql/08_add_diverse_narratives.sql
```

**Sample narratives added:**
- CARD-TESTING: "Penny authorization testing", "Micro-charge sweeping", "Sub-dollar auth burst", "Validation probing"
- BUST-OUT: "Limit ramped to ceiling", "Credit line drained", "Luxury goods spend-up", "Electronics purchase surge"
- STRUCTURING: "Sub-$10k wire pattern", "Just-below-threshold transfers", "Layered offshore movement", "Smurfing detected"

---

## Step 2: Build MADlib Features for Behavioral Clustering

### 2.1 Create Netflow Behavioral Features

Creates one row per account with **7 behavioral features**:

| Feature | What It Detects | Fraud Signature |
|---------|----------------|------------------|
| `txn_count` | Transaction volume | High = active testing |
| `distinct_merchants` | Merchant diversity | **High (>500) = CARD-TESTING** |
| `distinct_mccs` | MCC variety | High = testing across categories |
| `total_amount` | Total spend | **High (>$100k) = BUST-OUT** |
| `avg_amount` | Average ticket | Low (<$5) = testing; High (>$2k) = bust-out |
| `stddev_amount` | Amount variance | High = normal; Low = structuring |
| `amount_cv` | Coefficient of variation | **Low (<0.1) = STRUCTURING** |
| `merchant_concentration` | Merchant ratio | High = testing; Low = bust-out |

```run,wrap
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
```

---

## Step 3: Run MADlib K-Means Clustering

### Step 3.1:  Normalize Features (Z-Score Standardization)

- Converts all 6 features to z-scores (standard deviations from mean)
- Puts features on comparable scales for distance calculations
- Creates one normalized feature vector per account

```run,wrap
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
```

### Step 3.2:  Run MADlib K-Means++ Clustering

**⚠️ This is the heart of MADlib clustering - see how it works:**
Enable MADlib and run K-means++ with k=6 clusters:
```run,wrap
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
```

> [!NOTE]
> **What this does:** K-means assigns each account to one of 6 clusters based on behavioral similarity. The cluster_id numbers (0-5) are arbitrary and will vary between runs - that's normal!

### Step 3.3: Profile Clusters and Assign Labels

Now inspect what each cluster represents and assign meaningful labels:

```run,wrap
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
```

> [!IMPORTANT]
> **Look for extreme outliers:**
> - **CARD-TESTING**: ~20 accounts with ~50,000 merchants (vs ~60 for normal)
> - **BUST-OUT**: ~15 accounts with ~$100M spend (vs ~$25K for normal)
> - **STRUCTURING**: ~20 accounts with ~$245M spend + CV near 0.03 (vs 0.5-0.6 for normal)
> - **NORMAL**: ~50,000 accounts split into 3-4 natural sub-groups
>
> **Note the cluster_id for each fraud type** - you'll need this for the next step!

---

### Step 3.4: Create Labeled View

Create a view that adds labels to cluster assignments (no UPDATE needed - labels computed dynamically):

```run,wrap
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
```

> [!NOTE]
> **Why a VIEW?** This keeps clustering (Step 3.2) separate from interpretation (Step 3.4). Labels are always computed from actual cluster characteristics, so they never get stale!

### Step 3.5: Verify Final Results

Check label distribution:

```run
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
```

View detailed cluster profiles:

```run
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
```

> [!IMPORTANT]
> **Understanding K-Means Non-Determinism**
>
> K-means clustering is **non-deterministic** - each run may produce different `cluster_id` assignments (0-5). This is normal and expected! The algorithm randomly initializes cluster centers, so:
>
> - **Cluster IDs vary**: What's cluster 3 in your run might be cluster 1 in another attendee's run
> - **Labels stay meaningful**: The `inferred_label` column interprets each cluster's characteristics, so you'll always see CARD-TESTING, BUST-OUT, STRUCTURING, and NORMAL
> - **Extreme outliers stand out**: Look for dramatic differences - 50,000× more merchants, 4,000× higher spend, CV near zero
>
> **What to expect:**
> - **20 CARD-TESTING accounts**: ~50K merchants/account (vs ~60 for normal)
> - **15 BUST-OUT accounts**: ~$100M spend/account (vs ~$25K for normal)
> - **20 STRUCTURING accounts**: ~$245M spend/account, CV~0.03 (vs 0.5-0.6 for normal)
> - **~50,000 NORMAL accounts**: Baseline behavior, split into 3-4 natural sub-groups
>
> **Note**: Sometimes K-means may merge similar fraud types (e.g., bust-out + structuring) into one cluster if they're close in feature space. This is a realistic ML outcome - unsupervised learning finds patterns, but doesn't always perfectly separate every fraud type.


Quit WarehousePG:
```run
\quit
```

---

## Step 4: Visualize Clusters

Launch the application by running the following command in the terminal:

```run
python3.9 /scripts/apps/dashboard.py
```
Access the UI: Click the [button label="⚠️MADlib Dashboard Tab"](tab-1) at the top of your lab environment .

Explore the Data: Use the dropdown menus to change the scatter plot axes.

This allows you to visualize how different dimensions (like total_amount vs. distinct_merchants) impact the formation of the 4 behavioral clusters.

**Features:**
- Scatter plot: choose X/Y axes (e.g., `total_amount` vs `distinct_merchants`)
- Color-coded clusters
- Hover for account details

**Try this:** Select `total_amount` (X) vs `distinct_merchants` (Y) - you'll see 4 distinct clusters:

- Cluster near origin (0,0): Normal transactions - moderate spend, moderate merchants
- At ~3,500 merchants, low spend: CARD-TESTING - Every transaction is a different merchant testing card validity
- At ~$100k spend, few merchants: BUST-OUT - Massive spend concentrated at a handful of merchants
- Ccluster: STRUCTURING - Consistent amounts, offshore wires

Once finished, you can go back to [button label="⚠️WarehousePG Tab"](tab-0) and Press `CTRL+C` to quit application:

---

## Step 5: Launch the AI Factory Dashboard

1.  **Launch the AI Dashboard: [button label="⚠️WarehousePG Tab"](tab-0)
    ```run
    python3.9 /scripts/apps/app2.py
    ```
2.  Go to [button label="⚠️AI Analytics Tab"](tab-2)
3.  Execute different queries

### Panel A: pgvector Value

**Query A1: The Keyword Search Problem**
- Shows: `LIKE '%card testing%'` → 320K results, but `LIKE '%bust out%'` → 0 results
- **Observation:** Same fraud category, different terminology = missed!
- **Lesson:** Keyword search is fragile

**Query A2: pgvector Finds Threats by MEANING**
- Shows: 15+ semantically similar fraud narratives
- **Observation:** "Penny authorization testing", "Limit ramped to ceiling", "Sub-$10k wire pattern" - all found WITHOUT exact keywords!
- **Lesson:** Semantic search understands meaning

---

### Panel B: MADlib Value

**Query B1: MADlib Discovered 4 Fraud Personas**
- Shows: 5 clusters with their characteristics
- **Observation:** Clear behavioral signatures for CARD-TESTING, BUST-OUT, STRUCTURING, NORMAL
- **Lesson:** Unsupervised learning discovers patterns automatically

**Query B2: The Dramatic Differences**
- Shows: Quantified separation between clusters
- **Observation:**
  - CARD-TESTING: 3,456 avg merchants (2,841x more than normal!)
  - BUST-OUT: $48,725 avg spend (97x more than normal!)
  - STRUCTURING: amount_cv = 0.08 (zero variance = consistent wires)
- **Lesson:** These aren't subtle - they're mathematically undeniable

---

### The AI Factory

**Query C1: Fraud Pattern Correlation**
- Shows: How MADlib and pgvector independently found the SAME 3 fraud types
- **Observation:**
  - CARD-TESTING: 6K accounts (3,456 merchants - MADlib) + 320K narratives ("testing" - pgvector)
  - BUST-OUT: 4K accounts ($48K avg - MADlib) + 160K narratives ("limit drained" - pgvector)
  - STRUCTURING: 5K accounts (CV<0.1 - MADlib) + 200K narratives ("sub-$10k" - pgvector)
- **Lesson:** When both systems agree = high confidence fraud detection

**Query C2: Why This Matters**
- Shows: Comparison of traditional warehouse vs WarehousePG
- **Observation:**
  - Traditional: Export → Python → Train → Upload → Join (hours)
  - WarehousePG: One SQL query (<5 seconds on 13M transactions)
- **Lesson:** In-database ML eliminates "data movement tax"

---

## Key Takeaways

### Why This Lab Matters

1. **Keyword Search Fails:** Missing one fraud term = missing fraud patterns
2. **Semantic Search Wins:** Finds fraud by meaning, catches terminology variations
3. **MADlib Discovers:** Unsupervised clustering finds fraud humans might miss
4. **Statistics Don't Lie:** 2,841x merchants and $48K spend differences are undeniable
5. **Validation Through Agreement:** Two independent systems confirming = high confidence
6. **In-Database Speed:** Seconds vs hours - no Python export tax
7. **Compliance Advantage:** Transaction data never leaves the database

### Real-World Impact

In a fraud operations center, you can't predict every way a fraud analyst might describe suspicious activity. pgvector semantic search solves this. You can't manually set thresholds for every possible fraud pattern across millions of accounts. MADlib clustering discovers them automatically. And you can't afford hours of ETL latency or PII data movement. In-database ML delivers answers in seconds while maintaining compliance.

This is the future of financial analytics: **AI that runs where the sensitive data lives.**
