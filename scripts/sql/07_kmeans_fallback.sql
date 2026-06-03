-- ═══════════════════════════════════════════════════════════════════════════════
-- Lab 3 (cont.): Behavioural Clustering — MADlib K-Means OR Pure-SQL Fallback
-- ═══════════════════════════════════════════════════════════════════════════════
-- Produces  bfsi_demo.kmeans_assignments(account_id, cluster_id, inferred_label)
-- consumed by 06_lab3_ai_analytics.sql Part C.
--
-- Two paths:
--   PATH 1 (ACTIVE)   — MADlib kmeanspp on normalized features (madlib 2.1.0).
--                        See BLOCK A. This populates kmeans_assignments.
--   PATH 2 (fallback)  — pure-SQL threshold classifier for builds without MADlib.
--                        See BLOCK B (commented).
--
-- cluster_id is a GEOMETRIC id from k-means (arbitrary). The meaning is carried
-- by inferred_label (NORMAL / CARD-TESTING / BUST-OUT / STRUCTURING); downstream
-- queries filter on inferred_label, never on the numeric id.
-- ═══════════════════════════════════════════════════════════════════════════════
SET search_path TO bfsi_demo, public;

DROP TABLE IF EXISTS bfsi_demo.kmeans_assignments;
DROP TABLE IF EXISTS bfsi_demo.account_features_norm;

-- ── Normalize features to comparable scale (min-max over the population) ──────
CREATE TABLE bfsi_demo.account_features_norm AS
WITH bounds AS (
    SELECT
        min(txn_count) AS min_tc,  max(txn_count) AS max_tc,
        min(distinct_merchants) AS min_dm, max(distinct_merchants) AS max_dm,
        min(total_amount) AS min_ta, max(total_amount) AS max_ta,
        min(merchant_entropy) AS min_me, max(merchant_entropy) AS max_me,
        min(mcc_spread) AS min_ms, max(mcc_spread) AS max_ms,
        min(coalesce(amount_cv,0)) AS min_cv, max(coalesce(amount_cv,0)) AS max_cv
    FROM bfsi_demo.account_features
)
SELECT
    f.account_id,
    round(((f.txn_count - b.min_tc)::numeric / nullif(b.max_tc - b.min_tc,0)), 6)            AS n_txn_count,
    round(((f.distinct_merchants - b.min_dm)::numeric / nullif(b.max_dm - b.min_dm,0)), 6)   AS n_distinct_merchants,
    round(((f.total_amount - b.min_ta) / nullif(b.max_ta - b.min_ta,0)), 6)                  AS n_total_amount,
    round(((f.merchant_entropy - b.min_me) / nullif(b.max_me - b.min_me,0)), 6)              AS n_merchant_entropy,
    round(((f.mcc_spread - b.min_ms) / nullif(b.max_ms - b.min_ms,0)), 6)                    AS n_mcc_spread,
    round(((coalesce(f.amount_cv,0) - b.min_cv) / nullif(b.max_cv - b.min_cv,0)), 6)         AS n_amount_cv
FROM bfsi_demo.account_features f CROSS JOIN bounds b
DISTRIBUTED BY (account_id);

ANALYZE bfsi_demo.account_features_norm;


-- ═══════════════════════════════════════════════════════════════════════════════
-- BLOCK A — MADlib K-Means  (ACTIVE — requires the madlib extension)
-- ═══════════════════════════════════════════════════════════════════════════════
-- WarehousePG ships MADlib (verified here: madlib 2.1.0). kmeanspp seeds
-- well-separated centroids on the 6 normalized features; we then LABEL each
-- geometric cluster by its real feature profile (cv-first rule). The meaning
-- therefore lives in inferred_label and is INDEPENDENT of the arbitrary geometric
-- cluster_id MADlib returns.
CREATE EXTENSION IF NOT EXISTS madlib;

-- Points table: one float8[] feature vector per account.
DROP TABLE IF EXISTS bfsi_demo.km_points;
CREATE TABLE bfsi_demo.km_points AS
SELECT account_id,
       ARRAY[ n_txn_count, n_distinct_merchants, n_total_amount,
              n_merchant_entropy, n_mcc_spread, n_amount_cv ]::float8[] AS features
FROM bfsi_demo.account_features_norm
DISTRIBUTED BY (account_id);

-- Train k-means++.  k = 4  → NORMAL + the three fraud personas.
-- km_result columns: centroids (float8[][]), cluster_variance, objective_fn,
--                    frac_reassigned, num_iterations.
DROP TABLE IF EXISTS bfsi_demo.km_result;
CREATE TABLE bfsi_demo.km_result AS
SELECT * FROM madlib.kmeanspp(
    'bfsi_demo.km_points',        -- source relation
    'features',                   -- features column
    4,                            -- k
    'madlib.squared_dist_norm2',  -- distance function
    'madlib.avg',                 -- centroid aggregate
    20,                           -- max iterations
    0.001                         -- min fraction reassigned (convergence)
);

-- Assign every account to its nearest centroid (0-based geometric cluster id).
DROP TABLE IF EXISTS bfsi_demo.km_raw;
CREATE TABLE bfsi_demo.km_raw AS
SELECT p.account_id,
       (madlib.closest_column(c.centroids, p.features)).column_id::int AS cluster_id
FROM bfsi_demo.km_points p, bfsi_demo.km_result c
DISTRIBUTED BY (account_id);

-- Label each geometric cluster from its real (un-normalized) profile.
-- IMPORTANT: test amount_cv < 0.10 FIRST. The STRUCTURING cluster has a HIGH total
-- spend (many ~9.4k wires) so a spend-first rule would mislabel it as BUST-OUT.
DROP TABLE IF EXISTS bfsi_demo.kmeans_assignments;
CREATE TABLE bfsi_demo.kmeans_assignments AS
WITH cluster_profile AS (
    SELECT r.cluster_id,
           avg(coalesce(f.amount_cv,0)) AS avg_cv,
           avg(f.distinct_merchants)    AS avg_merch,
           avg(f.total_amount)          AS avg_spend
    FROM bfsi_demo.km_raw r
    JOIN bfsi_demo.account_features f USING (account_id)
    GROUP BY r.cluster_id
),
labelled AS (
    SELECT cluster_id,
           CASE
               WHEN avg_cv    < 0.10   THEN 'STRUCTURING'
               WHEN avg_merch > 50     THEN 'CARD-TESTING'
               WHEN avg_spend > 100000 THEN 'BUST-OUT'
               ELSE 'NORMAL'
           END AS inferred_label
    FROM cluster_profile
)
SELECT r.account_id, r.cluster_id, l.inferred_label
FROM bfsi_demo.km_raw r
JOIN labelled l USING (cluster_id)
DISTRIBUTED BY (account_id);

ANALYZE bfsi_demo.kmeans_assignments;


-- ═══════════════════════════════════════════════════════════════════════════════
-- BLOCK B — PURE-SQL PERSONA CLASSIFIER  (FALLBACK — only if MADlib is absent)
-- ═══════════════════════════════════════════════════════════════════════════════
-- If a build has no madlib extension, comment out BLOCK A above and uncomment this
-- block. It assigns the SAME persona labels with explicit, readable threshold
-- rules (same cv-first ordering), so everything downstream is identical.
--
-- CREATE TABLE bfsi_demo.kmeans_assignments AS
-- WITH classified AS (
--     SELECT f.account_id,
--         CASE
--             WHEN coalesce(f.amount_cv,0) < 0.15
--                  AND f.merchant_entropy < 0.20 AND f.txn_count >= 20  THEN 4   -- STRUCTURING
--             WHEN f.merchant_entropy > 0.70 AND f.avg_amount < 10      THEN 2   -- CARD-TESTING
--             WHEN f.avg_amount > 500 AND f.merchant_entropy < 0.60     THEN 3   -- BUST-OUT
--             ELSE 1                                                            -- NORMAL
--         END AS cluster_id
--     FROM bfsi_demo.account_features f
-- )
-- SELECT account_id, cluster_id,
--        CASE cluster_id WHEN 2 THEN 'CARD-TESTING' WHEN 3 THEN 'BUST-OUT'
--                        WHEN 4 THEN 'STRUCTURING' ELSE 'NORMAL' END AS inferred_label
-- FROM classified
-- DISTRIBUTED BY (account_id);
-- ANALYZE bfsi_demo.kmeans_assignments;


-- ═══════════════════════════════════════════════════════════════════════════════
-- CLUSTER CHARACTERISTICS — interpret each cluster (the teaching payoff)
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT
    a.cluster_id,
    a.inferred_label,
    count(*)                          AS accounts,
    round(avg(f.txn_count), 0)        AS avg_txns,
    round(avg(f.distinct_merchants),1) AS avg_merchants,
    round(avg(f.merchant_entropy), 3) AS avg_entropy,
    round(avg(f.total_amount), 0)     AS avg_total_amount,
    round(avg(f.avg_amount), 2)       AS avg_ticket,
    round(avg(coalesce(f.amount_cv,0)), 3) AS avg_amount_cv
FROM bfsi_demo.kmeans_assignments a
JOIN bfsi_demo.account_features f USING (account_id)
GROUP BY 1, 2
ORDER BY 1;

-- ── Validation against ground-truth account-id pools ─────────────────────────
-- (the seed planted personas in known account-id bands)
SELECT
    CASE
        WHEN account_id BETWEEN 100900001 AND 100900040 THEN 'CARD-TESTING'
        WHEN account_id BETWEEN 101900001 AND 101900030 THEN 'BUST-OUT'
        WHEN account_id BETWEEN 105900001 AND 105900040 THEN 'STRUCTURING'
        ELSE 'NORMAL'
    END                  AS ground_truth,
    inferred_label,
    count(*)             AS accounts
FROM bfsi_demo.kmeans_assignments
GROUP BY 1, 2
ORDER BY 1, 3 DESC;


-- ═══════════════════════════════════════════════════════════════════════════════
-- HYBRID FRAUD DISCOVERY — the payoff query (clusters now exist)
-- ═══════════════════════════════════════════════════════════════════════════════
-- Joins flagged clusters to case notes whose MEANING matches "sub-threshold
-- outbound movement" — intent retrieval, not keyword matching. (This is Lab 3
-- Part C1; it lives here because it depends on kmeans_assignments above.)
-- ── C1: Cluster-guided semantic search ───────────────────────────────────────
-- Flagged accounts (STRUCTURING / BUST-OUT clusters) + case notes that match the
-- INTENT of "sub-threshold outbound movement" — even without the word.
WITH flagged AS (
    SELECT f.account_id, f.total_amount, f.amount_cv, f.merchant_entropy, a.cluster_id
    FROM bfsi_demo.account_features f
    JOIN bfsi_demo.kmeans_assignments a ON a.account_id = f.account_id
    WHERE a.inferred_label <> 'NORMAL'           -- fraud clusters (label, not geometric id)
    ORDER BY f.total_amount DESC
    LIMIT 15
),
-- "unusual sub-threshold outbound movement" encoded in the same 32-dim space
query_vector AS (
    SELECT ARRAY[
        0.6,                       -- severity ~3
        0.0, 1.0, 0.0, 0.0,        -- queue aml-tm
        1.0, 1.0, 1.0,             -- transfer + threshold + round-trip  (STRUCTURING)
        0.0, 0.0, 0.0,             -- not bust-out
        0.0, 0.0, 0.0,             -- not card-testing
        1.0, 1.0,                  -- beneficiary/watch + high-risk corridor
        0.0, 0.0, 0.0, 0.0,        -- not dispute/kyc/routine/standing
        0.0, 1.0, 0.0,             -- error-level
        0.0,
        0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
    ]::vector(32) AS vec
),
similar_notes AS (
    SELECT ce.note_id, ce.account_id, ce.queue, left(ce.narrative,120) AS narrative,
           ce.severity, ce.persona,
           round((1 - (ce.embedding <=> qv.vec))::numeric, 4) AS similarity
    FROM bfsi_demo.case_embeddings ce CROSS JOIN query_vector qv
    ORDER BY ce.embedding <=> qv.vec
    LIMIT 50
)
SELECT
    fl.account_id          AS flagged_account,
    fl.cluster_id,
    round(fl.total_amount, 0) AS total_amount,
    fl.amount_cv,
    s.queue,
    s.narrative            AS semantic_match,
    s.similarity,
    s.persona              AS ground_truth
FROM flagged fl
CROSS JOIN similar_notes s
WHERE s.similarity > 0.70
ORDER BY fl.total_amount DESC, s.similarity DESC
LIMIT 40;

-- ── The "Aha!" contrast: keyword search finds nothing the analysts didn't name ─
SELECT count(*) AS literal_keyword_hits
FROM bfsi_demo.case_embeddings
WHERE narrative ILIKE '%structuring%' OR narrative ILIKE '%money laundering%';
-- → 0 rows, while the vector search above surfaces the real cases.
