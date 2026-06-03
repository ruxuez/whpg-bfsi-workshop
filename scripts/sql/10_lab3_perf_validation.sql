-- ═══════════════════════════════════════════════════════════════════════════════
-- 10_lab3_perf_validation.sql
-- Meridian Retail Bank — Lab 3 (pgvector + clustering) PERFORMANCE VALIDATION
--
-- Run this on the loaded `bank` database AFTER 06 / 07 / 08 have built the Lab 3
-- objects (case_embeddings, account_features, kmeans_assignments):
--
--     psql -d bank -f 10_lab3_perf_validation.sql
--
-- It EXPLAIN-ANALYZEs the heavy Lab 3 operations so you can confirm they scale on
-- the full June 2026 dataset before wiring up the dashboard. Read the plan notes
-- under each query — they say what a healthy plan looks like on WarehousePG.
-- ═══════════════════════════════════════════════════════════════════════════════

\timing on
SET search_path TO bfsi_demo, public;
SET enable_seqscan = on;     -- a cluster-wide enable_seqscan=off distorts these plans
SET statement_mem = '1500MB';   -- the feature-build aggregate sorts; give it room

-- ─────────────────────────────────────────────────────────────────────────────
-- [1] account_features build — the single heaviest Lab 3 step (TWO-PASS, as 06).
--     Pass 1 finds accounts with >= 5 txns (cheap count, no DISTINCT); pass 2 runs
--     the COUNT(DISTINCT)s only on those accounts' rows.
--     HEALTHY PLAN: HashAggregate (not GroupAggregate spilling to disk); a
--     Redistribute Motion by account_id is EXPECTED (transactions is DISTRIBUTED
--     BY region_id) and is a one-time cost. Watch for: sort spills ("external
--     merge Disk") → raise statement_mem; Broadcast of transactions → bad.
-- ─────────────────────────────────────────────────────────────────────────────
\echo
\echo ===== [1] account_features build (heavy aggregate over transactions) =====
EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
WITH active_accounts AS (
    SELECT account_id
    FROM transactions
    WHERE ts > '2026-06-01'::timestamp
    GROUP BY account_id
    HAVING count(*) >= 5
)
SELECT
    t.account_id,
    count(*)                                                AS txn_count,
    count(DISTINCT t.merchant_id)                           AS distinct_merchants,
    count(DISTINCT t.mcc)                                   AS distinct_mcc,
    sum(t.amount)                                           AS total_amount,
    avg(t.amount)                                           AS avg_amount,
    stddev_samp(t.amount)                                   AS stddev_amount,
    round(count(DISTINCT t.merchant_id)::numeric / nullif(count(*),0), 4) AS merchant_entropy,
    round(count(DISTINCT t.mcc)::numeric / nullif(count(*),0), 4)         AS mcc_spread,
    round(stddev_samp(t.amount) / nullif(avg(t.amount),0), 4)            AS amount_cv,
    count(*) FILTER (WHERE t.auth_response <> '00')         AS declines
FROM transactions t
JOIN active_accounts a USING (account_id)
WHERE t.ts > '2026-06-01'::timestamp
GROUP BY t.account_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- [1b] MADlib K-Means — assignment cost (07 BLOCK A).
--      kmeanspp TRAINING time is shown while 07 runs; here we measure the part
--      that scales with account count: scoring every account against the centroids
--      with madlib.closest_column. Requires km_points / km_result from 07 BLOCK A.
--      HEALTHY: a single pass over km_points; cluster_id is geometric (meaning is
--      in kmeans_assignments.inferred_label).
-- ─────────────────────────────────────────────────────────────────────────────
\echo
\echo ===== [1b] MADlib k-means assignment (closest_column over km_points) =====
EXPLAIN (ANALYZE, VERBOSE)
SELECT p.account_id,
       (madlib.closest_column(c.centroids, p.features)).column_id::int AS cluster_id
FROM bfsi_demo.km_points p, bfsi_demo.km_result c;

-- ─────────────────────────────────────────────────────────────────────────────
-- [2] pgvector semantic KNN — BEFORE an ANN index.
--     Cosine nearest-neighbour against a CONSTANT query vector (the realistic
--     "embed the analyst's query, then search" pattern). With no index this is a
--     full scan + Sort by distance.
--     GOTCHA: HNSW is only used when the query vector is a CONSTANT/parameter. If
--     you instead pull the reference vector from another row via a CTE/JOIN
--     (e.g. WITH ref AS (SELECT embedding ...) ... ORDER BY e.embedding <=> ref.v),
--     the planner CANNOT use the index and falls back to a seq scan. Pre-fetch the
--     reference into a constant/bind parameter first.
-- ─────────────────────────────────────────────────────────────────────────────
\echo
\echo ===== [2] pgvector KNN BEFORE HNSW index (seq scan + distance sort) =====
DROP INDEX IF EXISTS idx_case_embedding_hnsw;
EXPLAIN (ANALYZE, VERBOSE)
SELECT account_id, queue, persona,
       round((1 - (embedding <=> ARRAY[0.6,0.0,1.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,
              0.0,0.0,0.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,
              0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]::vector(32)))::numeric, 4) AS similarity
FROM case_embeddings
ORDER BY embedding <=> ARRAY[0.6,0.0,1.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,
        0.0,0.0,0.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,
        0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]::vector(32)
LIMIT 15;

-- ─────────────────────────────────────────────────────────────────────────────
-- [3] Build the HNSW ANN index, then run the SAME KNN again.
--     Requires pgvector >= 0.5 (verified: pgvector 0.7.4). The plan should now show
--     "Index Scan using idx_case_embedding_hnsw" and drop sharply in time.
--     NOTE: HNSW is approximate; raise hnsw.ef_search for higher recall.
-- ─────────────────────────────────────────────────────────────────────────────
\echo
\echo ===== [3] build HNSW index, then KNN AFTER (should use the ANN index) =====
CREATE INDEX idx_case_embedding_hnsw
    ON case_embeddings USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
ANALYZE case_embeddings;
SET hnsw.ef_search = 100;
EXPLAIN (ANALYZE, VERBOSE)
SELECT account_id, queue, persona,
       round((1 - (embedding <=> ARRAY[0.6,0.0,1.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,
              0.0,0.0,0.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,
              0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]::vector(32)))::numeric, 4) AS similarity
FROM case_embeddings
ORDER BY embedding <=> ARRAY[0.6,0.0,1.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,
        0.0,0.0,0.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,
        0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]::vector(32)
LIMIT 15;

-- ─────────────────────────────────────────────────────────────────────────────
-- [4] Keyword vs persona counts (the A1 "aha" contrast). Cheap scan of
--     case_embeddings; confirm it returns instantly.
-- ─────────────────────────────────────────────────────────────────────────────
\echo
\echo ===== [4] keyword-miss contrast (ILIKE scan) =====
EXPLAIN (ANALYZE)
SELECT COUNT(*) FILTER (WHERE narrative ILIKE '%structuring%') AS kw_structuring,
       COUNT(*) FILTER (WHERE narrative ILIKE '%smurfing%')    AS kw_smurfing,
       COUNT(*) FILTER (WHERE persona = 'structuring')         AS actual_structuring
FROM case_embeddings;

-- ─────────────────────────────────────────────────────────────────────────────
-- [5] Cluster characteristics (B1). Join of kmeans_assignments ⋈ account_features.
--     BOTH are DISTRIBUTED BY account_id, so this join should be LOCAL on every
--     segment — no Motion. If you see a Redistribute/Broadcast here, the
--     distribution keys drifted.
-- ─────────────────────────────────────────────────────────────────────────────
\echo
\echo ===== [5] cluster characteristics (co-located join, expect NO motion) =====
EXPLAIN (ANALYZE, VERBOSE)
SELECT a.cluster_id, a.inferred_label,
       COUNT(*)                               AS accounts,
       ROUND(AVG(f.distinct_merchants), 0)    AS avg_merchants,
       ROUND(AVG(f.total_amount)::numeric, 0) AS avg_spend,
       ROUND(AVG(f.amount_cv)::numeric, 4)    AS avg_cv
FROM kmeans_assignments a
JOIN account_features f USING (account_id)
GROUP BY 1, 2
ORDER BY accounts DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- [6] Hybrid discovery payoff (C1) — flagged clusters × semantic note match.
--     Small flagged set (LIMIT 15) + a KNN over case_embeddings. With the HNSW
--     index from step [3] the similar-notes CTE uses the ANN index.
-- ─────────────────────────────────────────────────────────────────────────────
\echo
\echo ===== [6] hybrid discovery payoff (flagged clusters x semantic notes) =====
EXPLAIN (ANALYZE, VERBOSE)
WITH flagged AS (
    SELECT f.account_id, f.total_amount, f.amount_cv, a.cluster_id, a.inferred_label
    FROM account_features f
    JOIN kmeans_assignments a ON a.account_id = f.account_id
    WHERE a.inferred_label <> 'NORMAL'   -- label, NOT geometric id (MADlib ids are arbitrary)
    ORDER BY f.total_amount DESC
    LIMIT 15
),
query_vector AS (
    SELECT ARRAY[0.6, 0.0,1.0,0.0,0.0, 1.0,1.0,1.0, 0.0,0.0,0.0, 0.0,0.0,0.0,
                 1.0,1.0, 0.0,0.0,0.0,0.0, 0.0,1.0,0.0, 0.0,
                 0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]::vector(32) AS vec
),
similar_notes AS (
    SELECT ce.account_id, ce.queue, left(ce.narrative,80) AS narrative, ce.persona,
           round((1 - (ce.embedding <=> qv.vec))::numeric, 4) AS similarity
    FROM case_embeddings ce CROSS JOIN query_vector qv
    ORDER BY ce.embedding <=> qv.vec
    LIMIT 50
)
SELECT fl.account_id AS flagged_account, fl.cluster_id,
       round(fl.total_amount,0) AS total_amount, fl.amount_cv,
       s.queue, s.narrative AS semantic_match, s.similarity, s.persona AS ground_truth
FROM flagged fl
JOIN similar_notes s ON s.account_id = fl.account_id
ORDER BY fl.total_amount DESC, s.similarity DESC;

\timing off
\echo
\echo ════════════════════════════════════════════════════════════════════════════
\echo  PERF CHECKLIST — what a healthy Lab 3 looks like on WarehousePG:
\echo   [1]  feature build: HashAggregate, no disk spill; one Redistribute by
\echo        account_id is fine. If it spills -> SET statement_mem higher.
\echo   [1b] MADlib assignment: single pass over km_points. Training (kmeanspp)
\echo        time is printed while 07 runs; a few iterations on ~thousands of
\echo        accounts is quick. cluster_id is geometric -> meaning is inferred_label.
\echo   [2]->[3] KNN: plan flips to "Index Scan using idx_case_embedding_hnsw"
\echo        and time drops. (Small note sets are already fast without it.)
\echo   [5]  cluster join: NO Motion (account_features & kmeans_assignments are
\echo        both DISTRIBUTED BY account_id -> local join). Filter on inferred_label.
\echo   [6]  hybrid: tiny flagged set + ANN lookup -> sub-second.
\echo ════════════════════════════════════════════════════════════════════════════
