-- ═══════════════════════════════════════════════════════════════════════════════
-- Lab 3: Hybrid Fraud Discovery — pgvector + MADlib on Persona-Based Data
-- ═══════════════════════════════════════════════════════════════════════════════
-- The dataset carries four behavioural personas:
--   • NORMAL        (~62%) — baseline retail spend
--   • CARD-TESTING  (~15%) — micro-auths across MANY merchants  (high entropy)
--   • BUST-OUT      (~10%) — huge spend, FEW merchants          (low entropy)
--   • STRUCTURING   (~13%) — near-identical sub-threshold wires  (low amount_cv)
--
-- INVESTIGATION WORKFLOW
--   Step A  BEHAVIOURAL CLUSTERING — MADlib K-Means groups accounts by
--           (total_amount, distinct_merchants, entropy, amount_cv). Students
--           identify the STRUCTURING cluster (low variance) and BUST-OUT
--           cluster (huge spend, low entropy).
--   Step B  SEMANTIC DEEP DIVE — pick a flagged account → pgvector finds case
--           notes about "sub-threshold outbound movement" via cosine similarity.
--   Step C  THE AHA! MOMENT — LIKE '%structuring%' finds nothing. Vector search
--           finds "Repeated outbound transfers just under reporting threshold",
--           "Series of near-identical 9k wires", "Round-tripping funds" —
--           intent-based retrieval, not keyword matching.
--
-- Run order:
--   01_schema → 02_seed_reference → 03_seed_traffic_with_personas
--   → 06_lab3_ai_analytics → 07_kmeans_fallback → 08_add_diverse_narratives
-- ═══════════════════════════════════════════════════════════════════════════════
SET search_path TO bfsi_demo, public;

-- Idempotent cleanup
DROP TABLE IF EXISTS bfsi_demo.kmeans_assignments;
DROP TABLE IF EXISTS bfsi_demo.account_features_norm;
DROP TABLE IF EXISTS bfsi_demo.account_features;
DROP TABLE IF EXISTS bfsi_demo.case_embeddings;
DROP INDEX  IF EXISTS bfsi_demo.idx_case_embedding_hnsw;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PART A: pgvector — Semantic Search on Case / SAR Narratives
-- ═══════════════════════════════════════════════════════════════════════════════
-- Goal: an analyst searches for "sub-threshold outbound movement" and finds the
-- structuring case notes even though none contain the word "structuring".
--
-- In production: embed narratives with a sentence-transformer (e.g.
-- all-MiniLM-L6-v2, 384-dim) via Python, then INSERT. For the workshop we derive
-- a 32-dim feature vector from narrative characteristics — enough to teach the
-- cosine-similarity concept clearly.
-- ═══════════════════════════════════════════════════════════════════════════════

-- This script builds the heavy account_features aggregate; give it memory and
-- let the planner use sequential scans (a cluster-wide enable_seqscan=off badly
-- distorts the aggregate plan and forces disk spills).
SET enable_seqscan = on;
SET statement_mem = '1500MB';

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE bfsi_demo.case_embeddings (
    note_id    BIGINT,
    account_id BIGINT,
    queue      TEXT,
    narrative  TEXT,
    severity   INT,
    persona    TEXT,          -- normal | card_testing | bust_out | structuring
    embedding  vector(32)
) DISTRIBUTED BY (note_id);

-- 32-dim feature layout:
--  [0] severity normalized
--  [1] queue=cards-fraud  [2] queue=aml-tm  [3] queue=sanctions  [4] queue=disputes
--  [5] transfer/wire/remittance/outbound       -> STRUCTURING
--  [6] threshold / under 10k / 9k / just under  -> STRUCTURING
--  [7] round-trip/layer/corridor/overseas       -> STRUCTURING
--  [8] maxed/credit line/drew down/utilisation  -> BUST-OUT
--  [9] high-value/large-ticket/jewellery/electronics -> BUST-OUT
--  [10] NSF/returned/written off/missed payment -> BUST-OUT
--  [11] sub-dollar/micro/1.00/tiny auth         -> CARD-TESTING
--  [12] decline/verification charge             -> CARD-TESTING
--  [13] many merchants/hundreds/MID/velocity    -> CARD-TESTING
--  [14] beneficiary/payee/watch                 -> AML
--  [15] high-risk/Brazil/Nigeria/corridor       -> AML geo
--  [16] dispute/chargeback/represent            -> disputes
--  [17] KYC/refresh/documents                   -> normal
--  [18] routine/verified/no action              -> normal
--  [19] standing order/scheduled                -> normal
--  [20] severity<=1  [21] severity=3  [22] severity=4
--  [23] queue=disputes flag
--  [24-31] reserved / noise padding

INSERT INTO bfsi_demo.case_embeddings (note_id, account_id, queue, narrative, severity, persona, embedding)
SELECT note_id, account_id, queue, narrative, severity, persona, embedding
FROM (
  SELECT
    note_id, account_id, coalesce(queue,'unknown') AS queue,
    left(narrative, 300) AS narrative, severity,
    CASE
      WHEN narrative ILIKE '%transfer%' OR narrative ILIKE '%wire%' OR narrative ILIKE '%remittance%'
        OR narrative ILIKE '%threshold%' OR narrative ILIKE '%round-trip%' OR narrative ILIKE '%layered%'
      THEN 'structuring'
      WHEN narrative ILIKE '%credit line%' OR narrative ILIKE '%maxed%' OR narrative ILIKE '%drew down%'
        OR narrative ILIKE '%high-value%' OR narrative ILIKE '%NSF%' OR narrative ILIKE '%utilisation%'
      THEN 'bust_out'
      WHEN narrative ILIKE '%authorisation%' OR narrative ILIKE '%micro%' OR narrative ILIKE '%decline%'
        OR narrative ILIKE '%verification charge%' OR narrative ILIKE '%merchants%' OR narrative ILIKE '%MID%'
      THEN 'card_testing'
      ELSE 'normal'
    END AS persona,
    ARRAY[
      severity::float / 5.0,
      CASE WHEN queue = 'cards-fraud' THEN 1.0 ELSE 0.0 END,
      CASE WHEN queue = 'aml-tm' THEN 1.0 ELSE 0.0 END,
      CASE WHEN queue = 'sanctions' THEN 1.0 ELSE 0.0 END,
      CASE WHEN queue = 'disputes' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%transfer%' OR narrative ILIKE '%wire%' OR narrative ILIKE '%remittance%' OR narrative ILIKE '%outbound%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%threshold%' OR narrative ILIKE '%under%' OR narrative ILIKE '%9k%' OR narrative ILIKE '%sub-10k%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%round-trip%' OR narrative ILIKE '%layer%' OR narrative ILIKE '%corridor%' OR narrative ILIKE '%overseas%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%credit line%' OR narrative ILIKE '%maxed%' OR narrative ILIKE '%drew down%' OR narrative ILIKE '%utilisation%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%high-value%' OR narrative ILIKE '%large-ticket%' OR narrative ILIKE '%jewellery%' OR narrative ILIKE '%electronics%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%NSF%' OR narrative ILIKE '%returned%' OR narrative ILIKE '%written off%' OR narrative ILIKE '%missed%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%sub-dollar%' OR narrative ILIKE '%micro%' OR narrative ILIKE '%1.00%' OR narrative ILIKE '%tiny%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%decline%' OR narrative ILIKE '%verification%' OR narrative ILIKE '%authorisation%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%merchants%' OR narrative ILIKE '%hundreds%' OR narrative ILIKE '%MID%' OR narrative ILIKE '%velocity%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%beneficiary%' OR narrative ILIKE '%payee%' OR narrative ILIKE '%watch%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%high-risk%' OR narrative ILIKE '%Brazil%' OR narrative ILIKE '%Nigeria%' OR narrative ILIKE '%corridor%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%dispute%' OR narrative ILIKE '%chargeback%' OR narrative ILIKE '%represent%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%KYC%' OR narrative ILIKE '%refresh%' OR narrative ILIKE '%documents%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%routine%' OR narrative ILIKE '%verified%' OR narrative ILIKE '%no action%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN narrative ILIKE '%standing order%' OR narrative ILIKE '%scheduled%' THEN 1.0 ELSE 0.0 END,
      CASE WHEN severity <= 1 THEN 1.0 ELSE 0.0 END,
      CASE WHEN severity = 3 THEN 1.0 ELSE 0.0 END,
      CASE WHEN severity = 4 THEN 1.0 ELSE 0.0 END,
      CASE WHEN queue = 'disputes' THEN 1.0 ELSE 0.0 END,
      random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05, random()*0.05
    ]::vector(32) AS embedding
  FROM bfsi_demo.case_narratives
  WHERE ts > '2026-06-01'::timestamp
) sub
WHERE persona <> 'normal' OR (persona = 'normal' AND note_id % 10 = 0)
LIMIT 200000;

-- HNSW index for fast ANN search (uncomment if pgvector >= 0.5 installed)
-- CREATE INDEX idx_case_embedding_hnsw
-- ON bfsi_demo.case_embeddings
-- USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

ANALYZE bfsi_demo.case_embeddings;

SELECT persona, count(*) AS note_count
FROM bfsi_demo.case_embeddings
GROUP BY 1 ORDER BY 2 DESC;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PART B: Per-Account Behavioural Features (input to K-Means)
-- ═══════════════════════════════════════════════════════════════════════════════
-- SIX dimensions designed for persona separation:
--   txn_count          : activity volume      -> CARD-TESTING HIGH
--   distinct_merchants : merchant breadth      -> CARD-TESTING EXTREME, BUST-OUT LOW
--   total_amount       : spend volume          -> BUST-OUT EXTREME, CARD-TESTING TINY
--   merchant_entropy   : distinct/count        -> CARD-TESTING HIGH, BUST-OUT/STRUCT LOW
--   mcc_spread         : distinct mcc / count   -> CARD-TESTING HIGH
--   amount_cv          : stddev/avg of amount   -> STRUCTURING VERY LOW (< 0.1)

-- TWO-PASS build. Most accounts have only 1-2 transactions, so a single GROUP BY
-- would build millions of singleton groups AND run two COUNT(DISTINCT)s (which
-- MPP expands via TupleSplit) over all of them, then throw 99% away at HAVING.
-- Instead: (1) cheaply find accounts with >= 5 txns (plain count, no DISTINCT),
-- (2) run the expensive DISTINCT aggregates ONLY on those accounts' rows.
-- Identical output, far less spill.
CREATE TABLE bfsi_demo.account_features AS
WITH active_accounts AS (
    SELECT account_id
    FROM bfsi_demo.transactions
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
FROM bfsi_demo.transactions t
JOIN active_accounts a USING (account_id)
WHERE t.ts > '2026-06-01'::timestamp
GROUP BY t.account_id
DISTRIBUTED BY (account_id);

ANALYZE bfsi_demo.account_features;

DO $$ BEGIN
    RAISE NOTICE 'account_features built — % accounts',
        (SELECT count(*) FROM bfsi_demo.account_features);
END $$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PART C: Hybrid Fraud Discovery (the MAIN teaching query)
-- ═══════════════════════════════════════════════════════════════════════════════
-- Run 07_kmeans_fallback.sql first to populate kmeans_assignments.

-- ── C1: Cluster-guided semantic search (THE main teaching query) ────────────
-- This query joins the flagged clusters (from 07) to the case notes whose
-- MEANING matches "sub-threshold outbound movement". Because it depends on
-- kmeans_assignments, the live, executable version runs at the END of
-- 07_kmeans_fallback.sql (where the clusters already exist). See it there.


-- ── C2: The "Aha!" contrast — LIKE vs vector search ──────────────────────────
-- Naive keyword search returns ~0 rows: the notes never say "structuring".
--
-- SELECT left(narrative,120) AS narrative, queue, severity
-- FROM bfsi_demo.case_embeddings
-- WHERE narrative ILIKE '%structuring%' OR narrative ILIKE '%money laundering%'
-- LIMIT 20;
-- → vector search above finds "near-identical 9k wires", "round-tripping funds",
--   "transfers just under reporting threshold" instead.


-- ── C3: Z-Score confirmation ─────────────────────────────────────────────────
-- WITH stats AS (
--   SELECT avg(distinct_merchants) mu_m, stddev_samp(distinct_merchants) sd_m,
--          avg(total_amount) mu_a, stddev_samp(total_amount) sd_a,
--          avg(amount_cv) mu_cv, stddev_samp(amount_cv) sd_cv
--   FROM bfsi_demo.account_features )
-- SELECT account_id,
--   CASE WHEN (distinct_merchants-mu_m)/nullif(sd_m,0) > 4 THEN 'CARD-TESTING'
--        WHEN (total_amount-mu_a)/nullif(sd_a,0) > 4       THEN 'BUST-OUT'
--        WHEN amount_cv < 0.1                              THEN 'STRUCTURING'
--        ELSE 'SUSPECT' END AS inferred_persona
-- FROM bfsi_demo.account_features, stats
-- ORDER BY total_amount DESC LIMIT 20;
