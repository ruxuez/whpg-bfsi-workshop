-- ═══════════════════════════════════════════════════════════════════════════════
-- 09_perf_fixes.sql  —  fix the slow Lab 1 watchlist-join plan
--
-- Symptom (EXPLAIN ANALYZE of query 1A on ~13M transactions):
--   * Broadcast Motion 4:4 re-sends ALL 13M transactions to every segment  (~13 s)
--   * Nested Loop probes the watchlist GiST index 13M times
--   * GroupAggregate sort spills to disk (external merge, ~336 MB)
--   * enable_seqscan is OFF in the session (the 1e10 "disable_cost" on every
--     Seq Scan in the plan), which distorts planning
--
-- Root cause: fraud_watchlists / bin_ranges were DISTRIBUTED BY a hash key, so the
-- range-containment join (t.card_bin <@ w.bin_range) can't be co-located and the
-- planner broadcasts the BIG fact table instead of the tiny lookup table.
--
-- Run this against an already-loaded database (no reload needed):
--     psql -d bank -f 09_perf_fixes.sql
-- (01_schema.sql already ships these tables as REPLICATED for fresh loads.)
-- ═══════════════════════════════════════════════════════════════════════════════

SET search_path TO bfsi_demo, public;

-- 1) Replicate the small lookup tables  → join becomes LOCAL on every segment,
--    no Broadcast Motion of the 13M-row fact table. This is the big win.
ALTER TABLE fraud_watchlists SET DISTRIBUTED REPLICATED;
ALTER TABLE bin_ranges       SET DISTRIBUTED REPLICATED;
ALTER TABLE country_risk     SET DISTRIBUTED REPLICATED;
ALTER TABLE regions          SET DISTRIBUTED REPLICATED;

-- 2) Refresh statistics. The plan's row estimates were wildly off
--    (est. 7.4M join rows vs 2M actual, 285k groups vs 300 actual) → stale stats.
ANALYZE transactions;
ANALYZE fraud_watchlists;
ANALYZE bin_ranges;
ANALYZE country_risk;

-- 3) Re-enable sequential scans (the plan shows them disabled) and give the
--    aggregate enough memory so the sort stays in RAM instead of spilling.
--    These are SESSION settings — set them before running the Lab 1 queries,
--    or set them at the role level with ALTER ROLE.
SET enable_seqscan = on;
SET statement_mem = '1GB';     -- covers the ~662 MB the sort wanted

-- ── Verify the new plan ──────────────────────────────────────────────────────
-- After the above, EXPLAIN ANALYZE of 1A should show, per segment and in
-- parallel, NO Broadcast Motion:
--     Seq Scan on transactions (local ~3.25M rows)
--       -> Nested Loop
--            -> Index Scan using idx_watchlist_gist on fraud_watchlists (local)
-- i.e. each segment joins only its own slice of transactions against a local
-- copy of the watchlist. Expect a drop from ~34 s to a few seconds.

EXPLAIN (ANALYZE, VERBOSE)
SELECT t.card_bin, w.feed_name, w.category, w.confidence,
    COUNT(*) AS hit_count, ROUND(SUM(t.amount),2) AS total_amount,
    MIN(t.ts) AS first_seen, MAX(t.ts) AS last_seen
FROM transactions t
JOIN fraud_watchlists w ON t.card_bin <@ w.bin_range
WHERE t.ts >= '2026-06-01'::timestamp
  AND w.active = TRUE AND w.confidence >= 80
GROUP BY 1, 2, 3, 4
ORDER BY hit_count DESC
LIMIT 20;


-- ═══════════════════════════════════════════════════════════════════════════════
-- OPTIONAL — remove the Nested Loop entirely with a CTE (range -> equality join)
--
-- WHY the nested loop is there: `t.card_bin <@ w.bin_range` is a RANGE-CONTAINMENT
-- predicate. Hash joins and merge joins need an EQUALITY key, so a containment join
-- can only be served by a nested loop driving the GiST index. That loop is correct
-- and idiomatic — the slowness was the Broadcast Motion (distribution), not the loop.
--
-- If you want maximum throughput anyway: the watchlist BIN bands are narrow, so we
-- can EXPAND them into their discrete BIN values in a CTE and join on equality.
-- The planner then builds a tiny hash table from the expanded set and scans the
-- 13M-row fact table ONCE — Hash Join, no nested loop, no 13M index probes. In
-- Greenplum the small hash side is broadcast (a few KB), so this also sidesteps the
-- big-table broadcast even without replicating the lookup table.
--
-- Tradeoff: this only works while the ranges are small enough to expand (here the
-- widest band is 10 000 BINs → a few-hundred-to-low-thousands-row hash table). For a
-- workshop, it also trades away the "one native <@ operator" teaching point. Keep
-- the containment form for the demo narrative; use this when you need the speed.

WITH watch_bins AS (
    SELECT w.feed_name, w.category, w.confidence,
           generate_series(lower(w.bin_range), upper(w.bin_range) - 1) AS card_bin
    FROM fraud_watchlists w
    WHERE w.active = TRUE
      AND w.confidence >= 80
      AND w.bin_range IS NOT NULL
      AND NOT isempty(w.bin_range)            -- guard against empty / unbounded ranges
)
SELECT t.card_bin, wb.feed_name, wb.category, wb.confidence,
       COUNT(*)                 AS hit_count,
       ROUND(SUM(t.amount), 2)  AS total_amount,
       MIN(t.ts)                AS first_seen,
       MAX(t.ts)                AS last_seen
FROM transactions t
JOIN watch_bins wb ON t.card_bin = wb.card_bin     -- EQUALITY -> hash join
WHERE t.ts >= '2026-06-01'::timestamp
GROUP BY 1, 2, 3, 4
ORDER BY hit_count DESC
LIMIT 20;
-- Verified: byte-for-byte identical results to the <@ containment version.


-- ═══════════════════════════════════════════════════════════════════════════════
-- LAB 3 — perf findings from a full-cluster run of 10_lab3_perf_validation.sql
-- ═══════════════════════════════════════════════════════════════════════════════
-- GOOD: MADlib kmeans assignment (closest_column) ~90 ms; HNSW KNN ~6 ms (21x vs
--       seq scan); co-located cluster join fine.
--
-- ISSUE 1 — account_features build took ~101 s. Three causes:
--   (a) enable_seqscan = 'off' is set CLUSTER-WIDE (visible in every plan's
--       "Settings:" line). It adds a 1e10 disable penalty to every Seq Scan and
--       badly distorts the aggregate plan. Find and remove it:
--           SHOW enable_seqscan;
--           SELECT setrole::regrole, setconfig FROM pg_db_role_setting;   -- look for it
--       Then make it stick (pick the one that applies):
--           ALTER ROLE gpadmin RESET enable_seqscan;          -- if set on the role
--           ALTER DATABASE bank RESET enable_seqscan;         -- if set on the db
--       -- or remove from postgresql.conf and: SELECT pg_reload_conf();
--   (b) The build spilled ~800 MB (HashAgg wanted ~2.3 GB). Give it memory:
--           SET statement_mem = '2GB';   -- now set at the top of 06
--   (c) Two count(DISTINCT) over MILLIONS of singleton accounts (most have <5
--       txns and get thrown away at HAVING). 06 now uses a TWO-PASS build: find
--       accounts with >= 5 txns first (plain count), then run the DISTINCT
--       aggregates only on those. Identical output, far less TupleSplit + spill.
--
-- ISSUE 2 — hybrid query [6] returned 0 rows. Cause: it filtered a.cluster_id >= 2,
--   but MADlib assigns ARBITRARY geometric ids, so ">= 2" caught the wrong
--   clusters. Fixed to filter a.inferred_label <> 'NORMAL' (both in 07 and in the
--   [6] validation query). After the fix it correctly surfaces the STRUCTURING
--   accounts matched to sub-threshold-wire case notes.
--
-- Re-run order after pulling the updated scripts:
--   psql -d bank -f 06_lab3_ai_analytics.sql      -- two-pass features (+ memory)
--   psql -d bank -f 07_kmeans_fallback.sql        -- MADlib k-means (BLOCK A)
--   psql -d bank -f 08_add_diverse_narratives.sql
--   psql -d bank -f 10_lab3_perf_validation.sql   -- should show [1] fast, [6] rows>0
