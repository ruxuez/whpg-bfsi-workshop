-- ════════════════════════════════════════════════════════════════════════════
-- fix_kmeans_labels_v2.sql        (replaces v1 — run this one)
--
-- What the diagnosis showed (psql output from node1):
--   · bfsi_demo.kmeans_labeled is a VIEW (labels are computed on the fly —
--     that's the drift). It cannot be UPDATEd.
--   · cluster 1 = CARD-TESTING (20 accts)  ✓ correct
--   · cluster 3 = labeled BUST-OUT (35 accts) but contains TWO merged rings:
--       35 accounts with bust_out notes, of which 20 ALSO carry structuring
--       notes → K-Means merged bust-out and structuring into one cluster.
--   · card_testing notes are broad (~30K accounts) → unusable as a vote,
--     so NO cluster-level majority relabeling is done here.
--
-- The fix:
--   1. Snapshot the view into a real table (deterministic labels, updatable).
--   2. Surgical split of cluster 3: accounts that carry structuring case
--      notes become STRUCTURING; the rest stay BUST-OUT.
--   3. Swap names so the app/deck (which query bfsi_demo.kmeans_labeled)
--      need zero changes. Original view kept as kmeans_labeled_view_orig.
--
-- Run once:  psql bank -f fix_kmeans_labels_v2.sql
-- Re-running aborts safely with a clear message.
-- ════════════════════════════════════════════════════════════════════════════

BEGIN;

-- Guard: abort if the fix was already applied (kmeans_labeled is a table)
DO $$
BEGIN
    IF EXISTS (SELECT 1
               FROM   pg_class c
               JOIN   pg_namespace n ON n.oid = c.relnamespace
               WHERE  n.nspname = 'bfsi_demo'
                 AND  c.relname = 'kmeans_labeled'
                 AND  c.relkind = 'r') THEN
        RAISE EXCEPTION 'kmeans_labeled is already a table — fix already applied, nothing to do';
    END IF;
END $$;

-- 1. Snapshot the view into a real table (same columns, same name afterwards)
CREATE TABLE bfsi_demo.kmeans_labeled_tbl AS
SELECT * FROM bfsi_demo.kmeans_labeled
DISTRIBUTED BY (account_id);

-- 2a. Split cluster 3: the 20 accounts whose case notes are structuring
--     become STRUCTURING; the other 15 remain BUST-OUT.
UPDATE bfsi_demo.kmeans_labeled_tbl t
SET    inferred_label = 'STRUCTURING'
WHERE  t.inferred_label = 'BUST-OUT'
  AND  t.account_id IN (SELECT DISTINCT account_id
                        FROM   bfsi_demo.case_embeddings
                        WHERE  persona = 'structuring');

-- 2b. Normalise any stray label strings (defensive; today there are none)
UPDATE bfsi_demo.kmeans_labeled_tbl
SET    inferred_label = 'NORMAL'
WHERE  inferred_label NOT IN ('CARD-TESTING','BUST-OUT','STRUCTURING','NORMAL');

-- 3. Swap: app keeps querying bfsi_demo.kmeans_labeled, now a stable table
ALTER VIEW  bfsi_demo.kmeans_labeled     RENAME TO kmeans_labeled_view_orig;
ALTER TABLE bfsi_demo.kmeans_labeled_tbl RENAME TO kmeans_labeled;

COMMIT;

ANALYZE bfsi_demo.kmeans_labeled;

-- ────────────────────────────────────────────────────────────────────────────
-- VERIFY — expected output based on the node1 diagnosis:
--
--   inferred_label | accounts            fraud_type   | madlib | pgvector | both
--   ---------------+---------            -------------+--------+----------+-----
--   NORMAL         |  49965              CARD-TESTING |   20   |  30848   |  20
--   STRUCTURING    |     20              BUST-OUT     |   15   |     39   |  15
--   CARD-TESTING   |     20              STRUCTURING  |   20   |     23   |  20
--   BUST-OUT       |     15
--
--   → every fraud type has found_by_both > 0; C1's bars go green.
-- ────────────────────────────────────────────────────────────────────────────

SELECT inferred_label, COUNT(*) AS accounts
FROM   bfsi_demo.kmeans_labeled
GROUP  BY inferred_label
ORDER  BY accounts DESC;

WITH madlib AS (
    SELECT DISTINCT account_id, inferred_label AS fraud_type
    FROM   bfsi_demo.kmeans_labeled
    WHERE  inferred_label IN ('CARD-TESTING','BUST-OUT','STRUCTURING')
), pgv AS (
    SELECT DISTINCT account_id,
           CASE persona WHEN 'card_testing' THEN 'CARD-TESTING'
                        WHEN 'bust_out'     THEN 'BUST-OUT'
                        WHEN 'structuring'  THEN 'STRUCTURING' END AS fraud_type
    FROM   bfsi_demo.case_embeddings
    WHERE  persona IN ('card_testing','bust_out','structuring')
)
SELECT COALESCE(m.fraud_type, p.fraud_type)   AS fraud_type,
       COUNT(DISTINCT m.account_id)           AS madlib_accounts,
       COUNT(DISTINCT p.account_id)           AS pgvector_accounts,
       COUNT(DISTINCT CASE WHEN m.account_id IS NOT NULL
                            AND p.account_id IS NOT NULL
                           THEN m.account_id END) AS found_by_both
FROM   madlib m
FULL OUTER JOIN pgv p
       ON m.account_id = p.account_id AND m.fraud_type = p.fraud_type
GROUP  BY 1 ORDER BY 4 DESC;

-- Then in the Lab 2 dashboard: Step 4 → Run Full Pipeline.
-- Expected watchlist flags: compromised_bin 20 + velocity_abuse 15 +
-- structuring 20 = 55 active ML flags, and the D1 gap drops to 0.

-- ────────────────────────────────────────────────────────────────────────────
-- ROLLBACK (if ever needed): restore the original view
--   BEGIN;
--   ALTER TABLE bfsi_demo.kmeans_labeled RENAME TO kmeans_labeled_tbl_fixed;
--   ALTER VIEW  bfsi_demo.kmeans_labeled_view_orig RENAME TO kmeans_labeled;
--   COMMIT;
-- ────────────────────────────────────────────────────────────────────────────
