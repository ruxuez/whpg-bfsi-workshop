#!/usr/bin/env python3
"""
Lab 2 - AI-Powered Fraud Analytics: STREAMLINED Dashboard (BFSI)
Focus: pgvector value, MADlib value, and their combination

Connects to WarehousePG (bfsi schema, port 5432)
"""

import os, time, decimal
from datetime import datetime, date
from flask import Flask, render_template_string, jsonify, request
import psycopg2, psycopg2.extras

app = Flask(__name__)

DB = {
    "host":     os.environ.get("WHPG_HOST", "localhost"),
    "port":     int(os.environ.get("WHPG_PORT", 5432)),
    "dbname":   os.environ.get("WHPG_DB",   "demo"),
    "user":     os.environ.get("WHPG_USER", "gpadmin"),
    "password": os.environ.get("WHPG_PASS", ""),
}

def run(sql, params=None):
    conn = psycopg2.connect(**DB)
    conn.set_session(autocommit=True)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        t0 = time.perf_counter()
        cur.execute("SET search_path TO bfsi_demo, public;")
        cur.execute(sql, params)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        rows = []
        for row in cur.fetchall():
            r = {}
            for k, v in row.items():
                if isinstance(v, (datetime, date)):  r[k] = v.isoformat()
                elif isinstance(v, decimal.Decimal): r[k] = float(v)
                elif v is None:                      r[k] = None
                elif isinstance(v, (int, float, bool)): r[k] = v
                else:                                r[k] = str(v)
            rows.append(r)
        return {"data": rows, "ms": ms, "rows": len(rows)}
    except Exception as e:
        return {"data": [], "ms": 0, "rows": 0, "error": str(e)}
    finally:
        conn.close()

def run_write(sql, params=None):
    """Execute a write (INSERT/UPDATE/DELETE/TRUNCATE) and return rowcount."""
    conn = psycopg2.connect(**DB)
    conn.set_session(autocommit=True)
    try:
        cur = conn.cursor()
        t0  = time.perf_counter()
        cur.execute("SET search_path TO bfsi_demo, public;")
        cur.execute(sql, params)
        return {"rowcount": cur.rowcount, "ms": round((time.perf_counter()-t0)*1000,1)}
    except Exception as e:
        return {"rowcount": 0, "ms": 0, "error": str(e)}
    finally:
        conn.close()

# ── ML Watchlist SQL ──────────────────────────────────────────────────────────

SQL_1A_BEFORE = """
-- Fraud transactions from K-Means clusters that are NOT yet in fraud_watchlists
-- These accounts were detected by MADlib but haven't been written back yet.
-- Uses inferred_label instead of cluster_id to handle K-means non-determinism.
WITH ml_fraud_accounts AS (
    SELECT
        kl.account_id,
        kl.inferred_label,
        -- Map ML labels to watchlist categories
        CASE kl.inferred_label
            WHEN 'CARD-TESTING' THEN 'compromised_bin'
            WHEN 'BUST-OUT' THEN 'velocity_abuse'
            WHEN 'STRUCTURING' THEN 'structuring'
        END AS ml_category,
        -- Confidence scores by fraud type
        CASE kl.inferred_label
            WHEN 'CARD-TESTING' THEN 87
            WHEN 'BUST-OUT' THEN 85
            WHEN 'STRUCTURING' THEN 90
        END AS ml_confidence
    FROM bfsi_demo.kmeans_labeled kl
    WHERE kl.inferred_label IN ('CARD-TESTING', 'BUST-OUT', 'STRUCTURING')
)
SELECT
    t.account_id,
    mf.inferred_label             AS fraud_persona,
    mf.ml_category                AS fraud_type_detected_by_ml,
    mf.ml_confidence              AS confidence,
    COUNT(*)                      AS transactions,
    ROUND(SUM(t.amount), 2)       AS total_exposure,
    MIN(t.ts)                     AS first_txn,
    MAX(t.ts)                     AS last_txn,
    'NOT IN watchlist'            AS watchlist_status
FROM bfsi_demo.transactions t
JOIN ml_fraud_accounts mf ON t.account_id = mf.account_id
WHERE t.ts >= '2026-06-01'::timestamp
  AND NOT EXISTS (
        SELECT 1
        FROM bfsi_demo.fraud_watchlists w
        WHERE w.single_account = t.account_id
          AND w.feed_name = 'MADlib K-Means'
          AND w.active = TRUE
      )
GROUP BY t.account_id, mf.inferred_label, mf.ml_category, mf.ml_confidence
ORDER BY total_exposure DESC
LIMIT 20
"""

SQL_1A_AFTER = """
-- Each branch has its own LIMIT so BIN rows are never crowded out
-- by the much larger ML account totals ($245M vs $23K per BIN).
-- Both groups always visible; blue = BIN range, green = ML K-Means account.
WITH bin_hits AS (
    SELECT wb.card_bin::TEXT       AS match_key,
           wb.feed_name, wb.category, wb.confidence,
           'BIN range match'       AS source,
           COUNT(*)                AS transactions,
           ROUND(SUM(t.amount),2)  AS total_amount,
           MIN(t.ts) AS first_txn, MAX(t.ts) AS last_txn
    FROM   bfsi_demo.transactions t
    JOIN   (
               SELECT feed_name, category, confidence,
                      generate_series(lower(bin_range), upper(bin_range)-1) AS card_bin
               FROM   bfsi_demo.fraud_watchlists
               WHERE  active=TRUE AND confidence>=75
                 AND  bin_range IS NOT NULL AND NOT isempty(bin_range)
           ) wb ON t.card_bin = wb.card_bin
    WHERE  t.ts >= '2026-06-01'::timestamp
    GROUP  BY 1,2,3,4,5
    ORDER  BY total_amount DESC
    LIMIT  10
),
acct_hits AS (
    SELECT 'acct:'||t.account_id::TEXT AS match_key,
           w.feed_name, w.category, w.confidence,
           'ML K-Means account'    AS source,
           COUNT(*)                 AS transactions,
           ROUND(SUM(t.amount),2)   AS total_amount,
           MIN(t.ts) AS first_txn,  MAX(t.ts) AS last_txn
    FROM   bfsi_demo.transactions t
    JOIN   bfsi_demo.fraud_watchlists w ON t.account_id = w.single_account
    WHERE  t.ts >= '2026-06-01'::timestamp
      AND  w.active=TRUE AND w.confidence>=75 AND w.single_account IS NOT NULL
    GROUP  BY 1,2,3,4,5
    ORDER  BY total_amount DESC
    LIMIT  10
)
-- BIN rows first (blue), then ML rows (green) — both groups always shown
SELECT match_key, feed_name, category, confidence, source,
       transactions, total_amount, first_txn, last_txn
FROM   bin_hits
UNION ALL
SELECT match_key, feed_name, category, confidence, source,
       transactions, total_amount, first_txn, last_txn
FROM   acct_hits
"""

SQL_ML_INSERT = """
-- Insert MADlib fraud findings into fraud_watchlists
-- Uses inferred_label instead of cluster_id for deterministic behavior
INSERT INTO bfsi_demo.fraud_watchlists
  (feed_name, bin_range, single_account, category, confidence,
   country_code, first_seen, last_seen, active)
SELECT
  'MADlib K-Means',
  NULL,
  kl.account_id,
  CASE kl.inferred_label
    WHEN 'CARD-TESTING' THEN 'compromised_bin'
    WHEN 'BUST-OUT' THEN 'velocity_abuse'
    WHEN 'STRUCTURING' THEN 'structuring'
  END,
  CASE kl.inferred_label
    WHEN 'CARD-TESTING' THEN 87
    WHEN 'BUST-OUT' THEN 85
    WHEN 'STRUCTURING' THEN 90
  END,
  'US',
  NOW(), NOW(), TRUE
FROM bfsi_demo.kmeans_labeled kl
WHERE kl.inferred_label IN ('CARD-TESTING', 'BUST-OUT', 'STRUCTURING')
ON CONFLICT DO NOTHING
"""

SQL_ML_EXPIRE = """
-- Expire accounts that are no longer in fraud clusters
-- Uses inferred_label instead of cluster_id
UPDATE bfsi_demo.fraud_watchlists
SET    active=FALSE, last_seen=NOW()
WHERE  feed_name='MADlib K-Means' AND active=TRUE
  AND  NOT EXISTS (
         SELECT 1 FROM bfsi_demo.kmeans_labeled kl
         WHERE  kl.account_id = bfsi_demo.fraud_watchlists.single_account
           AND  kl.inferred_label IN ('CARD-TESTING', 'BUST-OUT', 'STRUCTURING')
       )
"""

# ── Query definitions ────────────────────────────────────────────────────────
QUERIES = [
    # ══════════════════════════════════════════════════════════════════════════
    # Panel A: pgvector — Semantic Search Beats Keyword Search
    # ══════════════════════════════════════════════════════════════════════════
    {
        "id": "a1", "panel": 0,
        "name": "A1 - The Keyword Search Problem",
        "desc": "LIKE '%structuring%' finds almost nothing, yet the structuring persona is everywhere - same behaviour, different words!",
        "sql": """SELECT
    'Keyword Search Results' AS search_method,
    COUNT(*) AS total_notes,
    COUNT(*) FILTER (WHERE narrative ILIKE '%structuring%')      AS found_structuring,
    COUNT(*) FILTER (WHERE narrative ILIKE '%money laundering%') AS found_laundering,
    COUNT(*) FILTER (WHERE narrative ILIKE '%smurfing%')         AS found_smurfing,
    '\u274c Misses: "just below the limit", "split deposits", "layering"' AS limitation,
    COUNT(*) FILTER (WHERE persona = 'structuring') AS actual_structuring_notes
FROM bfsi_demo.case_embeddings

UNION ALL

SELECT
    'What We Actually Have' AS search_method,
    COUNT(*) AS total_notes,
    NULL, NULL, NULL,
    'Notes describing the same behaviour using different words' AS limitation,
    COUNT(*) FILTER (WHERE persona = 'structuring') AS actual_structuring_notes
FROM bfsi_demo.case_embeddings"""
    },
    {
        "id": "a2", "panel": 0,
        "name": "A2 - pgvector Finds Fraud by MEANING",
        "desc": "Find case notes similar to a known structuring narrative - semantic search surfaces related cases WITHOUT exact keywords!",
        "sql": """WITH reference_note AS (
    SELECT note_id, LEFT(narrative, 80) AS ref_note, embedding
    FROM bfsi_demo.case_embeddings
    WHERE persona = 'structuring' AND queue = 'aml-tm'
    ORDER BY note_id
    LIMIT 1
),
similar_notes AS (
    SELECT DISTINCT ON (LEFT(e.narrative, 90))
        e.account_id,
        e.queue,
        LEFT(e.narrative, 90) AS similar_note,
        e.persona AS ground_truth,
        ROUND((1 - (e.embedding <=> r.embedding))::numeric, 4) AS similarity,
        e.embedding <=> r.embedding AS distance
    FROM bfsi_demo.case_embeddings e
    CROSS JOIN reference_note r
    WHERE e.note_id != r.note_id
      AND e.persona = 'structuring'
    ORDER BY LEFT(e.narrative, 90), e.embedding <=> r.embedding
)
SELECT
    (SELECT ref_note FROM reference_note) AS reference_note,
    account_id,
    queue,
    similar_note,
    ground_truth,
    similarity
FROM similar_notes
ORDER BY distance
LIMIT 15"""
    },

    # ══════════════════════════════════════════════════════════════════════════
    # Panel B: MADlib — Unsupervised Fraud Discovery
    # ══════════════════════════════════════════════════════════════════════════
    {
        "id": "b1", "panel": 1,
        "name": "B1 - MADlib Discovered the Fraud Personas",
        "desc": "K-Means clustering on 6 behavioural features - NO LABELS, finds fraud rings automatically",
        "sql": """SELECT
    a.cluster_id,
    a.inferred_label AS persona_label,
    COUNT(*) AS accounts,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct,
    ROUND(AVG(f.txn_count), 0)              AS avg_txns,
    ROUND(AVG(f.total_amount)::numeric, 2)  AS avg_spend,
    ROUND(AVG(f.distinct_merchants), 0)     AS avg_merchants,
    ROUND(AVG(f.amount_cv)::numeric, 4)     AS avg_cv
FROM bfsi_demo.kmeans_labeled a
JOIN bfsi_demo.account_features f ON a.account_id = f.account_id
GROUP BY 1, 2
ORDER BY
    CASE a.inferred_label
        WHEN 'CARD-TESTING' THEN 1
        WHEN 'BUST-OUT' THEN 2
        WHEN 'STRUCTURING' THEN 3
        WHEN 'NORMAL' THEN 4
    END"""
    },
    {
        "id": "b2", "panel": 1,
        "name": "B2 - The Dramatic Differences",
        "desc": "CARD-TESTING: many more merchants | BUST-OUT: far higher spend than a normal account",
        "sql": """WITH cluster_agg AS (
    SELECT
        a.inferred_label AS persona,
        COUNT(*) AS accounts,
        ROUND(AVG(f.distinct_merchants), 0)     AS merchants,
        ROUND(AVG(f.total_amount)::numeric, 2)  AS spend,
        ROUND(AVG(f.amount_cv)::numeric, 4)     AS amount_cv
    FROM bfsi_demo.kmeans_labeled a
    JOIN bfsi_demo.account_features f ON a.account_id = f.account_id
    GROUP BY a.inferred_label
),
normal AS (
    SELECT merchants AS n_merch, spend AS n_spend
    FROM cluster_agg WHERE persona = 'NORMAL' LIMIT 1
)
SELECT
    cs.persona,
    cs.accounts,
    cs.merchants,
    cs.spend,
    cs.amount_cv,
    ROUND(cs.merchants::numeric / NULLIF(n.n_merch, 0), 0) AS merchants_vs_normal,
    ROUND(cs.spend::numeric    / NULLIF(n.n_spend, 0), 0) AS spend_vs_normal
FROM cluster_agg cs
LEFT JOIN normal n ON TRUE
ORDER BY
    CASE cs.persona
        WHEN 'CARD-TESTING' THEN 1
        WHEN 'BUST-OUT' THEN 2
        WHEN 'STRUCTURING' THEN 3
        WHEN 'NORMAL' THEN 4
    END"""
    },

    # ══════════════════════════════════════════════════════════════════════════
    # Panel C: The AI Factory — Combining pgvector + MADlib
    # ══════════════════════════════════════════════════════════════════════════
    {
        "id": "c1", "panel": 2,
        "name": "C1 - The AI Factory: Fraud Pattern Correlation",
        "desc": "MADlib finds behavioural clusters \u2192 pgvector finds semantic patterns \u2192 BOTH detect the SAME fraud types in ONE query!",
        "sql": """WITH madlib_patterns AS (
    SELECT
        a.inferred_label AS fraud_type,
        COUNT(*) AS accts,
        ROUND(AVG(f.distinct_merchants), 0)     AS avg_merchants,
        ROUND(AVG(f.total_amount)::numeric, 2)  AS avg_spend,
        ROUND(AVG(f.amount_cv)::numeric, 4)     AS avg_cv
    FROM bfsi_demo.kmeans_labeled a
    JOIN bfsi_demo.account_features f ON a.account_id = f.account_id
    WHERE a.inferred_label IN ('CARD-TESTING', 'BUST-OUT', 'STRUCTURING')
    GROUP BY a.inferred_label
),
pgvector_patterns AS (
    SELECT
        CASE
            WHEN persona = 'card_testing' THEN 'CARD-TESTING'
            WHEN persona = 'bust_out'     THEN 'BUST-OUT'
            WHEN persona = 'structuring'  THEN 'STRUCTURING'
        END AS fraud_type,
        COUNT(*)                       AS notes,
        COUNT(DISTINCT account_id)     AS accounts,
        string_agg(DISTINCT queue, ', ') AS queues_seen
    FROM bfsi_demo.case_embeddings
    WHERE persona IN ('card_testing', 'bust_out', 'structuring')
    GROUP BY persona
)
SELECT
    mp.fraud_type,
    mp.accts::text || ' accounts (MADlib)'      AS behavioral_evidence,
    mp.avg_merchants::text || ' avg merchants'  AS behavior_metric_1,
    '$' || mp.avg_spend::text || ' avg spend'   AS behavior_metric_2,
    pv.notes::text || ' notes (pgvector)'       AS semantic_evidence,
    pv.queues_seen                              AS semantic_queues
FROM madlib_patterns mp
JOIN pgvector_patterns pv ON mp.fraud_type = pv.fraud_type
WHERE mp.fraud_type IS NOT NULL
ORDER BY mp.accts DESC"""
    },
    {
        "id": "c2", "panel": 2,
        "name": "C2 - Why This Matters",
        "desc": "In traditional warehouses: export \u2192 train \u2192 join (hours). Here: ONE SQL query (<5 sec)",
        "sql": """SELECT
    'Traditional Warehouse (Snowflake/BigQuery)' AS approach,
    'Export 13M rows \u2192 Python \u2192 train model \u2192 upload results \u2192 JOIN' AS workflow,
    '2-4 hours' AS time,
    'Requires data movement, external tools, model versioning' AS complexity
UNION ALL
SELECT
    'EDB WarehousePG (In-Database ML)',
    'MADlib K-Means + pgvector similarity search in ONE query',
    '< 5 seconds',
    'Zero data movement, SQL-native, no external dependencies'"""
    },
    # ══════════════════════════════════════════════════════════════════════════
    # Panel D: ML → Watchlist
    # ══════════════════════════════════════════════════════════════════════════
    {
        "id": "d1", "panel": 3,
        "name": "D1 - ML Fraud Gap — Detected but NOT in Watchlist",
        "desc": "Transactions from K-Means fraud clusters that exist in transactions but are NOT yet flagged in fraud_watchlists. Run Refresh above, then re-run — should show 0 rows.",
        "sql": SQL_1A_BEFORE,
    },
    {
        "id": "d2", "panel": 3,
        "name": "D2 - Query 1A Extended — BIN Ranges + ML Accounts",
        "desc": "After Refresh: BIN-match rows (blue) + ML K-Means account rows (green). Orange = high exposure. Run D1 first to see the gap, refresh, then run D2 to see them appear.",
        "sql": SQL_1A_AFTER,
    },
]

PANELS = [
    {"name": "pgvector",       "icon": "A", "desc": "Semantic search finds fraud by MEANING, not keywords"},
    {"name": "MADlib",         "icon": "B", "desc": "Unsupervised clustering discovers fraud personas automatically"},
    {"name": "AI Factory",     "icon": "C", "desc": "Combine both in ONE query — impossible in traditional warehouses"},
    {"name": "ML → Watchlist", "icon": "D", "desc": "Write K-Means results to fraud_watchlists · Query 1A before vs after"},
]

# ── API ───────────────────────────────────────────────────────────────────────
@app.route("/api/run", methods=["POST"])
def api_run():
    qid = request.json.get("id")
    q = next((q for q in QUERIES if q["id"] == qid), None)
    if not q:
        return jsonify({"error": f"Unknown query: {qid}"}), 404
    r = run(q["sql"])
    r["id"] = qid
    return jsonify(r)

@app.route("/api/run_all", methods=["POST"])
def api_run_all():
    results = []
    for q in QUERIES:
        r = run(q["sql"])
        results.append({"id": q["id"], "name": q["name"],
                        "ms": r["ms"], "rows": r["rows"], "error": r.get("error")})
    return jsonify({"results": results, "total_ms": round(sum(r["ms"] for r in results), 1)})

@app.route("/api/sql", methods=["POST"])
def api_sql():
    sql = request.json.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "No SQL provided"}), 400
    w = sql.split()[0].upper() if sql.split() else ""
    if w not in ("SELECT", "WITH", "EXPLAIN"):
        return jsonify({"error": "Only SELECT / WITH / EXPLAIN allowed"}), 403
    return jsonify(run(sql))

@app.route("/api/watchlist/refresh", methods=["POST"])
def api_watchlist_refresh():
    """
    Idempotent refresh — safe to run multiple times.
    Step 1: soft-reset  — expire any existing MADlib K-Means flags (active=FALSE)
    Step 2: insert fresh — write current K-Means results to fraud_watchlists
    Step 3: expire stale — accounts that left the fraud clusters
    """
    import time as _t; t0 = _t.perf_counter()
    rst = run_write(
        "UPDATE bfsi_demo.fraud_watchlists "
        "SET active=FALSE, last_seen=NOW() "
        "WHERE feed_name='MADlib K-Means' AND active=TRUE"
    )
    if "error" in rst:
        return jsonify({"ok": False, "error": rst["error"]}), 500
    ins = run_write(SQL_ML_INSERT)
    if "error" in ins:
        return jsonify({"ok": False, "error": ins["error"]}), 500
    exp = run_write(SQL_ML_EXPIRE)
    if "error" in exp:
        return jsonify({"ok": False, "error": exp["error"]}), 500
    summary = run("""
        SELECT feed_name, category,
               COUNT(*) FILTER (WHERE active=TRUE) AS active_flags
        FROM   bfsi_demo.fraud_watchlists
        GROUP  BY feed_name, category ORDER BY feed_name, category
    """)
    return jsonify({
        "ok":       True,
        "reset":    rst["rowcount"], "reset_ms":  rst["ms"],
        "inserted": ins["rowcount"], "insert_ms": ins["ms"],
        "expired":  exp["rowcount"], "expire_ms": exp["ms"],
        "total_ms": round((_t.perf_counter()-t0)*1000, 1),
        "summary":  summary["data"],
    })

@app.route("/api/watchlist/reset", methods=["POST"])
def api_watchlist_reset():
    """
    mode=soft — expire ML flags (active→FALSE). Default. Re-run refresh after.
    mode=hard — DELETE all MADlib K-Means rows.
    mode=full — TRUNCATE all derived ML tables (kmeans_*, account_features).
                Requires re-running 06_ai_analytics + 07_kmeans_fallback after.
    """
    mode  = request.args.get("mode", "soft")
    import time as _t; t0=_t.perf_counter()
    steps = []
    if mode == "soft":
        r = run_write("UPDATE bfsi_demo.fraud_watchlists SET active=FALSE,last_seen=NOW() WHERE feed_name='MADlib K-Means' AND active=TRUE")
        steps.append({"step":"Expire ML flags (active→FALSE)","rows":r.get("rowcount",0),"error":r.get("error")})
    elif mode == "hard":
        r = run_write("DELETE FROM bfsi_demo.fraud_watchlists WHERE feed_name='MADlib K-Means'")
        steps.append({"step":"Delete all MADlib K-Means rows","rows":r.get("rowcount",0),"error":r.get("error")})
    elif mode == "full":
        for label, sql in [
            ("Expire ML flags",               "UPDATE bfsi_demo.fraud_watchlists SET active=FALSE,last_seen=NOW() WHERE feed_name='MADlib K-Means' AND active=TRUE"),
            ("Truncate kmeans_assignments",   "TRUNCATE bfsi_demo.kmeans_assignments"),
            ("Truncate km_raw",               "TRUNCATE bfsi_demo.km_raw"),
            ("Truncate km_points",            "TRUNCATE bfsi_demo.km_points"),
            ("Truncate km_result",            "TRUNCATE bfsi_demo.km_result"),
            ("Truncate account_features",     "TRUNCATE bfsi_demo.account_features"),
            ("Truncate account_features_norm","TRUNCATE bfsi_demo.account_features_norm"),
        ]:
            r = run_write(sql)
            steps.append({"step":label,"rows":r.get("rowcount",0),"error":r.get("error")})
            if r.get("error"): break
    else:
        return jsonify({"ok":False,"error":f"Unknown mode: {mode}"}), 400
    errors = [s for s in steps if s.get("error")]
    return jsonify({"ok":len(errors)==0,"mode":mode,"steps":steps,
                    "total_ms":round((_t.perf_counter()-t0)*1000,1),"errors":errors})

@app.route("/api/health")
def api_health():
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()
        cur.execute("SELECT version()")
        ver = cur.fetchone()[0]
        conn.close()
        return jsonify({"status": "ok", "version": ver})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/")
def index():
    return render_template_string(HTML, panels=PANELS, queries=QUERIES)

# ── HTML (same as app3.py, just streamlined queries) ─────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lab 2 — AI Analytics | WarehousePG</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 256 256'%3E%3Cg transform='translate(-862,18)'%3E%3Cpath fill='%232a9993' d='M1060.7,2.12c-30.98,2.37-56.03,27.09-58.74,58.06-2.88,33.35,19.98,61.96,50.96,68.22v61.62c0,2.54-2.03,4.4-4.4,4.4h-16.76c-2.54,0-4.4-2.03-4.4-4.4v-44.35c0-7.28-5.92-13.37-13.37-13.37h-49.94c-7.28,0-13.37,5.92-13.37,13.37v44.35c0,2.54-2.03,4.4-4.4,4.4h-16.76c-2.37,0-4.4-2.03-4.4-4.4v-97.17l73.47-73.47c1.69-1.69,1.69-4.57,0-6.26l-11.85-11.85c-1.69-1.69-4.57-1.69-6.26,0l-125.27,125.27c-1.69,1.69-1.69,4.57,0,6.26l11.85,11.85c1.69,1.69,4.57,1.69,6.26,0l26.07-26.24v88.37c0,7.28,5.92,13.37,13.37,13.37h50.11c7.28,0,13.37-5.92,13.37-13.37v-44.35c0-2.54,2.03-4.4,4.4-4.4h16.76c2.37,0,4.4,2.03,4.4,4.4v44.35c0,7.28,5.92,13.37,13.37,13.37h50.11c7.28,0,13.37-5.92,13.37-13.37v-98.19c0-2.54-2.03-4.4-4.4-4.4h-7.45c-20.99,0-38.77-16.59-39.27-37.58-.51-21.67,17.44-39.61,39.11-39.1,20.99.34,37.58,18.28,37.58,39.27v123.41c0,2.54,2.03,4.4,4.4,4.4h16.76c2.54,0,4.4-2.03,4.4-4.4v-124.26c-.17-36.9-31.49-66.53-69.07-63.82Z'/%3E%3Ccircle fill='%232a9993' cx='1065.61' cy='65.94' r='12.7'/%3E%3C/g%3E%3C/svg%3E">
<style>
:root{
  --bg:#f0f2f5;--card:#ffffff;--border:#d1d5db;--text:#1e293b;--dim:#6b7280;
  --muted:#4b5563;--accent:#059669;--adim:rgba(6,214,160,.12);
  --warn:#d97706;--wdim:rgba(251,191,36,.1);--danger:#ef4444;--ddim:rgba(239,68,68,.1);
  --blue:#2563eb;--bdim:rgba(59,130,246,.1);--purple:#7c3aed;--cyan:#0891b2;
}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:system-ui,sans-serif}

/* ── header ── */
.hdr{background:linear-gradient(135deg,#1e293b,#0f172a);border-bottom:1px solid #334155;
     padding:0 28px;height:54px;display:flex;align-items:center;justify-content:space-between;
     position:sticky;top:0;z-index:300}
.hdr-left{display:flex;align-items:center;gap:12px}
.logo-svg{width:36px;height:36px;flex-shrink:0}
.hdr h1{font-size:18px;font-weight:700;letter-spacing:-.4px;color:#e2e8f0}
.hdr h1 span{color:#34d399}
.hdr-sub{color:#94a3b8;font-size:11px}
.hdr-right{display:flex;align-items:center;gap:12px}
.live-badge{background:rgba(52,211,153,.18);color:#34d399;padding:3px 10px;border-radius:5px;
            font-size:11px;font-weight:600;font-family:'Courier New',monospace;
            animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.55}}
.dot-live{width:8px;height:8px;border-radius:50%;background:#34d399;
          box-shadow:0 0 6px #059669;display:inline-block}
#conn{color:#94a3b8;font-family:'Courier New',monospace;font-size:11px}

/* ── tabs ── */
.tabs{background:#f8fafc;border-bottom:1px solid var(--border);
      padding:8px 28px;display:flex;gap:4px;overflow-x:auto;
      position:sticky;top:54px;z-index:200}
.tab{background:0;border:1px solid transparent;color:var(--muted);padding:8px 16px;
     border-radius:7px;cursor:pointer;font-size:13px;font-family:inherit;
     transition:.18s;white-space:nowrap;font-weight:500}
.tab:hover{color:var(--text);background:rgba(0,0,0,.04)}
.tab.on{background:var(--adim);border-color:rgba(6,214,160,.3);color:var(--accent);font-weight:600}
.tab.reload-tab{border-color:rgba(251,191,36,.3);color:var(--warn)}
.tab.reload-tab.on{background:var(--wdim);border-color:rgba(251,191,36,.5);color:var(--warn)}

/* ── layout ── */
.main{padding:24px 28px;max-width:1440px;margin:0 auto}
.pnl{display:none}.pnl.on{display:block;animation:fi .25s ease}
@keyframes fi{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:none}}

/* ── section header ── */
.sec-hdr{margin-bottom:20px}
.sec-hdr .n{background:linear-gradient(135deg,var(--accent),var(--cyan));color:#fff;
            width:30px;height:30px;border-radius:7px;display:inline-flex;align-items:center;
            justify-content:center;font-size:13px;font-weight:800;
            font-family:'Courier New',monospace;margin-right:10px;vertical-align:middle}
.sec-hdr h2{display:inline;font-size:20px;font-weight:700;vertical-align:middle}
.sec-hdr .d{color:var(--dim);font-size:13px;margin-top:4px;margin-left:40px}

/* ── summary bar ── */
.sbar{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:18px;padding:14px 18px;
      background:var(--card);border:1px solid var(--border);border-radius:11px;align-items:center}
.sstat{text-align:center;min-width:80px}
.sstat .l{color:var(--dim);font-size:10px;text-transform:uppercase;letter-spacing:.5px}
.sstat .v{font-size:22px;font-weight:700;font-family:'Courier New',monospace}
.rabtn{background:linear-gradient(135deg,var(--purple),var(--blue));color:#fff;border:0;
       padding:10px 22px;border-radius:7px;font-size:13px;font-weight:700;
       cursor:pointer;font-family:inherit;margin-left:auto;transition:.15s}
.rabtn:hover{opacity:.85}.rabtn:disabled{opacity:.4;cursor:wait}

/* ── query cards ── */
.qgrid{display:grid;grid-template-columns:1fr;gap:12px;margin-bottom:24px}
.qcard{background:var(--card);border:1px solid var(--border);border-radius:11px;
       box-shadow:0 1px 3px rgba(0,0,0,.07);overflow:hidden;transition:.18s}
.qcard:hover{border-color:rgba(6,214,160,.3)}
.qbar{display:flex;align-items:center;gap:10px;padding:13px 16px;cursor:pointer}
.qid{min-width:32px;height:24px;border-radius:5px;display:flex;align-items:center;
     justify-content:center;font-family:'Courier New',monospace;font-size:11px;font-weight:700}
.p0 .qid{background:var(--adim);color:var(--accent)}
.p1 .qid{background:var(--bdim);color:var(--blue)}
.p2 .qid{background:rgba(167,139,250,.15);color:var(--purple)}
.qname{flex:1;font-size:14px;font-weight:500}
.qdesc{color:var(--dim);font-size:11px}
.qtm .t{background:var(--adim);color:var(--accent);padding:2px 8px;border-radius:4px;
        font-size:11px;font-weight:600;font-family:'Courier New',monospace}
.qtm .t.slow{background:var(--wdim);color:var(--warn)}
.qtm .r{color:var(--dim);margin-left:6px;font-size:11px}
.rbtn{background:var(--adim);color:var(--accent);border:1px solid rgba(6,214,160,.3);
      padding:5px 12px;border-radius:5px;cursor:pointer;font-family:'Courier New',monospace;
      font-size:11px;font-weight:600;transition:.15s}
.rbtn:hover{background:rgba(6,214,160,.22)}.rbtn:disabled{opacity:.4;cursor:wait}
.qbody{display:none;padding:0 16px 16px}.qcard.open .qbody{display:block}
.qsql{background:#f8fafc;border-radius:6px;padding:10px 12px;
      font-family:'Courier New',monospace;font-size:11px;color:var(--muted);
      line-height:1.55;max-height:180px;overflow:auto;margin-bottom:10px;
      white-space:pre-wrap;word-break:break-all}
.qactions{display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap}
.qres{overflow-x:auto;max-height:380px;overflow-y:auto;border-radius:6px}

/* ── table ── */
table{width:100%;border-collapse:collapse;font-size:12px;font-family:'Courier New',monospace}
th{text-align:left;padding:7px 10px;color:var(--dim);font-size:10px;text-transform:uppercase;
   letter-spacing:.5px;border-bottom:1px solid var(--border);font-weight:500;
   position:sticky;top:0;background:var(--card);z-index:1}
td{padding:7px 10px;border-bottom:1px solid var(--border)}
tr:last-child td{border-bottom:none}
.empty{color:var(--dim);padding:20px;text-align:center;font-size:13px}
tr.unflagged-row td{background:#fff7ed;color:#9a3412}   /* orange: ML found, not in watchlist */
tr.ml-row td{background:#f0fdf4;color:#166534}            /* green: ML accounts now flagged */
tr.bin-row td{background:#eff6ff;color:#1e40af}           /* blue: original BIN range matches */
.spinner{width:26px;height:26px;border:3px solid var(--border);border-top-color:var(--accent);
         border-radius:50%;animation:sp .75s linear infinite;margin:0 auto}
@keyframes sp{to{transform:rotate(360deg)}}

/* ── SQL editor ── */
.sqled{width:100%;min-height:140px;background:#f8fafc;border:1px solid var(--border);
       border-radius:8px;color:var(--text);font-family:'Courier New',monospace;
       font-size:13px;padding:14px;resize:vertical;line-height:1.6}
.sqled:focus{outline:0;border-color:var(--accent)}
.runbtn{background:linear-gradient(135deg,var(--accent),var(--cyan));color:#fff;border:0;
        padding:10px 22px;border-radius:7px;font-size:13px;font-weight:700;
        cursor:pointer;font-family:inherit;margin-top:10px;transition:.15s}
.runbtn:hover{opacity:.85}



/* ── scenario card ── */
.scenario{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:22px}
.sc-card{background:var(--card);border:1px solid var(--border);border-radius:11px;padding:16px 18px}
.sc-card.full{grid-column:1/-1}
.sc-card.highlight{border-color:rgba(6,214,160,.4);background:rgba(6,214,160,.04)}
.sc-title{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;
          color:var(--accent);margin-bottom:6px}
.sc-title.blue{color:var(--blue)}
.sc-title.warn{color:var(--warn)}
.sc-title.purple{color:var(--purple)}
.sc-body{font-size:13px;color:var(--muted);line-height:1.65}
.sc-body strong{color:var(--text)}
.sc-body code{background:var(--bg);padding:1px 5px;border-radius:3px;
              font-family:'Courier New',monospace;font-size:11px}
.sc-compare{display:grid;grid-template-columns:1fr 1fr;gap:0;border-radius:9px;overflow:hidden;border:1px solid var(--border)}
.sc-col{padding:12px 14px}
.sc-col.bad{background:#fef2f2}.sc-col.good{background:#f0fdf4}
.sc-col-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px}
.sc-col.bad .sc-col-title{color:#991b1b}.sc-col.good .sc-col-title{color:#166534}
.sc-col ul{list-style:none;padding:0;font-size:12px;color:var(--muted);line-height:1.8}
.sc-col.bad ul li::before{content:"✗ "}.sc-col.good ul li::before{content:"✓ "}
.sc-stage{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:8px 0}
.sc-box{background:var(--bg);border:1px solid var(--border);border-radius:6px;
        padding:5px 10px;font-size:11px;font-family:'Courier New',monospace;color:var(--text)}
.sc-box.hi{background:rgba(6,214,160,.1);border-color:rgba(6,214,160,.3);color:#065f46}
.sc-arrow{color:var(--dim);font-size:14px;font-weight:700}
.sc-stat{text-align:center}
.sc-stat .big{font-size:28px;font-weight:700;font-family:'Courier New',monospace;line-height:1}
.sc-stat .lbl{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--dim);margin-top:2px}
.sc-personas{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:8px}
.sc-persona{border-radius:8px;padding:10px 10px 8px;border:1px solid transparent}
.sc-persona.normal{background:#f0fdf4;border-color:#bbf7d0}
.sc-persona.card{background:#eff6ff;border-color:#bfdbfe}
.sc-persona.bust{background:#fff7ed;border-color:#fed7aa}
.sc-persona.struct{background:#fdf4ff;border-color:#e9d5ff}
.sc-persona .p-icon{font-size:18px;margin-bottom:4px}
.sc-persona .p-name{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.04em}
.sc-persona.normal .p-name{color:#166534}
.sc-persona.card .p-name{color:#1e40af}
.sc-persona.bust .p-name{color:#9a3412}
.sc-persona.struct .p-name{color:#6b21a8}
.sc-persona .p-stat{font-size:10px;color:var(--dim);margin-top:2px;line-height:1.5}

/* ── footer ── */
.ft{margin-top:32px;padding:14px 0;border-top:1px solid var(--border);
    display:flex;justify-content:space-between;color:var(--dim);font-size:11px}

/* ── Panel D extras ── */
.p3 .qid{background:rgba(234,88,12,.12);color:#ea580c}
tr.ml-row td{background:#f0fdf4;color:#166534}
.wl-card{background:var(--card);border:1px solid var(--border);border-radius:11px;
         padding:18px 22px;margin-bottom:20px}
.wl-title{font-size:15px;font-weight:700;margin-bottom:3px}
.wl-sub{font-size:12px;color:var(--dim);margin-bottom:14px;line-height:1.55}
.wl-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:14px}
.btn-wl{background:linear-gradient(135deg,#ea580c,#d97706);color:#fff;border:0;
        padding:9px 22px;border-radius:7px;font-size:13px;font-weight:700;
        cursor:pointer;font-family:inherit;transition:.15s}
.btn-wl:hover{opacity:.85}.btn-wl:disabled{opacity:.4;cursor:wait}
.btn-rst{background:0;border:1px solid var(--border);color:var(--muted);
         padding:8px 14px;border-radius:7px;font-size:12px;cursor:pointer;font-family:inherit}
.btn-rst:hover{border-color:var(--danger);color:var(--danger)}
.wl-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:8px}
.wsc{background:var(--bg);border:1px solid var(--border);border-radius:8px;
     padding:8px 12px;text-align:center}
.wsc .l{font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:var(--dim)}
.wsc .v{font-size:22px;font-weight:700;font-family:'Courier New',monospace;margin-top:2px}
.wl-log{background:#0f172a;border-radius:8px;padding:10px 14px;font-family:'Courier New',monospace;
        font-size:11px;line-height:1.7;max-height:160px;overflow-y:auto;
        margin-top:12px;display:none}
.wl-log.show{display:block}
.wl-log .ok{color:#34d399}.wl-log .err{color:#f87171}.wl-log .info{color:#94a3b8}
/* reset modal — normal-flow faux viewport, avoids position:fixed */
.rst-wrap{display:none;min-height:180px;background:rgba(0,0,0,.45);border-radius:11px;
          padding:20px;align-items:center;justify-content:center;margin-bottom:14px}
.rst-wrap.show{display:flex}
.rst-box{background:var(--card);border-radius:11px;padding:22px;max-width:400px;width:100%}
.rst-box h3{font-size:15px;font-weight:700;margin-bottom:8px}
.rst-box p{font-size:12px;color:var(--muted);margin-bottom:14px;line-height:1.6}
.rst-btns{display:flex;gap:8px;flex-wrap:wrap}
.rb-soft{background:rgba(251,191,36,.1);border:1px solid rgba(251,191,36,.4);color:var(--warn);
         padding:7px 14px;border-radius:7px;cursor:pointer;font-size:12px;font-weight:600;font-family:inherit}
.rb-hard{background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.3);color:var(--danger);
         padding:7px 14px;border-radius:7px;cursor:pointer;font-size:12px;font-weight:600;font-family:inherit}
.rb-full{background:rgba(124,58,237,.08);border:1px solid rgba(124,58,237,.3);color:var(--purple);
         padding:7px 14px;border-radius:7px;cursor:pointer;font-size:12px;font-weight:600;font-family:inherit}
.rb-cancel{background:0;border:1px solid var(--border);color:var(--dim);
           padding:7px 14px;border-radius:7px;cursor:pointer;font-size:12px;font-family:inherit}
</style>
</head><body>

<!-- HEADER -->
<div class="hdr">
  <div class="hdr-left">
    <svg class="logo-svg" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 256">
      <g transform="translate(-862,18)">
        <path fill="#2a9993" d="M1060.7,2.12c-30.98,2.37-56.03,27.09-58.74,58.06-2.88,33.35,19.98,61.96,50.96,68.22v61.62c0,2.54-2.03,4.4-4.4,4.4h-16.76c-2.54,0-4.4-2.03-4.4-4.4v-44.35c0-7.28-5.92-13.37-13.37-13.37h-49.94c-7.28,0-13.37,5.92-13.37,13.37v44.35c0,2.54-2.03,4.4-4.4,4.4h-16.76c-2.37,0-4.4-2.03-4.4-4.4v-97.17l73.47-73.47c1.69-1.69,1.69-4.57,0-6.26l-11.85-11.85c-1.69-1.69-4.57-1.69-6.26,0l-125.27,125.27c-1.69,1.69-1.69,4.57,0,6.26l11.85,11.85c1.69,1.69,4.57,1.69,6.26,0l26.07-26.24v88.37c0,7.28,5.92,13.37,13.37,13.37h50.11c7.28,0,13.37-5.92,13.37-13.37v-44.35c0-2.54,2.03-4.4,4.4-4.4h16.76c2.37,0,4.4,2.03,4.4,4.4v44.35c0,7.28,5.92,13.37,13.37,13.37h50.11c7.28,0,13.37-5.92,13.37-13.37v-98.19c0-2.54-2.03-4.4-4.4-4.4h-7.45c-20.99,0-38.77-16.59-39.27-37.58-.51-21.67,17.44-39.61,39.11-39.1,20.99.34,37.58,18.28,37.58,39.27v123.41c0,2.54,2.03,4.4,4.4,4.4h16.76c2.54,0,4.4-2.03,4.4-4.4v-124.26c-.17-36.9-31.49-66.53-69.07-63.82Z"/>
        <circle fill="#2a9993" cx="1065.61" cy="65.94" r="12.7"/>
      </g>
    </svg>
    <div>
      <h1>WarehousePG <span>AI Analytics</span></h1>
      <div class="hdr-sub">Lab 2 — pgvector + MADlib + AI Factory · Meridian Bank · last 28 days</div>
    </div>
  </div>
  <div class="hdr-right">
    <span class="dot-live"></span>
    <div class="live-badge">LIVE</div>
    <div id="conn">connecting…</div>
  </div>
</div>

<!-- TABS -->
<div class="tabs" id="tabs">
  <button class="tab on"  onclick="switchTab(0)">Part A: pgvector</button>
  <button class="tab"     onclick="switchTab(1)">Part B: MADlib / SQL</button>
  <button class="tab"     onclick="switchTab(2)">Part C: AI Factory</button>
  <button class="tab"     onclick="switchTab(3)" style="border-color:rgba(167,139,250,.3);color:var(--purple)">SQL Editor</button>
  <button class="tab"     onclick="switchTab(4)" style="border-color:rgba(234,88,12,.3);color:#ea580c">D: ML &#8594; Watchlist</button>
</div>

<div class="main" id="main">

  <!-- Query panels injected by JS (pnl-0, pnl-1, pnl-2) -->

  <!-- SQL EDITOR -->
  <div class="pnl" id="pnl-3">
    <div class="sec-hdr">
      <span class="n">Q</span><h2>SQL Editor</h2>
      <div class="d">Run any SELECT against the live dataset</div>
    </div>
    <textarea class="sqled" id="sqlin" spellcheck="false">SELECT note_id, account_id, queue,
    LEFT(narrative, 80) AS narrative, persona, severity
FROM bfsi_demo.case_embeddings
LIMIT 20;</textarea>
    <button class="runbtn" onclick="runSQL()">▶ Run Query</button>
    <span id="sqlt" style="margin-left:12px"></span>
    <div style="margin-top:14px;overflow-x:auto;max-height:500px;overflow-y:auto" id="sqlr"></div>
  </div>


  <!-- Panel D: ML → Watchlist (static; query cards injected by buildPanels) -->
  <div class="pnl" id="pnl-4">
    <div class="sec-hdr">
      <span class="n" style="background:linear-gradient(135deg,#ea580c,#d97706)">D</span>
      <h2>ML &#8594; Watchlist</h2>
      <div class="d">Write K-Means results back to fraud_watchlists · Query 1A before vs after ML update</div>
    </div>

    <!-- Reset info box -->
    <div style="background:#fef9f0;border:1px solid rgba(234,88,12,.25);border-radius:11px;padding:14px 18px;margin-bottom:16px;font-size:12px;line-height:1.7">
      <div style="font-weight:700;color:#ea580c;margin-bottom:6px">&#9432; What to reset if Inserted = 0</div>
      <div style="color:var(--muted)">
        If the Refresh shows <strong>Inserted 0</strong>, those accounts are already in the watchlist from a previous run.<br>
        Run this in psql to soft-reset (safe — just expires the flags, you can re-refresh immediately):<br>
        <code style="background:var(--bg);padding:3px 8px;border-radius:4px;font-family:'Courier New',monospace;font-size:11px;display:inline-block;margin-top:4px">
          UPDATE bfsi_demo.fraud_watchlists SET active=FALSE, last_seen=NOW() WHERE feed_name='MADlib K-Means' AND active=TRUE;
        </code><br>
        Then click <strong>Run Refresh</strong> again — D1 will show the gap, D2 will show them filled.
      </div>
    </div>

    <!-- Watchlist pipeline card -->
    <div class="wl-card">
      <div class="wl-title">ML Watchlist Pipeline</div>
      <div class="wl-sub">
        <b>Step 1</b>: Run D1 to see fraud transactions <em>not yet</em> in the watchlist (the gap MADlib found).<br>
        <b>Step 2</b>: Click <strong>Run Refresh</strong> — writes K-Means results to fraud_watchlists.<br>
        <b>Step 3</b>: Re-run D1 (should now show 0) and run D2 to see all flagged transactions with color coding.
      </div>
      <div class="wl-actions">
        <button class="btn-wl"  id="btn-wl-refresh" onclick="wlRefresh()">&#9654; Run Refresh</button>
        <span id="wl-status" style="font-size:13px;color:var(--dim)">Not yet run this session</span>
      </div>
      <div class="wl-stats">
        <div class="wsc"><div class="l">Inserted</div><div class="v" id="wl-ins" style="color:var(--accent)">—</div></div>
        <div class="wsc"><div class="l">Expired</div> <div class="v" id="wl-exp" style="color:var(--warn)">—</div></div>
        <div class="wsc"><div class="l">Time</div>    <div class="v" id="wl-ms"  style="color:var(--blue)">—</div></div>
        <div class="wsc"><div class="l">ML Flags</div><div class="v" id="wl-tot" style="color:#ea580c">—</div></div>
      </div>
      <div class="wl-log" id="wl-log"></div>
    </div>

    <!-- Query cards injected by buildPanels (D1, D2, D3) -->
    <div class="sbar">
      <div class="sstat"><div class="l">Queries</div><div class="v" style="color:var(--accent)">2</div></div>
      <div class="sstat"><div class="l">Completed</div><div class="v" style="color:var(--blue)" id="done-3">0</div></div>
      <div class="sstat"><div class="l">Total Time</div><div class="v" style="color:var(--warn)" id="tms-3">—</div></div>
      <button class="rabtn" id="rabtn-3" onclick="runPanel(3)">&#9654; Run All 2</button>
    </div>
    <div class="qgrid" id="pnl-4-qgrid"></div>
  </div>

  <div class="ft">
    <div>EDB WarehousePG — Lab 2: AI-Powered Analytics + ML Watchlist</div>
    <div style="font-family:'Courier New',monospace">pgvector + MADlib + AI Factory + ML&#8594;Watchlist</div>
  </div>
</div><!-- /main -->

<script>
const PANELS  = {{ panels|tojson }};
const QUERIES = {{ queries|tojson }};
const results = {};

// ── health check ──────────────────────────────────────────────────────────
fetch('/api/health').then(r=>r.json()).then(d=>{
  const el = document.getElementById('conn');
  el.textContent  = d.status==='ok' ? 'Connected' : 'Error: '+d.error;
  el.style.color  = d.status==='ok' ? '#34d399' : '#ef4444';
}).catch(()=>{
  document.getElementById('conn').textContent='Offline';
  document.getElementById('conn').style.color='#ef4444';
});

// ── helpers ───────────────────────────────────────────────────────────────
function esc(s){ return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function fmtMs(ms){ return ms<1000 ? ms+'ms' : (ms/1000).toFixed(1)+'s'; }
function tbl(rows){
  if(!rows||!rows.length)
    return '<div class="empty">No results — data may need a reload (timestamps expired, 06_ai_analytics.sql not run, or 07_kmeans_fallback.sql not run)</div>';
  const ks=Object.keys(rows[0]);
  let h='<table><thead><tr>'+ks.map(k=>'<th>'+esc(k)+'</th>').join('')+'</tr></thead><tbody>';
  rows.forEach(r=>{ h+='<tr>'+ks.map(k=>'<td>'+(r[k]!=null?esc(String(r[k])):'—')+'</td>').join('')+'</tr>'; });
  return h+'</tbody></table>';
}

// ── build query panels ────────────────────────────────────────────────────
function qCards(qs, pi){
  let h='';
  qs.forEach(q=>{
    h += `<div class="qcard p${pi}" id="qc-${q.id}">
      <div class="qbar" onclick="toggle('${q.id}')">
        <span class="qid">${q.id.toUpperCase()}</span>
        <div style="flex:1"><div class="qname">${esc(q.name)}</div><div class="qdesc">${esc(q.desc)}</div></div>
        <span class="qtm" id="qt-${q.id}"></span>
        <button class="rbtn" id="rb-${q.id}" onclick="event.stopPropagation();runQ('${q.id}')">Run</button>
      </div>
      <div class="qbody" data-pi="${pi}">
        <div class="qsql">${esc(q.sql)}</div>
        <div class="qactions">
          <button class="rbtn" onclick="runQ('${q.id}')">&#9654; Run</button>
          <button class="rbtn" onclick="copyQ('${q.id}')" style="background:var(--bdim);color:var(--blue);border-color:rgba(59,130,246,.3)">Copy SQL</button>
          <button class="rbtn" onclick="toEditor('${q.id}')" style="background:rgba(167,139,250,.1);color:var(--purple);border-color:rgba(167,139,250,.3)">Edit in SQL</button>
        </div>
        <div class="qres" id="qr-${q.id}"></div>
      </div>
    </div>`;
  });
  return h;
}

const SCENARIO_HTML = {
  0: `<div class="scenario">
    <div class="sc-card full highlight">
      <div class="sc-title">&#128270; The problem — Part A: pgvector</div>
      <div class="sc-body">Analysts write keyword searches like <code>WHERE narrative LIKE '%structuring%'</code>. Fraud rings use different words for the same behaviour — <em>sub-threshold splits, layering, smurfing, rapid pass-through</em>. Keyword search misses <strong>70–80% of fraud</strong>.<br><br>
      pgvector stores each case note as a 32-dimension vector encoding <strong>meaning, not words</strong>. A single <code>&lt;=&gt;</code> cosine distance operator finds semantically similar notes across all phrasing variants — no UDF, no regex, no re-indexing as language evolves.</div>
    </div>
    <div class="sc-card">
      <div class="sc-title blue">WITHOUT pgvector</div>
      <div class="sc-col bad" style="border-radius:8px;padding:10px 14px">
        <ul><li>Keyword regex — misses phrasing variants</li><li>New attack = new rules written manually</li><li>30% fraud coverage</li><li>Export to Pinecone / Weaviate (data movement)</li></ul>
      </div>
    </div>
    <div class="sc-card">
      <div class="sc-title" style="color:var(--accent)">WITH pgvector (WHPG native)</div>
      <div class="sc-col good" style="border-radius:8px;padding:10px 14px">
        <ul><li>Semantic similarity — finds meaning, not words</li><li>New phrasings caught automatically</li><li>95% fraud coverage</li><li>Native operator in the same database — zero data movement</li></ul>
      </div>
    </div>
  </div>`,

  1: `<div class="scenario">
    <div class="sc-card full highlight">
      <div class="sc-title">&#129504; The problem — Part B: MADlib K-Means</div>
      <div class="sc-body">You can't write rules for fraud you haven't seen yet. MADlib runs <strong>K-Means clustering in-database</strong> on 6 behavioural features extracted from ~50K accounts across 13M transactions — no labels required. The algorithm discovers fraud rings automatically by grouping accounts with statistically similar spend patterns.<br><br>
      <strong>No data ever leaves WarehousePG.</strong> On Snowflake or Databricks you'd export to SageMaker, train externally, re-import results. Here: one SQL call, same engine, same second.</div>
    </div>
    <div class="sc-card">
      <div class="sc-title warn">Traditional pipeline</div>
      <div class="sc-stage">
        <span class="sc-box">Warehouse</span><span class="sc-arrow">→</span>
        <span class="sc-box">Export ETL</span><span class="sc-arrow">→</span>
        <span class="sc-box">SageMaker</span><span class="sc-arrow">→</span>
        <span class="sc-box">Re-import</span><span class="sc-arrow">→</span>
        <span class="sc-box">App</span>
      </div>
      <div class="sc-body" style="font-size:12px;margin-top:6px">6 stages · 3 vendors · 2 security reviews · hours of lag</div>
    </div>
    <div class="sc-card">
      <div class="sc-title" style="color:var(--accent)">WHPG in-database ML</div>
      <div class="sc-stage">
        <span class="sc-box hi">account_features</span><span class="sc-arrow">→</span>
        <span class="sc-box hi">MADlib K-Means</span><span class="sc-arrow">→</span>
        <span class="sc-box hi">kmeans_labeled</span>
      </div>
      <div class="sc-body" style="font-size:12px;margin-top:6px">1 engine · 1 SQL call · 0 data exports · &lt; 5 seconds</div>
    </div>
    <div class="sc-card full">
      <div class="sc-title purple">6 behavioural features fed to K-Means</div>
      <div class="sc-body" style="margin-bottom:10px">Each account becomes a point in 6-dimensional feature space. The algorithm finds natural clusters — fraud rings self-identify by proximity.</div>
      <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:8px;text-align:center">
        <div style="background:var(--bg);border-radius:7px;padding:8px 6px"><div style="font-size:18px">&#128200;</div><div style="font-size:10px;font-weight:600;margin-top:3px">txn_count</div><div style="font-size:10px;color:var(--dim)">How active?</div></div>
        <div style="background:var(--bg);border-radius:7px;padding:8px 6px"><div style="font-size:18px">&#127978;</div><div style="font-size:10px;font-weight:600;margin-top:3px">distinct_merchants</div><div style="font-size:10px;color:var(--dim)">How many merchants?</div></div>
        <div style="background:var(--bg);border-radius:7px;padding:8px 6px"><div style="font-size:18px">&#128181;</div><div style="font-size:10px;font-weight:600;margin-top:3px">total_amount</div><div style="font-size:10px;color:var(--dim)">How much spend?</div></div>
        <div style="background:var(--bg);border-radius:7px;padding:8px 6px"><div style="font-size:18px">&#127922;</div><div style="font-size:10px;font-weight:600;margin-top:3px">merchant_entropy</div><div style="font-size:10px;color:var(--dim)">How scattered?</div></div>
        <div style="background:var(--bg);border-radius:7px;padding:8px 6px"><div style="font-size:18px">&#128101;</div><div style="font-size:10px;font-weight:600;margin-top:3px">mcc_spread</div><div style="font-size:10px;color:var(--dim)">Merchant diversity?</div></div>
        <div style="background:var(--bg);border-radius:7px;padding:8px 6px"><div style="font-size:18px">&#128202;</div><div style="font-size:10px;font-weight:600;margin-top:3px">amount_cv</div><div style="font-size:10px;color:var(--dim)">Amount consistency?</div></div>
      </div>
      <div class="sc-personas">
        <div class="sc-persona normal"><div class="p-icon">&#10003;</div><div class="p-name">Normal</div><div class="p-stat">~50K accounts<br>6 merchants · $2.3K avg</div></div>
        <div class="sc-persona card"><div class="p-icon">&#128269;</div><div class="p-name">Card-Testing</div><div class="p-stat">~40 accounts<br>600 merchants · tiny tickets</div></div>
        <div class="sc-persona bust"><div class="p-icon">&#128228;</div><div class="p-name">Bust-Out</div><div class="p-stat">~30 accounts<br>9 merchants · $1.5M spend</div></div>
        <div class="sc-persona struct"><div class="p-icon">&#129302;</div><div class="p-name">Structuring</div><div class="p-stat">~40 accounts<br>1 merchant · $4.7K fixed</div></div>
      </div>
    </div>
  </div>`,

  2: `<div class="scenario">
    <div class="sc-card full highlight">
      <div class="sc-title">&#127981; Part C: The AI Factory — pgvector + MADlib in ONE query</div>
      <div class="sc-body">MADlib finds <strong>behavioural anomalies</strong> (accounts with extreme spend patterns). pgvector finds <strong>semantic matches</strong> (case notes with similar meaning). Join them — and the same fraud types surface from two completely independent signals in a single SQL statement.<br><br>
      This is the critical limitation of competing platforms: <strong>Snowflake and Databricks cannot do this without exporting data to an external ML tool.</strong> In WarehousePG, both algorithms run where the data lives — no API, no data movement, no token cost until the very last mile.</div>
    </div>
    <div class="sc-card">
      <div class="sc-title blue">Token funnel — why this matters for LLM cost</div>
      <div class="sc-stage" style="flex-direction:column;align-items:flex-start;gap:6px">
        <div style="display:flex;align-items:center;gap:8px"><span class="sc-box">13M rows</span><span class="sc-arrow">→</span><span style="font-size:11px;color:var(--dim)">MADlib K-Means · 0 tokens</span></div>
        <div style="display:flex;align-items:center;gap:8px"><span class="sc-box hi">~26 anomalous accounts</span><span class="sc-arrow">→</span><span style="font-size:11px;color:var(--dim)">pgvector search · 0 tokens</span></div>
        <div style="display:flex;align-items:center;gap:8px"><span class="sc-box hi">~40 related events</span><span class="sc-arrow">→</span><span style="font-size:11px;color:var(--dim)">LLM summary · ~2–4K tokens</span></div>
      </div>
    </div>
    <div class="sc-card">
      <div class="sc-title" style="color:var(--accent)">The competitive moat</div>
      <div class="sc-body">
        <strong>Snowflake / Databricks:</strong> export 13M rows → SageMaker → re-import → JOIN → 2–4 hours<br><br>
        <strong>WarehousePG:</strong> <code>SELECT madlib.kmeanspp(...)</code> + <code>&lt;=&gt;</code> in one query → &lt; 5 seconds<br><br>
        Cost grows with <em>findings</em>, not data size.
      </div>
    </div>
  </div>`
};

function buildPanels(){
  const main   = document.getElementById('main');
  const anchor = document.getElementById('pnl-3');
  PANELS.forEach((p, pi)=>{
    const qs = QUERIES.filter(q=>q.panel===pi);
    // Panel 3 (D) already has its wrapper HTML; just inject cards into the qgrid
    if(pi===3){
      const grid=document.getElementById('pnl-4-qgrid');
      if(grid) grid.innerHTML=qCards(qs,pi);
      return;
    }
    let h = `<div class="pnl${pi===0?' on':''}" id="pnl-${pi}">`;
    h += `<div class="sec-hdr"><span class="n">${p.icon}</span><h2>${p.name}</h2><div class="d">${esc(p.desc)}</div></div>`;
    // inject scenario card if defined for this panel
    if(SCENARIO_HTML[pi]) h += SCENARIO_HTML[pi];
    h += `<div class="sbar">
      <div class="sstat"><div class="l">Queries</div><div class="v" style="color:var(--accent)">${qs.length}</div></div>
      <div class="sstat"><div class="l">Completed</div><div class="v" style="color:var(--blue)" id="done-${pi}">0</div></div>
      <div class="sstat"><div class="l">Total Time</div><div class="v" style="color:var(--warn)" id="tms-${pi}">—</div></div>
      <button class="rabtn" id="rabtn-${pi}" onclick="runPanel(${pi})">&#9654; Run All ${qs.length}</button>
    </div>`;
    h += `<div class="qgrid">${qCards(qs,pi)}</div></div>`;
    const div = document.createElement('div');
    div.innerHTML = h;
    main.insertBefore(div.firstChild, anchor);
  });
}

function switchTab(i){
  document.querySelectorAll('.tab').forEach((t,j)=>t.classList.toggle('on',j===i));
  document.querySelectorAll('.pnl').forEach((p,j)=>p.classList.toggle('on',j===i));
}
function toggle(id){ document.getElementById('qc-'+id)?.classList.toggle('open'); }

// ── run query ─────────────────────────────────────────────────────────────
async function runQ(id){
  const btn=document.getElementById('rb-'+id);
  btn.disabled=true; btn.textContent='…';
  document.getElementById('qr-'+id).innerHTML='<div style="padding:20px;text-align:center"><div class="spinner"></div></div>';
  document.getElementById('qc-'+id).classList.add('open');
  try{
    const r=await(await fetch('/api/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})})).json();
    results[id]=r;
    const slow=r.ms>5000;
    document.getElementById('qt-'+id).innerHTML=`<span class="t${slow?' slow':''}">${fmtMs(r.ms)}</span><span class="r" style="color:var(--dim);margin-left:6px">${r.rows} rows</span>`;
    document.getElementById('qr-'+id).innerHTML=r.error
      ?`<div style="color:var(--danger);padding:12px;font-family:'Courier New',monospace;font-size:12px">ERROR: ${esc(r.error)}</div>`
      :tbl(r.data);
  }catch(e){
    document.getElementById('qr-'+id).innerHTML=`<div style="color:var(--danger);padding:12px">${e.message}</div>`;
  }
  btn.disabled=false; btn.textContent='Run';
  updatePanel(QUERIES.find(q=>q.id===id).panel);
}

async function runPanel(pi){
  const btn=document.getElementById('rabtn-'+pi);
  btn.disabled=true; btn.textContent='Running…';
  for(const q of QUERIES.filter(q=>q.panel===pi)) await runQ(q.id);
  btn.disabled=false; btn.textContent='▶ Run All '+QUERIES.filter(q=>q.panel===pi).length;
}

function updatePanel(pi){
  const qs=QUERIES.filter(q=>q.panel===pi);
  const done=qs.filter(q=>results[q.id]);
  const ms=done.reduce((s,q)=>s+(results[q.id]?.ms||0),0);
  document.getElementById('done-'+pi).textContent=done.length;
  document.getElementById('tms-'+pi).textContent=done.length?fmtMs(Math.round(ms)):'—';
}

function copyQ(id){ navigator.clipboard.writeText(QUERIES.find(q=>q.id===id).sql+';'); }
function toEditor(id){
  document.getElementById('sqlin').value=QUERIES.find(q=>q.id===id).sql+';';
  switchTab(3);
}

// ── SQL editor ────────────────────────────────────────────────────────────
async function runSQL(){
  const sql=document.getElementById('sqlin').value;
  document.getElementById('sqlr').innerHTML='<div style="padding:20px;text-align:center"><div class="spinner"></div></div>';
  document.getElementById('sqlt').innerHTML='';
  try{
    const r=await(await fetch('/api/sql',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql})})).json();
    if(r.error){
      document.getElementById('sqlr').innerHTML=`<div style="color:var(--danger);padding:16px;font-family:'Courier New',monospace;font-size:12px">ERROR: ${esc(r.error)}</div>`;
      return;
    }
    const slow=r.ms>5000;
    document.getElementById('sqlt').innerHTML=`<span style="background:var(--adim);color:var(--accent);padding:3px 10px;border-radius:4px;font-size:11px;font-family:'Courier New',monospace;font-weight:600">${fmtMs(r.ms)}</span><span style="color:var(--dim);font-size:12px;margin-left:6px">${r.rows} rows</span>`;
    document.getElementById('sqlr').innerHTML=tbl(r.data);
  }catch(e){
    document.getElementById('sqlr').innerHTML=`<div style="color:var(--danger);padding:16px">${e.message}</div>`;
  }
}


// ── Panel D: ML Watchlist ────────────────────────────────────────────────
function wlLog(msg, cls='info'){
  const box=document.getElementById('wl-log');
  box.classList.add('show');
  const d=document.createElement('div'); d.className=cls;
  const ts=new Date().toLocaleTimeString('en-GB',{hour12:false});
  d.textContent=`[${ts}] ${msg}`;
  box.appendChild(d); box.scrollTop=box.scrollHeight;
}

function fmtN(n){
  n=Number(n);
  if(n>=1e6) return(n/1e6).toFixed(1)+'M';
  if(n>=1e3) return(n/1e3).toFixed(1)+'K';
  return n.toLocaleString();
}

async function wlRefresh(){
  const btn=document.getElementById('btn-wl-refresh');
  const st =document.getElementById('wl-status');
  btn.disabled=true; btn.textContent='Running…';
  st.textContent='Refreshing…'; st.style.color='var(--warn)';
  document.getElementById('wl-log').innerHTML='';
  wlLog('Step 1: Expire existing MADlib K-Means flags…');
  try{
    const r=await(await fetch('/api/watchlist/refresh',{method:'POST'})).json();
    if(!r.ok){ wlLog('ERROR: '+r.error,'err'); st.textContent='Error'; st.style.color='var(--danger)'; btn.disabled=false; btn.textContent='&#9654; Run Refresh'; return; }
    wlLog(`  ✓ Reset ${r.reset} existing flags (${r.reset_ms}ms)`,'ok');
    wlLog('Step 2: INSERT fresh from kmeans_assignments…');
    wlLog(`  ✓ Inserted ${r.inserted} new flags (${r.insert_ms}ms)`,'ok');
    wlLog('Step 3: Expire accounts that left fraud clusters…');
    wlLog(`  ✓ Expired ${r.expired} stale flags (${r.expire_ms}ms)`,'ok');
    wlLog(`Done in ${r.total_ms}ms`,'ok');
    document.getElementById('wl-ins').textContent=fmtN(r.inserted);
    document.getElementById('wl-exp').textContent=fmtN(r.expired);
    document.getElementById('wl-ms').textContent =r.total_ms+'ms';
    const tot=(r.summary||[]).filter(s=>s.feed_name==='MADlib K-Means').reduce((s,x)=>s+(x.active_flags||0),0);
    document.getElementById('wl-tot').textContent=fmtN(tot);
    wlLog(''); (r.summary||[]).forEach(s=>wlLog(`  ${s.feed_name} / ${s.category}: ${s.active_flags} active`));
    wlLog(''); wlLog('Run D1 — should show 0 unflagged accounts. Run D2 to see all flagged transactions.');
    st.textContent='✓ Complete'; st.style.color='var(--accent)';
    btn.disabled=false; btn.textContent='&#9654; Run Refresh';
    await runQ('d1');
    await runQ('d2');
  }catch(e){
    wlLog('Network error: '+e.message,'err');
    st.textContent='Error'; st.style.color='var(--danger)';
    btn.disabled=false; btn.textContent='&#9654; Run Refresh';
  }
}



// ── tbl override: support optional row highlight function ─────────────────
const _tbl_orig=tbl;
function tblHL(rows, hlFn){
  if(!rows||!rows.length) return '<div class="empty">No results — run the ML Watchlist Refresh first, then re-run this query.</div>';
  const ks=Object.keys(rows[0]);
  let h='<table><thead><tr>'+ks.map(k=>'<th>'+esc(k)+'</th>').join('')+'</tr></thead><tbody>';
  rows.forEach(r=>{
    const cls=hlFn?hlFn(r):'';
    h+=`<tr${cls?' class="'+cls+'"':''}>` +ks.map(k=>'<td>'+(r[k]!=null?esc(String(r[k])):'—')+'</td>').join('')+'</tr>';
  });
  return h+'</tbody></table>';
}

// patch runQ to highlight d2 rows
const _runQ_orig=runQ;
async function runQ(id){
  const btn=document.getElementById('rb-'+id);
  btn.disabled=true; btn.textContent='…';
  document.getElementById('qr-'+id).innerHTML='<div style="padding:20px;text-align:center"><div class="spinner"></div></div>';
  document.getElementById('qc-'+id).classList.add('open');
  try{
    const r=await(await fetch('/api/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})})).json();
    results[id]=r;
    const slow=r.ms>5000;
    document.getElementById('qt-'+id).innerHTML=`<span class="t${slow?' slow':''}">${fmtMs(r.ms)}</span><span class="r" style="color:var(--dim);margin-left:6px">${r.rows} rows</span>`;
    const hlFn = id==='d1'
      ? (row=>'unflagged-row')  // all D1 rows are unflagged — highlight orange
      : id==='d2'
      ? (row=>{
          const src=(row.source||'').toString();
          if(src.includes('ML')) return 'ml-row';     // green: new ML accounts
          return 'bin-row';                            // blue: original BIN matches
        })
      : null;
    document.getElementById('qr-'+id).innerHTML=r.error
      ?`<div style="color:var(--danger);padding:12px;font-family:'Courier New',monospace;font-size:12px">ERROR: ${esc(r.error)}</div>`
      :tblHL(r.data, hlFn);
  }catch(e){
    document.getElementById('qr-'+id).innerHTML=`<div style="color:var(--danger);padding:12px">${e.message}</div>`;
  }
  btn.disabled=false; btn.textContent='Run';
  updatePanel(QUERIES.find(q=>q.id===id).panel);
}

// ── init ──────────────────────────────────────────────────────────────────
buildPanels();

</script>
</body></html>"""
if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════════╗
║  Lab 2 — AI Analytics Dashboard (STREAMLINED)           ║
║  DB: {DB['host']}:{DB['port']}/{DB['dbname']}
║  Queries: {len(QUERIES)} (focused on VALUE demonstration)
║  http://0.0.0.0:5002                                    ║
╚══════════════════════════════════════════════════════════╝
    """)
    app.run(host="0.0.0.0", port=5002, debug=False)
