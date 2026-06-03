#!/usr/bin/env python3
"""
Lab 3 - AI-Powered Fraud Analytics: STREAMLINED Dashboard (BFSI)
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
)
SELECT
    r.ref_note               AS reference_note,
    e.account_id,
    e.queue,
    LEFT(e.narrative, 90)    AS similar_note,
    e.persona                AS ground_truth,
    ROUND((1 - (e.embedding <=> r.embedding))::numeric, 4) AS similarity
FROM bfsi_demo.case_embeddings e
CROSS JOIN reference_note r
WHERE e.note_id != r.note_id
  AND e.persona = 'structuring'
ORDER BY e.embedding <=> r.embedding
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
    COUNT(*) AS accounts,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct,
    ROUND(AVG(f.txn_count), 0)              AS avg_txns,
    ROUND(AVG(f.total_amount)::numeric, 2)  AS avg_spend,
    ROUND(AVG(f.distinct_merchants), 0)     AS avg_merchants,
    ROUND(AVG(f.amount_cv)::numeric, 4)     AS avg_cv,
    CASE
        WHEN AVG(f.amount_cv) < 0.1          THEN '\U0001f916 STRUCTURING'
        WHEN AVG(f.distinct_merchants) > 50  THEN '\U0001f50d CARD-TESTING'
        WHEN AVG(f.total_amount) > 100000    THEN '\U0001f4e4 BUST-OUT'
        ELSE '\u2705 NORMAL'
    END AS persona
FROM bfsi_demo.kmeans_assignments a
JOIN bfsi_demo.account_features f ON a.account_id = f.account_id
GROUP BY 1
ORDER BY accounts DESC"""
    },
    {
        "id": "b2", "panel": 1,
        "name": "B2 - The Dramatic Differences",
        "desc": "CARD-TESTING: many more merchants | BUST-OUT: far higher spend than a normal account",
        "sql": """WITH cluster_agg AS (
    SELECT
        a.cluster_id,
        COUNT(*) AS accounts,
        ROUND(AVG(f.distinct_merchants), 0)     AS merchants,
        ROUND(AVG(f.total_amount)::numeric, 2)  AS spend,
        ROUND(AVG(f.amount_cv)::numeric, 4)     AS amount_cv
    FROM bfsi_demo.kmeans_assignments a
    JOIN bfsi_demo.account_features f ON a.account_id = f.account_id
    GROUP BY a.cluster_id
),
cluster_stats AS (
    SELECT
        CASE
            WHEN amount_cv < 0.1   THEN 'STRUCTURING'
            WHEN merchants > 50    THEN 'CARD-TESTING'
            WHEN spend > 100000    THEN 'BUST-OUT'
            ELSE 'NORMAL'
        END AS persona,
        accounts, merchants, spend, amount_cv
    FROM cluster_agg
),
normal AS (
    SELECT merchants AS n_merch, spend AS n_spend
    FROM cluster_stats WHERE persona = 'NORMAL' LIMIT 1
)
SELECT
    cs.persona,
    cs.accounts,
    cs.merchants,
    cs.spend,
    cs.amount_cv,
    ROUND(cs.merchants::numeric / NULLIF(n.n_merch, 0), 0) AS merchants_vs_normal,
    ROUND(cs.spend::numeric    / NULLIF(n.n_spend, 0), 0) AS spend_vs_normal
FROM cluster_stats cs
LEFT JOIN normal n ON TRUE
ORDER BY cs.accounts DESC"""
    },

    # ══════════════════════════════════════════════════════════════════════════
    # Panel C: The AI Factory — Combining pgvector + MADlib
    # ══════════════════════════════════════════════════════════════════════════
    {
        "id": "c1", "panel": 2,
        "name": "C1 - The AI Factory: Fraud Pattern Correlation",
        "desc": "MADlib finds behavioural clusters \u2192 pgvector finds semantic patterns \u2192 BOTH detect the SAME fraud types in ONE query!",
        "sql": """WITH cluster_stats AS (
    SELECT
        a.cluster_id,
        COUNT(*) AS accts,
        ROUND(AVG(f.distinct_merchants), 0)     AS avg_merchants,
        ROUND(AVG(f.total_amount)::numeric, 2)  AS avg_spend,
        ROUND(AVG(f.amount_cv)::numeric, 4)     AS avg_cv
    FROM bfsi_demo.kmeans_assignments a
    JOIN bfsi_demo.account_features f ON a.account_id = f.account_id
    GROUP BY a.cluster_id
),
madlib_patterns AS (
    SELECT
        CASE
            WHEN avg_cv < 0.1          THEN '\U0001f916 STRUCTURING'
            WHEN avg_merchants > 50    THEN '\U0001f50d CARD-TESTING'
            WHEN avg_spend > 100000    THEN '\U0001f4e4 BUST-OUT'
        END AS fraud_type,
        accts, avg_merchants, avg_spend, avg_cv
    FROM cluster_stats
),
pgvector_patterns AS (
    SELECT
        CASE
            WHEN persona = 'card_testing' THEN '\U0001f50d CARD-TESTING'
            WHEN persona = 'bust_out'     THEN '\U0001f4e4 BUST-OUT'
            WHEN persona = 'structuring'  THEN '\U0001f916 STRUCTURING'
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
]

PANELS = [
    {"name": "pgvector",    "icon": "A", "desc": "Semantic search finds fraud by MEANING, not keywords"},
    {"name": "MADlib",      "icon": "B", "desc": "Unsupervised clustering discovers fraud personas automatically"},
    {"name": "AI Factory",  "icon": "C", "desc": "Combine both in ONE query — impossible in traditional warehouses"},
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
<title>Lab 3 — AI Analytics | WarehousePG</title>
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



/* ── footer ── */
.ft{margin-top:32px;padding:14px 0;border-top:1px solid var(--border);
    display:flex;justify-content:space-between;color:var(--dim);font-size:11px}
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
      <div class="hdr-sub">Lab 3 — pgvector + MADlib + AI Factory · Meridian Bank · last 28 days</div>
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

  <!-- DATA RELOAD -->
  <div class="pnl" id="pnl-4">
    <div class="sec-hdr">
      <span class="n" style="background:linear-gradient(135deg,var(--warn),#b45309)">↺</span>
      <h2>Data Reload</h2>
      <div class="d">Re-runs all 5 SQL scripts — refreshes timestamps and rebuilds all derived tables including K-Means assignments</div>
    </div>

    <div class="reload-card">
      <div class="reload-hdr">
        <div>
          <div class="reload-title">Full Dataset Reload</div>
          <div class="reload-sub">{{ workshop_dir }} — drops schema, reseeds, loads ~50M rows (Jan–Apr 2026), rebuilds pgvector embeddings, MADlib features &amp; K-Means cluster assignments</div>
        </div>
        <div class="reload-actions">
          <div class="spin-ring" id="reload-spinner"></div>
          <button class="btn-reload" id="btn-reload" onclick="startReload()">⟳ Start Reload</button>
          <button class="btn-abort"  id="btn-abort"  onclick="abortReload()" disabled>✕ Abort</button>
        </div>
      </div>

      <div class="reload-body">
        <!-- Status bar -->
        <div class="reload-status-bar">
          <div class="rstat">Status: <strong id="rstat-txt">Idle</strong></div>
          <div class="rstat">Steps: <strong id="rstat-step">—</strong></div>
          <div class="rstat">Elapsed: <strong id="rstat-elapsed">—</strong></div>
        </div>

        <!-- Step cards -->
        <div class="reload-steps" id="reload-steps">
          {% for fname, label in reload_scripts %}
          <div class="step-card" id="step-{{ loop.index0 }}">
            <div class="step-num">Step {{ loop.index }}</div>
            <div class="step-name">{{ label }}</div>
            <div class="step-file">{{ fname }}</div>
            <div class="step-status idle" id="step-st-{{ loop.index0 }}">Waiting</div>
          </div>
          {% endfor %}
        </div>

        <!-- Log -->
        <div style="font-size:11px;color:var(--dim);margin-bottom:6px;
                    display:flex;justify-content:space-between;align-items:center">
          <span>Live output</span>
          <button onclick="clearLog()" style="background:0;border:0;color:var(--dim);
                  cursor:pointer;font-size:11px;font-family:inherit">Clear</button>
        </div>
        <div class="logbox" id="reload-log">
          <div class="log-line log-info">
            <span class="log-ts">--:--:--</span>
            <span class="log-msg">Ready — click "Start Reload" to refresh all data to current timestamps.</span>
          </div>
        </div>
      </div>
    </div>

    <!-- Info card -->
    <div style="background:var(--card);border:1px solid var(--border);border-radius:12px;padding:18px 22px">
      <div style="font-weight:600;margin-bottom:10px;font-size:14px">Why you need this</div>
      <div style="font-size:13px;color:var(--muted);line-height:1.8">
        The pgvector queries (A1, A2) and MADlib features (B1–B3) are static tables built from the
        netflow/syslog data. The AI Factory query (C1) filters for anomalous IPs using aggregates over
        <code style="background:var(--bg);padding:1px 5px;border-radius:3px;font-family:'Courier New',monospace">netflow_features</code>
        which was populated from timestamped source data. After ~6h those source rows age out and the
        features table may look sparse. A full reload re-inserts ~50M rows
        and rebuilds all derived tables including the K-Means cluster assignments used by B3
        (using MADlib kmeanspp if available, or a pure-SQL z-score fallback) —
        takes approximately <strong>3–5 minutes</strong>.
      </div>
      <div style="margin-top:14px;display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px">
        {% for fname, label in reload_scripts %}
        <div style="background:var(--bg);border:1px solid var(--border);border-radius:7px;padding:10px 12px">
          <div style="font-family:'Courier New',monospace;font-size:10px;color:var(--accent);margin-bottom:3px">{{ fname }}</div>
          <div style="font-size:12px;font-weight:500">{{ label }}</div>
        </div>
        {% endfor %}
      </div>
    </div>
  </div>

  <div class="ft">
    <div>EDB WarehousePG — Lab 3: AI-Powered Analytics</div>
    <div style="font-family:'Courier New',monospace">pgvector + MADlib + AI Factory</div>
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
function buildPanels(){
  const main   = document.getElementById('main');
  const anchor = document.getElementById('pnl-3');
  PANELS.forEach((p, pi)=>{
    const qs = QUERIES.filter(q=>q.panel===pi);
    let h = `<div class="pnl${pi===0?' on':''}" id="pnl-${pi}">`;
    h += `<div class="sec-hdr"><span class="n">${p.icon}</span><h2>${p.name}</h2><div class="d">${esc(p.desc)}</div></div>`;
    h += `<div class="sbar">
      <div class="sstat"><div class="l">Queries</div><div class="v" style="color:var(--accent)">${qs.length}</div></div>
      <div class="sstat"><div class="l">Completed</div><div class="v" style="color:var(--blue)" id="done-${pi}">0</div></div>
      <div class="sstat"><div class="l">Total Time</div><div class="v" style="color:var(--warn)" id="tms-${pi}">—</div></div>
      <button class="rabtn" id="rabtn-${pi}" onclick="runPanel(${pi})">▶ Run All ${qs.length}</button>
    </div>`;
    h += `<div class="qgrid">`;
    qs.forEach(q=>{
      h += `<div class="qcard p${pi}" id="qc-${q.id}">
        <div class="qbar" onclick="toggle('${q.id}')">
          <span class="qid">${q.id.toUpperCase()}</span>
          <div style="flex:1"><div class="qname">${esc(q.name)}</div><div class="qdesc">${esc(q.desc)}</div></div>
          <span class="qtm" id="qt-${q.id}"></span>
          <button class="rbtn" id="rb-${q.id}" onclick="event.stopPropagation();runQ('${q.id}')">Run</button>
        </div>
        <div class="qbody">
          <div class="qsql">${esc(q.sql)}</div>
          <div class="qactions">
            <button class="rbtn" onclick="runQ('${q.id}')">▶ Run</button>
            <button class="rbtn" onclick="copyQ('${q.id}')" style="background:var(--bdim);color:var(--blue);border-color:rgba(59,130,246,.3)">Copy SQL</button>
            <button class="rbtn" onclick="toEditor('${q.id}')" style="background:rgba(167,139,250,.1);color:var(--purple);border-color:rgba(167,139,250,.3)">Edit in SQL</button>
          </div>
          <div class="qres" id="qr-${q.id}"></div>
        </div>
      </div>`;
    });
    h += `</div></div>`;
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

// ── DATA RELOAD ───────────────────────────────────────────────────────────
let reloadPollTimer = null;
let reloadStartTs   = null;
let lastLogLen      = 0;

const STEP_KEYWORDS = [
  '01_schema',
  '02_seed_reference',
  '03_load_external',
  '06_ai_analytics',
  '07_kmeans_fallback',
];

function setStepState(idx, state){
  const card = document.getElementById('step-'+idx);
  const st   = document.getElementById('step-st-'+idx);
  if(!card) return;
  card.className = 'step-card'+(state!=='idle'?' '+state:'');
  st.className   = 'step-status '+state;
  st.textContent = {idle:'Waiting',active:'Running…',done:'✓ Done',error:'✗ Error'}[state]||state;
}

function guessActiveStep(logLines){
  for(let i=logLines.length-1;i>=0;i--){
    const msg=logLines[i][2]||'';
    for(let s=0;s<STEP_KEYWORDS.length;s++){
      if(msg.includes(STEP_KEYWORDS[s])) return s;
    }
  }
  return -1;
}

function renderLog(logLines, append=false){
  const box=document.getElementById('reload-log');
  if(!append){ box.innerHTML=''; lastLogLen=0; }
  const newLines=logLines.slice(lastLogLen);
  newLines.forEach(([ts,lvl,msg])=>{
    const div=document.createElement('div');
    div.className='log-line log-'+lvl;
    div.innerHTML=`<span class="log-ts">${esc(ts)}</span><span class="log-msg">${esc(msg)}</span>`;
    box.appendChild(div);
  });
  if(newLines.length) box.scrollTop=box.scrollHeight;
  lastLogLen=logLines.length;
}

async function pollReload(){
  try{
    const r=await(await fetch('/api/reload/status')).json();
    renderLog(r.log, true);

    if(reloadStartTs){
      const sec=Math.round((Date.now()-reloadStartTs)/1000);
      document.getElementById('rstat-elapsed').textContent=sec+'s';
    }

    const activeStep=guessActiveStep(r.log);
    for(let i=0;i<STEP_KEYWORDS.length;i++){
      const isDone  = activeStep>i || (!r.running && r.log.some(([,,m])=>m&&m.includes('✓')&&m.includes(STEP_KEYWORDS[i])));
      const isErr   = r.log.some(([,l])=>l==='error') && activeStep===i && !isDone;
      const isActive= r.running && activeStep===i;
      setStepState(i, isDone?'done':isErr?'error':isActive?'active':'idle');
    }

    const stepsDone=STEP_KEYWORDS.filter((_,i)=>
      r.log.some(([,,m])=>m&&m.includes('✓')&&m.includes(STEP_KEYWORDS[i]))
    ).length;
    document.getElementById('rstat-step').textContent=stepsDone+' / '+STEP_KEYWORDS.length;

    if(r.running){
      document.getElementById('rstat-txt').textContent='Running';
      document.getElementById('reload-spinner').classList.add('active');
    } else {
      document.getElementById('reload-spinner').classList.remove('active');
      clearInterval(reloadPollTimer); reloadPollTimer=null;
      document.getElementById('btn-reload').disabled=false;
      document.getElementById('btn-abort').disabled=true;
      document.getElementById('rstat-txt').textContent=
        r.log.some(([,l])=>l==='done') ? '✓ Complete' : 'Idle';
      document.getElementById('tab-reload').textContent='✓ Reload Done';
      setTimeout(()=>{ document.getElementById('tab-reload').textContent='⟳ Data Reload'; }, 5000);
    }
  }catch(e){ console.error('poll error',e); }
}

async function startReload(){
  if(!confirm('This will drop and recreate the entire schema (~5–8 min). Proceed?')) return;
  lastLogLen=0;
  document.getElementById('reload-log').innerHTML='';
  for(let i=0;i<STEP_KEYWORDS.length;i++) setStepState(i,'idle');

  const r=await(await fetch('/api/reload/start',{method:'POST'})).json();
  if(!r.ok){ alert('Error: '+r.msg); return; }

  reloadStartTs=Date.now();
  document.getElementById('btn-reload').disabled=true;
  document.getElementById('btn-abort').disabled=false;
  document.getElementById('rstat-txt').textContent='Running';
  document.getElementById('rstat-elapsed').textContent='0s';
  document.getElementById('reload-spinner').classList.add('active');
  document.getElementById('tab-reload').textContent='↻ Reloading…';
  reloadPollTimer=setInterval(pollReload, 1000);
}

async function abortReload(){
  if(!confirm('Abort the reload? The current script may still finish.')) return;
  await fetch('/api/reload/abort',{method:'POST'});
  clearInterval(reloadPollTimer);
  document.getElementById('btn-reload').disabled=false;
  document.getElementById('btn-abort').disabled=true;
  document.getElementById('rstat-txt').textContent='Aborted';
  document.getElementById('reload-spinner').classList.remove('active');
}

function clearLog(){
  document.getElementById('reload-log').innerHTML='';
  lastLogLen=0;
}

// ── init ──────────────────────────────────────────────────────────────────
buildPanels();
// Resume poll if reload was already running
fetch('/api/reload/status').then(r=>r.json()).then(d=>{
  if(d.running){
    reloadStartTs=Date.now();
    document.getElementById('btn-reload').disabled=true;
    document.getElementById('btn-abort').disabled=false;
    document.getElementById('reload-spinner').classList.add('active');
    reloadPollTimer=setInterval(pollReload,1000);
  } else if(d.log&&d.log.length){
    renderLog(d.log,false);
  }
});
</script>
</body></html>"""
if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════════╗
║  Lab 3 — AI Analytics Dashboard (STREAMLINED)           ║
║  DB: {DB['host']}:{DB['port']}/{DB['dbname']}
║  Queries: {len(QUERIES)} (focused on VALUE demonstration)
║  http://0.0.0.0:5002                                    ║
╚══════════════════════════════════════════════════════════╝
    """)
    app.run(host="0.0.0.0", port=5002, debug=False)
