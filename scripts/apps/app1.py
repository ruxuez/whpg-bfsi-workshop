#!/usr/bin/env python3
"""
NetVista × EDB WarehousePG — Network Analytics Demo (Workshop Edition)
TRIMMED: 7 queries across 3 panels (was 12/4) — paced for a 45-min lab slot.

What changed vs the original:
  Panel 1 (Network Traffic):     1A  +  1C        (dropped 1B z-score)
  Panel 2 (Log Analytics):       2A  +  2C        (dropped 2B suspicious DNS)
  Panel 3 (IPAM/SLA + Forensic): 3C  +  4B        (dropped 3A, 3B, 4A, 4C; 4B promoted)
  Panel 4: removed entirely
  + a new "Comprehension Check" tab with 2 discussion questions

SETUP:
    pip3 install flask psycopg2-binary
    export WHPG_HOST=localhost WHPG_PORT=5432 WHPG_DB=gpadmin WHPG_USER=gpadmin
    python3 app.py

Then:  ssh -L 5001:localhost:5001 ec2-user@<ec2-ip>  →  http://localhost:5001
"""

import os, time, decimal, json, subprocess, threading
from datetime import datetime, date
from flask import Flask, render_template_string, jsonify, request, Response, stream_with_context
import psycopg2, psycopg2.extras

app = Flask(__name__)

DB = {
    "host":     os.environ.get("WHPG_HOST", "localhost"),
    "port":     int(os.environ.get("WHPG_PORT", 5432)),
    "dbname":   os.environ.get("WHPG_DB",   "demo"),
    "user":     os.environ.get("WHPG_USER", "gpadmin"),
    "password": os.environ.get("WHPG_PASS", ""),
}

# Schema where the workshop tables live. Override with WHPG_SCHEMA=...
# if the lab loaded into a different schema name.
SCHEMA = os.environ.get("WHPG_SCHEMA", "netvista_demo")

# ── Reload scripts (in order) ───────────────────────────────────────────────
WORKSHOP_DIR = os.environ.get("WORKSHOP_DIR", "/scripts/sql/")
RELOAD_SCRIPTS = [
    ("01_schema.sql",            "Drop & recreate schema"),
    ("02_seed_reference.sql",    "Seed reference tables"),
    ("03_load_external.sql",     "Seed traffic data (~50M rows, Jan-Apr 2026)"),
    ("06_ai_analytics.sql",      "Build AI / pgvector analytics"),
    ("07_kmeans_fallback.sql",   "K-Means assignments (MADlib or SQL fallback)"),
]


# ── DB helper ───────────────────────────────────────────────────────────────
def run(sql, params=None):
    # Set search_path via connection options — this applies BEFORE any query
    # runs and survives MPP dispatch; avoids races with autocommit + cur.execute.
    conn = psycopg2.connect(
        options=f'-c search_path={SCHEMA},public',
        **DB,
    )
    conn.set_session(autocommit=True)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        t0 = time.perf_counter()
        cur.execute(sql, params)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        rows = []
        for row in cur.fetchall():
            r = {}
            for k, v in row.items():
                if isinstance(v, (datetime, date)): r[k] = v.isoformat()
                elif isinstance(v, decimal.Decimal): r[k] = float(v)
                elif v is None: r[k] = None
                elif isinstance(v, (int, float, bool)): r[k] = v
                else: r[k] = str(v)
            rows.append(r)
        return {"data": rows, "ms": ms, "rows": len(rows)}
    except Exception as e:
        # Helpful diagnostic when the schema/tables are missing
        msg = str(e)
        if "does not exist" in msg and "relation" in msg:
            msg += (f"  [hint: search_path is set to '{SCHEMA},public'. "
                    f"Verify schema exists: \\dn in psql, "
                    f"or set WHPG_SCHEMA env var to the correct name.]")
        return {"data": [], "ms": 0, "rows": 0, "error": msg}
    finally:
        conn.close()


# ── Diagnostic endpoint: confirm schema + table visibility ──────────────────
def diagnose_schema():
    """Returns what the app sees: current schema, search_path, and table list."""
    try:
        conn = psycopg2.connect(
            options=f'-c search_path={SCHEMA},public',
            **DB,
        )
        conn.set_session(autocommit=True)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SHOW search_path;")
        sp = cur.fetchone()
        cur.execute("""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog','information_schema','pg_toast')
            ORDER BY schemaname, tablename;
        """)
        tables = cur.fetchall()
        conn.close()
        return {
            "configured_schema": SCHEMA,
            "active_search_path": dict(sp).get("search_path"),
            "tables_visible": [dict(t) for t in tables],
            "ok": True,
        }
    except Exception as e:
        return {"configured_schema": SCHEMA, "ok": False, "error": str(e)}


# ── 7 curated queries (trimmed from 12) ──────────────────────────────────────
QUERIES = [
    # ── Panel 1: Network Traffic ─────────────────────────────────────────────
    {
        "id": "1a", "panel": 0,
        "name": "1A · Threat Intel Match",
        "desc": "Native inet <<= join — 6 LOC vs 52 on Snowflake",
        "sql": """SELECT n.src_ip::text, t.feed_name, t.category, t.confidence,
    COUNT(*) AS hit_count, SUM(n.bytes) AS total_bytes,
    MIN(n.ts) AS first_seen, MAX(n.ts) AS last_seen
FROM netflow_logs n
JOIN threat_intel_feeds t ON n.src_ip <<= t.ip_range
WHERE n.ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
  AND t.active = TRUE AND t.confidence >= 80
GROUP BY 1, 2, 3, 4
ORDER BY hit_count DESC LIMIT 20"""
    },
    {
        "id": "1c", "panel": 0,
        "name": "1C · Top Talkers by Subnet",
        "desc": "Dynamic /24 grouping with set_masklen() — impossible on Snowflake",
        "sql": """SELECT network(set_masklen(src_ip, 24)) AS src_subnet,
    COUNT(*) AS flows, SUM(bytes) AS total_bytes,
    COUNT(DISTINCT dst_ip) AS unique_destinations
FROM netflow_logs WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
GROUP BY 1 ORDER BY total_bytes DESC LIMIT 15"""
    },

    # ── Panel 2: Log Analytics ────────────────────────────────────────────────
    {
        "id": "2a", "panel": 1,
        "name": "2A · Cross-Source Correlation",
        "desc": "syslog + firewall + DNS in one query — replaces Splunk",
        "sql": """SELECT s.ts AS event_time, s.src_ip::text, s.hostname, s.program,
    LEFT(s.message, 80) AS syslog_msg,
    f.action AS fw_action, f.dst_port AS fw_port,
    d.query_name AS dns_query, d.response_code AS dns_rcode
FROM syslog_events s
JOIN firewall_logs f ON s.src_ip = f.src_ip
    AND f.ts BETWEEN s.ts - interval '5 seconds' AND s.ts + interval '5 seconds'
LEFT JOIN dns_logs d ON s.src_ip = d.client_ip
    AND d.ts BETWEEN s.ts - interval '10 seconds' AND s.ts + interval '10 seconds'
WHERE s.severity <= 2 AND s.ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
ORDER BY s.ts DESC LIMIT 30"""
    },
    {
        "id": "2c", "panel": 1,
        "name": "2C · Log Volume Dashboard",
        "desc": "All 5 sources — $2M+ Splunk savings",
        "sql": """SELECT 'netflow' AS source, COUNT(*) AS events
    FROM netflow_logs WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
UNION ALL SELECT 'dns', COUNT(*)
    FROM dns_logs WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
UNION ALL SELECT 'firewall', COUNT(*)
    FROM firewall_logs WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
UNION ALL SELECT 'syslog', COUNT(*)
    FROM syslog_events WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
UNION ALL SELECT 'bgp', COUNT(*)
    FROM bgp_events WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
ORDER BY events DESC"""
    },

    # ── Panel 3: IPAM, SLA & Forensic Bonus ──────────────────────────────────
    {
        "id": "3c", "panel": 2,
        "name": "3C · QoE Scorecard",
        "desc": "Per-customer quality scoring — worst-first for churn prevention",
        "sql": """SELECT c.customer_name, c.tier, r.region_code,
    ROUND(AVG(m.latency_ms), 1) AS avg_latency,
    ROUND(AVG(m.jitter_ms), 1) AS avg_jitter,
    ROUND(AVG(m.packet_loss_pct), 2) AS avg_loss,
    ROUND(AVG(m.mos_score), 1) AS avg_mos,
    netvista_demo.calc_qoe_score(AVG(m.latency_ms), AVG(m.jitter_ms), AVG(m.packet_loss_pct)) AS qoe_score,
    sc.latency_sla_ms
FROM customers c
JOIN sla_contracts sc ON c.customer_id = sc.customer_id AND sc.effective_to IS NULL
JOIN regions r ON c.region_id = r.region_id
JOIN network_metrics m ON c.customer_id = m.customer_id AND m.ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
GROUP BY 1, 2, 3, 9
ORDER BY qoe_score ASC"""
    },
    {
        "id": "4b", "panel": 2,
        "name": "★ BONUS · Forensic IP Trace",
        "desc": "Trace 185.220.101.34 across ALL log sources in one query",
        "sql": """SELECT * FROM (
    (SELECT 'netflow' AS source, ts, 'src→' || host(dst_ip) || ':' || dst_port AS detail, bytes::text AS extra
        FROM netflow_logs WHERE src_ip = '185.220.101.34'::inet AND ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
        ORDER BY ts DESC LIMIT 15)
    UNION ALL
    (SELECT 'firewall', ts, action || ' ' || host(dst_ip) || ':' || dst_port, zone_src || '→' || zone_dst
        FROM firewall_logs WHERE src_ip = '185.220.101.34'::inet AND ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
        ORDER BY ts DESC LIMIT 15)
    UNION ALL
    (SELECT 'dns', ts, query_name || ' (' || query_type || ')', response_code
        FROM dns_logs WHERE client_ip = '185.220.101.34'::inet AND ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
        ORDER BY ts DESC LIMIT 15)
    UNION ALL
    (SELECT 'syslog', ts, LEFT(message, 80), hostname
        FROM syslog_events WHERE src_ip = '185.220.101.34'::inet AND ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59'
        ORDER BY ts DESC LIMIT 15)
) forensic ORDER BY ts DESC LIMIT 40"""
    },
]

PANELS = [
    {"name": "Network Traffic", "icon": "1", "desc": "Native inet operators on 31M+ netflow rows"},
    {"name": "Log Analytics",   "icon": "2", "desc": "Cross-source correlation — replace Splunk"},
    {"name": "IPAM/SLA + Forensic Bonus", "icon": "3", "desc": "Customer QoE scoring + multi-source IP trace"},
]

# ── Comprehension check questions (rendered as a tab in the dashboard) ──────
CHECK_QUESTIONS = [
    {
        "kind": "concept",
        "title": "Why was src_ip <<= ip_range fast on 33M flows?",
        "ask": "You ran the threat-intel join in roughly 1 second. Name three things that made that fast — that wouldn't be true on single-node Postgres or on Snowflake.",
        "listen": "Native inet/cidr type (no string parsing) · MPP parallelism across all segments · no UDF overhead — the operator is built into the planner",
    },
    {
        "kind": "practical",
        "title": "If we changed netflow_logs' distribution key…",
        "ask": "Today netflow_logs is DISTRIBUTED BY (region_id). If we re-distributed it by src_ip instead, which Lab 1 query gets faster, and which gets slower?",
        "listen": "Faster: 1A threat-intel join — rows now co-located by src_ip, no Motion needed. Slower: 1C top talkers by subnet — the GROUP BY no longer aligns with distribution, forcing a Redistribute.",
    },
]


# ── API routes ───────────────────────────────────────────────────────────────

@app.route("/api/queries")
def api_queries():
    return jsonify({"panels": PANELS, "queries": QUERIES, "check": CHECK_QUESTIONS})


@app.route("/api/diag")
def api_diag():
    """Visit /api/diag to see what schema/tables the app can see."""
    return jsonify(diagnose_schema())


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
        results.append({"id": q["id"], "name": q["name"], "ms": r["ms"],
                        "rows": r["rows"], "error": r.get("error")})
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


@app.route("/")
def index():
    return render_template_string(HTML, panels=PANELS, queries=QUERIES,
                                  check=CHECK_QUESTIONS,
                                  reload_scripts=RELOAD_SCRIPTS,
                                  workshop_dir=WORKSHOP_DIR)


# ── HTML template ────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NetVista × WarehousePG</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 256 256'%3E%3Cg transform='translate(-862,18)'%3E%3Cpath fill='%232a9993' d='M1060.7,2.12c-30.98,2.37-56.03,27.09-58.74,58.06-2.88,33.35,19.98,61.96,50.96,68.22v61.62c0,2.54-2.03,4.4-4.4,4.4h-16.76c-2.54,0-4.4-2.03-4.4-4.4v-44.35c0-7.28-5.92-13.37-13.37-13.37h-49.94c-7.28,0-13.37,5.92-13.37,13.37v44.35c0,2.54-2.03,4.4-4.4,4.4h-16.76c-2.37,0-4.4-2.03-4.4-4.4v-97.17l73.47-73.47c1.69-1.69,1.69-4.57,0-6.26l-11.85-11.85c-1.69-1.69-4.57-1.69-6.26,0l-125.27,125.27c-1.69,1.69-1.69,4.57,0,6.26l11.85,11.85c1.69,1.69,4.57,1.69,6.26,0l26.07-26.24v88.37c0,7.28,5.92,13.37,13.37,13.37h50.11c7.28,0,13.37-5.92,13.37-13.37v-44.35c0-2.54,2.03-4.4,4.4-4.4h16.76c2.37,0,4.4,2.03,4.4,4.4v44.35c0,7.28,5.92,13.37,13.37,13.37h50.11c7.28,0,13.37-5.92,13.37-13.37v-98.19c0-2.54-2.03-4.4-4.4-4.4h-7.45c-20.99,0-38.77-16.59-39.27-37.58-.51-21.67,17.44-39.61,39.11-39.1,20.99.34,37.58,18.28,37.58,39.27v123.41c0,2.54,2.03,4.4,4.4,4.4h16.76c2.54,0,4.4-2.03,4.4-4.4v-124.26c-.17-36.9-31.49-66.53-69.07-63.82Z'/%3E%3Ccircle fill='%232a9993' cx='1065.61' cy='65.94' r='12.7'/%3E%3C/g%3E%3C/svg%3E">
<style>
:root{
  --bg:#f0f2f5;--card:#ffffff;--border:#d1d5db;--text:#1e293b;--dim:#6b7280;
  --muted:#4b5563;--accent:#059669;--adim:rgba(6,214,160,.12);
  --warn:#d97706;--wdim:rgba(251,191,36,.1);--danger:#ef4444;--ddim:rgba(239,68,68,.1);
  --blue:#2563eb;--bdim:rgba(59,130,246,.1);--purple:#7c3aed;--cyan:#0891b2;
}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Outfit',system-ui,sans-serif}

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
            font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace;
            animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.55}}
.dot-live{width:8px;height:8px;border-radius:50%;background:#34d399;
          box-shadow:0 0 6px #059669;display:inline-block}
#ttl{color:#e2e8f0;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600}

/* ── tabs ── */
.tabs{background:#f8fafc;border-bottom:1px solid var(--border);
      padding:8px 28px;display:flex;gap:4px;overflow-x:auto;position:sticky;top:54px;z-index:200}
.tab{background:0;border:1px solid transparent;color:var(--muted);padding:8px 16px;
     border-radius:7px;cursor:pointer;font-size:13px;font-family:inherit;
     transition:.18s;white-space:nowrap;font-weight:500}
.tab:hover{color:var(--text);background:rgba(0,0,0,.04)}
.tab.on{background:var(--adim);border-color:rgba(6,214,160,.3);color:var(--accent);font-weight:600}
.tab.check-tab{border-color:rgba(217,119,6,.3);color:var(--warn)}
.tab.check-tab.on{background:var(--wdim);border-color:rgba(217,119,6,.5);color:var(--warn)}

/* ── layout ── */
.main{padding:24px 28px;max-width:1440px;margin:0 auto}
.pnl{display:none}.pnl.on{display:block;animation:fi .25s ease}
@keyframes fi{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:none}}

/* ── section header ── */
.sec-hdr{margin-bottom:20px}
.sec-hdr .n{background:linear-gradient(135deg,var(--accent),var(--cyan));color:#fff;
            width:30px;height:30px;border-radius:7px;display:inline-flex;align-items:center;
            justify-content:center;font-size:13px;font-weight:800;
            font-family:'JetBrains Mono',monospace;margin-right:10px;vertical-align:middle}
.sec-hdr h2{display:inline;font-size:20px;font-weight:700;vertical-align:middle}
.sec-hdr .d{color:var(--dim);font-size:13px;margin-top:4px;margin-left:40px}

/* ── summary bar ── */
.sbar{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:18px;padding:14px 18px;
      background:var(--card);border:1px solid var(--border);border-radius:11px;align-items:center}
.sstat{text-align:center;min-width:80px}
.sstat .l{color:var(--dim);font-size:10px;text-transform:uppercase;letter-spacing:.5px}
.sstat .v{font-size:22px;font-weight:700;font-family:'JetBrains Mono',monospace}
.rabtn{background:linear-gradient(135deg,var(--purple),var(--blue));color:#fff;border:0;
       padding:10px 22px;border-radius:7px;font-size:13px;font-weight:700;
       cursor:pointer;font-family:inherit;margin-left:auto;transition:.15s}
.rabtn:hover{opacity:.85}.rabtn:disabled{opacity:.4;cursor:wait}

/* ── query grid ── */
.qgrid{display:grid;grid-template-columns:1fr;gap:12px;margin-bottom:24px}
.qcard{background:var(--card);border:1px solid var(--border);border-radius:11px;
       box-shadow:0 1px 3px rgba(0,0,0,.07);overflow:hidden;transition:.18s}
.qcard:hover{border-color:rgba(6,214,160,.3)}
.qbar{display:flex;align-items:center;gap:10px;padding:13px 16px;cursor:pointer}
.qid{min-width:32px;height:24px;border-radius:5px;display:flex;align-items:center;
     justify-content:center;font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:700}
.p0 .qid{background:var(--adim);color:var(--accent)}
.p1 .qid{background:var(--bdim);color:var(--blue)}
.p2 .qid{background:rgba(167,139,250,.15);color:var(--purple)}
.qname{flex:1;font-size:14px;font-weight:500}
.qdesc{color:var(--dim);font-size:11px}
.qtm .t{background:var(--adim);color:var(--accent);padding:2px 8px;border-radius:4px;
        font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace}
.qtm .t.slow{background:var(--wdim);color:var(--warn)}
.qtm .r{color:var(--dim);margin-left:6px;font-size:11px;font-family:'JetBrains Mono',monospace}
.rbtn{background:var(--adim);color:var(--accent);border:1px solid rgba(6,214,160,.3);
      padding:5px 12px;border-radius:5px;cursor:pointer;font-family:'JetBrains Mono',monospace;
      font-size:11px;font-weight:600;transition:.15s}
.rbtn:hover{background:rgba(6,214,160,.22)}.rbtn:disabled{opacity:.4;cursor:wait}
.qbody{display:none;padding:0 16px 16px}.qcard.open .qbody{display:block}
.qsql{background:#f8fafc;border-radius:6px;padding:10px 12px;
      font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--muted);
      line-height:1.55;max-height:180px;overflow:auto;margin-bottom:10px;
      white-space:pre-wrap;word-break:break-all}
.qactions{display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap}
.qres{overflow-x:auto;max-height:380px;overflow-y:auto;border-radius:6px}

/* ── table ── */
table{width:100%;border-collapse:collapse;font-size:12px;font-family:'JetBrains Mono',monospace}
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
       border-radius:8px;color:var(--text);font-family:'JetBrains Mono',monospace;
       font-size:13px;padding:14px;resize:vertical;line-height:1.6}
.sqled:focus{outline:0;border-color:var(--accent)}
.runbtn{background:linear-gradient(135deg,var(--accent),var(--cyan));color:#fff;border:0;
        padding:10px 22px;border-radius:7px;font-size:13px;font-weight:700;
        cursor:pointer;font-family:inherit;margin-top:10px;transition:.15s}
.runbtn:hover{opacity:.85}

/* ── COMPREHENSION CHECK PANEL ── */
.check-intro{background:linear-gradient(135deg,#fef3c7,#fde68a);border:1px solid rgba(217,119,6,.3);
             border-radius:11px;padding:18px 22px;margin-bottom:20px}
.check-intro h2{color:#92400e;font-size:18px;margin-bottom:6px}
.check-intro p{color:#78350f;font-size:13px;line-height:1.55}
.check-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:24px}
@media(max-width:900px){.check-grid{grid-template-columns:1fr}}
.qcheck{background:var(--card);border:1px solid var(--border);border-radius:11px;overflow:hidden}
.qcheck .head{padding:12px 18px;color:#fff;font-weight:700;font-size:12px;letter-spacing:.5px;text-transform:uppercase}
.qcheck.concept .head{background:var(--accent)}
.qcheck.practical .head{background:#1A4767}
.qcheck .body{padding:18px 22px}
.qcheck .ttl{font-size:15px;font-weight:600;color:#1A4767;margin-bottom:12px;line-height:1.4}
.qcheck .ttl code{background:#eef2f4;padding:2px 7px;border-radius:3px;font-family:'JetBrains Mono',monospace;color:#c0392b;font-size:13px}
.qcheck .ask{font-size:13px;color:var(--text);margin-bottom:14px;line-height:1.6}
.qcheck .ask code{background:#eef2f4;padding:1px 6px;border-radius:3px;font-family:'JetBrains Mono',monospace;color:#c0392b;font-size:11.5px}
.qcheck .reveal-btn{background:var(--bdim);color:var(--blue);border:1px solid rgba(59,130,246,.3);
                    padding:7px 14px;border-radius:6px;cursor:pointer;font-family:inherit;
                    font-size:12px;font-weight:600;transition:.15s}
.qcheck .reveal-btn:hover{background:rgba(59,130,246,.18)}
.qcheck .listen{display:none;margin-top:14px;padding:14px 16px;background:#eef2f4;border-left:3px solid var(--accent);border-radius:0 6px 6px 0;font-size:12px;color:var(--muted);line-height:1.6;font-style:italic}
.qcheck .listen.on{display:block}
.qcheck .listen b{color:#1A4767;font-style:normal}
.qcheck .listen-label{font-size:10px;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;font-style:normal}

/* ── footer ── */
.ft{margin-top:32px;padding:14px 0;border-top:1px solid var(--border);
    display:flex;justify-content:space-between;color:var(--dim);font-size:11px}
</style>
</head>
<body>

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
      <h1>WarehousePG <span>Network Analytics</span></h1>
      <div class="hdr-sub">NetVista × EDB — Live on WHPG · ~50M rows · Jan–Apr 2026</div>
    </div>
  </div>
  <div class="hdr-right">
    <span class="dot-live"></span>
    <div class="live-badge">LIVE</div>
    <div id="ttl"></div>
  </div>
</div>

<!-- TABS -->
<div class="tabs" id="tabs">
  <button class="tab on" onclick="switchTab(0)">Network Traffic</button>
  <button class="tab"    onclick="switchTab(1)">Log Analytics</button>
  <button class="tab"    onclick="switchTab(2)">IPAM/SLA + Bonus</button>
  <button class="tab check-tab" onclick="switchTab(3)">✓ Check Understanding</button>
  <button class="tab"    onclick="switchTab(4)" style="margin-left:4px;border-color:rgba(167,139,250,.3);color:var(--purple)">SQL Editor</button>
</div>

<div class="main" id="main">

  <!-- Query panels 0-2 injected by JS -->

  <!-- Comprehension Check panel -->
  <div class="pnl" id="pnl-3">
    <div class="check-intro">
      <h2>Check your understanding</h2>
      <p>Two questions to surface what stuck. <strong>Talk to the person next to you</strong> — compare answers, then we'll regroup. Click "Reveal answer" once you've discussed.</p>
    </div>
    <div class="check-grid" id="check-grid"></div>
  </div>

  <!-- SQL EDITOR panel -->
  <div class="pnl" id="pnl-4">
    <div class="sec-hdr">
      <span class="n">Q</span><h2>SQL Editor</h2>
      <div class="d">Run any SELECT against the live 50M row dataset</div>
    </div>
    <textarea class="sqled" id="sqlin" spellcheck="false">
SELECT
    n.nspname,
    c.relname,
    CASE WHEN c.relkind = 'p' THEN 'Partitioned Root' ELSE 'Standard Table' END as type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'netvista_demo'
  AND c.relkind IN ('r', 'p')
  AND c.relispartition = false;
  </textarea>
    <button class="runbtn" onclick="runSQL()">▶ Run Query</button>
    <span id="sqlt" style="margin-left:12px"></span>
    <div style="margin-top:14px;overflow-x:auto;max-height:500px;overflow-y:auto;border-radius:8px" id="sqlr"></div>
  </div>

  <div class="ft">
    <div>EDB WarehousePG — Native network types + MPP parallel engine</div>
    <div style="font-family:'JetBrains Mono',monospace" id="ftr">7 queries · 3 panels</div>
  </div>
</div><!-- /main -->

<script>
const PANELS = {{ panels|tojson }};
const QUERIES = {{ queries|tojson }};
const CHECK = {{ check|tojson }};
const results = {};
let activeTab = 0;

// ── Clock ──────────────────────────────────────────────────────────────────
function tickClock(){
  const t = new Date().toLocaleTimeString('en-GB',{hour12:false});
  document.getElementById('ttl').textContent = t;
}
tickClock(); setInterval(tickClock, 1000);

// ── Helpers ────────────────────────────────────────────────────────────────
function esc(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function fmt(n){
  if(n==null) return '—';
  n = Number(n);
  if(n>=1e9) return (n/1e9).toFixed(1)+'B';
  if(n>=1e6) return (n/1e6).toFixed(1)+'M';
  if(n>=1e3) return (n/1e3).toFixed(1)+'K';
  return n.toLocaleString();
}
function fmtMs(ms){ return ms < 1000 ? ms+'ms' : (ms/1000).toFixed(1)+'s'; }

function tbl(rows){
  if(!rows || !rows.length) return '<div class="empty">No results — data may need a reload (timestamps expired)</div>';
  const ks = Object.keys(rows[0]);
  let h = '<table><thead><tr>'+ks.map(k=>'<th>'+esc(k)+'</th>').join('')+'</tr></thead><tbody>';
  rows.forEach(r=>{ h += '<tr>'+ks.map(k=>'<td>'+(r[k]!=null?esc(String(r[k])):'—')+'</td>').join('')+'</tr>'; });
  return h+'</tbody></table>';
}

// ── Build query panels (0–2) ───────────────────────────────────────────────
function buildPanels(){
  const main = document.getElementById('main');
  const anchor = document.getElementById('pnl-3');

  PANELS.forEach((p, pi)=>{
    const qs = QUERIES.filter(q=>q.panel===pi);
    let h = `<div class="pnl${pi===0?' on':''}" id="pnl-${pi}">`;
    h += `<div class="sec-hdr"><span class="n">${p.icon}</span><h2>${p.name}</h2><div class="d">${p.desc}</div></div>`;
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

// ── Build comprehension check ──────────────────────────────────────────────
function buildCheck(){
  const grid = document.getElementById('check-grid');
  CHECK.forEach((q, idx)=>{
    const div = document.createElement('div');
    div.className = 'qcheck '+q.kind;
    div.innerHTML = `
      <div class="head">Question ${idx+1} — ${q.kind}</div>
      <div class="body">
        <div class="ttl">${q.title.replace(/`([^`]+)`/g, '<code>$1</code>')}</div>
        <div class="ask">${q.ask.replace(/`([^`]+)`/g, '<code>$1</code>')}</div>
        <button class="reveal-btn" onclick="this.parentElement.querySelector('.listen').classList.toggle('on');this.textContent=this.textContent.includes('Reveal')?'Hide answer':'Reveal answer'">Reveal answer</button>
        <div class="listen"><div class="listen-label">What we're listening for</div>${esc(q.listen).replace(/Faster:|Slower:/g, m=>'<b>'+m+'</b>')}</div>
      </div>`;
    grid.appendChild(div);
  });
}

function switchTab(i){
  document.querySelectorAll('.tab').forEach((t,j)=>t.classList.toggle('on', j===i));
  document.querySelectorAll('.pnl').forEach((p,j)=>p.classList.toggle('on', j===i));
  activeTab = i;
}

function toggle(id){ document.getElementById('qc-'+id)?.classList.toggle('open'); }

// ── Run query ──────────────────────────────────────────────────────────────
async function runQ(id){
  const btn = document.getElementById('rb-'+id);
  btn.disabled=true; btn.textContent='…';
  document.getElementById('qr-'+id).innerHTML = '<div style="padding:20px;text-align:center"><div class="spinner"></div></div>';
  document.getElementById('qc-'+id).classList.add('open');
  try{
    const r = await(await fetch('/api/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})})).json();
    results[id] = r;
    const slow = r.ms > 5000;
    document.getElementById('qt-'+id).innerHTML =
      `<span class="t${slow?' slow':''}">${fmtMs(r.ms)}</span><span class="r">${r.rows} rows</span>`;
    document.getElementById('qr-'+id).innerHTML = r.error
      ? `<div style="color:var(--danger);padding:12px;font-family:'JetBrains Mono',monospace;font-size:12px">ERROR: ${esc(r.error)}</div>`
      : tbl(r.data);
  } catch(e){
    document.getElementById('qr-'+id).innerHTML = `<div style="color:var(--danger);padding:12px">${e.message}</div>`;
  }
  btn.disabled=false; btn.textContent='Run';
  updatePanel(QUERIES.find(q=>q.id===id).panel);
}

async function runPanel(pi){
  const btn = document.getElementById('rabtn-'+pi);
  const qs = QUERIES.filter(q=>q.panel===pi);
  btn.disabled=true; btn.textContent='Running…';
  for(const q of qs) await runQ(q.id);
  btn.disabled=false; btn.textContent='▶ Run All '+qs.length;
}

function updatePanel(pi){
  const qs = QUERIES.filter(q=>q.panel===pi);
  const done = qs.filter(q=>results[q.id]);
  const ms = done.reduce((s,q)=>s+(results[q.id]?.ms||0),0);
  document.getElementById('done-'+pi).textContent = done.length;
  document.getElementById('tms-'+pi).textContent = done.length ? fmtMs(Math.round(ms)) : '—';
}

function copyQ(id){ navigator.clipboard.writeText(QUERIES.find(q=>q.id===id).sql+';'); }
function toEditor(id){
  document.getElementById('sqlin').value = QUERIES.find(q=>q.id===id).sql+';';
  switchTab(4);
}

// ── SQL editor ─────────────────────────────────────────────────────────────
async function runSQL(){
  const sql = document.getElementById('sqlin').value;
  document.getElementById('sqlr').innerHTML = '<div style="padding:20px;text-align:center"><div class="spinner"></div></div>';
  document.getElementById('sqlt').innerHTML = '';
  try{
    const r = await(await fetch('/api/sql',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql})})).json();
    if(r.error){
      document.getElementById('sqlr').innerHTML = `<div style="color:var(--danger);padding:16px;font-family:'JetBrains Mono',monospace;font-size:12px">ERROR: ${esc(r.error)}</div>`;
      return;
    }
    const slow = r.ms > 5000;
    document.getElementById('sqlt').innerHTML =
      `<span style="background:var(--adim);color:var(--accent);padding:3px 10px;border-radius:4px;font-size:11px;font-family:'JetBrains Mono',monospace;font-weight:600" class="${slow?'slow':''}">${fmtMs(r.ms)}</span>
       <span style="color:var(--dim);font-size:12px;margin-left:6px">${r.rows} rows</span>`;
    document.getElementById('sqlr').innerHTML = tbl(r.data);
  } catch(e){ document.getElementById('sqlr').innerHTML = `<div style="color:var(--danger);padding:16px">${e.message}</div>`; }
}

// ── Row count ──────────────────────────────────────────────────────────────
fetch('/api/sql',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql:
  "SELECT SUM(c)::bigint AS total FROM (SELECT COUNT(*) AS c FROM netflow_logs WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59' UNION ALL SELECT COUNT(*) FROM dns_logs WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59' UNION ALL SELECT COUNT(*) FROM firewall_logs WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59' UNION ALL SELECT COUNT(*) FROM syslog_events WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59' UNION ALL SELECT COUNT(*) FROM bgp_events WHERE ts BETWEEN '2026-04-01' AND '2026-04-23 23:59:59' UNION ALL SELECT COUNT(*) FROM network_metrics) x"
})}).then(r=>r.json()).then(d=>{
  const t = d.data?.[0]?.total;
  if(t){ document.getElementById('ttl').textContent = fmt(t)+' rows'; document.getElementById('ftr').textContent = fmt(t)+' rows · 7 queries'; }
}).catch(()=>{});

// ── Init ───────────────────────────────────────────────────────────────────
buildPanels();
buildCheck();
</script>
</body></html>"""


if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════════════════╗
║  NetVista × WarehousePG — Workshop Edition          ║
║  DB: {DB['host']}:{DB['port']}/{DB['dbname']}
║  Queries: {len(QUERIES)} across {len(PANELS)} panels (trimmed)
║  Data: Jan 1 – Apr 23 2026  (~50M rows)             ║
║  http://0.0.0.0:5001                                ║
╚══════════════════════════════════════════════════════╝
    """)
    app.run(host="0.0.0.0", port=5001, debug=False)
