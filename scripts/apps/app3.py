#!/usr/bin/env python3
"""
PGAA Lab 3 Dashboard - Lakehouse Analytics with Iceberg (Workshop Edition)

Demonstrates querying Iceberg data directly from object storage with good performance:
  1. Query Lakehouse Data  - run analytics queries on Iceberg tables
  2. Performance Results   - see execution times (this is impressive for external data!)
  3. Comprehension Check   - discussion questions with reveal-answer
  4. Challenge             - fill-in-the-blank SQL with solution

USAGE
  pip3 install flask psycopg2-binary
  python3 app3.py                 # default: port 5000
  python3 app3.py --port 5050     # specific port
  PORT=5050 python3 app3.py       # via env var

DB OVERRIDES (env vars):
  WHPG_HOST, WHPG_PORT, WHPG_DB, WHPG_USER, WHPG_PASS
"""

import os, time, argparse, concurrent.futures
import psycopg2
from flask import Flask, jsonify, Response, request

app = Flask(__name__)

DB_CONFIG = {
    'host':     os.environ.get('WHPG_HOST', 'localhost'),
    'port':     int(os.environ.get('WHPG_PORT', 5432)),
    'database': os.environ.get('WHPG_DB',   'demo'),
    'user':     os.environ.get('WHPG_USER', 'gpadmin'),
    'password': os.environ.get('WHPG_PASS', ''),
}


def query(sql):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    ms = round((time.perf_counter() - t0) * 1000, 2)
    cols = [d[0] for d in cur.description] if cur.description else []
    cur.close(); conn.close()
    return {'columns': cols, 'rows': rows, 'row_count': len(rows), 'exec_time_ms': ms}


QUERIES = {
    'revenue': {
        'name': 'Revenue by Category',
        'desc': 'Simple JOIN - sets the baseline',
        'sql': '''SELECT p.category,
       COUNT(DISTINCT oi.order_id)             AS orders,
       SUM(oi.quantity)                        AS units_sold,
       ROUND(SUM(oi.quantity * oi.unit_price)::numeric, 2) AS revenue
FROM   products_iceberg    p
JOIN   order_items_iceberg oi ON p.product_id = oi.product_id
GROUP  BY p.category
ORDER  BY revenue DESC''',
    },
    'top20': {
        'name': 'Top 20 Customers',
        'desc': 'Multi-table JOIN - ranked by spend',
        'sql': '''SELECT c.customer_id,
       c.first_name || ' ' || c.last_name      AS customer,
       COUNT(o.order_id)                       AS orders,
       ROUND(SUM(o.total_amount)::numeric, 2)  AS lifetime_value
FROM   customers_iceberg c
JOIN   orders_iceberg    o ON c.customer_id = o.customer_id
GROUP  BY 1, 2
ORDER  BY lifetime_value DESC
LIMIT  20''',
    },
    'funnel': {
        'name': 'Conversion Funnel',
        'desc': 'CTE on events - view -> cart -> purchase',
        'sql': '''WITH funnel AS (
    SELECT customer_id,
           MAX(CASE WHEN event_type = 'page_view'   THEN 1 ELSE 0 END) AS viewed,
           MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS carted,
           MAX(CASE WHEN event_type = 'purchase'    THEN 1 ELSE 0 END) AS purchased
    FROM   events_iceberg
    GROUP  BY customer_id
)
SELECT SUM(viewed)       AS viewers,
       SUM(carted)       AS cart_adds,
       SUM(purchased)    AS purchases,
       ROUND(100.0 * SUM(carted)    / NULLIF(SUM(viewed), 0), 2) AS view_to_cart_pct,
       ROUND(100.0 * SUM(purchased) / NULLIF(SUM(carted), 0), 2) AS cart_to_buy_pct
FROM   funnel''',
    },
    'summary': {
        'name': 'Executive Summary',
        'desc': '5 parallel COUNT(*) - quick scan-cost check',
        'sql': '''SELECT (SELECT COUNT(*) FROM customers_iceberg)   AS customers,
       (SELECT COUNT(*) FROM products_iceberg)    AS products,
       (SELECT COUNT(*) FROM orders_iceberg)      AS orders,
       (SELECT COUNT(*) FROM order_items_iceberg) AS order_items,
       (SELECT COUNT(*) FROM events_iceberg)      AS events''',
    },
    'daily': {
        'name': 'Daily Dashboard (5-table JOIN)',
        'desc': 'The complex query - biggest perf gap between engines',
        'sql': '''SELECT o.order_date,
       COUNT(DISTINCT o.order_id)                     AS orders,
       ROUND(SUM(o.total_amount)::numeric, 2)         AS revenue,
       COUNT(DISTINCT o.customer_id)                  AS unique_customers,
       SUM(oi.quantity)                               AS units_sold,
       COUNT(*) FILTER (WHERE o.status = 'delivered') AS delivered,
       COUNT(DISTINCT e.session_id)                   AS sessions
FROM   orders_iceberg      o
JOIN   order_items_iceberg oi ON o.order_id    = oi.order_id
JOIN   products_iceberg    p  ON oi.product_id = p.product_id
JOIN   customers_iceberg   c  ON o.customer_id = c.customer_id
LEFT JOIN events_iceberg   e  ON c.customer_id = e.customer_id
                              AND e.event_date = o.order_date
GROUP  BY o.order_date
ORDER  BY o.order_date DESC
LIMIT  30''',
    },
}


CHECK_QUESTIONS = [
    {
        'kind': 'concept',
        'title': 'How does WHPG query Iceberg data without loading it into the database first?',
        'ask': "You just ran SQL directly against Iceberg tables stored in MinIO object storage. "
               "What technology lets WHPG read external lakehouse data as if it were native tables?",
        'listen': "PGAA (Postgres AI & Analytics) exposes Iceberg tables as foreign tables via the foreign data wrapper (FDW) interface. "
                  "The DirectScan vectorized engine reads Parquet files directly from object storage, pushing down predicates and projections. "
                  "The MPP executor distributes the scan across segments in parallel - same query planner, different storage backend.",
    },
    {
        'kind': 'practical',
        'title': 'Why is this performance impressive for external data?',
        'ask': "Traditional databases require ETL to load external data before querying. You just ran complex analytics "
               "queries (multi-table JOINs, aggregations) on data that lives in object storage. Why is this a big deal?",
        'listen': "No ETL overhead - query the lakehouse directly where it lives. PGAA's DirectScan uses columnar vectorized "
                  "execution on Parquet, so you get near-native performance without data movement. Query times in the 2-5 second "
                  "range for complex multi-table JOINs on external data means you can do real analytics on your data lake without "
                  "copying it into the warehouse first. This is lakehouse federation in action.",
    },
]


CHALLENGE = {
    'title': 'Find the top 5 products by revenue',
    'context': "You're handed this query but the JOIN condition is missing. "
               "Fill in the blank to make it run. Hint: products and order_items share one obvious key.",
    'template': '''-- Top 5 products by revenue
SELECT p.product_id,
       p.name        AS product_name,
       p.category,
       SUM(oi.quantity)                                    AS units_sold,
       ROUND(SUM(oi.quantity * oi.unit_price)::numeric, 2) AS revenue
FROM   products_iceberg    p
JOIN   order_items_iceberg oi
       ON   /* FILL IN THE JOIN CONDITION */
GROUP  BY p.product_id, p.name, p.category
ORDER  BY revenue DESC
LIMIT  5''',
    'solution': 'p.product_id = oi.product_id',
    'why_it_matters': "products and order_items share product_id as the natural join key. "
                      "This query runs directly on Iceberg data in object storage (MinIO) - no ETL, no data copying. "
                      "PGAA's DirectScan handles the JOIN across Parquet files with vectorized execution, demonstrating "
                      "that you can run complex analytics on your data lakehouse without moving data into the warehouse.",
}


@app.route('/')
def index():
    return Response(DASHBOARD_HTML, mimetype='text/html')


@app.route('/api/queries')
def list_queries():
    return jsonify({k: {'name': v['name'], 'desc': v['desc'], 'sql': v['sql']}
                    for k, v in QUERIES.items()})


@app.route('/api/check')
def list_check():
    return jsonify(CHECK_QUESTIONS)


@app.route('/api/challenge')
def get_challenge():
    out = {k: v for k, v in CHALLENGE.items() if k != 'solution'}
    out['solution'] = CHALLENGE['solution']
    return jsonify(out)


@app.route('/api/challenge/check', methods=['POST'])
def check_challenge():
    user = (request.json or {}).get('answer', '').lower().replace(' ', '')
    expected = CHALLENGE['solution'].lower().replace(' ', '')
    alt = 'oi.product_id=p.product_id'
    return jsonify({
        'correct': expected in user or alt in user,
        'solution': CHALLENGE['solution'],
        'why': CHALLENGE['why_it_matters'],
    })


@app.route('/api/challenge/run', methods=['POST'])
def run_challenge():
    join_clause = (request.json or {}).get('join_clause', '').strip()
    if not join_clause:
        return jsonify({'error': 'No JOIN condition provided'}), 400
    sql = CHALLENGE['template'].replace(
        '/* FILL IN THE JOIN CONDITION */', join_clause)
    try:
        return jsonify({'sql': sql, 'result': query(sql)})
    except Exception as e:
        return jsonify({'error': str(e), 'sql': sql}), 500


@app.route('/api/run/<qid>')
def run_query(qid):
    if qid not in QUERIES:
        return jsonify({'error': 'Not found'}), 404
    q = QUERIES[qid]
    try:
        result = query(q['sql'])
        return jsonify({'name': q['name'], 'result': result})
    except Exception as e:
        return jsonify({'error': str(e), 'exec_time_ms': 0}), 500


@app.route('/api/run_all')
def run_all():
    def run_one(qid, q):
        try:
            r = query(q['sql'])
            return {'id': qid, 'name': q['name'], 'exec_time_ms': r['exec_time_ms'],
                    'row_count': r['row_count']}
        except Exception as e:
            return {'id': qid, 'name': q['name'], 'error': str(e), 'exec_time_ms': 0}

    order = list(QUERIES.keys())
    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(run_one, qid, q): qid for qid, q in QUERIES.items()}
        results = [f.result() for f in concurrent.futures.as_completed(futs)]
    wall = round((time.perf_counter() - t0) * 1000, 2)
    results.sort(key=lambda x: order.index(x['id']))

    return jsonify({
        'queries': results,
        'wall_time_ms': wall,
        'total_queries': len(results)
    })


@app.route('/api/diag')
def diag():
    out = {'iceberg_tables': [], 'errors': []}
    for t in ['customers', 'products', 'orders', 'order_items', 'events']:
        name = f'{t}_iceberg'
        try:
            r = query(f'SELECT COUNT(*) FROM {name}')
            out['iceberg_tables'].append({'table': name, 'rows': r['rows'][0][0]})
        except Exception as e:
            out['errors'].append({'table': name, 'error': str(e)})
    return jsonify(out)


@app.route('/api/sql', methods=['POST'])
def run_sql():
    """Run an arbitrary SELECT/WITH/EXPLAIN — used by the SQL Editor tab."""
    sql = (request.json or {}).get('sql', '').strip().rstrip(';')
    if not sql:
        return jsonify({'error': 'No SQL provided'}), 400
    first = sql.split(None, 1)[0].upper() if sql else ''
    if first not in ('SELECT', 'WITH', 'EXPLAIN'):
        return jsonify({'error': 'Only SELECT, WITH, or EXPLAIN allowed'}), 403
    try:
        return jsonify(query(sql))
    except Exception as e:
        return jsonify({'error': str(e)}), 200


# =============================================================================
# HTML template (single-file dashboard)
# =============================================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Lab 3 - Lakehouse Analytics with Iceberg</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<style>
:root{
  --teal:#3DBFBF; --teal-d:#1D8080; --teal-l:#E6F6F6;
  --ok:#27A67A; --ok-l:#E7F6F0;
  --err:#D94040; --err-l:#FDEAEA;
  --warn:#E8972A; --warn-l:#FEF5E6;
  --tx:#222; --txs:#555; --txm:#888;
  --bdr:#E2E2E2; --bg:#F5F5F5; --bg2:#FAFAFA;
  --rl:12px;
  --font:'IBM Plex Sans',sans-serif; --mono:'IBM Plex Mono',monospace;
  --sh:0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:var(--font);background:var(--bg);color:var(--tx);min-height:100vh;line-height:1.55;font-size:14.5px}

.nav{background:#fff;border-top:3px solid var(--teal);border-bottom:1px solid var(--bdr);
     height:56px;display:flex;align-items:center;padding:0 24px;gap:16px;
     position:sticky;top:0;z-index:50;box-shadow:var(--sh)}
.nav-brand{font-size:16px;font-weight:700;color:var(--ok);letter-spacing:.5px}
.nav-brand span{color:var(--teal-d)}
.nav-div{width:1px;height:22px;background:var(--bdr)}
.nav-title{font-size:13px;font-weight:500;color:var(--txs)}
.nav-sp{flex:1}
.nav-pill{display:flex;align-items:center;gap:7px;font-size:12px;color:var(--txs);
          background:var(--bg);border:1px solid var(--bdr);padding:5px 13px;border-radius:20px}
.sdot{width:7px;height:7px;border-radius:50%;background:#CCC}
.sdot.on{background:var(--ok);box-shadow:0 0 6px rgba(39,166,122,.5);animation:blink 2.5s infinite}
.sdot.err{background:var(--err)}
@keyframes blink{0%,100%{opacity:1}60%{opacity:.4}}

.tabs{background:#fff;border-bottom:1px solid var(--bdr);padding:0 24px;display:flex;
      position:sticky;top:56px;z-index:40;overflow-x:auto}
.tab{background:0;border:0;color:var(--txs);padding:14px 20px;cursor:pointer;
     font-family:inherit;font-size:13.5px;font-weight:500;white-space:nowrap;
     border-bottom:3px solid transparent;transition:.15s}
.tab:hover{color:var(--tx)}
.tab.on{color:var(--teal-d);border-bottom-color:var(--teal);font-weight:600}
.tab.warn-tab.on{color:var(--warn);border-bottom-color:var(--warn)}

.page{max-width:1280px;margin:0 auto;padding:24px}
.pane{display:none}.pane.on{display:block;animation:fi .2s ease}
@keyframes fi{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}

.lead{font-size:13.5px;color:var(--txs);margin-bottom:20px;max-width:820px;line-height:1.6}
.lead strong{color:var(--tx)}

.qgrid{display:flex;flex-direction:column;gap:12px}
.qcard{background:#fff;border:1px solid var(--bdr);border-radius:var(--rl);overflow:hidden;box-shadow:var(--sh)}
.qhead{display:flex;align-items:center;gap:12px;padding:14px 18px;cursor:pointer;border-bottom:1px solid transparent;transition:.15s}
.qhead:hover{background:var(--bg2)}
.qcard.open .qhead{border-bottom-color:var(--bdr)}
.qnum{width:30px;height:30px;border-radius:6px;background:var(--teal-l);color:var(--teal-d);
      display:flex;align-items:center;justify-content:center;font-family:var(--mono);font-size:11px;font-weight:700;flex-shrink:0}
.qmeta{flex:1;min-width:0}
.qname{font-size:14px;font-weight:600;color:var(--tx)}
.qdesc{font-size:12px;color:var(--txm);margin-top:1px}
.qtimes{display:flex;gap:6px;align-items:center}
.tchip{padding:3px 9px;border-radius:11px;font-size:10.5px;font-weight:600;font-family:var(--mono);white-space:nowrap}
.tchip.nat{background:var(--ok-l);color:#1A7A57}
.tchip.ice{background:var(--teal-l);color:var(--teal-d)}
.tchip.pending{background:var(--bg);color:var(--txm)}
.qbtn{background:linear-gradient(135deg,#4ECDC4,#3DBFBF);color:#fff;border:0;
      padding:7px 14px;border-radius:6px;cursor:pointer;font-family:inherit;font-size:12px;font-weight:600;transition:.15s}
.qbtn:hover{filter:brightness(1.08)}
.qbtn:disabled{opacity:.5;cursor:wait}
.qbody{display:none;padding:0 18px 18px}
.qcard.open .qbody{display:block}
.qsql{font-family:var(--mono);font-size:11.5px;color:#444;background:#F5FAFA;padding:12px 14px;
      border-radius:6px;margin:0;white-space:pre-wrap;line-height:1.7;max-height:200px;overflow:auto}
.qsql-pair{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:14px 0}
@media(max-width:900px){.qsql-pair{grid-template-columns:1fr}}
.qsql-side{display:flex;flex-direction:column;gap:6px;min-width:0}
.qsql-lbl{font-size:11px;font-weight:600;color:var(--txs);display:flex;align-items:center;gap:6px;text-transform:uppercase;letter-spacing:.4px}
.qsql-lbl .cd{width:8px;height:8px;border-radius:50%}
.qsql-lbl .cd.ice{background:var(--teal,#3DBFBF)}
.qsql-lbl .cd.nat{background:var(--ok,#27A67A)}
.qcols{display:grid;grid-template-columns:1fr 1fr;border:1px solid var(--bdr);border-radius:6px;overflow:hidden}
@media(max-width:780px){.qcols{grid-template-columns:1fr}}
.qcol{border-right:1px solid var(--bdr)}
.qcol:last-child{border-right:none}
.qcolh{padding:10px 14px;background:var(--bg2);font-size:11px;font-weight:600;color:var(--txs);
       display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--bdr)}
.cd{width:8px;height:8px;border-radius:50%}.cd.nat{background:var(--ok)}.cd.ice{background:var(--teal)}
.qres{max-height:280px;overflow:auto}
table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:11.5px}
th{text-align:left;padding:7px 12px;color:var(--txm);font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;
   border-bottom:1px solid var(--bdr);background:#fff;position:sticky;top:0;white-space:nowrap}
td{padding:6px 12px;border-bottom:1px solid #F0F0F0;white-space:nowrap}
tr:last-child td{border-bottom:none}

.run-bar{display:flex;gap:10px;align-items:center;margin-bottom:20px;flex-wrap:wrap}
.run-btn{background:linear-gradient(135deg,#4ECDC4,#45A89C);color:#fff;border:0;padding:10px 20px;
         border-radius:7px;cursor:pointer;font-family:inherit;font-size:13.5px;font-weight:600;transition:.15s}
.run-btn:hover{filter:brightness(1.08)}
.run-btn:disabled{opacity:.5;cursor:wait}
.totals{display:flex;gap:24px;background:#fff;padding:14px 20px;border:1px solid var(--bdr);
        border-radius:var(--rl);margin-bottom:18px;flex-wrap:wrap}
.tot{display:flex;flex-direction:column;gap:2px}
.tot-l{font-size:10.5px;font-weight:600;text-transform:uppercase;letter-spacing:.6px;color:var(--txm)}
.tot-v{font-family:var(--mono);font-size:18px;font-weight:700}
.tot.nat .tot-v{color:#1A7A57}.tot.ice .tot-v{color:var(--teal-d)}

.bars{background:#fff;border:1px solid var(--bdr);border-radius:var(--rl);padding:18px 20px}
.brow{display:grid;grid-template-columns:200px 1fr 130px;gap:16px;align-items:center;padding:12px 0;border-bottom:1px solid #F0F0F0}
.brow:last-child{border-bottom:none}
.bname{font-size:13px;font-weight:500}
.bvis{display:flex;flex-direction:column;gap:6px}
.bone{display:grid;grid-template-columns:54px 1fr 80px;gap:10px;align-items:center;font-size:11.5px}
.bone-lbl{color:var(--txm);font-size:10.5px;font-weight:600}
.bbar{height:14px;background:var(--bg);border-radius:7px;overflow:hidden;position:relative}
.bfill{height:100%;border-radius:7px;transition:width .5s ease}
.bfill.nat{background:linear-gradient(90deg,#27A67A,#1A7A57)}
.bfill.ice{background:linear-gradient(90deg,#4ECDC4,#3DBFBF)}
.btime{font-family:var(--mono);font-size:11.5px;font-weight:600;text-align:right}
.bratio{font-family:var(--mono);font-size:12px;font-weight:600;text-align:right;color:var(--ok,#27A67A)}
.bratio.dim{color:var(--txm)}

.check-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}
@media(max-width:760px){.check-grid{grid-template-columns:1fr}}
.qq{background:#fff;border:1px solid var(--bdr);border-radius:var(--rl);overflow:hidden}
.qq-head{padding:12px 18px;color:#fff;font-weight:700;font-size:11.5px;letter-spacing:.5px;text-transform:uppercase}
.qq.concept .qq-head{background:var(--teal)}
.qq.practical .qq-head{background:#1A4767}
.qq-body{padding:18px 22px}
.qq-title{font-size:14.5px;font-weight:600;color:#1A4767;margin-bottom:12px;line-height:1.4}
.qq-ask{font-size:13px;color:#333;margin-bottom:14px;line-height:1.6}
.reveal-btn{background:rgba(59,130,246,.1);color:#2563eb;border:1px solid rgba(59,130,246,.3);
            padding:7px 14px;border-radius:6px;cursor:pointer;font-family:inherit;font-size:12px;font-weight:600}
.reveal-btn:hover{background:rgba(59,130,246,.18)}
.listen{display:none;margin-top:14px;padding:14px 16px;background:#EEF2F4;border-left:3px solid var(--teal);
        border-radius:0 6px 6px 0;font-size:12px;color:var(--txs);line-height:1.6;font-style:italic}
.listen.on{display:block}
.listen-l{font-size:10px;font-weight:700;color:var(--teal-d);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;font-style:normal}

.ch-card{background:#fff;border:1px solid var(--bdr);border-radius:var(--rl);overflow:hidden;margin-bottom:18px}
.ch-head{padding:14px 20px;background:linear-gradient(135deg,#FEF5E6,#FFF8E1);border-bottom:1px solid var(--bdr)}
.ch-title{font-size:15px;font-weight:600;color:#8A5A10;margin-bottom:4px}
.ch-context{font-size:12.5px;color:var(--txs);line-height:1.55}
.ch-body{padding:18px 20px}
.ch-tmpl{font-family:var(--mono);font-size:12px;background:#F5FAFA;padding:14px 16px;border-radius:6px;
         white-space:pre-wrap;line-height:1.7;color:#333;margin-bottom:14px}
.ch-tmpl .blank{background:#FFF8E1;color:#8A5A10;padding:1px 6px;border-radius:3px;font-weight:600}
.ch-input{width:100%;font-family:var(--mono);font-size:13px;padding:10px 14px;
          border:2px solid var(--bdr);border-radius:6px;background:#fff}
.ch-input:focus{outline:0;border-color:var(--teal)}
.ch-actions{display:flex;gap:10px;margin-top:12px;flex-wrap:wrap}
.ch-feedback{margin-top:14px;padding:14px 16px;border-radius:6px;display:none;font-size:13px;line-height:1.6}
.ch-feedback.on{display:block}
.ch-feedback.ok{background:var(--ok-l);color:#1A7A57;border-left:3px solid var(--ok)}
.ch-feedback.no{background:var(--err-l);color:#A02020;border-left:3px solid var(--err)}
.ch-solution{margin-top:14px;padding:14px 16px;background:#F0F4F8;border-left:3px solid #1A4767;
             border-radius:0 6px 6px 0;font-size:12.5px;display:none}
.ch-solution.on{display:block}
.ch-solution code{font-family:var(--mono);background:#fff;padding:2px 7px;border-radius:3px;color:#C0392B;font-size:12px}
.ch-solution-l{font-size:10px;font-weight:700;color:#1A4767;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}

.spinner{display:inline-block;width:14px;height:14px;border:2px solid rgba(255,255,255,.3);border-top-color:#fff;border-radius:50%;animation:sp .7s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes sp{to{transform:rotate(360deg)}}
.empty{padding:30px 20px;text-align:center;color:var(--txm);font-size:13px}
</style>
</head>
<body>

<nav class="nav">
  <span class="nav-brand">EDB <span>WHPG</span></span>
  <div class="nav-div"></div>
  <span class="nav-title">Lab 3 - Lakehouse Analytics with Iceberg</span>
  <div class="nav-sp"></div>
  <div class="nav-pill">
    <span class="sdot" id="sdot"></span>
    <span id="stxt">Connecting...</span>
  </div>
</nav>

<div class="tabs">
  <button class="tab on" data-tab="0">Query Lakehouse Data</button>
  <button class="tab" data-tab="1">Performance Results</button>
  <button class="tab warn-tab" data-tab="2">Check Understanding</button>
  <button class="tab warn-tab" data-tab="3">Challenge</button>
  <button class="tab" data-tab="4" style="margin-left:4px">SQL Editor</button>
</div>

<div class="page">

  <div class="pane on" id="pane-0">
    <p class="lead">Five analytics queries running directly on <strong>Iceberg tables stored in MinIO object storage</strong>.
       No ETL, no data loading - just pure SQL against your data lakehouse. Click <strong>Run</strong> to execute
       and see the results. Notice the query times - <strong>this is impressive for external data!</strong></p>
    <div class="qgrid" id="qgrid"></div>
  </div>

  <div class="pane" id="pane-1">
    <p class="lead">Hit <strong>Run All</strong> to execute all five queries on the Iceberg lakehouse data concurrently.
       Watch the execution times - these queries are running on data stored in object storage (MinIO), not in the database.
       <strong>Sub-second to few-second response times on external data = lakehouse federation working.</strong></p>
    <div class="run-bar">
      <button class="run-btn" id="run-all-btn" onclick="runAll()">Run All Queries</button>
      <span id="run-status" style="font-size:12.5px;color:var(--txs)"></span>
    </div>
    <div class="totals" id="totals" style="display:none">
      <div class="tot ice"><span class="tot-l">Total Wall Time</span><span class="tot-v" id="tot-wall">-</span></div>
      <div class="tot"><span class="tot-l">Queries Run</span><span class="tot-v" id="tot-count" style="color:var(--teal-d)">-</span></div>
      <div class="tot"><span class="tot-l">Data Source</span><span class="tot-v" id="tot-src" style="color:var(--txs);font-size:14px">MinIO/S3</span></div>
    </div>
    <div class="bars" id="bars"><div class="empty">Run the queries to see performance metrics.</div></div>
  </div>

  <div class="pane" id="pane-2">
    <p class="lead">Two questions to surface what stuck. <strong>Talk to the person next to you</strong> -
       compare answers, then click "Reveal" to see what we're listening for.</p>
    <div class="check-grid" id="check-grid"></div>
  </div>

  <div class="pane" id="pane-3">
    <p class="lead">Your turn to write SQL. Below is a partially-completed query - fill in the missing
       <strong>JOIN condition</strong>, then click <strong>Check</strong> to validate or <strong>Run</strong>
       to execute it against the live Iceberg dataset.</p>
    <div class="ch-card" id="ch-card"></div>
  </div>

  <div class="pane" id="pane-4">
    <p class="lead">Run any <code>SELECT</code>, <code>WITH</code>, or <code>EXPLAIN</code> against the live dataset.
       Use either Iceberg names (<code>customers_iceberg</code>, <code>orders_iceberg</code>, ...) or
       schema-qualified native names (<code>demo.customers</code>, ...).</p>
    <textarea id="sqled" spellcheck="false"
      style="width:100%;min-height:160px;background:#F8FAFA;border:1px solid var(--bdr);border-radius:8px;padding:14px;font-family:var(--mono);font-size:12.5px;color:var(--tx);resize:vertical;line-height:1.6">SELECT category, COUNT(DISTINCT oi.order_id) AS orders,
       ROUND(SUM(oi.quantity * oi.unit_price)::numeric, 2) AS revenue
FROM   products_iceberg p
JOIN   order_items_iceberg oi ON p.product_id = oi.product_id
GROUP  BY 1
ORDER  BY revenue DESC;</textarea>
    <div style="margin-top:12px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
      <button class="run-btn" onclick="runSqlEditor()">Run Query</button>
      <span id="sqled-status" style="font-size:12px;color:var(--txs);font-family:var(--mono)"></span>
    </div>
    <div id="sqled-result" style="margin-top:14px"></div>
  </div>

</div>

<script>
const fmtMs = ms => ms == null ? '-' : ms < 1000 ? Math.round(ms) + 'ms' : (ms/1000).toFixed(2) + 's';
const fmtN  = n  => n == null ? '-' : n >= 1e6 ? (n/1e6).toFixed(1) + 'M' : n >= 1e3 ? (n/1e3).toFixed(1) + 'K' : String(n);
const esc   = s  => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

let QUERIES = {};
let CHECK = [];
let CHALLENGE = {};

document.querySelectorAll('.tab').forEach(t => {
  t.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(x => x.classList.toggle('on', x === t));
    document.querySelectorAll('.pane').forEach(p =>
      p.classList.toggle('on', p.id === 'pane-' + t.dataset.tab));
  });
});

async function ping() {
  try {
    const r = await fetch('/api/queries');
    if (r.ok) {
      document.getElementById('sdot').classList.add('on');
      document.getElementById('stxt').textContent = 'Connected';
    }
  } catch (e) {
    document.getElementById('sdot').classList.add('err');
    document.getElementById('stxt').textContent = 'DB unavailable';
  }
}

function buildQueryCards() {
  const grid = document.getElementById('qgrid');
  grid.innerHTML = '';
  Object.entries(QUERIES).forEach(([qid, q], i) => {
    const div = document.createElement('div');
    div.className = 'qcard'; div.id = 'qc-' + qid;
    div.innerHTML =
      '<div class="qhead" onclick="toggleCard(\'' + qid + '\')">' +
        '<span class="qnum">' + (i+1) + '</span>' +
        '<div class="qmeta"><div class="qname">' + esc(q.name) + '</div><div class="qdesc">' + esc(q.desc) + '</div></div>' +
        '<div class="qtimes" id="qt-' + qid + '">' +
          '<span class="tchip pending">time: -</span>' +
        '</div>' +
        '<button class="qbtn" id="qb-' + qid + '" onclick="event.stopPropagation();runOne(\'' + qid + '\')">Run</button>' +
      '</div>' +
      '<div class="qbody">' +
        '<div style="margin-bottom:14px">' +
          '<div class="qsql-lbl" style="margin-bottom:6px"><span class="cd ice"></span>Iceberg Query &mdash; runs directly on MinIO object storage</div>' +
          '<div class="qsql">' + esc(q.sql) + '</div>' +
        '</div>' +
        '<div id="qr-' + qid + '"><div class="empty">Click Run to execute this query.</div></div>' +
      '</div>';
    grid.appendChild(div);
  });
}

function toggleCard(qid) { document.getElementById('qc-' + qid).classList.toggle('open'); }

function renderResultTable(res) {
  if (res.error) return '<div style="padding:14px 16px;color:var(--err);font-family:var(--mono);font-size:11.5px">' + esc(res.error) + '</div>';
  if (!res.rows || !res.rows.length) return '<div class="empty">No rows returned</div>';
  return '<div class="qres"><table><thead><tr>' +
    res.columns.map(c => '<th>' + esc(c) + '</th>').join('') +
    '</tr></thead><tbody>' +
    res.rows.map(r => '<tr>' + r.map(v => '<td>' + (v == null ? 'NULL' : esc(v)) + '</td>').join('') + '</tr>').join('') +
    '</tbody></table></div>';
}

async function runOne(qid) {
  const btn = document.getElementById('qb-' + qid);
  const card = document.getElementById('qc-' + qid);
  btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>Running';
  card.classList.add('open');
  document.getElementById('qr-' + qid).innerHTML = '<div class="empty">Running query on Iceberg data...</div>';
  try {
    const r = await fetch('/api/run/' + qid).then(x => x.json());
    if (r.error) throw new Error(r.error);
    const ms = r.result.exec_time_ms || 0;
    document.getElementById('qt-' + qid).innerHTML =
      '<span class="tchip ice">time: ' + fmtMs(ms) + '</span>';
    document.getElementById('qr-' + qid).innerHTML =
      '<div style="border:1px solid var(--bdr);border-radius:6px;overflow:hidden">' +
        '<div class="qcolh"><span class="cd ice"></span>Result from Iceberg (PGAA) - ' + fmtMs(ms) + ' &middot; ' + fmtN(r.result.row_count) + ' rows</div>' +
        renderResultTable(r.result) +
      '</div>';
  } catch (e) {
    document.getElementById('qr-' + qid).innerHTML =
      '<div style="padding:14px;color:var(--err)">Error: ' + esc(e.message) + '</div>';
  }
  btn.disabled = false; btn.textContent = 'Run';
}

async function runAll() {
  const btn = document.getElementById('run-all-btn');
  btn.disabled = true; btn.innerHTML = '<span class="spinner"></span>Running...';
  document.getElementById('run-status').textContent = 'Executing all queries on Iceberg data...';
  document.getElementById('bars').innerHTML = '<div class="empty">Running...</div>';
  try {
    const d = await fetch('/api/run_all').then(x => x.json());
    renderBars(d);
    document.getElementById('run-status').textContent = '';
  } catch (e) {
    document.getElementById('run-status').innerHTML =
      '<span style="color:var(--err)">Error: ' + esc(e.message) + '</span>';
  }
  btn.disabled = false; btn.textContent = 'Run All Queries';
}

function renderBars(d) {
  const queries = d.queries;
  const wall = d.wall_time_ms;
  document.getElementById('totals').style.display = 'flex';
  document.getElementById('tot-wall').textContent = fmtMs(wall);
  document.getElementById('tot-count').textContent = d.total_queries;

  const allTimes = queries.map(q => q.exec_time_ms || 0);
  const mx = Math.max(...allTimes, 1);
  const html = queries.map(q => {
    const ms = q.exec_time_ms || 0;
    const pct = Math.max(2, Math.round(ms / mx * 100));
    const status = q.error ? '<span style="color:var(--err);font-size:11px">Error</span>' :
                             '<span class="btime">' + fmtMs(ms) + '</span>';
    return '<div class="brow">' +
      '<div class="bname">' + esc(q.name) + '</div>' +
      '<div class="bvis">' +
        '<div class="bone" style="grid-template-columns:80px 1fr 100px">' +
          '<span class="bone-lbl">ICEBERG</span>' +
          '<div class="bbar"><div class="bfill ice" style="width:' + pct + '%"></div></div>' +
          status +
        '</div>' +
      '</div>' +
      '<span style="font-size:11.5px;color:var(--txm)">' + fmtN(q.row_count || 0) + ' rows</span>' +
      '</div>';
  }).join('');
  document.getElementById('bars').innerHTML = html;
}

function buildCheck() {
  const grid = document.getElementById('check-grid');
  grid.innerHTML = '';
  CHECK.forEach((q, i) => {
    const div = document.createElement('div');
    div.className = 'qq ' + q.kind;
    div.innerHTML =
      '<div class="qq-head">Question ' + (i+1) + ' &mdash; ' + q.kind + '</div>' +
      '<div class="qq-body">' +
        '<div class="qq-title">' + esc(q.title) + '</div>' +
        '<div class="qq-ask">' + esc(q.ask) + '</div>' +
        '<button class="reveal-btn" onclick="this.parentElement.querySelector(\'.listen\').classList.toggle(\'on\');this.textContent=this.textContent.startsWith(\'Reveal\')?\'Hide answer\':\'Reveal answer\'">Reveal answer</button>' +
        '<div class="listen"><div class="listen-l">What we\'re listening for</div>' + esc(q.listen) + '</div>' +
      '</div>';
    grid.appendChild(div);
  });
}

function buildChallenge() {
  const tmplHtml = esc(CHALLENGE.template).replace(
    /\/\* FILL IN THE JOIN CONDITION \*\//g,
    '<span class="blank">[ FILL IN ]</span>');
  document.getElementById('ch-card').innerHTML =
    '<div class="ch-head">' +
      '<div class="ch-title">Challenge: ' + esc(CHALLENGE.title) + '</div>' +
      '<div class="ch-context">' + esc(CHALLENGE.context) + '</div>' +
    '</div>' +
    '<div class="ch-body">' +
      '<div class="ch-tmpl">' + tmplHtml + '</div>' +
      '<label style="font-size:12px;font-weight:600;color:var(--txs);margin-bottom:6px;display:block">Your JOIN condition:</label>' +
      '<input class="ch-input" id="ch-input" placeholder="e.g. table_a.col = table_b.col" />' +
      '<div class="ch-actions">' +
        '<button class="run-btn" onclick="checkChallenge()">Check Answer</button>' +
        '<button class="qbtn" onclick="runChallenge()">Run It</button>' +
        '<button class="reveal-btn" onclick="document.getElementById(\'ch-solution\').classList.toggle(\'on\')">Reveal Solution</button>' +
      '</div>' +
      '<div class="ch-feedback" id="ch-feedback"></div>' +
      '<div id="ch-result"></div>' +
      '<div class="ch-solution" id="ch-solution">' +
        '<div class="ch-solution-l">Solution</div>' +
        '<code>' + esc(CHALLENGE.solution || '') + '</code>' +
        '<p style="margin-top:10px;font-size:12px;line-height:1.6;color:var(--txs)">' + esc(CHALLENGE.why_it_matters || '') + '</p>' +
      '</div>' +
    '</div>';
}

async function checkChallenge() {
  const ans = document.getElementById('ch-input').value.trim();
  if (!ans) return;
  const r = await fetch('/api/challenge/check', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({answer: ans})
  }).then(x => x.json());
  const fb = document.getElementById('ch-feedback');
  fb.classList.add('on');
  if (r.correct) {
    fb.className = 'ch-feedback on ok';
    fb.innerHTML = '<strong>Correct!</strong> ' + esc(r.why);
  } else {
    fb.className = 'ch-feedback on no';
    fb.innerHTML = 'Not quite. Try again &mdash; hint: products and order_items share <code style="background:#fff;padding:1px 5px;border-radius:3px;font-family:var(--mono)">product_id</code>.';
  }
}

async function runChallenge() {
  const ans = document.getElementById('ch-input').value.trim();
  if (!ans) {
    document.getElementById('ch-result').innerHTML = '<div style="margin-top:14px;padding:12px;color:var(--err);font-size:13px">Enter a JOIN condition first.</div>';
    return;
  }
  document.getElementById('ch-result').innerHTML = '<div style="margin-top:14px;padding:12px;color:var(--txm);font-size:13px"><span class="spinner" style="border-top-color:var(--teal)"></span>Running...</div>';
  try {
    const r = await fetch('/api/challenge/run', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({join_clause: ans})
    }).then(x => x.json());
    if (r.error) {
      document.getElementById('ch-result').innerHTML = '<div style="margin-top:14px;padding:12px;color:var(--err);font-family:var(--mono);font-size:11.5px">' + esc(r.error) + '</div>';
      return;
    }
    const res = r.result;
    document.getElementById('ch-result').innerHTML =
      '<div style="margin-top:14px;border:1px solid var(--bdr);border-radius:6px;overflow:hidden">' +
        '<div class="qcolh"><span class="cd ice"></span>Result &mdash; ' + fmtMs(res.exec_time_ms) + ' &middot; ' + fmtN(res.row_count) + ' rows</div>' +
        renderResultTable(res) +
      '</div>';
  } catch (e) {
    document.getElementById('ch-result').innerHTML = '<div style="margin-top:14px;padding:12px;color:var(--err);font-size:13px">Error: ' + esc(e.message) + '</div>';
  }
}

async function runSqlEditor() {
  const sql = document.getElementById('sqled').value.trim();
  const status = document.getElementById('sqled-status');
  const out = document.getElementById('sqled-result');
  if (!sql) { status.textContent = 'Enter a query first.'; return; }
  status.innerHTML = '<span class="spinner"></span>Running...';
  out.innerHTML = '';
  try {
    const r = await fetch('/api/sql', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sql})
    });
    const d = await r.json();
    if (d.error) {
      status.innerHTML = '<span style="color:var(--err)">Error</span>';
      out.innerHTML = '<div style="padding:12px;color:var(--err);font-family:var(--mono);font-size:12px;background:var(--err-l);border-radius:6px">' + esc(d.error) + '</div>';
      return;
    }
    status.innerHTML = fmtMs(d.exec_time_ms) + ' &middot; ' + fmtN(d.row_count) + ' rows';
    out.innerHTML = renderResultTable(d);
  } catch (e) {
    status.innerHTML = '<span style="color:var(--err)">Error: ' + esc(e.message) + '</span>';
  }
}

async function init() {
  await ping();
  try {
    QUERIES = await fetch('/api/queries').then(r => r.json());
    CHECK = await fetch('/api/check').then(r => r.json());
    CHALLENGE = await fetch('/api/challenge').then(r => r.json());
    buildQueryCards();
    buildCheck();
    buildChallenge();
  } catch (e) {
    console.error('Init failed:', e);
  }
}
init();
</script>
</body>
</html>
"""


def parse_args():
    parser = argparse.ArgumentParser(description='PGAA Lab 3 Dashboard')
    parser.add_argument('--port', type=int,
                        default=int(os.environ.get('PORT', 5000)),
                        help='Port to listen on (default: 5000, or PORT env var)')
    parser.add_argument('--host', default=os.environ.get('HOST', '0.0.0.0'),
                        help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--debug', action='store_true', help='Enable Flask debug mode')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    print(f"""
+-----------------------------------------------------+
|  PGAA Lab 3 Dashboard - Lakehouse Analytics        |
|  DB:     {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}
|  Tables: *_iceberg (Iceberg tables on MinIO)
|  Listen: http://{args.host}:{args.port}
+-----------------------------------------------------+
""")
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)
