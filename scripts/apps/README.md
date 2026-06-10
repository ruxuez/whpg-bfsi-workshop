# Meridian Retail Bank — Workshop Applications

Interactive dashboards and data generators for the EDB WarehousePG (Greenplum 7 /
PostgreSQL 12) workshop, reskinned from the original NetVista network-security
theme to the **Meridian Retail Bank** card-fraud + AML use case.

Each app's HTML / CSS / JS scaffolding is unchanged — only the **SQL queries,
labels, schema names, and branding** were rewritten to match the BFSI SQL suite
(`01_schema.sql` … `08_add_diverse_narratives.sql`). Every executed query was
validated against a live PostgreSQL 16 + pgvector harness loaded with the BFSI
schema and persona seed.

---

## 1. File inventory

| File | Type | Lab | Purpose |
|------|------|-----|---------|
| `app1.py` | Flask dashboard | Lab 1 | JSONB (ISO 20022) + `int8range` operators on card transactions |
| `app2.py` | Flask dashboard | Lab 2 | PGAA lakehouse — Iceberg-on-MinIO vs native AOCO |
| `app3.py` | Flask dashboard | Lab 3 | pgvector semantic search + MADlib clustering ("AI Factory") |
| `dashboard.py` | Dash / Plotly | Lab 3 | K-Means fraud-cluster explorer (scatter / radar / heatmap) |
| `iceberg_data_generator.py` | Generator | Lab 2 | Writes the 5 BFSI lakehouse tables to Iceberg on MinIO |
| `data_generator_personas.py` | Generator | Lab 3 | Tight persona `transactions` + `case_narratives` for clustering |
| `03_load_external_bfsi.sql` | Loader | all | gpfdist external tables + INSERT for the CSV path |

---

## 2. Prerequisites

```bash
# dashboards
pip3 install flask psycopg2-binary                                   # app1 / app2 / app3
pip3 install dash dash-bootstrap-components pandas plotly psycopg2-binary   # dashboard.py

# generators (only if you use them)
pip3 install "pyiceberg[s3fs]" pyarrow      # iceberg_data_generator.py
# data_generator_*.py use only the Python standard library
```

A running WarehousePG cluster with the BFSI schema loaded (see section 5).

---

## 3. Database connection (environment variables)

`app1` / `app2` / `app3` read:

```bash
export WHPG_HOST=localhost
export WHPG_PORT=5432
export WHPG_DB=bank     # database that contains the bfsi_demo schema
export WHPG_USER=gpadmin
export WHPG_PASS=
```

- `app1.py` also honours `WHPG_SCHEMA` (default `bfsi_demo`).
- `app2.py` also honours `WHPG_NATIVE_SCHEMA` (default `bfsi_analytics`) and `--port`.
- `dashboard.py` uses its own names: `WPGHOST`, `WPGPORT`, `WPGDB`, `WPGUSER`, `WPGPASS` (also default `bank`).

---

## 4. Running the apps

```bash
python3 apps/app1.py                  # Lab 1  ->  http://localhost:5001
python3 apps/app2.py --port 5000      # Lab 2  ->  http://localhost:5000
python3 apps/app3.py                  # Lab 3  ->  http://localhost:5002
python3 apps/dashboard.py             # Lab 3  ->  http://localhost:5003
```

If the cluster is remote, tunnel the port, e.g.:

```bash
ssh -L 5001:localhost:5001 ec2-user@<cluster-ip>   # then open http://localhost:5001
```

Each Flask app exposes `/api/diag` (app1) or `/api/health` (app3) to confirm what
schema and tables it can see.

### What each dashboard shows

**app1 — Lab 1 (3 panels + comprehension check + SQL editor)**
- *Card Transactions:* `1A` watchlist BIN match (`card_bin <@ bin_range`),
  `1C` high-value SEPA-Instant scan (JSONB `@>` + jsonpath `@?` on the GIN index).
- *Case & Auth Analytics:* `2A` case x auth same-day correlation, `2C` 5-source event-volume rollup.
- *BIN, Limits + Bonus:* `3C` customer risk scorecard via `fraud_risk_score()`,
  `4B` forensic trace of structuring account `105900001` across transactions / auth / device / wires.

**app2 — Lab 2 (same SQL, two engines)**
- `Spend by Card Category`, `Top 20 Customers by Spend`, `Digital Banking Funnel`,
  `Executive Summary`, `Daily Dashboard (5-table JOIN)` — each run against Iceberg
  (PGAA) and the native `bfsi_analytics` AOCO copies, side by side, plus a
  fill-in-the-blank JOIN challenge.

**app3 — Lab 3 (pgvector / MADlib / AI Factory)**
- `A1` keyword search misses, `A2` pgvector finds structuring cases by meaning,
  `B1` MADlib discovers the fraud personas, `B2` dramatic differences vs normal,
  `C1` the combined behavioural-plus-semantic correlation, `C2` why it matters.

**dashboard.py — K-Means explorer**
- Scatter (any two of txns / merchants / MCCs / spend / entropy / spread),
  cluster-size bars, radar centroid profiles, z-scored heatmap, and a per-cluster
  drilldown table over `kmeans_assignments` joined to `account_features`.

> **Persona-labeling rule (important):** the STRUCTURING cluster has *high* total
> spend (many sub-$10k wires), so the apps test `amount_cv < 0.1` **before** the
> spend threshold — otherwise structuring is mislabeled as BUST-OUT.

---

## 5. Loading the data — three options

The apps query whatever is in `bfsi_demo` / `bfsi_analytics`. Pick one path.

### Option A — in-database seed (simplest, default)
```bash
createdb bank        # one-time (run from repo root)
psql -d bank -f sql/01_schema.sql
psql -d bank -f sql/02_seed_reference.sql
psql -d bank -f sql/03_seed_traffic_with_personas.sql   # ~13M rows, June 2026
psql -d bank -f sql/06_lab2_ai_analytics.sql
psql -d bank -f sql/08_add_diverse_narratives.sql      # curated notes for app3 Part A
```

### Option B — CSV via gpfdist (bulk-load alternative to `03`)
```bash
python3 generators/data_generator_updated.py --output-dir ./csv_data --scale 1.0   # 6 fact tables, ~50M rows, June 2026
# or the tight Lab 3 persona feed (transactions + case_narratives only):
python3 generators/data_generator_personas.py --scale small

gpfdist -d ./csv_data -p 8081 &
# edit GPFDIST_HOST in the loader first, then:
psql -d bank -f sql/01_schema.sql && psql -d bank -f sql/02_seed_reference.sql
psql -d bank -f sql/03_load_external_bfsi.sql
psql -d bank -f sql/06_lab2_ai_analytics.sql && psql -d bank -f sql/07_kmeans_fallback.sql
```
CSVs are headerless and follow `01_schema` column order (minus each serial PK).
`transactions.iso_msg` is emitted as quoted JSON and loads straight into the `jsonb`
column. **Scale:** `--scale 1.0` ≈ 50M rows total (the workshop default); `2.0` ≈ 100M; `0.1` ≈ 5M for a quick test. The generators need only the Python standard library.

### Option C — Lab 2 lakehouse (Iceberg on MinIO, required for app2)
```bash
# start MinIO, then:
python3 generators/iceberg_data_generator.py --scale 10     # writes analytics.{5 tables} to MinIO
psql -f 05_pgaa_tables.sql                        # PGAA in-place + native AOCO copies
python3 apps/app2.py --port 5000
```

---

## 6. Validation status

| App | How validated |
|-----|---------------|
| app1 | All 6 queries run live against `bfsi_demo`; every panel returns rows |
| app2 | 5 queries + challenge EXPLAIN-validated on `bfsi_analytics` (needs MinIO to run live) |
| app3 | All 5 queries run live; persona labels resolve correctly; C1 correlates clusters with semantic notes |
| dashboard.py | Both queries run live; clusters separate cleanly into STRUCTURING / CARD-TESTING / BUST-OUT |
| `data_generator_updated.py` | All 6 CSVs COPY-load; `iso_msg` parses as JSONB; watchlist join hits all 3 persona BINs |
| `data_generator_personas.py` | Loads + feature check: card-testing ~600 merchants, bust-out ~9 / high spend, structuring CV ~0.03 |
| `iceberg_data_generator.py` | Emits Arrow tables matching `05_pgaa_tables.sql` column-for-column |

---

## 7. Troubleshooting

- **"relation … does not exist"** — the app can't see the schema. Check `WHPG_SCHEMA`
  / `WHPG_NATIVE_SCHEMA`, hit `/api/diag`, and confirm the seed scripts ran into the
  database named in `WHPG_DB`.
- **Empty result sets** — the seeds are time-relative (`now() - 28 days`). If data was
  loaded long ago, re-run `03_seed_traffic_with_personas.sql` (or the CSV path) so the
  rolling window still contains rows.
- **app2 errors on Iceberg queries** — Lab 2 needs MinIO + `iceberg_data_generator.py`
  + `05_pgaa_tables.sql` first; the native-only side works once `bfsi_analytics` exists.
- **app3 Part A returns little** — load `08_add_diverse_narratives.sql` (or run
  `data_generator_personas.py`) so `case_embeddings` has the curated semantic notes.
- **Port already in use** — app2 takes `--port`; the others set the port at the bottom
  of the file (`app.run(... port=NNNN)`).
