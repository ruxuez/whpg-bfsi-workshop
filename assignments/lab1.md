# Lab 1: Card Fraud & AML Analytics (WarehousePG)

In this lab, you will explore the power of native **int8range** operators in WarehousePG for financial services analytics. You will perform high-speed fraud detection on approximately 13 million card transactions, demonstrating how native range types outperform standard cloud data warehouses.

---

## 1. Lab Overview
* **Application:** `app1.py`
* **Access Port:** `5001`
* **Database:** `demo`
* **Schema:** `bfsi_demo`

### Dashboard Content: 3 Core Queries
| ID | Query | Key Demonstration |
| :--- | :--- | :--- |
| **1A** | **BIN Range Match** | `int8range <@` join — native containment vs string parsing |
| **2A** | **Cross-Source Alert Correlation** | Links transactions + auth + device events |
| **3A** | **Account Forensic Trace** | Trace account activity across ALL log sources |

### Terminal Setup

Prepare 2 Shell Tabs/Windows:
- **Terminal Tab**: Your local host under this repo
- **WarehousePG Tab**: Connection to `cdw` environment shell:
```bash
docker exec -u gpadmin -w /home/gpadmin -it cdw /bin/bash
```

---

## 2. Monitor Startup (Terminal Tab)
Before starting the lab, ensure the WarehousePG cluster is fully initialized.

1. Open your **Terminal Tab**
2. Watch the coordinator logs:
   ```bash
   docker logs -f cdw
   ```
3. **Wait** for the `DEPLOYMENT SUCCESSFUL` banner to appear before proceeding. Ensure there is no ERROR in logs.

> [!WARNING]
> Press `Ctrl+C` to stop watching logs.

---

## 3. Verify Cluster Status (Terminal Tab)
Check that all components of the distributed cluster are healthy.

1. **Verify Containers:**
   ```bash
   docker ps | grep -E "cdw|sdw"
   ```
   
2. **Check WarehousePG Cluster State:**
   ```bash
   docker exec -u gpadmin cdw \
     bash -c " \
       source /usr/local/greenplum-db/greenplum_path.sh && \
       export COORDINATOR_DATA_DIRECTORY=/data/master/gpseg-1 && \
       gpstate \
    "
   ```

When verifying the containers, you should see three containers, all using the `whpg_cdw` image, with names (rightmost column) `sdw1`, `sdw2`, and `cdw`. The `cdw` container is the **coordinator** and the `sdw1` and `sdw2` containers are the **segment hosts**.

It is EXPECTED to see `Mirrors not configured on this array` and `No coordinator standby configured`. This is a demo setup. You should see 4 primary segments.

---

## 4. Initialize Lab 1 Data (WarehousePG Tab)
Switch to the **WarehousePG Tab** to execute these instructions.

You will now create the schema and seed the database with banking transaction data.

### Step 1: Load Schema

This command creates the BFSI data model with partitioned tables:
```bash
psql demo -e -f /scripts/sql/01_schema.sql
```

**What this creates:**
- **Fact tables**: transactions, case_narratives, device_events, auth_decisions, wire_events, account_kpis
- **Dimension tables**: regions, bin_ranges, customers, risk_profiles, fraud_watchlists, country_risk
- **Views**: Pre-built analytics queries for fraud detection and AML monitoring

### Step 2: Seed Reference Data
```bash
psql demo -e -f /scripts/sql/02_seed_reference.sql
```

**What this loads:**
- 7 regions (US-EAST, US-WEST, EMEA, APAC, etc.)
- 12 BIN ranges (Visa, Mastercard, Amex card issuers)
- 10 customers (bank portfolio segments)
- 10 risk profiles (transaction thresholds)
- 10 fraud watchlists (compromised BIN feeds)
- 14 countries (AML risk scores)

### Step 3: Seed Traffic Logs (13M Rows)

*  **`gpfdist`: External Parallel Load**

**gpfdist** is the WarehousePG Database parallel file distribution program.
It can be used to read external table files to all WarehousePG Database segments in parallel.

We are going to use `gpfdist` to load CSV files in parallel to WarehousePG Database.

Our CSV files are stored in `/csv_data` directory. Let's serve files from here using port 8081 (and start gpfdist in the background):
```bash
gpfdist -d /csv_data -p 8081 > /home/gpadmin/gpfdist.log &
```

Then run following command to load CSV data in parallel to WarehousePG thanks to `gpfdist`:

```bash
psql demo -e -f /scripts/sql/03_load_external_bfsi.sql
```

> [!IMPORTANT]
> This process will take 1-2 minutes to load 13M rows.

* **`Analyzedb`** (1-2mins)

Once data load finished, let's run `analyzedb` to perform **ANALYZE** operations on tables incrementally and concurrently.
```bash
analyzedb -d demo -s bfsi_demo -a
```

This generates:
- **13M transactions** with 4 fraud personas (Normal, Card-Testing, Bust-Out, Structuring)
- **1.6M case narratives** for Lab 2 semantic search
- **Device events, auth decisions, account KPIs**

---

## 5. WarehousePG Quick Tests (WarehousePG Tab)
Execute these from the **WarehousePG Tab**.

You can use these commands to verify the internal state, extensions, and distributed configuration of your WarehousePG environment.
```bash
psql demo
```

---

### 5.1 Check Version and Build
Verify that you are running the correct version of WarehousePG.
```sql
select version();
```

---

### 5.2 Explore Cluster Configuration
WarehousePG is a distributed system. Use this query to see the **Master (Coordinator)** and **Segment** instances, their status, and which port/address they are using.
```sql
SELECT dbid, content, role, preferred_role,
       mode, status, hostname, address, port
FROM gp_segment_configuration ORDER BY dbid;
```

---

### 5.3 Explore Databases
List all databases currently initialized in the cluster to ensure `demo` and system databases are present.
```sql
\l
```

---

### 5.4 Explore Extensions in `demo` Database
WarehousePG uses several advanced extensions for AI and Analytics. Check which ones are active in the `demo` database (e.g., `vector`, `pgaa`, `pgfs`).
```sql
\dx
```

Check schemas (e.g., `madlib`, `pgaa`, `pgfs`):
```sql
\dn
```

---

### 5.5 Explore Runtime Configuration (GUCs)
Check specific "Grand Unified Configuration" (GUC) parameters that control the behavior of the WarehousePG optimizer and parallel execution.

Check if the GPORCA optimizer is enabled:
```sql
SHOW optimizer;
```

Check maximum connections allowed across the cluster:
```sql
SHOW max_connections;
```

Quit WarehousePG:
```sql
\quit
```

---

## 6. Launch Lab 1 Dashboard (Terminal Tab)

Switch back to your **Terminal Tab** and launch the Flask application:

### Option 1: Run from Host (if Python dependencies installed locally)
```bash
python3.9 scripts/apps/app1.py
```

### Option 2: Run from inside CDW container
```bash
docker exec -it \
  -e WHPG_HOST=localhost \
  -e WHPG_PORT=5432 \
  -e WHPG_DB=demo \
  -e WHPG_USER=gpadmin \
  cdw python3.9 /scripts/apps/app1.py
```

**Access the dashboard:**
- Open your browser to: **http://localhost:5001**

---

## 7. Dashboard Exploration

### SQL Editor

Before exploring the queries, use the **SQL Editor** tab to explore the `bfsi_demo` schema:

```sql
SELECT
    n.nspname,
    c.relname,
    CASE WHEN c.relkind = 'p' THEN 'Partitioned Root' ELSE 'Standard Table' END as type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'bfsi_demo'
  AND c.relkind IN ('r', 'p')
  AND c.relispartition = false;
```

### Panel: Fraud Detection & Forensics

The dashboard has 3 core queries demonstrating WarehousePG's fraud detection capabilities:

**Query 1A: Watchlist BIN Match**
- Demonstrates native `int8range` containment joins
- Expands watchlist BIN ranges into individual BINs via `generate_series()`
- Uses hash join instead of nested-loop GiST probe per row
- **Performance**: Sub-second on 13M transactions

**Query 2A: Case × Auth Correlation**
- Cross-source join: case narratives + authorization decisions
- Time-windowed correlation (±15 minutes)
- Replaces traditional SIEM log correlation
- **Note**: Optimized with matching distribution keys on (account_id, card_bin)

**Query 3A: Forensic Account Trace**
- Multi-source UNION query across 4 fact tables
- Traces single account activity across transactions, auth, device, wire events
- **Use case**: Fraud investigation, account review, SAR filing

### Run All Queries

Click **"▶ Run All 3"** to execute all queries in sequence. Observe:
- Execution times (displayed in milliseconds)
- Row counts returned
- Data patterns across fraud personas

---

## 8. Check Understanding

Navigate to the **Check Understanding** tab in the dashboard.

Two questions to surface what stuck:
1. **Concept**: Why was Query 1A fast on 13M transactions?
2. **Practical**: What made Query 2A slow before the distribution key fix?

Talk through the answers, then click "Reveal answer" to see the expected insights.

---

## 9. Key Takeaways

### Why WarehousePG for Fraud Detection?

1. **Native Range Types** (`int8range`)
   - BIN range matching without string parsing
   - GiST indexes for containment queries
   - Impossible to replicate in Snowflake/BigQuery

2. **MPP Parallelism**
   - Distributed across 4 segments
   - Co-located joins via distribution keys
   - Sub-second queries on millions of rows

3. **Cross-Source Correlation**
   - All fact tables in one database
   - No export to SIEM required
   - Time-windowed joins in SQL

4. **Distribution Key Optimization**
   - Matching distribution keys eliminate data motion
   - Co-located joins on (account_id, card_bin)
   - Query 2A: 15s → 2s after distribution key fix

---

## 10. Cleanup (Optional)

To stop the dashboard:
```bash
# In Terminal Tab, press Ctrl+C to stop app1.py
```

To stop WarehousePG cluster (if needed):
```bash
docker-compose down
```

---

## Next Steps

Proceed to **Lab 2** for AI-powered fraud detection with pgvector and MADlib.
