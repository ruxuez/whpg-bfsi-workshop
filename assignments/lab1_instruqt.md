# Lab 1: Card Fraud & AML Analytics (WarehousePG)

In this lab, you will explore the power of native **int8range**  operators in WarehousePG for financial services analytics. You will perform high-speed fraud detection on approximately 13 million card transactions, demonstrating how native range types  outperform standard cloud data warehouses.

## ⚠️ DO NOT HIT "Next" button yet! Wait for your instructor! ⚠️

---

## 1. Lab Overview
* **Application:** `lab1.py`
* **Access Port:** `5001`
* **Database:** `demo`

### Dashboard Content: 6 Queries Across 3 Panels
| ID | Query | Key Demonstration |
| :--- | :--- | :--- |
| **1A** | **BIN Range Match** | `int8range <@` join — native containment vs string parsing |
| **2A** | **Cross-Source Alert Correlation** | Links transactions + auth + device events |
| **3A** | **Account Forensic Trace** | Trace account activity across ALL log sources |

---

## 2. Monitor Startup ([button label="⚠️Terminal Tab"](tab-0))
Before starting the lab, ensure the WarehousePG cluster is fully initialized.

1. Open the [button label="⚠️Terminal Tab"](tab-0).
2. Watch the coordinator logs:
   ```run
   docker logs -f cdw
   ```
3. **Wait** for the `DEPLOYMENT SUCCESSFUL` banner to appear before proceeding.  Ensure there is no ERROR in logs.

> [!WARNING]
> Press the `Ctrl+C` key on your keyboard to take the control back.

---

## 3. Verify Cluster Status ([button label="⚠️Terminal Tab"](tab-0))
Check that all components of the distributed cluster are healthy.

1. **Verify Containers:**
   ```run
   docker ps | grep -E "cdw|sdw"
   ```
2. **Check WarehousePG Cluster State:**
   ```run
   docker exec -u gpadmin cdw \
	 bash -c " \
		 source /usr/local/greenplum-db/greenplum_path.sh && \
		 export COORDINATOR_DATA_DIRECTORY=/data/master/gpseg-1 && \
		 gpstate \
	"
   ```

When verifying the containers, you should see three containers, all using the `whpg_cdw` image, with names (rightmost column) `sdw1`, `sdw2`, and `cdw`. The `cdw` container is the **coordinator** and the `sdw1` and `sdw2` containers are the **segment hosts**.

It is EXPECTED to see `Mirrors not configured on this array` and `No coordinator standby configured`. This is a demo setup running in your own VM. You should see 4 primary segments.

---

## 4. Initialize Lab 1 Data ([button label="⚠️WarehousePG Tab"](tab-1))
Switch to the [button label="⚠️WarehousePG Tab"](tab-1) to execute these instructions.

You will now create the schema and seed the database with banking transaction data.
Execute the following commands to build the tables and load the transactions. We use the full path to `psql` and source the environment to ensure connectivity.

### Step 1: Load Schema

This command creates the BFSI data model with partitioned tables:
```run
psql demo -e -f /scripts/sql/01_schema.sql
```
**What this creates:**
- **Fact tables**: transactions, case_narratives, device_events, auth_decisions, wire_events, account_kpis
- **Dimension tables**: regions, bin_ranges, customers, risk_profiles, fraud_watchlists, country_risk
- **Views**: Pre-built analytics queries for fraud detection and AML monitoring

### Step 2: Seed Reference Data
```run
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
It can also be used to accept output streams from WarehousePG Database segments in parallel and write them out to a file.

We are going to use `gpfdist` to load CSV files in parallel to WarehousePG Database.

Our CSV files are stored in `/csv_data` directory,  let's serve files from here using port 8081 (and start gpfdist in the background):
```run
gpfdist -d /csv_data -p 8081 > /home/gpadmin/gpfdist.log &
```

Then run following command to load CSV data in parallel to WarehousePG thanks to `gpfdist`.

```run
psql demo -e -f /scripts/sql/03_load_external_bfsi.sql
```
> [!IMPORTANT]
> ❗️This process will take 1-2 minutes to load 13M rows, please let us know when you are here! 🙋‍


* **`Analyzedb`** (1-2mins)

Once data load finished, let's run `analyzedb` to perform **ANALYZE** operations on tables incrementally and concurrently.
```run
analyzedb -d demo -s bfsi_demo -a
```

This generates:
- **13M transactions** with 4 fraud personas (Normal, Card-Testing, Bust-Out, Structuring)
- **1.6M case narratives** for Lab 3 semantic search
- **Device events, auth decisions, account KPIs**

---

## 5. WarehousePG Quick Tests ([button label="⚠️WarehousePG Tab"](tab-1))
Execute these from the [button label="⚠️WarehousePG Tab"](tab-1).

You can use these commands to verify the internal state, extensions, and distributed configuration of your WarehousePG environment.
```run
psql demo
```
---

### 5.1 Check Version and Build
Verify that you are running the correct version of WarehousePG.
```run
select version();
```

---

### 5.2 Explore Cluster Configuration
WarehousePG is a distributed system. Use this query to see the **Master (Coordinator)** and **Segment** instances, their status, and which port/address they are using.
```run
SELECT dbid, content, role, preferred_role,
		mode, status, hostname, address, port
FROM gp_segment_configuration ORDER BY dbid;
```
---

### 5.3 Explore Databases
List all databases currently initialized in the cluster to ensure `demo`, and system databases are present.
```run
\l
```

---

### 5.4 Explore Extensions in `demo` Database
WarehousePG uses several advanced extensions for AI and Analytics. Check which ones are active in the `demo` database (e.g., `vector`, `pgaa`, `pgfs`).
```run
\dx
```
Check schemas (e.g., `madlib`, `pgaa`, `pgfs`)
```run
\dn
```

---

### 5.5 Explore Runtime Configuration (GUCs)
Check specific "Grand Unified Configuration" (GUC) parameters that control the behavior of the WarehousePG optimizer and parallel execution.

Check if the GPORCA optimizer is enabled
```run
SHOW optimizer;
```

Check maximum connections allowed across the cluster
```run
SHOW max_connections;
```

Quit WarehousePG:
```run
\quit
```
---

## 6. Launch Lab 1 Dashboard ([button label="⚠️Terminal Tab"](tab-0))

1. Open the [button label="⚠️Terminal Tab"](tab-0).
2. Execute following commands to launch the Card Fraud Analytics Application
```run
docker exec -it \
  -e WHPG_HOST=localhost \
  -e WHPG_PORT=5432 \
  -e WHPG_DB=demo \
  -e WHPG_USER=gpadmin \
  cdw python3.9 /scripts/apps/app1.py
```
3. Open the [button label="⚠️Card Fraud Analytics Tab"](tab-2) .
4. Go to **SQL Editor Sub Tab** of the application, run following command to explore schema `bfsi_demo`
``` sql
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
5. Explore the 3 panels and execute the pre-defined queries to see WarehousePG in action.

## 7. Check Understanding ([button label="⚠️Card Fraud Analytics Tab"](tab-2))
Go to **Check Understanding Sub Tab** of the application.

Two questions to surface what stuck. Talk to the person next to you - compare answers, then click "Reveal" to see what we're listening for.

> [!WARNING]
> ⚠️Please DON'T click on "Next" button now, wait for our instruction ! ⚠️
