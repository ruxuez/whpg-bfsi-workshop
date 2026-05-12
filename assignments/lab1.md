# Lab 1: Network Analytics (WarehousePG)

In this lab, you will explore the power of native **inet** and **cidr** operators in WarehousePG. You will perform high-speed analytics on approximately 100 million rows of synthetic network telemetry, demonstrating how native networking types outperform standard cloud data warehouses.

---

## 1. Lab Overview
* **Application:** `lab1.py`
* **Access Port:** `5001`
* **Database:** `demo`

### Dashboard Content: 6 Queries Across 3 Panels
| ID | Query | Key Demonstration |
| :--- | :--- | :--- |
| **1A** | **Threat Intel Match** | `inet <<=` join — 6 LOC vs 52 on Snowflake |
| **1C** | **Top Talkers** | Dynamic `/24` grouping with `set_masklen()` |
| **2A** | **Cross-Source Correlation** | Catches correlated incidents no single tool sees |
| **2C** | **Log Volume** | $2M+ potential Splunk savings via log offloading |
| **3C** | **QoE Scorecard** | Aggregates 151K live metrics |
| **4B** | **Forensic IP Trace** | Trace a single IP across ALL log sources instantly |

### Tabs preparation

Prepare 2 Shell Tabs:
- Your local host under this repo (Terminal Tab)
- Connection to `cdw` envionment shell (WarehousePG Tab):
```bash
docker exec -u gpadmin -w /home/gpadmin -it cdw /bin/bash
```

---

## 2. Monitor Startup (⚠️Terminal Tab)
Before starting the lab, ensure the WarehousePG cluster is fully initialized.

1. Open the **⚠️Terminal Tab**.
2. Watch the coordinator logs:
   ```bash
   docker logs -f cdw
   ```
3. **Wait** for the `DEPLOYMENT SUCCESSFUL` banner to appear before proceeding.  Ensure there is no ERROR in logs.

> [!WARNING]
> Press the `Ctrl+C` key on your keyboard to take the control back.

---

## 3. Verify Cluster Status (⚠️Terminal Tab)
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

It is EXPECTED to see `Mirrors not configured on this array` and `No coordinator standby configured`. This is a demo setup running in your own VM.

---

## 4. Initialize Lab 1 Data (**⚠️WarehousePG Tab**)
Switch to the **⚠️WarehousePG Tab** to execute these instructions.

You will now create the schema and seed the database with network telemetry data.
Execute the following commands to build the tables and load the telemetry. We use the full path to `psql` and source the environment to ensure connectivity.

### Step 1: Load Schema

This command takes only few seconds to create 700+ partitions:
```bash
psql demo -e -f /scripts/sql/01_schema.sql
```

### Step 2: Seed Reference Data
```bash
psql demo -e -f /scripts/sql/02_seed_reference.sql
```

### Step 3: Seed Traffic Logs (50M Rows)

*  **`gpfdist`: External Parallel Load**

**gpfdist** is the WarehousePG Database parallel file distribution program.
It can be used to read external table files to all WarehousePG Database segments in parallel.
It can also be used to accept output streams from WarehousePG Database segments in parallel and write them out to a file.

We are going to use `gpfdist` to load CSV files in parallel to WarehousePG Database.

Our CSV files are stored in `/csv_data` directory,  let's serve files from here using port 8081 (and start gpfdist in the background):
```bash
gpfdist -d /csv_data -p 8081 > /home/gpadmin/gpfdist.log &
```

Then run following command to load CSV data in parallel to WarehousePG thanks to `gpfdist`.

```bash
psql demo -e -f /scripts/sql/03_load_external.sql
```
> [!IMPORTANT]
> ❗️This process will take 1-2 minutes to load 50M rows, please let us know when you are here! 🙋‍


* **`Analyzedb`** (1-2mins)

Once data load finished, let's run `analyzedb` to perform **ANALYZE** operations on tables incrementally and concurrently.
```bash
analyzedb -d demo -s netvista_demo -a
```

> [!NOTE]
> If you want to recreate Data, rerun from ***Step 1: Load Schema***

---

## 5. WarehousePG Quick Tests (**⚠️WarehousePG Tab**)
Execute these from the **⚠️WarehousePG Tab**.

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
Greenplum is a distributed system. Use this query to see the **Master (Coordinator)** and **Segment** instances, their status, and which port/address they are using.
```sql
SELECT dbid, content, role, preferred_role,
		mode, status, hostname, address, port
FROM gp_segment_configuration ORDER BY dbid;
```
---

### 5.3 Explore Databases
List all databases currently initialized in the cluster to ensure `demo`, and system databases are present.
```sql
\l
```

---

### 5.4 Explore Extensions in `demo` Database
WarehousePG uses several advanced extensions for AI and Analytics. Check which ones are active in the `demo` database (e.g., `vector`, `pgaa`, `pgfs`).
```sql
\dx
```
Check schemas (e.g., `madlib`, `pgaa`, `pgfs`)
```sql
\dn
```

---

### 5.5 Explore Runtime Configuration (GUCs)
Check specific "Grand Unified Configuration" (GUC) parameters that control the behavior of the WarehousePG optimizer and parallel execution.

Check if the GPORCA optimizer is enabled
```sql
SHOW optimizer;
```

Check maximum connections allowed across the cluster
```sql
SHOW max_connections;
```

Quit WarehousePG:
```sql
\quit
```
---

## 6. Launch Lab 1 Dashboard (⚠️Terminal Tab)

1. Open the **⚠️Terminal Tab**.
2. Execute following commands to launch Networks Analytics Application
```bash
docker exec -it \
  -e WHPG_HOST=localhost \
  -e WHPG_PORT=5432 \
  -e WHPG_DB=demo \
  -e WHPG_USER=gpadmin \
  cdw python3.9 /scripts/apps/app1.py
```
3. Open the application from your browser:
`http://localhost:5001`
4. Go to **SQL Editor Sub Tab** of the application, run following command to explore schema `netvista_demo`
``` sql
SELECT
    n.nspname,
    c.relname,
    CASE WHEN c.relkind = 'p' THEN 'Partitioned Root' ELSE 'Standard Table' END as type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'netvista_demo'
  AND c.relkind IN ('r', 'p')
  AND c.relispartition = false;
```
5. Explore the 3 panels and execute the pre-defined queries to see WarehousePG in action.

## 7. Check Understanding (Networks Analytics App)

Go to **Check Understanding Sub Tab** of the application.

Two questions to surface what stuck. Talk to the person next to you - compare answers, then click "Reveal" to see what we're listening for.