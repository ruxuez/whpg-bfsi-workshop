# Lab 1: Card Fraud & AML Analytics (WarehousePG)

In this lab, you will explore the power of native **int8range** operators and **native JSONB** handling in WarehousePG for financial services analytics. You will perform high-speed fraud detection on approximately 12.7 million events (5.2M card transactions plus authorization, device, case and wire data), demonstrating how native range types and **native JSON operators** outperform standard cloud data warehouses that rely on VARIANT types or string parsing.

## ⚠️ DO NOT HIT "Next" button yet! Wait for your instructor! ⚠️

---

## 1. Lab Overview
* **Application:** `lab1.py`
* **Access Port:** `5001`
* **Database:** `demo`

### Dashboard Content: 6 Queries Across 3 Panels

**Panel 1: Card Transactions** (Native Types)
| ID | Query | Key Demonstration |
| :--- | :--- | :--- |
| **1A** | **Watchlist BIN Match** | Native `int8range` with `<@` operator — no string parsing needed |
| **1C** | **High-Value Instant Payments (JSONB)** | **Native JSON operators** (`@>`, `@?`) run in parallel on every segment, with partition pruning — no string parsing |

**Panel 2: Case & Auth Analytics**
| ID | Query | Key Demonstration |
| :--- | :--- | :--- |
| **2A** | **Case × Auth Correlation** | Join case narratives to auth decisions within 15-min window — replaces SIEM |
| **2C** | **Event Volume Dashboard** | All 5 fact sources in one UNION — one engine, no Splunk |

**Panel 3: BIN Inventory, Limits & Forensic Bonus**
| ID | Query | Key Demonstration |
| :--- | :--- | :--- |
| **3C** | **Customer Risk Scorecard** | Per-customer risk scoring via fraud_risk_score() — worst-first review |
| **4B** | **★ BONUS: Forensic Account Trace** | Trace account 105900001 across transactions, auth, device & wires in one query |

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

You should see `Total primary segments = 4` and `Total mirror segments = 4`, with no failures. Each mirror runs on the *other* segment host from its primary (the primaries on `sdw1` are mirrored on `sdw2`, and the other way round), so losing one container never loses data. It is EXPECTED to see `No coordinator standby configured`: a standby coordinator is not part of this workshop. You will use the mirrors in the high-availability test in Section 6.

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

### Step 3: Load the Traffic Data (~12.7M Rows)

*  **`gpfdist`: External Parallel Load**

**gpfdist** is the WarehousePG Database parallel file distribution program.
It can be used to read external table files to all WarehousePG Database segments in parallel.
It can also be used to accept output streams from WarehousePG Database segments in parallel and write them out to a file.

We are going to use `gpfdist` to load CSV files in parallel to WarehousePG Database.

Our CSV files are stored in `/csv_data` directory,  let's serve files from here using port 8081 (and start gpfdist in the background):
```run
gpfdist -d /csv_data -p 8081 > /home/gpadmin/gpfdist.log &
```

**Before loading, count the rows.** The fact tables exist but are still empty. This script counts all six of them and shows how long the counts take:
```run
psql demo -f /scripts/sql/03a_row_counts.sql
```
Every `row_count` should be `0`.

Then run following command to load CSV data in parallel to WarehousePG thanks to `gpfdist`. Timing is switched on, so you see how long each table takes.

```run
psql demo -e -f /scripts/sql/03_load_external_bfsi.sql
```
> [!IMPORTANT]
> ❗️This process will take 2-3 minutes to load 12.7M rows, please let us know when you are here! 🙋‍


When the load finishes, the output shows one block per table: the `>>> Loading <table> ...` marker, the number of rows loaded (`INSERT 0 <rows>`) and how long that table took (`Time: <ms> ms`):
```text
>>> Loading transactions ...
INSERT 0 5200000
Time: 128193.827 ms (02:08.194)
```
Your times will differ, but `transactions` is the largest table and takes the longest (about two minutes here); the small tables finish in a second or two.

**After loading, count the rows again** and compare them with the `INSERT 0 <rows>` lines above:
```run
psql demo -f /scripts/sql/03a_row_counts.sql
```

| Table | Expected rows |
| :--- | ---: |
| `transactions` | 5,200,000 |
| `auth_decisions` | 5,200,000 |
| `device_events` | 1,200,000 |
| `case_narratives` | 600,000 |
| `wire_events` | 500,000 |
| `account_kpis` | 6,720 |

* **`Analyzedb`** (1-2mins)

Once data load finished, let's run `analyzedb` to perform **ANALYZE** operations on tables incrementally and concurrently.
```run
analyzedb -d demo -s bfsi_demo -a
```

This generates:
- **5.2M transactions** with 4 fraud personas (Normal, Card-Testing, Bust-Out, Structuring)
- **5.2M auth decisions**, **1.2M device events**, **500K wire events** and account KPIs
- **600K case narratives** for Lab 2 semantic search

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

## 6. High Availability Test: Failover, Recovery & Rebalance ([button label="⚠️WarehousePG Tab"](tab-1))
Every primary segment has a mirror on the other segment host. In this section you will crash one primary, watch WarehousePG fail over to its mirror automatically, bring the failed segment back and then rebalance the cluster. You will run everything from the ([button label="⚠️WarehousePG Tab"](tab-1)).

### 6.1 Check the cluster is healthy
```run
psql demo -c "SELECT content, role, preferred_role, mode, status, hostname, port FROM gp_segment_configuration WHERE content >= 0 ORDER BY content, role DESC;"
```
You should see 8 rows: for each of the 4 content IDs, one primary (`role = p`) and one mirror (`role = m`). In this healthy state, `role` equals `preferred_role` for every row, `status` is `u` (up) and `mode` is `s` (synchronized).

Check the mirror status with `gpstate`:
```run
gpstate -m
```

Run a query that you will repeat after the failure:
```run
psql demo -c "SELECT COUNT(*) FROM bfsi_demo.transactions;"
```

### 6.2 Crash a primary
Look up the primary for content `0` and its mirror:
```run
psql demo -c "SELECT content, role, hostname, port, datadir FROM gp_segment_configuration WHERE content = 0 ORDER BY role DESC;"
```
The primary runs on `sdw1` (port 6000) and its mirror on `sdw2` (port 7000). Stop the primary abruptly, as if the process had crashed:
```run
ssh sdw1 "source /usr/local/greenplum-db/greenplum_path.sh && pg_ctl stop -D /data1/primary/gpseg0 -m immediate"
```

### 6.3 Watch the automatic failover
WarehousePG's fault tolerance service (FTS) detects a failed primary and promotes its mirror without any action from you. Trigger it by running the same count again:
```run
psql demo -c "SELECT COUNT(*) FROM bfsi_demo.transactions;"
```
Right after the crash this first attempt is expected to fail with `FTS detected one or more segments are down`: the failed segment is what makes WarehousePG start the failover. Wait a few seconds and run the count again. It now succeeds, served by the mirror, and returns the same number as in 6.1.

Now look at the segments for content `0`. If the roles have not changed yet, wait a few seconds and repeat (without a query to trigger it, detection can take up to a minute):
```run
psql demo -c "SELECT content, role, preferred_role, mode, status, hostname, port FROM gp_segment_configuration WHERE content = 0 ORDER BY port;"
```
When failover is done:
- The mirror on `sdw2` (port 7000) now has `role = p`. It took over as primary, even though its `preferred_role` is still `m`.
- The crashed segment on `sdw1` (port 6000) has `role = m`, `status = d` (down) and `mode = n` (not in sync).

The database stayed available throughout. Show the segments that need attention:
```run
gpstate -e
```
Look for `Segments with Primary and Mirror Roles Switched` and `Downed Segments`.

### 6.4 Recover the failed segment
Bring the crashed segment back as a mirror of the new primary. Full recovery re-copies the segment from the new primary with `pg_basebackup` and takes about 15 seconds here:
```run
gprecoverseg -F -a
```
> [!NOTE]
> Plain `gprecoverseg -a` (incremental recovery) only replays the changes made since the crash. After a hard crash on a busy cluster it can fail with `pg_rewind: fatal: could not find previous WAL record`, and WarehousePG then asks for a full recovery (`-F`), so we use it directly.

Check the resynchronization until `mode` is `s` for every row (repeat if it shows `n`, not in sync):
```run
psql demo -c "SELECT content, role, preferred_role, mode, status, hostname, port FROM gp_segment_configuration WHERE content >= 0 ORDER BY content, role DESC;"
```
All segments are up again, but the roles are still swapped for content `0`: the segment on `sdw2` is primary while it prefers to be the mirror.

### 6.5 Rebalance the cluster
Return every segment to its preferred role:
```run
gprecoverseg -r -a
```
> [!WARNING]
> Rebalancing switches roles for the unbalanced segments and briefly interrupts queries that are running at that moment. Do it in a quiet period.

Confirm that `role` equals `preferred_role` for all rows and `mode` is `s`:
```run
psql demo -c "SELECT content, role, preferred_role, mode, status, hostname, port FROM gp_segment_configuration WHERE content >= 0 ORDER BY content, role DESC;"
```
```run
gpstate -m
```
The cluster is back to its original, balanced state. You lost no data, and the database kept answering queries throughout.

---

## 7. Launch Lab 1 Dashboard ([button label="⚠️Terminal Tab"](tab-0))

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
5. Now work through the three panels in order, as described below.

### Explore the 3 Panels ([button label="⚠️Card Fraud Analytics Tab"](tab-2))
Each panel is a tab at the top of the dashboard. For every query, click its row to expand it and press **Run**; the elapsed time and the number of rows returned appear on the right. **Run All 2** at the top of a panel runs both of its queries and shows the **Total Time**. **Edit in SQL** copies a query into the SQL Editor, where you can put `EXPLAIN ANALYZE` in front of it and press **Run Query** to read the plan.

#### Panel 1: Card Transactions
Native types on the 5.2M-row `transactions` table.

| Query | What to do | What to notice |
| :--- | :--- | :--- |
| **1A · Watchlist BIN Match** | Click **Run**. | Returns the 20 card BINs with the most hits against high-confidence watchlist feeds, with hit count, total amount and first and last seen. The watchlist stores BINs as `int8range` bands. The query expands them with `generate_series()` and uses an equality hash join, which avoids probing a GiST index for every row. In the `EXPLAIN ANALYZE` plan, look for `Gather Motion 4:1`: your 4 primary segments each work on their own slice in parallel. |
| **1C · High-Value Instant Payments (JSONB)** | Click **Run**. | Returns the 20 largest SEPA instant payments with a settlement amount above 9,000, with fields taken out of the ISO 20022 JSON message. Both filters work directly on the JSONB column, `@>` (containment) and `@?` (jsonpath), with no string parsing. In the `EXPLAIN ANALYZE` plan, notice `Number of partitions to scan: 7 (out of 13)` (partition pruning from the date filter) and that the JSONB filter runs on all 4 segments in parallel. About 520,000 rows match, so scanning the relevant partitions in parallel is cheaper than an index lookup here. The GIN index `idx_txn_iso_gin` on `iso_msg` pays off for more selective filters. |

#### Panel 2: Case & Auth Analytics
Joins across fact tables that a SIEM would normally correlate.

| Query | What to do | What to notice |
| :--- | :--- | :--- |
| **2A · Case × Auth Correlation** | Click **Run**. | For each fraud case, counts the declines and approvals within a window from 5 minutes before to 10 minutes after the case was opened, and sorts by `declines_nearby`. It joins 600K case narratives to 5.2M authorization decisions. Both tables are distributed on `(account_id, card_bin)`, so each segment joins its own rows locally. Accounts with many declines right when a case opens point to card testing or bust-out. |
| **2C · Event Volume Dashboard** | Click **Run**. | One `UNION ALL` counts events from five fact tables: transactions, device events, authorization decisions, case narratives and wire events. Compare the counts with the ones you saw after the load (this query only counts events from 2026-06-01). Then click **Run All 2** and read the **Total Time** for both queries. |

#### Panel 3: BIN Inventory, Limits + Forensic Bonus
Customer risk scoring and a multi-source investigation.

| Query | What to do | What to notice |
| :--- | :--- | :--- |
| **3C · Customer Risk Scorecard** | Click **Run**. | Returns the 10 customer portfolios ranked from worst to best `risk_score`, calculated by the SQL function `fraud_risk_score()` from decline rate, fraud basis points and average ticket. Compare each `avg_decline_pct` with its `max_decline_rate` limit to see which customers breach their risk profile. |
| **★ BONUS · Forensic Account Trace** | Click **Run**. | Builds one timeline of up to 40 events for account `105900001` from four different tables (`transaction`, `auth`, `device` and `wire` in the `source` column). This account belongs to the structuring persona, which sends wires just under the 10,000 reporting threshold. Find those wires, and notice that you needed no export and no second tool to see the full picture. |

When you have run all six queries, continue with the check-understanding questions.


## 8. Check Understanding ([button label="⚠️Card Fraud Analytics Tab"](tab-2))
Go to **Check Understanding Sub Tab** of the application.

Two questions to surface what stuck. Talk to the person next to you - compare answers, then click "Reveal" to see what we're listening for.

> [!WARNING]
> ⚠️Please DON'T click on "Next" button now, wait for our instruction ! ⚠️
