# Lab 2: Making Data Smart (In-Database AI)

## Overview
In this lab, you will see how **pgvector** semantic search and **Apache MADlib** behavioral clustering discover fraud patterns automatically - all running **inside the database**. No Python export. No Spark jobs. Just SQL.

**The Traditional Problem:**
- **Keyword search fails**: `LIKE '%bust out%'` finds ZERO results (same fraud, different words)
- **External ML is slow**: Export → Train → Import = 4+ hours
- **PII leaves the database**: Compliance risk with every export

**The WarehousePG Solution:**
- **Semantic search IN the database**: Finds fraud by meaning, not exact keywords
- **ML clustering IN the database**: Discovers patterns automatically
- **800× faster**: What takes hours in traditional warehouses takes seconds here
- **Zero export**: PII never leaves the database

## ⚠️ DO NOT HIT "Next" button without instructions ! ⚠️

---

## What You'll See

### Panel A: Semantic Search (pgvector)
**The Problem:** Keyword search misses fraud described with different terminology

| Query | What It Shows |
| :--- | :--- |
| **A1: Keyword Search Fails** | `LIKE '%structuring%'` finds almost nothing, yet structuring fraud is everywhere - just described differently! |
| **A2: Semantic Search Wins** | Finds fraud by **meaning**: "just below the limit", "layering offshore", "similar-sized wires" - NO exact keywords needed! |

**Key Insight:** pgvector semantic search runs IN the database. Traditional warehouses require exporting PII to Python for NLP (~4 hours). WarehousePG does it in SQL (4 seconds).

---

### Panel B: Behavioral Clustering (MADlib)
**The Power:** Unsupervised ML discovers fraud patterns automatically - no rules written!

| Query | What It Shows |
| :--- | :--- |
| **B1: ML Discovered 4 Personas** | K-Means found 4 clusters: 🃏 CARD-TESTING, 💳 BUST-OUT, 💰 STRUCTURING, ✅ NORMAL - automatically! |
| **B2: The Statistical Proof** | CARD-TESTING: **3,456 merchants** (57× normal). BUST-OUT: **$48,725 spend** (97× normal). These aren't subtle! |

**Key Insight:** MADlib clustering runs IN the database. Traditional warehouses require exporting to Spark for ML (~6 hours). WarehousePG does it in SQL (6 seconds).

---

## Lab Setup

### Prerequisites
Data was already loaded in Lab 1. This lab builds AI/ML analytics on that existing data.

**Embedded Fraud Personas:**
The 13M transactions contain 4 behavioral patterns:
- **Normal (70%)**: Baseline card activity
- **Card-Testing (12%)**: Hundreds of merchants, tiny amounts ($0.40-$4)
- **Bust-Out (8%)**: Massive spend ($800-$5000), few merchants
- **Structuring (10%)**: Sub-$10k wires, consistent amounts, high-risk countries

> [!NOTE]
> Execute following from the [button label="⚠️WarehousePG Tab"](tab-0).

---

## Step 1: Prepare AI Models (One Command, 2-3 Minutes)

In traditional data warehouses, you'd export PII to Python/Spark, train models externally, then re-import results (4-6 hours). WarehousePG does it all IN the database - just run one script.

### 1.1 Open psql

```run
psql demo
```

### 1.2 Run the AI Preparation Script

```run
\i /scripts/sql/lab2_data_preparation.sql
```

> [!IMPORTANT]
> ⏱️ **This takes 2-3 minutes.** Watch for progress messages:
> - `[1/5] Cleaning up...`
> - `[2/5] Building pgvector embeddings... (60-90 seconds)`
> - `[3/5] Extracting behavioral features... (15-20 seconds)`
> - `[4/5] Running MADlib clustering... (20-30 seconds)`
> - `[5/5] Creating labeled views...`
> - `✓ Setup Complete!`

**What this script does:**
1. **pgvector embeddings** - Converts 200K fraud narratives to vectors for semantic search
2. **MADlib features** - Extracts behavioral patterns from 50K accounts
3. **K-means clustering** - Automatically discovers 4 fraud personas
4. **Labeled views** - Makes results queryable with SQL

When complete, you'll see a summary showing what ML discovered:
```
Fraud Persona Distribution:
 inferred_label | accounts 
----------------+----------
 CARD-TESTING   |       20
 BUST-OUT       |       15
 STRUCTURING    |       20
 NORMAL         |    50000
```

### 1.3 Quit psql
```run
\quit
```

---

## Step 2: Visualize What ML Discovered (5 Minutes)

See the fraud personas ML found automatically - no rules written, just math finding patterns.

### 2.1 Launch the Cluster Dashboard

Switch to the [button label="⚠️Terminal Tab"](tab-0) and run:

```run
docker exec -it \
  -e WHPG_HOST=localhost \
  -e WHPG_PORT=5432 \
  -e WHPG_DB=demo \
  -e WHPG_USER=gpadmin \
  cdw python3.9 /scripts/apps/dashboard.py
```

### 2.2 Explore the Visualization

Open [button label="⚠️MADlib Dashboard Tab"](tab-1)

**Try this:**
1. Select axes: `total_amount` (X) vs `distinct_merchants` (Y)
2. Look for 4 distinct clusters:
   - **Near origin (0,0)**: ✅ NORMAL - moderate spend, moderate merchants
   - **At ~3,500 merchants, low spend**: 🃏 CARD-TESTING - testing card validity across thousands of merchants
   - **At ~$100K spend, few merchants**: 💳 BUST-OUT - massive spend concentrated at a handful of merchants
   - **High spend, ultra-low variance**: 💰 STRUCTURING - consistent $9.5K wires (below reporting threshold)

**What you're seeing:** MADlib K-means automatically separated accounts into behavioral groups. The distance between clusters shows how different these fraud patterns are from normal behavior - not subtle, **mathematically undeniable**.

### 2.3 Stop the Dashboard

Press `CTRL+C` in the [button label="⚠️Terminal Tab"](tab-0) when done.

---

## Step 3: Query the AI Results (10 Minutes)

See how in-database AI outperforms traditional approaches - semantic search vs keywords, ML discovery vs manual rules.

### 3.1 Launch the AI Analytics Dashboard

From the [button label="⚠️Terminal Tab"](tab-0), run:

```run
docker exec -it \
  -e WHPG_HOST=localhost \
  -e WHPG_PORT=5432 \
  -e WHPG_DB=demo \
  -e WHPG_USER=gpadmin \
  cdw python3.9 /scripts/apps/app2.py
```

### 3.2 Open the Dashboard

Go to [button label="⚠️AI Analytics Tab"](tab-2)

---

### Panel A: Semantic Search (pgvector)

**Query A1: The Keyword Search Problem**
- Try searching: `LIKE '%structuring%'`
- **Result:** Finds almost nothing (0-5 results)
- **But:** The `actual_structuring_notes` column shows hundreds exist!
- **Problem:** Same fraud behavior, different words = missed fraud

**Query A2: Semantic Search Solution**
- Uses pgvector to find fraud by **meaning**, not exact keywords
- **Result:** Finds 15+ narratives describing structuring:
  - "just below the limit"
  - "layering offshore"
  - "similar-sized wires"
  - NO exact keyword "structuring" needed!
- ⏱️ **Speed:** 4-5 seconds on 13M transactions

> **What This Means:**
> - Traditional warehouse: Export PII to Python for NLP → ~4 hours
> - WarehousePG: Semantic search runs IN the database → 4 seconds
> - **800× faster. Zero export. Zero compliance risk.**

---

### Panel B: Behavioral Clustering (MADlib)

**Query B1: ML-Discovered Personas**
- Shows the 4 clusters MADlib found automatically
- **Result:** CARD-TESTING, BUST-OUT, STRUCTURING, NORMAL
- **Key Point:** No rules written - ML discovered these patterns from behavioral data alone

**Query B2: The Dramatic Differences**
- Shows quantified differences between fraud and normal behavior
- **Result:**
  - 🃏 CARD-TESTING: **3,456 merchants** (57× more than normal)
  - 💳 BUST-OUT: **$48,725 spend** (97× more than normal)
  - 💰 STRUCTURING: **amount_cv = 0.08** (ultra-low variance = consistent $9.5K wires)
- ⏱️ **Speed:** 5-6 seconds on 50K accounts

> **What This Means:**
> - Traditional warehouse: Export to Spark for clustering → ~6 hours
> - WarehousePG: MADlib runs IN the database → 6 seconds
> - **600× faster. No orchestration. Just SQL.**

---

### Panel C: The AI Factory (Cross-Validation)

**The Power of Independent Validation**

When two independent AI systems analyze the same data from different angles and arrive at the same conclusions, confidence is high. This is the "AI Factory" concept:

- **MADlib**: Examines **behavioral patterns** (transaction counts, merchant diversity, spend amounts) - never reads text
- **pgvector**: Examines **semantic patterns** (case narrative language) - never sees transaction amounts

They don't share methodology or features - yet they independently discover the same fraud types.

**Query C1: Cross-Check - Do Both Detectives Agree?**

This query JOIN the suspect lists ON account_id to find the overlap:

```sql
-- MADlib suspects: accounts flagged by behavioral clustering
-- pgvector suspects: accounts with fraud-related case narratives
-- found_by_both: accounts BOTH systems flagged
```

- **Result:** Shows agreement percentage for each fraud type
- **Why This Matters:** When behavioral math and semantic analysis both flag the same accounts, false positives drop dramatically
- **The Overlap:** Accounts in the "found_by_both" column are high-confidence fraud candidates

> **Production Insight:**
> 
> Traditional fraud detection: Rules trigger → Analyst investigates → Manual decision
> 
> AI Factory approach: Two independent AI systems agree → High confidence → Prioritize for investigation
> 
> When MADlib (numbers detective) and pgvector (language detective) both point at the same account, you have **converging evidence from independent sources** - the foundation of strong fraud cases.

---

### Panel D: Closing the Loop (ML → Operations)

**From Discovery to Action**

MADlib doesn't just discover fraud - it can **predict who should be on your watchlist** before manual investigation. Traditional fraud watchlists are reactive (you add a BIN after it's compromised). MADlib is predictive (it flags accounts exhibiting fraud-like patterns before they hit your watchlist).

**Query D1: The ML Fraud Gap**
- Shows transactions from accounts **MADlib flagged as fraud but NOT yet in your fraud_watchlists table**
- **Result:** These are **predictive alerts** - ML found them, but your operational system hasn't caught up yet
- **The Gap:** In production, these are the accounts you'd route to fraud analysts for investigation

**Refresh Watchlist Button**
- Click "Refresh Watchlist" to write ML results back to the operational watchlist table
- Runs:
  ```sql
  INSERT INTO fraud_watchlists 
  SELECT account_id, 'MADlib K-Means', confidence, ...
  FROM kmeans_labeled 
  WHERE inferred_label IN ('CARD-TESTING', 'BUST-OUT', 'STRUCTURING')
  ```

**Query D2: Extended Watchlist (After Refresh)**
- Shows Query 1A from Lab 1 extended with **BOTH**:
  - **Blue rows**: Traditional BIN-range matches
  - **Green rows**: ML-discovered accounts (from MADlib)
- **Result:** One query, two fraud detection methods working together

> **Production Use Case:**
> 
> Run MADlib clustering **nightly** on the previous day's transactions:
> ```sql
> -- Overnight batch job (runs at 2 AM)
> REFRESH MATERIALIZED VIEW ml_fraud_clusters;
> INSERT INTO fraud_watchlists 
> SELECT * FROM ml_fraud_clusters WHERE risk_score > 0.85;
> ```
> 
> By morning, fraud analysts see behavioral anomalies detected by ML - often catching fraud rings **2-3 weeks before** traditional rule-based systems. No Python export, no external ML platform, no data movement - just SQL writing directly back to operational tables.

---

## Key Takeaways

### What You Just Saw

1. ✅ **Semantic search runs in SQL** - No Python export needed
2. ✅ **ML clustering runs in SQL** - No Spark jobs needed  
3. ✅ **Results join to transactions in SQL** - No orchestration needed
4. ✅ **PII never leaves the database** - Compliance built-in
5. ✅ **800× faster** - No data movement = no waiting

### The Speed Advantage

| Approach | pgvector Semantic Search | MADlib Clustering | Total Time |
|----------|-------------------------|-------------------|------------|
| **Traditional Warehouse** | Export → Python NLP → Re-import | Export → Spark ML → Re-import | **~10 hours** |
| **WarehousePG** | Just query the table | Just query the table | **~10 seconds** |

**Why the difference?** In-database AI eliminates the "data movement tax" - no export, no external systems, no waiting.

### What DBAs Should Remember

🎯 **For Your Team:**
- "I can do semantic search without exporting PII"
- "I can run ML without Spark or Python"
- "Results are 800× faster because data doesn't move"
- "One system, one interface, one security model"

🎯 **For Your Fraud Analysts:**
- "Search by meaning, not just keywords"
- "ML finds patterns you didn't know to look for"
- "Answers in seconds instead of hours"

🎯 **For Your Compliance Team:**
- "PII never leaves the database"
- "Same access controls, same audit logs"
- "No external ML platforms to govern"

### Real-World Impact

In production, you'd run MADlib clustering nightly on new transactions. By morning, fraud analysts see updated risk scores and anomaly flags - **without any data leaving the database**. Traditional approaches require exporting PII to Python/Spark, training models externally, and re-importing results (4-10 hours + compliance overhead). WarehousePG does it all in SQL.

**This is the future of financial analytics: AI that runs where the sensitive data lives.**

> [!WARNING]
> ⚠️ **Please DON'T click "Next" button yet, wait for instructor guidance!** ⚠️
