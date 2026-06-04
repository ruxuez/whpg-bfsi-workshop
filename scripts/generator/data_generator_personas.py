#!/usr/bin/env python3
"""
Meridian Retail Bank × EDB WarehousePG — Persona-Based Data Generator
Lab 3: Hybrid Forensic Discovery (MADlib + pgvector)

Generates transactions and case_narratives with FOUR distinct behavioural
personas that create clear, discoverable patterns for MADlib K-Means clustering
and pgvector semantic search:

  1. NORMAL        — baseline card activity (bulk of the dataset)
  2. CARD-TESTING  — tiny auth amounts sprayed across MANY merchants
  3. BUST-OUT      — limit ramped to the ceiling at a FEW merchants, high spend
  4. STRUCTURING   — many sub-$10k wires to BR/NG/RU, near-zero amount variance

These shapes are tuned to the validated account_features separators:
  card-testing => high distinct_merchants ; bust-out => high total_amount ;
  structuring  => amount_cv < 0.1 (test this FIRST or it mislabels as bust-out).

Usage:
    pip3 install numpy faker        # optional; falls back to stdlib random
    python3 data_generator_personas.py [--scale medium]

Outputs (plain CSV, no header — loadable by 03_load_external_bfsi.sql):
    csv_data/transactions.csv
    csv_data/case_narratives.csv
"""

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta

# ── Configuration ─────────────────────────────────────────────────────────────

SCALES = {
    "small":  {"transactions": 200_000,    "narratives": 50_000,    "device_events": 25_000,    "auth_decisions": 200_000,    "wire_events": 20_000,    "account_kpis": True},
    "medium": {"transactions": 5_200_000,  "narratives": 600_000,   "device_events": 1_200_000, "auth_decisions": 5_200_000,  "wire_events": 500_000,   "account_kpis": True},
    "large":  {"transactions": 15_800_000, "narratives": 2_000_000, "device_events": 4_000_000, "auth_decisions": 15_800_000, "wire_events": 1_580_000, "account_kpis": True},
}
# Target total rows: small ~0.5M | medium ~12.7M | large ~43M
# Workshop optimization: auth_decisions = transactions (1:1 simplification), narratives ~12% (fraud/dispute cases only)

# All data lives in JUNE 2026 (2026-06-01 .. 2026-07-01)
NOW   = datetime(2026, 7, 1)          # window end
START = datetime(2026, 6, 1)          # window start

# ── Account pools per persona (mirror 03_seed_traffic_with_personas.sql) ───────
NORMAL_ACCT       = (100000000, 49999)
CARD_TESTING_ACCT = (100900001, 19)    # Visa BIN 41010100-199 (20 accounts)
BUST_OUT_ACCT     = (101900001, 14)    # MC   BIN 52020200-299 (15 accounts)
STRUCTURING_ACCT  = (105900001, 19)    # BIN 53030300-399, sub-$10k wires (20 accounts)

BICS       = ["CHASUS33", "BARCGB22", "DEUTDEFF", "BNPAFRPP"]
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]
MCCS       = [5411, 5812, 5942, 5999, 6011, 4829, 7995, 5734, 4111, 5651]
MERCHANTS  = ["Retail Merchant", "Corner Grocer", "Fuel Stop", "Online Bazaar",
              "City Pharmacy", "Transit Authority", "Electronics Hub", "Cafe Central"]
COUNTRIES  = ["US", "US", "US", "GB", "DE", "FR"]
HIGH_RISK  = ["BR", "NG", "RU"]
ANALYSTS   = ["a.okafor", "l.nguyen", "m.silva", "r.haddad", "t.olsen", "s.kapoor"]

# Bust-out reuses a tight set of merchant ids (low distinct_merchants)
BUST_OUT_MERCHANTS = [8800001 + i for i in range(9)]

# ── Narrative templates per persona (drive pgvector semantic search) ──────────
NARR = {
    "card_testing": [
        "Repeated penny authorisations checking which card numbers are still live",
        "Burst of small declines followed by one larger successful charge",
        "Dozens of sub-dollar auth attempts across many unrelated online merchants",
        "Rapid low-value probing pattern consistent with a freshly dumped card set",
        "Automated micro-charges sweeping a BIN range to validate active accounts",
    ],
    "bust_out": [
        "Newly raised limit consumed almost entirely within two days at a narrow set of merchants",
        "Balance ramped to the ceiling on resaleable electronics, then the account went dark",
        "Aggressive spend-up immediately preceding a returned payment and charge-off",
        "Account behaved perfectly for months then drained the full line in a single weekend",
        "Concentrated high-ticket purchases at two outlets, repayment bounced, written off",
    ],
    "structuring": [
        "Steady stream of payments each landing a few hundred below the mandatory reporting line",
        "Dozens of similar-sized outbound wires, none individually notable, same overseas counterparty",
        "Just-below-limit disbursements suggest deliberate avoidance of the declaration threshold",
        "Funds enter and leave within minutes, hopping through a prepaid wallet before heading offshore",
        "Layered movement of value across linked accounts to obscure the ultimate destination",
    ],
    "normal": [
        "Routine dispute over a duplicate point-of-sale charge, refunded after review",
        "Customer confirmed travel; temporary geo block lifted after identity verification",
        "Standard chargeback for an undelivered online order, merchant credited the cardholder",
        "Card reported lost; replacement issued and prior authorisations reconciled cleanly",
        "Minor address-verification mismatch cleared once the cardholder updated their profile",
    ],
}
QUEUE_FOR = {"card_testing": "cards-fraud", "bust_out": "cards-fraud",
             "structuring": "aml-tm", "normal": "disputes"}
BIN_FOR   = {"card_testing": 41010100, "bust_out": 52020200,
             "structuring": 53030300, "normal": 40000000}

# ── Additional event data (for Lab 1 full support) ────────────────────────────
CHANNELS_ALL = ["POS", "ECOM", "ATM", "MOBILE", "WEB"]
DECISIONS    = ["APPROVE", "APPROVE", "APPROVE", "APPROVE", "DECLINE", "STEP_UP"]
DEVICE_EVENTS = ["login", "login", "payee_add", "pwd_reset", "new_device"]
RESULTS      = ["OK", "OK", "OK", "FAIL", "FLAG"]
WIRE_TYPES   = ["SENT", "RETURNED", "RECALLED", "AMENDED"]
RAILS        = ["SWIFT", "SEPA", "ACH", "FEDWIRE"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def acct(pool):
    base, span = pool
    return base + random.randint(0, span)


def rand_ts(start, end):
    return (start + timedelta(seconds=random.random() * (end - start).total_seconds())).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def pan4():
    return f"{random.randint(0, 9999):04d}"


def iso_msg(svc, instr, value, ccy, cdtr_ctry, bic=None):
    msg = {
        "GrpHdr": {"MsgId": f"MSG{random.randint(1, 10**9)}", "SttlmInf": {"SttlmMtd": "CLRG"}},
        "PmtTpInf": {"SvcLvl": {"Cd": svc}, "LclInstrm": {"Cd": instr}},
        "IntrBkSttlmAmt": {"Ccy": ccy, "value": value},
        "Dbtr": {"Nm": "Meridian Cardholder", "CtryOfRes": "US"},
        "Cdtr": {"Nm": "Retail Merchant", "CtryOfRes": cdtr_ctry},
        "RmtInf": {"Ustrd": "Card purchase"},
    }
    if bic:
        msg["CdtrAgt"] = {"FinInstnId": {"BICFI": bic}}
    return json.dumps(msg, separators=(",", ":"))


# ── Transaction row builders ──────────────────────────────────────────────────
# Column order = 01_schema transactions minus the serial PK txn_id:
#   ts,account_id,card_bin,pan_last4,amount,currency,mcc,merchant_id,merchant_name,
#   merchant_country,channel,txn_type,auth_response,beneficiary_account,
#   beneficiary_country,iso_msg,region_id

def make_normal_txn():
    a = acct(NORMAL_ACCT); amt = round(3 + random.random() * 800, 2)
    ccy = random.choice(CURRENCIES); ctry = random.choice(COUNTRIES)
    svc = random.choice(["CARD", "CARD", "SEPA", "NURG"]); instr = random.choice(["POS", "ECOM", "ATM"])
    return [rand_ts(START, NOW), a, random.randint(40000000, 49999999), pan4(), amt, ccy,
            random.choice(MCCS), random.randint(1, 9_000_000), random.choice(MERCHANTS), ctry,
            random.choice(["POS", "ECOM", "ATM"]), "PURCHASE", random.choice(["A", "A", "A", "D"]),
            "", "", iso_msg(svc, instr, amt, ccy, ctry, random.choice(BICS)), random.randint(1, 7)]


def make_card_testing_txn():
    a = acct(CARD_TESTING_ACCT); amt = round(0.40 + random.random() * 3.60, 2)
    # MANY distinct merchants -> high distinct_merchants feature
    return [rand_ts(NOW - timedelta(hours=6), NOW), a, 41010100 + random.randint(0, 99), pan4(), amt, "USD",
            5999, random.randint(1, 9_000_000), "TEST-MERCHANT", "US", "ECOM", "PURCHASE",
            random.choice(["A", "D", "D"]), "", "", iso_msg("CARD", "ECOM", amt, "USD", "US"),
            random.randint(1, 7)]


def make_bust_out_txn():
    a = acct(BUST_OUT_ACCT); amt = round(800 + random.random() * 4200, 2)
    # FEW merchants -> low distinct_merchants, high total spend
    return [rand_ts(NOW - timedelta(hours=48), NOW), a, 52020200 + random.randint(0, 99), pan4(), amt, "USD",
            5944, random.choice(BUST_OUT_MERCHANTS), "LUXURY-GOODS", "US", "POS", "PURCHASE", "A",
            "", "", iso_msg("CARD", "POS", amt, "USD", "US"), random.randint(1, 7)]


def make_structuring_txn():
    a = acct(STRUCTURING_ACCT); amt = round(9000 + random.random() * 900, 2)
    ctry = random.choice(HIGH_RISK); ben = 700000000 + random.randint(0, 999999)
    # near-zero merchant variety + tight amount band -> amount_cv < 0.1
    return [rand_ts(START, NOW), a, 53030300 + random.randint(0, 99), pan4(), amt, "USD",
            6011, 9900001, "Beneficiary Co", ctry, random.choice(["WIRE", "INST", "P2P"]), "PAYMENT", "A",
            ben, ctry, iso_msg("SEPA", "INST", amt, "USD", ctry, random.choice(BICS)), random.randint(1, 7)]


# ── Narrative row builder ─────────────────────────────────────────────────────
# Column order = 01_schema case_narratives minus serial PK note_id:
#   ts,account_id,card_bin,analyst,queue,severity,narrative,region_id

def make_narrative(persona, pool):
    a = acct(pool)
    return [rand_ts(START, NOW), a, BIN_FOR[persona] + random.randint(0, 99),
            random.choice(ANALYSTS), QUEUE_FOR[persona], random.randint(1, 3),
            random.choice(NARR[persona]), random.randint(1, 7)]


# ── Auth Decision row builder ─────────────────────────────────────────────────
# Column order = 01_schema auth_decisions minus serial PK decision_id:
#   ts,account_id,card_bin,mcc,amount,decision,rule_id,channel,merchant_country,region_id

def make_auth_decision(persona, pool):
    """Generate auth decision matching transaction patterns per persona"""
    a = acct(pool)
    if persona == "card_testing":
        amt = round(0.40 + random.random() * 3.60, 2)
        decision = random.choice(["APPROVE", "DECLINE", "DECLINE"])  # Higher decline rate
        bin_val = 41010100 + random.randint(0, 99)
    elif persona == "bust_out":
        amt = round(800 + random.random() * 4200, 2)
        decision = "APPROVE"  # Bust-out approvals succeed
        bin_val = 52020200 + random.randint(0, 99)
    elif persona == "structuring":
        amt = round(9000 + random.random() * 900, 2)
        decision = "APPROVE"
        bin_val = 53030300 + random.randint(0, 99)
    else:  # normal
        amt = round(3 + random.random() * 800, 2)
        decision = random.choice(["APPROVE", "APPROVE", "APPROVE", "DECLINE"])
        bin_val = random.randint(40000000, 49999999)

    return [rand_ts(START, NOW), a, bin_val, random.choice(MCCS), amt,
            decision, random.randint(1, 100), random.choice(CHANNELS_ALL),
            random.choice(COUNTRIES), random.randint(1, 7)]


# ── Device Event row builder ──────────────────────────────────────────────────
# Column order = 01_schema device_events minus serial PK device_evt_id:
#   ts,account_id,device_fingerprint,ip_country,channel,event_type,result,region_id

def make_device_event(persona, pool):
    """Generate device/login events"""
    a = acct(pool)
    # Card-testing and fraud personas have higher FLAG rates
    if persona in ["card_testing", "bust_out"]:
        result = random.choice(["OK", "OK", "FLAG", "FAIL"])
        country = random.choice(COUNTRIES + HIGH_RISK)
    else:
        result = random.choice(["OK", "OK", "OK", "FAIL"])
        country = random.choice(COUNTRIES)

    return [rand_ts(START, NOW), a, f"fp{random.randint(100000, 999999)}",
            country, random.choice(["WEB", "MOBILE"]), random.choice(DEVICE_EVENTS),
            result, random.randint(1, 7)]


# ── Wire Event row builder ────────────────────────────────────────────────────
# Column order = 01_schema wire_events minus serial PK wire_id:
#   ts,ordering_account,beneficiary_bic,beneficiary_country,event_type,rail,amount,region_id

def make_wire_event(persona, pool):
    """Generate wire/SWIFT events - primarily for structuring persona"""
    a = acct(pool)
    if persona == "structuring":
        # Structuring: consistent sub-$10k wires to high-risk countries
        amt = round(9000 + random.random() * 900, 2)
        country = random.choice(HIGH_RISK)
        rail = random.choice(["SWIFT", "SEPA"])
    elif persona == "bust_out":
        # Occasional large wire after bust-out
        amt = round(5000 + random.random() * 15000, 2)
        country = random.choice(COUNTRIES)
        rail = "SWIFT"
    else:
        # Normal wires
        amt = round(500 + random.random() * 5000, 2)
        country = random.choice(COUNTRIES)
        rail = random.choice(RAILS)

    return [rand_ts(START, NOW), a, random.choice(BICS), country,
            random.choice(WIRE_TYPES), rail, amt, random.randint(1, 7)]


# ── Main ──────────────────────────────────────────────────────────────────────

def generate(scale="medium"):
    cfg = SCALES[scale]
    n_txn = cfg["transactions"]
    n_narr = cfg["narratives"]
    n_device = cfg["device_events"]
    n_auth = cfg["auth_decisions"]
    n_wire = cfg["wire_events"]

    total_rows = n_txn + n_narr + n_device + n_auth + n_wire + 6720  # +6720 for account_kpis
    print("=" * 70)
    print("  Meridian Bank Persona-Based Data Generator (6 tables)")
    print(f"  Scale: {scale}  |  Total Rows: ~{total_rows:,}")
    print("=" * 70)

    # Mix: large NORMAL baseline + tight fraud-persona clusters
    # Increased fraud percentages to create dramatic separation in clusters
    #   Normal 60% | Card-testing 20% | Bust-out 10% | Structuring 10%
    n_normal = int(n_txn * 0.60)
    n_ctest  = int(n_txn * 0.20)
    n_bust   = int(n_txn * 0.10)
    n_struct = n_txn - n_normal - n_ctest - n_bust

    print("\nGenerating transactions...")
    txn_rows = []
    print(f"  Normal       : {n_normal:,}")
    for _ in range(n_normal):  txn_rows.append(make_normal_txn())
    print(f"  Card-testing : {n_ctest:,}")
    for _ in range(n_ctest):   txn_rows.append(make_card_testing_txn())
    print(f"  Bust-out     : {n_bust:,}")
    for _ in range(n_bust):    txn_rows.append(make_bust_out_txn())
    print(f"  Structuring  : {n_struct:,}")
    for _ in range(n_struct):  txn_rows.append(make_structuring_txn())
    random.shuffle(txn_rows)

    # Auth decisions: mirror transaction distribution (1:1 with transactions)
    print("\nGenerating auth_decisions...")
    auth_rows = []
    print(f"  Normal       : {int(n_auth * 0.60):,}")
    for _ in range(int(n_auth * 0.60)):  auth_rows.append(make_auth_decision("normal", NORMAL_ACCT))
    print(f"  Card-testing : {int(n_auth * 0.20):,}")
    for _ in range(int(n_auth * 0.20)):  auth_rows.append(make_auth_decision("card_testing", CARD_TESTING_ACCT))
    print(f"  Bust-out     : {int(n_auth * 0.10):,}")
    for _ in range(int(n_auth * 0.10)):  auth_rows.append(make_auth_decision("bust_out", BUST_OUT_ACCT))
    print(f"  Structuring  : {n_auth - int(n_auth * 0.90):,}")
    for _ in range(n_auth - int(n_auth * 0.90)):  auth_rows.append(make_auth_decision("structuring", STRUCTURING_ACCT))
    random.shuffle(auth_rows)

    # Device events: spread across personas
    print("\nGenerating device_events...")
    device_rows = []
    for _ in range(int(n_device * 0.70)):  device_rows.append(make_device_event("normal", NORMAL_ACCT))
    for _ in range(int(n_device * 0.15)):  device_rows.append(make_device_event("card_testing", CARD_TESTING_ACCT))
    for _ in range(int(n_device * 0.10)):  device_rows.append(make_device_event("bust_out", BUST_OUT_ACCT))
    for _ in range(n_device - int(n_device * 0.95)):  device_rows.append(make_device_event("structuring", STRUCTURING_ACCT))
    random.shuffle(device_rows)

    # Wire events: heavily weighted toward structuring
    print("\nGenerating wire_events...")
    wire_rows = []
    for _ in range(int(n_wire * 0.20)):  wire_rows.append(make_wire_event("normal", NORMAL_ACCT))
    for _ in range(int(n_wire * 0.05)):  wire_rows.append(make_wire_event("bust_out", BUST_OUT_ACCT))
    for _ in range(n_wire - int(n_wire * 0.25)):  wire_rows.append(make_wire_event("structuring", STRUCTURING_ACCT))
    random.shuffle(wire_rows)

    # Narratives: weighted toward the fraud personas so semantic search is rich
    s_normal = int(n_narr * 0.40)
    s_ctest  = int(n_narr * 0.20)
    s_bust   = int(n_narr * 0.20)
    s_struct = n_narr - s_normal - s_ctest - s_bust

    print("\nGenerating case_narratives...")
    narr_rows = []
    print(f"  Normal       : {s_normal:,}")
    for _ in range(s_normal):  narr_rows.append(make_narrative("normal", NORMAL_ACCT))
    print(f"  Card-testing : {s_ctest:,}")
    for _ in range(s_ctest):   narr_rows.append(make_narrative("card_testing", CARD_TESTING_ACCT))
    print(f"  Bust-out     : {s_bust:,}")
    for _ in range(s_bust):    narr_rows.append(make_narrative("bust_out", BUST_OUT_ACCT))
    print(f"  Structuring  : {s_struct:,}")
    for _ in range(s_struct):  narr_rows.append(make_narrative("structuring", STRUCTURING_ACCT))
    random.shuffle(narr_rows)

    # Account KPIs: 10 customers × 28 days × 24 hours = 6,720 rows
    print("\nGenerating account_kpis...")
    kpi_rows = []
    num_customers = 10
    num_hours = 28 * 24
    for cid in range(1, num_customers + 1):
        for h in range(num_hours):
            ts = (NOW - timedelta(hours=num_hours - h)).strftime("%Y-%m-%d %H:%M:%S")
            rid = 1 + (cid % 7)
            kpi_rows.append([
                ts, cid, rid,
                round(5 + random.random() * 60, 1),      # txn_velocity
                round(20 + random.random() * 300, 2),    # avg_ticket
                round(random.random() * 25, 2),          # decline_rate_pct
                round(random.random() * 120, 1),         # fraud_bps
                round(random.random() * 3, 2)            # chargeback_rate_pct
            ])
    print(f"  Generated {len(kpi_rows):,} rows (10 customers × 28 days × 24 hrs)")

    print("\nWriting CSV files...")
    os.makedirs("csv_data", exist_ok=True)

    with open("csv_data/transactions.csv", "w", newline="") as f:
        csv.writer(f).writerows(txn_rows)
    print(f"  ✓ transactions.csv     : {len(txn_rows):,} rows")

    with open("csv_data/auth_decisions.csv", "w", newline="") as f:
        csv.writer(f).writerows(auth_rows)
    print(f"  ✓ auth_decisions.csv   : {len(auth_rows):,} rows")

    with open("csv_data/device_events.csv", "w", newline="") as f:
        csv.writer(f).writerows(device_rows)
    print(f"  ✓ device_events.csv    : {len(device_rows):,} rows")

    with open("csv_data/wire_events.csv", "w", newline="") as f:
        csv.writer(f).writerows(wire_rows)
    print(f"  ✓ wire_events.csv      : {len(wire_rows):,} rows")

    with open("csv_data/case_narratives.csv", "w", newline="") as f:
        csv.writer(f).writerows(narr_rows)
    print(f"  ✓ case_narratives.csv  : {len(narr_rows):,} rows")

    with open("csv_data/account_kpis.csv", "w", newline="") as f:
        csv.writer(f).writerows(kpi_rows)
    print(f"  ✓ account_kpis.csv     : {len(kpi_rows):,} rows")

    print("\n" + "=" * 70)
    print("  DONE — Dataset Summary:")
    print(f"  {'Table':<22} {'Rows':>12}  {'Size Estimate':>12}")
    print(f"  {'-'*52}")
    print(f"  {'transactions':<22} {len(txn_rows):>12,}  ~{len(txn_rows) * 0.5 / 1024:.1f} MB")
    print(f"  {'auth_decisions':<22} {len(auth_rows):>12,}  ~{len(auth_rows) * 0.15 / 1024:.1f} MB")
    print(f"  {'device_events':<22} {len(device_rows):>12,}  ~{len(device_rows) * 0.12 / 1024:.1f} MB")
    print(f"  {'wire_events':<22} {len(wire_rows):>12,}  ~{len(wire_rows) * 0.15 / 1024:.1f} MB")
    print(f"  {'case_narratives':<22} {len(narr_rows):>12,}  ~{len(narr_rows) * 0.13 / 1024:.1f} MB")
    print(f"  {'account_kpis':<22} {len(kpi_rows):>12,}  ~{len(kpi_rows) * 0.08 / 1024:.1f} MB")
    print(f"  {'-'*52}")
    total = len(txn_rows) + len(auth_rows) + len(device_rows) + len(wire_rows) + len(narr_rows) + len(kpi_rows)
    print(f"  {'TOTAL':<22} {total:>12,}")
    print("=" * 70)
    print("\nNext step:")
    print("  gpfdist -d ./csv_data -p 8081 &")
    print("  psql -f 01_schema.sql && psql -f 02_seed_reference.sql")
    print("  psql -f 03_load_external_bfsi.sql && psql -f 06_lab3_ai_analytics.sql")
    print("  psql -f 07_kmeans_fallback.sql")
    print("  python3 app3.py\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Meridian Bank persona-based data generator")
    parser.add_argument("--scale", choices=["small", "medium", "large"],
                        default="medium", help="Dataset size (default: medium)")
    args = parser.parse_args()
    generate(args.scale)
