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
    "small":  {"transactions": 200_000,    "narratives": 50_000},
    "medium": {"transactions": 2_000_000,  "narratives": 400_000},
    "large":  {"transactions": 15_800_000, "narratives": 7_500_000},
}

# All data lives in JUNE 2026 (2026-06-01 .. 2026-07-01)
NOW   = datetime(2026, 7, 1)          # window end
START = datetime(2026, 6, 1)          # window start

# ── Account pools per persona (mirror 03_seed_traffic_with_personas.sql) ───────
NORMAL_ACCT       = (100000000, 49999)
CARD_TESTING_ACCT = (100900001, 39)    # Visa BIN 41010100-199
BUST_OUT_ACCT     = (101900001, 29)    # MC   BIN 52020200-299
STRUCTURING_ACCT  = (105900001, 39)    # BIN 53030300-399, sub-$10k wires

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


# ── Main ──────────────────────────────────────────────────────────────────────

def generate(scale="medium"):
    cfg = SCALES[scale]
    n_txn = cfg["transactions"]
    n_narr = cfg["narratives"]

    print("=" * 60)
    print("  Meridian Bank Persona-Based Data Generator")
    print(f"  Scale: {scale}  |  Transactions: {n_txn:,}  |  Narratives: {n_narr:,}")
    print("=" * 60)

    # Mix: large NORMAL baseline + tight fraud-persona clusters
    #   Normal 70% | Card-testing 12% | Bust-out 8% | Structuring 10%
    n_normal = int(n_txn * 0.70)
    n_ctest  = int(n_txn * 0.12)
    n_bust   = int(n_txn * 0.08)
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

    print("\nWriting CSV files...")
    os.makedirs("csv_data", exist_ok=True)
    with open("csv_data/transactions.csv", "w", newline="") as f:
        csv.writer(f).writerows(txn_rows)            # no header (gpfdist external table)
    print(f"  Wrote {len(txn_rows):,} rows -> csv_data/transactions.csv")
    with open("csv_data/case_narratives.csv", "w", newline="") as f:
        csv.writer(f).writerows(narr_rows)
    print(f"  Wrote {len(narr_rows):,} rows -> csv_data/case_narratives.csv")

    print("\n" + "=" * 60)
    print("  DONE — Persona statistics:")
    print(f"  {'Persona':<18} {'Transactions':>14}  {'Narratives':>12}")
    print(f"  {'-'*46}")
    print(f"  {'Normal':<18} {n_normal:>14,}  {s_normal:>12,}")
    print(f"  {'Card-testing':<18} {n_ctest:>14,}  {s_ctest:>12,}")
    print(f"  {'Bust-out':<18} {n_bust:>14,}  {s_bust:>12,}")
    print(f"  {'Structuring':<18} {n_struct:>14,}  {s_struct:>12,}")
    print(f"  {'-'*46}")
    print(f"  {'TOTAL':<18} {n_txn:>14,}  {n_narr:>12,}")
    print("=" * 60)
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
