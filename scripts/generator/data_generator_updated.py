#!/usr/bin/env python3
"""
Meridian Retail Bank × WarehousePG — CSV Data Generator (June 2026, ~50M rows)

Generates CSV files for the 6 major BFSI fact tables that can be served via
gpfdist and loaded through WHPG READABLE EXTERNAL TABLEs. This is the bulk-load
alternative to the in-database seed (03_seed_traffic_with_personas.sql); the
column order matches 01_schema.sql (minus the auto-generated serial PK on each
table, exactly like the original network generator).

Usage:
    python3 data_generator_updated.py [--output-dir /path/to/csv] [--scale 1]

Scale factor:
    1 (default) -> ~50M rows   (workshop dataset, June 2026)
    2           -> ~100M rows  (stress)
    0.1         -> ~5M rows    (quick test)
    0.01        -> ~0.5M rows  (dev / smoke test)

Output (no header row -- gpfdist CSV external tables don't expect one):
    <output-dir>/transactions.csv      (~16.7M rows at scale=1)
    <output-dir>/device_events.csv     (~13M rows)
    <output-dir>/auth_decisions.csv    (~11.7M rows)
    <output-dir>/case_narratives.csv   (~7.8M rows)
    <output-dir>/wire_events.csv        (~0.8M rows)
    <output-dir>/account_kpis.csv      (customers x hours)

Then:
    gpfdist -d <output-dir> -p 8081 &
    psql -f 03_load_external_bfsi.sql
"""

import argparse
import csv
import json
import math
import os
import random
from datetime import datetime, timedelta

# -- Persona pools (mirror 03_seed_traffic_with_personas.sql) -------------------
# CARD-TESTING : small accounts, Visa BIN 41010100-199, tiny auth amounts
# BUST-OUT     : tiny pool,      MC BIN 52020200-299,   high-value spend-up
# STRUCTURING  : small pool,     BIN 53030300-399,      sub-$10k wires to BR/NG/RU
CARD_TESTING_ACCT = (100900001, 39)
BUST_OUT_ACCT     = (101900001, 29)
STRUCTURING_ACCT  = (105900001, 39)
NORMAL_ACCT       = (100000000, 50000)

BICS        = ["CHASUS33", "BARCGB22", "DEUTDEFF", "BNPAFRPP"]
CURRENCIES  = ["USD", "USD", "USD", "EUR", "GBP"]
MCCS        = [5411, 5812, 5942, 5999, 6011, 4829, 7995, 5734, 4111, 5651]
MERCHANTS   = ["Retail Merchant", "Corner Grocer", "Fuel Stop", "Online Bazaar",
               "City Pharmacy", "Transit Authority", "Electronics Hub", "Cafe Central"]
COUNTRIES   = ["US", "US", "US", "GB", "DE", "FR"]
HIGH_RISK   = ["BR", "NG", "RU"]
CHANNELS    = ["POS", "ECOM", "ATM"]
DECISIONS   = ["APPROVE", "APPROVE", "APPROVE", "APPROVE", "DECLINE", "STEP_UP", "BLOCK"]
QUEUES      = ["cards-fraud", "aml-tm", "disputes", "sanctions"]
ANALYSTS    = ["a.okafor", "l.nguyen", "m.silva", "r.haddad", "t.olsen", "s.kapoor"]
RESULTS     = ["OK", "OK", "OK", "FAIL", "STEP_UP"]
EVENT_TYPES = ["login", "login", "payee_add", "new_device"]
RAILS       = ["SWIFT", "SEPA", "ACH"]

NARRATIVES = {
    "card_testing": [
        "Repeated penny authorisations checking which card numbers are still live",
        "Burst of small declines followed by one larger successful charge",
        "Dozens of sub-dollar auth attempts across many unrelated merchants",
    ],
    "bust_out": [
        "Newly raised limit consumed almost entirely within two days",
        "Balance ramped to the ceiling on resaleable goods then account went dark",
        "Sudden burst of big-value transactions exhausting available credit",
    ],
    "structuring": [
        "Steady stream of payments each landing a few hundred below the reporting line",
        "Dozens of similar-sized outbound wires to the same overseas counterparty",
        "Just-below-limit disbursements suggesting deliberate threshold avoidance",
    ],
    "normal": [
        "Routine dispute over a duplicate point-of-sale charge, refunded",
        "Customer confirmed travel; temporary geo block lifted after verification",
        "Standard chargeback for an undelivered online order, resolved",
    ],
}

# All data lives in JUNE 2026 (2026-06-01 00:00 .. 2026-07-01 00:00)
WINDOW_START = datetime(2026, 6, 1)
WINDOW_END   = datetime(2026, 7, 1)
_WINDOW_SECS = (WINDOW_END - WINDOW_START).total_seconds()


def rand_ts(days_back=None):
    """Random timestamp uniformly within June 2026."""
    return (WINDOW_START + timedelta(seconds=random.random() * _WINDOW_SECS)).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def acct(pool):
    base, span = pool
    return base + random.randint(0, span)


def pan4():
    return f"{random.randint(0, 9999):04d}"


def progress(label, current, total):
    if total <= 0:
        return
    if current % max(1, total // 20) == 0 or current == total:
        pct = current * 100 // total
        print(f"\r  {label}: {pct:3d}% ({current:,}/{total:,})", end="", flush=True)
    if current == total:
        print()


def _iso_msg(svc, instr, value, ccy, cdtr_ctry, bic=None):
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


# -- Generators -----------------------------------------------------------------

def gen_transactions(writer, count):
    """transactions: ts,account_id,card_bin,pan_last4,amount,currency,mcc,merchant_id,
    merchant_name,merchant_country,channel,txn_type,auth_response,beneficiary_account,
    beneficiary_country,iso_msg,region_id"""
    base   = int(count * 0.947)
    ctest  = int(count * 0.030)
    bust   = int(count * 0.013)
    struct = count - base - ctest - bust
    n = 0

    for _ in range(base):
        a = acct(NORMAL_ACCT)
        amt = round(3 + random.random() * 800, 2)
        ccy = random.choice(CURRENCIES)
        ctry = random.choice(COUNTRIES)
        svc = random.choice(["CARD", "CARD", "SEPA", "NURG"])
        instr = random.choice(["POS", "ECOM", "ATM"])
        writer.writerow([rand_ts(), a, random.randint(40000000, 49999999), pan4(), amt, ccy,
                         random.choice(MCCS), random.randint(1, 9_000_000), random.choice(MERCHANTS),
                         ctry, random.choice(CHANNELS), "PURCHASE", random.choice(["A", "A", "A", "D"]),
                         "", "", _iso_msg(svc, instr, amt, ccy, ctry, random.choice(BICS)),
                         random.randint(1, 7)])
        n += 1; progress("transactions (normal)", n, count)

    for _ in range(ctest):
        a = acct(CARD_TESTING_ACCT)
        amt = round(0.40 + random.random() * 3.60, 2)
        writer.writerow([rand_ts(), a, 41010100 + random.randint(0, 99), pan4(), amt, "USD",
                         5999, random.randint(1, 9_000_000), "TEST-MERCHANT", "US", "ECOM",
                         "PURCHASE", random.choice(["A", "D", "D"]), "", "",
                         _iso_msg("CARD", "ECOM", amt, "USD", "US"), random.randint(1, 7)])
        n += 1; progress("transactions (card-testing)", n, count)

    for _ in range(bust):
        a = acct(BUST_OUT_ACCT)
        amt = round(800 + random.random() * 4200, 2)
        writer.writerow([rand_ts(), a, 52020200 + random.randint(0, 99), pan4(), amt, "USD",
                         5944, random.randint(1, 9_000_000), "LUXURY-GOODS", "US", "POS",
                         "PURCHASE", "A", "", "", _iso_msg("CARD", "POS", amt, "USD", "US"),
                         random.randint(1, 7)])
        n += 1; progress("transactions (bust-out)", n, count)

    for _ in range(struct):
        a = acct(STRUCTURING_ACCT)
        amt = round(9000 + random.random() * 900, 2)
        ctry = random.choice(HIGH_RISK)
        ben = 700000000 + random.randint(0, 999999)
        writer.writerow([rand_ts(), a, 53030300 + random.randint(0, 99), pan4(), amt, "USD",
                         6011, random.randint(1, 9_000_000), "Beneficiary Co", ctry,
                         random.choice(["WIRE", "INST", "P2P"]), "PAYMENT", "A", ben, ctry,
                         _iso_msg("SEPA", "INST", amt, "USD", ctry, random.choice(BICS)),
                         random.randint(1, 7)])
        n += 1; progress("transactions (structuring)", n, count)


def gen_device_events(writer, count):
    """device_events: ts,account_id,device_fingerprint,ip_country,channel,event_type,result,region_id"""
    pools = [NORMAL_ACCT, CARD_TESTING_ACCT, BUST_OUT_ACCT, STRUCTURING_ACCT]
    for i in range(1, count + 1):
        a = acct(random.choice(pools))
        ctry = random.choice(COUNTRIES + HIGH_RISK)
        writer.writerow([rand_ts(), a, f"fp{random.randint(0, 999999)}", ctry,
                         random.choice(["POS", "ECOM", "MOBILE"]), random.choice(EVENT_TYPES),
                         random.choice(RESULTS), random.randint(1, 7)])
        progress("device_events", i, count)


def gen_auth_decisions(writer, count):
    """auth_decisions: ts,account_id,card_bin,mcc,amount,decision,rule_id,channel,merchant_country,region_id"""
    pools = [CARD_TESTING_ACCT, BUST_OUT_ACCT, STRUCTURING_ACCT, NORMAL_ACCT]
    bins = [41010100, 52020200, 53030300, 40000000]
    for i in range(1, count + 1):
        idx = random.randint(0, 3)
        a = acct(pools[idx])
        writer.writerow([rand_ts(), a, bins[idx] + random.randint(0, 99), random.choice(MCCS),
                         round(random.random() * 900, 2), random.choice(DECISIONS),
                         random.randint(0, 40), random.choice(CHANNELS),
                         random.choice(COUNTRIES + HIGH_RISK), random.randint(1, 7)])
        progress("auth_decisions", i, count)


def gen_case_narratives(writer, count):
    """case_narratives: ts,account_id,card_bin,analyst,queue,severity,narrative,region_id"""
    personas = [("card_testing", CARD_TESTING_ACCT, 41010100),
                ("bust_out", BUST_OUT_ACCT, 52020200),
                ("structuring", STRUCTURING_ACCT, 53030300),
                ("normal", NORMAL_ACCT, 40000000)]
    weights = [3, 3, 3, 1]
    for i in range(1, count + 1):
        persona, pool, binbase = random.choices(personas, weights=weights)[0]
        a = acct(pool)
        q = "aml-tm" if persona == "structuring" else ("cards-fraud" if persona in ("card_testing", "bust_out") else random.choice(QUEUES))
        writer.writerow([rand_ts(), a, binbase + random.randint(0, 99), random.choice(ANALYSTS),
                         q, random.randint(1, 3), random.choice(NARRATIVES[persona]),
                         random.randint(1, 7)])
        progress("case_narratives", i, count)


def gen_wire_events(writer, count):
    """wire_events: ts,ordering_account,beneficiary_bic,beneficiary_country,event_type,rail,amount,region_id"""
    for i in range(1, count + 1):
        # structuring-heavy: most wires originate from the structuring pool
        a = acct(STRUCTURING_ACCT) if random.random() < 0.7 else acct(NORMAL_ACCT)
        ctry = random.choice(HIGH_RISK + ["US", "GB"])
        amt = round(9000 + random.random() * 900, 2) if a >= STRUCTURING_ACCT[0] else round(random.random() * 50000, 2)
        writer.writerow([rand_ts(), a, random.choice(BICS), ctry,
                         random.choice(["outbound", "outbound", "inbound"]),
                         random.choice(RAILS), amt, random.randint(1, 7)])
        progress("wire_events", i, count)


def gen_account_kpis(writer, num_customers=10, days=28):
    """account_kpis: ts,customer_id,region_id,txn_velocity,avg_ticket,decline_rate_pct,fraud_bps,chargeback_rate_pct
    One row per customer per hour over the window."""
    hours = days * 24
    total = num_customers * hours
    count = 0
    for cid in range(1, num_customers + 1):
        rid = 1 + (cid % 7)
        for h in range(hours):
            ts = (WINDOW_END - timedelta(hours=hours - h)).strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([ts, cid, rid,
                             round(5 + random.random() * 60, 1),
                             round(20 + random.random() * 300, 2),
                             round(random.random() * 25, 2),
                             round(random.random() * 120, 1),
                             round(random.random() * 3, 2)])
            count += 1
            progress("account_kpis", count, total)


# -- Main -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Meridian Bank BFSI CSV Data Generator")
    parser.add_argument("--output-dir", default="./csv_data", help="Output directory for CSV files")
    parser.add_argument("--scale", type=float, default=1.0,
                        help="Scale factor (1.0 = ~50M rows total; 2.0 = ~100M, 0.1 = ~5M)")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    s = args.scale

    # Base counts sum to ~50,000,000 rows at scale=1.0 (June 2026 dataset).
    targets = {
        "transactions":    int(16700000 * s),
        "device_events":   int(13000000 * s),
        "auth_decisions":  int(11700000 * s),
        "case_narratives": int(7800000 * s),
        "wire_events":     int(800000 * s),
        "account_kpis":    0,  # special: customers x hours
    }

    print("+-------------------------------------------------------+")
    print(f"|  Meridian Bank BFSI CSV Generator  (scale={s})        |")
    print(f"|  Output: {args.output_dir:<44s}|")
    print("+-------------------------------------------------------+")
    for t, c in targets.items():
        if t == "account_kpis":
            c = 10 * 28 * 24  # customers x days x hours
        print(f"|  {t:<22s} : {c:>12,} rows          |")
    print("+-------------------------------------------------------+")
    print()

    generators = [
        ("transactions", ["ts", "account_id", "card_bin", "pan_last4", "amount", "currency", "mcc",
                          "merchant_id", "merchant_name", "merchant_country", "channel", "txn_type",
                          "auth_response", "beneficiary_account", "beneficiary_country", "iso_msg", "region_id"],
         lambda w, n: gen_transactions(w, n), targets["transactions"]),
        ("device_events", ["ts", "account_id", "device_fingerprint", "ip_country", "channel",
                           "event_type", "result", "region_id"],
         lambda w, n: gen_device_events(w, n), targets["device_events"]),
        ("auth_decisions", ["ts", "account_id", "card_bin", "mcc", "amount", "decision", "rule_id",
                            "channel", "merchant_country", "region_id"],
         lambda w, n: gen_auth_decisions(w, n), targets["auth_decisions"]),
        ("case_narratives", ["ts", "account_id", "card_bin", "analyst", "queue", "severity",
                             "narrative", "region_id"],
         lambda w, n: gen_case_narratives(w, n), targets["case_narratives"]),
        ("wire_events", ["ts", "ordering_account", "beneficiary_bic", "beneficiary_country",
                         "event_type", "rail", "amount", "region_id"],
         lambda w, n: gen_wire_events(w, n), targets["wire_events"]),
        ("account_kpis", ["ts", "customer_id", "region_id", "txn_velocity", "avg_ticket",
                          "decline_rate_pct", "fraud_bps", "chargeback_rate_pct"],
         lambda w, n: gen_account_kpis(w), 0),
    ]

    for name, headers, gen_fn, count in generators:
        path = os.path.join(args.output_dir, f"{name}.csv")
        print(f"\n[*] Generating {name} -> {path}")
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            # No header row -- gpfdist external tables don't expect headers
            gen_fn(writer, count)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"    -> {size_mb:.1f} MB")

    print(f"\nAll CSV files generated in {args.output_dir}/")
    print(f"  Next: gpfdist -d {args.output_dir} -p 8081 &")
    print(f"  Then: psql -f 03_load_external_bfsi.sql")


if __name__ == "__main__":
    main()
