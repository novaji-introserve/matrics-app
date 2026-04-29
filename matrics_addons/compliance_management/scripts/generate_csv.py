#!/usr/bin/env python3
"""
Generate sample CSV files for manual upload into iComply screening lists.

Output files (one per model):
  - pep_list_sample.csv          → pep.list
  - sanction_list_sample.csv     → sanction.list
  - watchlist_sample.csv         → res.partner.watchlist
  - blacklist_sample.csv         → res.partner.blacklist

Usage:
  python3 generate_sample_csvs.py
  python3 generate_sample_csvs.py --rows 50 --out /tmp/csvs/

Each file contains 10 sample rows by default. Edit the DATA section below
to add your real records, or use --rows to control the number of dummy rows.
"""

import csv
import os
import uuid
import argparse
from datetime import date

# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Generate sample iComply list CSVs")
parser.add_argument("--rows", type=int, default=10, help="Number of sample rows per file (default: 10)")
parser.add_argument("--out", default=".", help="Output directory (default: current dir)")
args = parser.parse_args()

os.makedirs(args.out, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# SAMPLE DATA POOL  — replace / extend with real names as needed
# ─────────────────────────────────────────────────────────────────────────────
FIRST_NAMES = [
    "Emeka", "Ngozi", "Tunde", "Amina", "Chidi",
    "Fatima", "Biodun", "Kemi", "Uche", "Sule",
    "Adaeze", "Musa", "Chioma", "Bola", "Yusuf",
]
LAST_NAMES = [
    "Okonkwo", "Adeyemi", "Ibrahim", "Nwosu", "Bello",
    "Adeleke", "Okafor", "Lawal", "Eze", "Danjuma",
    "Obi", "Abubakar", "Chukwu", "Olawale", "Maikudi",
]
MIDDLE_NAMES = ["Chukwuemeka", "Oluwaseun", "Abdullahi", "Obiageli", "Nnamdi", ""]
NATIONALITIES = ["Nigerian", "Ghanaian", "Kenyan", "South African", "Senegalese"]
POSITIONS = [
    "Minister of Finance", "Governor", "Senator", "Director General",
    "Permanent Secretary", "Ambassador", "Commissioner", "Board Chairman",
]
SOURCES = ["EFCC", "NFIU", "Manual Entry", "CBN Watchlist", "Interpol"]


def _name(i):
    f = FIRST_NAMES[i % len(FIRST_NAMES)]
    l = LAST_NAMES[(i + 3) % len(LAST_NAMES)]
    return f, l, f"{f} {l}"


def _mid(i):
    return MIDDLE_NAMES[i % len(MIDDLE_NAMES)]


# ─────────────────────────────────────────────────────────────────────────────
# 1. pep.list  →  pep_list_sample.csv
#    Required columns: firstname, lastname, name, position
#    unique_id is auto-generated here as a reference; Odoo also auto-generates
#    one on import, but supplying it prevents duplicates on re-upload.
# ─────────────────────────────────────────────────────────────────────────────
pep_list_file = os.path.join(args.out, "pep_list_sample.csv")
pep_list_headers = ["unique_id", "firstname", "lastname", "name", "position"]

with open(pep_list_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=pep_list_headers)
    writer.writeheader()
    for i in range(args.rows):
        fn, ln, full = _name(i)
        writer.writerow({
            "unique_id": str(uuid.uuid4()),
            "firstname": fn,
            "lastname": ln,
            "name": full,
            "position": POSITIONS[i % len(POSITIONS)],
        })

print(f"[OK] {pep_list_file}  ({args.rows} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# 2. sanction.list  →  sanction_list_sample.csv
#    Columns: name, sanction_id, nationality, surname, first_name,
#             middle_name, source, active
# ─────────────────────────────────────────────────────────────────────────────
sanction_file = os.path.join(args.out, "sanction_list_sample.csv")
sanction_headers = [
    "name", "sanction_id", "nationality",
    "surname", "first_name", "middle_name", "source", "active",
]

with open(sanction_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=sanction_headers)
    writer.writeheader()
    for i in range(args.rows):
        fn, ln, full = _name(i)
        writer.writerow({
            "name": full,
            "sanction_id": f"SAN-{date.today().year}-{i+1:04d}",
            "nationality": NATIONALITIES[i % len(NATIONALITIES)],
            "surname": ln,
            "first_name": fn,
            "middle_name": _mid(i),
            "source": SOURCES[i % len(SOURCES)],
            "active": "True",
        })

print(f"[OK] {sanction_file}  ({args.rows} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# 3. res.partner.watchlist  →  watchlist_sample.csv
#    Columns: name, watchlist_id, nationality, surname, first_name,
#             middle_name, bvn, source
#    NOTE: customer_id (Many2one) is intentionally omitted — link via UI
#          if you want to tie to an existing res.partner record.
# ─────────────────────────────────────────────────────────────────────────────
watchlist_file = os.path.join(args.out, "watchlist_sample.csv")
watchlist_headers = [
    "name", "watchlist_id", "nationality",
    "surname", "first_name", "middle_name", "bvn", "source",
]

with open(watchlist_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=watchlist_headers)
    writer.writeheader()
    for i in range(args.rows):
        fn, ln, full = _name(i)
        # BVN: 11-digit numeric string — replace with real BVNs
        bvn = f"{22000000000 + i:011d}"
        writer.writerow({
            "name": full,
            "watchlist_id": f"WL-{date.today().year}-{i+1:04d}",
            "nationality": NATIONALITIES[i % len(NATIONALITIES)],
            "surname": ln,
            "first_name": fn,
            "middle_name": _mid(i),
            "bvn": bvn,
            "source": SOURCES[i % len(SOURCES)],
        })

print(f"[OK] {watchlist_file}  ({args.rows} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# 4. res.partner.blacklist  →  blacklist_sample.csv
#    Columns: name, surname, first_name, middle_name, bvn, active
#    NOTE: customer_id is Many2one (optional) — omitted here.
#          surname and first_name are required by the model.
# ─────────────────────────────────────────────────────────────────────────────
blacklist_file = os.path.join(args.out, "blacklist_sample.csv")
blacklist_headers = ["name", "surname", "first_name", "middle_name", "bvn", "active"]

with open(blacklist_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=blacklist_headers)
    writer.writeheader()
    for i in range(args.rows):
        fn, ln, full = _name(i)
        bvn = f"{33000000000 + i:011d}"
        writer.writerow({
            "name": full,
            "surname": ln,
            "first_name": fn,
            "middle_name": _mid(i),
            "bvn": bvn,
            "active": "True",
        })

print(f"[OK] {blacklist_file}  ({args.rows} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
print()
print("Upload instructions:")
print("  Odoo → Settings → Technical → Import → select model → upload CSV")
print("  OR use the list view's ⚙ Action → Import Records button.")
print()
print("Model → CSV file mapping:")
print(f"  pep.list                →  pep_list_sample.csv")
print(f"  sanction.list           →  sanction_list_sample.csv")
print(f"  res.partner.watchlist   →  watchlist_sample.csv")
print(f"  res.partner.blacklist   →  blacklist_sample.csv")
print()
print("NOTE: res.partner.fep is NOT included — it requires a customer_id")
print("  (linked to an existing res.partner). Add FEP records via the UI")
print("  or pass customer_id as the external ID / database ID in the CSV.")
