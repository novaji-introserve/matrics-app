#!/usr/bin/env python3

import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import odoo_connect
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / "compliance_management" / "resources" / "scripts" / ".env"

TRANSACTION_COUNT = 500
COUNTRY_CODE = "NG"
TRANSACTION_MODES = ["ATM", "BRANCH", "MOBILE", "ONLINE", "POS", "TRANSFER"]
FUND_CODES = ["BUSINESS", "CASH", "FAMILY", "INCOME", "INVESTMENT", "SAVINGS"]
TELLERS = ["SYSTEM", "AJOHNSON", "BOKAFOR", "CMOHAMMED", "DNWOSU", "EADENIYI"]
AUTHORIZERS = ["OPS_LEAD", "RISK_CTRL", "SUPERVISOR_A", "SUPERVISOR_B"]
LOCATION_FALLBACKS = ["Abuja", "Ibadan", "Kano", "Lagos", "Port Harcourt"]
NARRATIVES = [
    "Customer initiated transfer",
    "Inter-account funds movement",
    "Business settlement payment",
    "Routine account funding",
    "Vendor settlement transfer",
    "Treasury placement transfer",
]
TRANSACTION_TYPES = ["debit", "credit"]


def load_environment():
    if not ENV_PATH.exists():
        raise FileNotFoundError(f"Environment file not found: {ENV_PATH}")
    load_dotenv(ENV_PATH)


def connect_to_odoo():
    return odoo_connect.connect(
        url=os.getenv("HOST_URL"),
        database=os.getenv("DB"),
        username=os.getenv("USERNAME"),
        password=os.getenv("PASSWORD"),
    )


def fetch_accounts(env):
    account_model = env["bank.account"]
    return account_model.search_read(
        [("status_code", "=", "active")],
        fields=[
            "id",
            "account_number",
            "account_name",
            "currency_code",
            "balance",
            "branch",
            "institution_name",
        ],
        limit=2000,
    )


def group_accounts_by_currency(accounts):
    grouped = {}
    for account in accounts:
        currency_code = account.get("currency_code") or "NGN"
        grouped.setdefault(currency_code, []).append(account)
    return grouped


def choose_accounts(accounts_by_currency):
    eligible_currencies = [
        currency_code
        for currency_code, accounts in accounts_by_currency.items()
        if len(accounts) >= 2
    ]
    if eligible_currencies:
        currency_code = random.choice(eligible_currencies)
        source_account, destination_account = random.sample(
            accounts_by_currency[currency_code], 2
        )
        return source_account, destination_account, currency_code

    flat_accounts = [
        account
        for account_group in accounts_by_currency.values()
        for account in account_group
    ]
    if len(flat_accounts) < 2:
        raise ValueError("At least two active bank.account records are required")

    source_account, destination_account = random.sample(flat_accounts, 2)
    currency_code = source_account.get("currency_code") or "NGN"
    return source_account, destination_account, currency_code


def random_transaction_datetime():
    now = datetime.now()
    start = now - timedelta(days=180)
    random_seconds = random.randint(0, int((now - start).total_seconds()))
    return start + timedelta(seconds=random_seconds)


def random_amount(currency_code, tran_type):
    if currency_code == "NGN":
        amount = round(random.uniform(25000, 15000000), 2)
    else:
        amount = round(random.uniform(100, 50000), 2)
    return -amount if tran_type == "debit" else amount


def generate_transaction_number():
    return f"BTX{datetime.now().strftime('%Y%m%d')}{uuid4().hex[:10].upper()}"


def transaction_exists(transaction_model, transaction_number):
    return bool(
        transaction_model.search([("transaction_number", "=", transaction_number)], limit=1)
    )


def build_payload(source_account, destination_account, currency_code):
    date_transaction = random_transaction_datetime()
    transaction_number = generate_transaction_number()
    tran_type = random.choice(TRANSACTION_TYPES)
    branch = (
        source_account.get("branch")
        or destination_account.get("branch")
        or random.choice(LOCATION_FALLBACKS)
    )
    narrative = random.choice(NARRATIVES)
    direction_text = "debited from" if tran_type == "debit" else "credited to"

    return {
        "transaction_number": transaction_number,
        "internal_ref_number": f"REF{uuid4().hex[:12].upper()}",
        "transaction_location": branch,
        "transaction_description": (
            f"{narrative}. Funds {direction_text} {source_account['account_name']} "
            f"for {destination_account['account_name']}"
        ),
        "date_transaction": date_transaction.strftime("%Y-%m-%d %H:%M:%S"),
        "value_date": (date_transaction + timedelta(minutes=random.randint(5, 180))).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "teller": random.choice(TELLERS),
        "authorized": random.choice(AUTHORIZERS),
        "late_deposit": random.random() < 0.08,
        "transmode_code": random.choice(TRANSACTION_MODES),
        "tran_type": tran_type,
        "amount_local": random_amount(currency_code, tran_type),
        "currency_code": currency_code,
        "from_account_id": source_account["id"],
        "to_account_id": destination_account["id"],
        "from_funds_code": random.choice(FUND_CODES),
        "to_funds_code": random.choice(FUND_CODES),
        "from_country": COUNTRY_CODE,
        "to_country": COUNTRY_CODE,
    }


def create_transactions(env, total=TRANSACTION_COUNT):
    accounts = fetch_accounts(env)
    if len(accounts) < 2:
        raise ValueError("Not enough active bank.account records found to create transactions")

    accounts_by_currency = group_accounts_by_currency(accounts)
    transaction_model = env["bank.transaction"]
    created = 0

    for index in range(1, total + 1):
        source_account, destination_account, currency_code = choose_accounts(
            accounts_by_currency
        )
        payload = build_payload(source_account, destination_account, currency_code)

        while transaction_exists(transaction_model, payload["transaction_number"]):
            payload["transaction_number"] = generate_transaction_number()

        try:
            record_id = transaction_model.create(payload)
            print(
                f"[{index}/{total}] Created bank.transaction "
                f"{payload['transaction_number']} with ID {record_id}"
            )
            created += 1
        except Exception as exc:
            print(
                f"[{index}/{total}] Failed to create bank.transaction "
                f"{payload['transaction_number']}: {exc}"
            )

    return created


def main():
    load_environment()
    env = connect_to_odoo()
    created = create_transactions(env, TRANSACTION_COUNT)
    print(f"Created {created} bank.transaction records")
    return 0


if __name__ == "__main__":
    sys.exit(main())
