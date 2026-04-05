#!/usr/bin/env python3

import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import odoo_connect
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / "compliance_management" / "resources" / "scripts" / ".env"

ACCOUNT_COUNT = 100
INSTITUTIONS = [
    ("Access Bank", "044", "ABNGNGLA"),
    ("First Bank", "011", "FBNINGLA"),
    ("GTBank", "058", "GTBINGLA"),
    ("UBA", "033", "UNAFNGLA"),
    ("Zenith Bank", "057", "ZEIBNGLA"),
]
CURRENCIES = ["NGN", "USD", "EUR", "GBP"]
STATUS_CODES = ["active", "inactive", "frozen"]
ACCOUNT_TYPES = ["Savings", "Current", "Domicilliary", "Corporate"]
BRANCHES = ["Lagos", "Abuja", "Port Harcourt", "Kano", "Ibadan"]
BENEFICIARIES = ["Primary Holder", "Joint Holder", "Business Owner", "Trustee"]
FIRST_NAMES = [
    "Amina", "Chinedu", "Bola", "Ifeanyi", "Kemi", "Tunde", "Ngozi", "David",
    "Maryam", "Samuel", "Zainab", "Emeka", "Fatima", "Joseph", "Adaeze",
]
LAST_NAMES = [
    "Okafor", "Adebayo", "Mohammed", "Balogun", "Eze", "Ibrahim", "Ojo",
    "Nwosu", "Usman", "Adeyemi", "Yusuf", "Obi", "Sule", "Umeh", "Lawal",
]
OCCUPATIONS = [
    "Trader", "Engineer", "Consultant", "Analyst", "Lawyer", "Doctor",
    "Business Owner", "Teacher", "Banker", "Developer",
]
WEALTH_SOURCES = [
    "Salary", "Business Income", "Investments", "Consulting", "Family Business"
]


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


def random_opened_date():
    now = datetime.now()
    start = now - timedelta(days=365 * 5)
    delta_days = random.randint(0, (now - start).days)
    delta_seconds = random.randint(0, 86399)
    opened_at = start + timedelta(days=delta_days, seconds=delta_seconds)
    return opened_at.strftime("%Y-%m-%d %H:%M:%S")


def generate_account_payload(index, owner_name):
    institution_name, institution_code, swift_code = random.choice(INSTITUTIONS)
    currency_code = random.choice(CURRENCIES)
    branch = random.choice(BRANCHES)
    client_number = f"CLI{random.randint(100000, 999999)}"
    balance = round(random.uniform(1000, 50000000), 2)

    return {
        "institution_seed": {
            "name": institution_name,
            "inst_code": institution_code,
            "swift_code": swift_code,
            "non_bank_institution": False,
        },
        "currency_seed": currency_code,
        "account_name": owner_name,
        "balance": balance,
        "branch": branch,
        "client_number": client_number,
        "personal_account_type": random.choice(ACCOUNT_TYPES),
        "opened": random_opened_date(),
        "status_code": random.choice(STATUS_CODES),
        "beneficiary": owner_name,
    }


def account_exists(account_model, account_number):
    return bool(account_model.search([("account_number", "=", account_number)], limit=1))


def get_or_create_institution(env, institution_seed):
    institution_model = env["bank.institution"]
    existing = institution_model.search(
        [("inst_code", "=", institution_seed["inst_code"])],
        limit=1,
    )
    if existing:
        return existing[0] if isinstance(existing, (list, tuple)) else existing
    return institution_model.create(institution_seed)


def get_account_type_id(env, account_type_name):
    account_type_model = env["bank.account.type"]
    existing = account_type_model.search([("name", "=", account_type_name)], limit=1)
    if existing:
        return existing[0] if isinstance(existing, (list, tuple)) else existing
    return account_type_model.create({"name": account_type_name})


def get_currency_id(env, currency_code):
    currency_model = env["res.currency"]
    existing = currency_model.search([("name", "=", currency_code)], limit=1)
    if existing:
        return existing[0] if isinstance(existing, (list, tuple)) else existing
    raise ValueError(f"Currency not found in res.currency: {currency_code}")


def signatory_exists(signatory_model, account_id):
    return bool(signatory_model.search([("account_id", "=", account_id)], limit=1))


def generate_person_payload(index):
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    return {
        "first_name": first_name,
        "last_name": f"{last_name}{index:02d}",
        "title": random.choice(["Mr", "Mrs", "Ms", "Dr"]),
        "gender": random.choice(["M", "F"]),
        "birthdate": (
            datetime.now() - timedelta(days=random.randint(25 * 365, 60 * 365))
        ).strftime("%Y-%m-%d"),
        "nationality1": "NG",
        "nationality2": "NG",
        "residence": "NG",
        "occupation": random.choice(OCCUPATIONS),
        "source_of_wealth": random.choice(WEALTH_SOURCES),
        "tax_number": f"TIN{random.randint(10000000, 99999999)}",
        "tax_reg_number": f"REG{random.randint(100000, 999999)}",
    }


def create_owner_person(env, index):
    person_model = env["bank.person"]
    payload = generate_person_payload(index)
    person_id = person_model.create(payload)
    return person_id, f"{payload['first_name']} {payload['last_name']}"


def attach_signatory(env, account_id, person_id):
    signatory_model = env["account.signatory"]
    if signatory_exists(signatory_model, account_id) or not person_id:
        return False

    payload = {
        "account_id": account_id,
        "person_id": person_id,
        "is_primary": True,
    }
    signatory_model.create(payload)
    return True


def create_accounts(env, total=ACCOUNT_COUNT):
    account_model = env["bank.account"]
    created = 0
    signatories_created = 0

    for index in range(1, total + 1):
        owner_person_id = None
        owner_name = None
        account_number = f"{random.randint(10**9, (10**10) - 1)}{index:02d}"
        if account_exists(account_model, account_number):
            print(
                f"[{index}/{total}] Skipping existing bank.account "
                f"{account_number}"
            )
            continue
        owner_person_id, owner_name = create_owner_person(env, index)
        payload = generate_account_payload(index, owner_name)
        institution_seed = payload.pop("institution_seed")
        currency_code = payload.pop("currency_seed")
        account_type_name = payload.pop("personal_account_type")
        payload["institution_id"] = get_or_create_institution(env, institution_seed)
        payload["currency_id"] = get_currency_id(env, currency_code)
        payload["personal_account_type"] = get_account_type_id(env, account_type_name)
        payload["account_number"] = account_number
        try:
            record_id = account_model.create(payload)
            if attach_signatory(env, record_id, owner_person_id):
                signatories_created += 1
            print(
                f"[{index}/{total}] Created bank.account {payload['account_number']} "
                f"with ID {record_id}"
            )
            created += 1
        except Exception as exc:
            print(
                f"[{index}/{total}] Failed to create bank.account "
                f"{payload['account_number']}: {exc}"
            )

    return created, signatories_created


def main():
    load_environment()
    env = connect_to_odoo()
    created, signatories_created = create_accounts(env, ACCOUNT_COUNT)
    print(
        f"Created {created} bank.account records and "
        f"{signatories_created} account.signatory records"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
