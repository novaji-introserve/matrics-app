import os
import random
import uuid
from datetime import datetime

import odoo_connect
from dotenv import load_dotenv
from odoo_connect.explore import explore


load_dotenv()

env = odoo_connect.connect(
    url=os.getenv("HOST_URL"),
    database=os.getenv("DB"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
)


SYSTEM_USER = "SYSTEM"
DEFAULT_CURRENCY = "NGN"
DEFAULT_PER_EVENT = 4


def get_default_branch():
    return explore(env["res.branch"]).search([], limit=1)


def get_random_branch():
    branches = explore(env["res.branch"]).search([], limit=500)
    return random.choice(branches) if branches else None


def get_default_currency():
    currency_model = explore(env["res.currency"])
    return currency_model.search([("name", "=", DEFAULT_CURRENCY)], limit=1) or currency_model.search([], limit=1)


def get_default_account_officer():
    return explore(env["account.officers"]).search([], limit=1)


def get_internal_accounts():
    account_model = explore(env["res.partner.account"])
    fields = account_model.fields_get()

    if "ledger_type_id" in fields:
        return account_model.search([("ledger_type_id.name", "=", "Internal")], limit=500)

    if "account_type" in fields:
        return account_model.search([("account_type", "=", "Internal")], limit=500)

    return []


def get_transaction_type(trancode):
    transaction_type = explore(env["res.transaction.type"])
    return transaction_type.search([("trancode", "=", trancode)], limit=1)


def build_account_index(accounts):
    index = {
        "accounts": accounts,
        "by_id": {account.id: account for account in accounts},
    }
    for account in accounts:
        haystack = " ".join(
            filter(
                None,
                [
                    getattr(account, "name", ""),
                    getattr(account, "account_name", ""),
                    getattr(getattr(account, "ledger_id", None), "name", ""),
                    getattr(getattr(account, "ledger_id", None), "ledger_code", ""),
                ],
            )
        ).lower()
        index[account.id] = haystack
    return index


def match_accounts(index, keywords, exclude_ids=None):
    exclude_ids = set(exclude_ids or [])
    matches = []
    for account in index["accounts"]:
        if account.id in exclude_ids:
            continue
        haystack = index[account.id]
        if any(keyword in haystack for keyword in keywords):
            matches.append(account)
    return matches


def choose_account(index, preferred_keywords, exclude_ids=None):
    candidates = match_accounts(index, preferred_keywords, exclude_ids=exclude_ids)
    if candidates:
        return random.choice(candidates)

    remaining = [
        account
        for account in index["accounts"]
        if account.id not in set(exclude_ids or [])
    ]
    return random.choice(remaining) if remaining else None


def current_transaction_datetime():
    return datetime.now()


def generate_reference(prefix="ITR"):
    return f"{prefix}{str(uuid.uuid4()).replace('-', '')[:12].upper()}"


def format_amount(amount):
    return f"{amount:,.2f}"


def build_narrative(template, amount, debit_account, credit_account, event_date, metadata):
    return template.format(
        amount=format_amount(amount),
        debit_account=debit_account.account_name or debit_account.name,
        credit_account=credit_account.account_name or credit_account.name,
        debit_code=getattr(getattr(debit_account, "ledger_id", None), "ledger_code", debit_account.name),
        credit_code=getattr(getattr(credit_account, "ledger_id", None), "ledger_code", credit_account.name),
        date=event_date.strftime("%d-%b-%Y"),
        cycle=metadata.get("cycle", ""),
        branch=metadata.get("branch", ""),
        batch=metadata.get("batch", ""),
    )


def transaction_values(
    account,
    amount,
    transaction_type,
    narration,
    event_date,
    reference,
    batch_code,
    inputter=SYSTEM_USER,
    authorizer=SYSTEM_USER,
):
    default_currency = get_default_currency()
    default_officer = get_default_account_officer()
    customer = getattr(account, "customer_id", None)
    branch = (
        getattr(customer, "branch_id", None)
        or getattr(account, "branch_id", None)
        or get_random_branch()
        or get_default_branch()
    )
    currency = getattr(account, "currency_id", None) or default_currency
    officer = getattr(account, "account_officer_id", None) or default_officer
    trans_code = (
        f"{transaction_type.trancode}{event_date.strftime('%Y%m%d')}"
        if transaction_type
        else None
    )
    return {
        "name": reference,
        "transaction_number": reference,
        "internal_ref_number": reference,
        "account_id": account.id,
        "customer_id": customer.id if customer else None,
        "currency_id": currency.id if currency else None,
        "currency": account.currency or (currency.name if currency else DEFAULT_CURRENCY),
        "branch_id": branch.id if branch else None,
        "branch_code": getattr(account, "branch_code", None)
        or getattr(branch, "code", None),
        "account_officer_id": officer.id if officer else None,
        "date_created": event_date.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amount,
        "amount_local": amount,
        "narration": narration,
        "batch_code": batch_code,
        "tran_type": transaction_type.id if transaction_type else None,
        "trans_code": trans_code,
        "transaction_type": transaction_type.trantype if transaction_type else None,
        "inputter": inputter,
        "authorizer": authorizer,
        "state": "done",
    }


EVENT_TEMPLATES = [
    {
        "name": "bank_charge_sweep",
        "debit_keywords": ["cash clearing", "interbranch clearing", "suspense"],
        "credit_keywords": ["bank charge income", "charge income", "fee income"],
        "amount_range": (2500, 45000),
        "narratives": [
            "Monthly account maintenance charge sweep for {date}; debit {debit_code} credit {credit_code}; batch {batch}.",
            "Bank charge income recognition for internal fee collection cycle {cycle} on {date}; settlement from {debit_account} to {credit_account}.",
            "Recovery of accumulated service charges as at {date}; charges routed from {debit_account} into {credit_account}.",
        ],
        "debit_trancode": "CHG",
        "credit_trancode": "DEP",
    },
    {
        "name": "interest_accrual",
        "debit_keywords": ["unearned interest income", "suspense", "cash clearing"],
        "credit_keywords": ["accrued interest payable"],
        "amount_range": (5000, 120000),
        "narratives": [
            "Daily interest accrual for deposit liabilities on {date}; transfer from {debit_account} to {credit_account}.",
            "Interest provisioning entry for cycle {cycle}; accrued amount {amount} posted on {date}.",
            "Interest expense accrual recognised on {date}; debit {debit_code} and credit {credit_code}.",
        ],
        "debit_trancode": "WDR",
        "credit_trancode": "DEP",
    },
    {
        "name": "suspense_clearance",
        "debit_keywords": ["suspense", "treasury trading suspense", "remittance suspense"],
        "credit_keywords": ["cash clearing", "interbranch clearing", "disbursements in transit", "nostro", "vostro"],
        "amount_range": (15000, 350000),
        "narratives": [
            "Clear-down of long outstanding suspense items on {date}; funds moved from {debit_account} to {credit_account}.",
            "Back-office suspense liquidation batch {batch} dated {date}; resolved entries transferred into {credit_account}.",
            "Suspense regularisation posting for reconciliation cycle {cycle}; source {debit_code}, destination {credit_code}.",
        ],
        "debit_trancode": "WDR",
        "credit_trancode": "DEP",
    },
    {
        "name": "atm_settlement_funding",
        "debit_keywords": ["cash clearing", "nostro", "interbranch clearing"],
        "credit_keywords": ["atm cash settlement"],
        "amount_range": (50000, 750000),
        "narratives": [
            "ATM cash settlement funding for {date}; vault support transferred from {debit_account} to {credit_account}.",
            "Channel settlement top-up batch {batch} posted on {date}; ATM position funded via {credit_account}.",
            "Internal ATM replenishment entry for cycle {cycle}; funds routed from {debit_code} to {credit_code}.",
        ],
        "debit_trancode": "WDR",
        "credit_trancode": "DEP",
    },
    {
        "name": "interbranch_rebalancing",
        "debit_keywords": ["cash clearing", "nostro", "vostro", "disbursements in transit"],
        "credit_keywords": ["interbranch clearing"],
        "amount_range": (100000, 1250000),
        "narratives": [
            "Interbranch liquidity rebalancing entry for {date}; central funding passed into {credit_account}.",
            "Daily branch position balancing batch {batch}; transfer from {debit_account} to {credit_account}.",
            "Internal branch settlement on {date}; excess funds re-routed through {credit_code} for regional balancing.",
        ],
        "debit_trancode": "WDR",
        "credit_trancode": "DEP",
    },
    {
        "name": "official_cheque_cover",
        "debit_keywords": ["cash clearing", "disbursements in transit", "nostro"],
        "credit_keywords": ["official cheques payable"],
        "amount_range": (25000, 400000),
        "narratives": [
            "Official cheque cover lodged on {date}; settlement from {debit_account} into {credit_account}.",
            "Manager's cheque funding batch {batch}; payable position created on {date}.",
            "Internal issuance support for official cheques; value {amount} posted to {credit_account} on {date}.",
        ],
        "debit_trancode": "WDR",
        "credit_trancode": "DEP",
    },
    {
        "name": "payroll_funding",
        "debit_keywords": ["accrued salary expense", "cash clearing", "interbranch clearing"],
        "credit_keywords": ["payroll clearing"],
        "amount_range": (75000, 900000),
        "narratives": [
            "Payroll funding for staff salary run on {date}; debit {debit_account} credit {credit_account}.",
            "Monthly payroll prefunding cycle {cycle}; salary obligations moved into {credit_account}.",
            "Salary clearing transfer batch {batch} posted on {date}; operational funding from {debit_code}.",
        ],
        "debit_trancode": "WDR",
        "credit_trancode": "DEP",
    },
    {
        "name": "tax_provision",
        "debit_keywords": ["bank charge income", "unearned interest income", "currency exchange gain/loss"],
        "credit_keywords": ["income tax payable"],
        "amount_range": (10000, 150000),
        "narratives": [
            "Tax provision on internal income booked for {date}; amount {amount} transferred to {credit_account}.",
            "Income tax accrual for cycle {cycle}; source {debit_account}, payable ledger {credit_account}.",
            "Regulatory tax provisioning batch {batch} dated {date}; credit raised in {credit_code}.",
        ],
        "debit_trancode": "WDR",
        "credit_trancode": "DEP",
    },
]


def create_event_postings(transaction_model, index, event_template, iterations, debit_type, credit_type):
    created = 0

    for _ in range(iterations):
        debit_account = choose_account(index, event_template["debit_keywords"])
        if not debit_account:
            continue

        credit_account = choose_account(
            index,
            event_template["credit_keywords"],
            exclude_ids={debit_account.id},
        )
        if not credit_account:
            continue

        amount = round(random.uniform(*event_template["amount_range"]), 2)
        event_date = current_transaction_datetime()
        batch_code = generate_reference(prefix="BAT")
        metadata = {
            "cycle": event_date.strftime("%Y%m"),
            "branch": debit_account.branch_id.name if getattr(debit_account, "branch_id", None) else "HQ",
            "batch": batch_code,
        }
        narration_template = random.choice(event_template["narratives"])
        narration = build_narrative(
            narration_template,
            amount,
            debit_account,
            credit_account,
            event_date,
            metadata,
        )

        debit_reference = generate_reference(prefix="ITD")
        credit_reference = generate_reference(prefix="ITC")

        debit_values = transaction_values(
            account=debit_account,
            amount=amount,
            transaction_type=debit_type,
            narration=narration,
            event_date=event_date,
            reference=debit_reference,
            batch_code=batch_code,
        )
        credit_values = transaction_values(
            account=credit_account,
            amount=amount,
            transaction_type=credit_type,
            narration=narration,
            event_date=event_date,
            reference=credit_reference,
            batch_code=batch_code,
        )

        try:
            transaction_model.create(debit_values)
            transaction_model.create(credit_values)
            created += 2
            print(
                f"Created {event_template['name']} pair: {debit_reference} / {credit_reference} "
                f"for {format_amount(amount)}"
            )
        except Exception as exc:
            print(
                f"Error creating {event_template['name']} entry for "
                f"{debit_account.name} and {credit_account.name}: {exc}"
            )

    return created


def create_internal_transactions():
    accounts = get_internal_accounts()
    if len(accounts) < 2:
        print(
            "Not enough internal accounts found. Create internal accounts first "
            "using 2_internal_account.py."
        )
        return 0

    print(f"Found {len(accounts)} internal account(s)")
    index = build_account_index(accounts)
    transaction_model = env["res.customer.transaction"]
    debit_type_default = get_transaction_type("WDR")
    credit_type_default = get_transaction_type("DEP")

    total_created = 0
    for template in EVENT_TEMPLATES:
        debit_type = get_transaction_type(template["debit_trancode"]) or debit_type_default
        credit_type = get_transaction_type(template["credit_trancode"]) or credit_type_default
        if not debit_type or not credit_type:
            print(
                f"Skipping {template['name']}: required transaction types are missing."
            )
            continue

        total_created += create_event_postings(
            transaction_model=transaction_model,
            index=index,
            event_template=template,
            iterations=DEFAULT_PER_EVENT,
            debit_type=debit_type,
            credit_type=credit_type,
        )

    return total_created


if __name__ == "__main__":
    created = create_internal_transactions()
    print(
        f"Created {created} internal transaction records"
        if created
        else "No internal transactions created"
    )
