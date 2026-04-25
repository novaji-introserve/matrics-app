import os

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


def get_internal_ledgers():
    ledger_model = explore(env["res.ledger"])
    fields = ledger_model.fields_get()

    if "type" in fields:
        return ledger_model.search([("type", "=", "internal")])

    if "ledger_type_id" in fields:
        return ledger_model.search([("ledger_type_id.name", "=", "Internal")])

    return []


def account_exists(account_model, ledger):
    return bool(
        account_model.search(
            ["|", ("ledger_id", "=", ledger.id), ("name", "=", ledger.ledger_code)],
            limit=1,
        )
    )


def create_internal_accounts():
    account_model = explore(env["res.partner.account"])
    created = 0
    skipped = 0

    ledgers = get_internal_ledgers()
    print(f"Found {len(ledgers)} internal ledger(s)")

    for ledger in ledgers:
        if account_exists(account_model, ledger):
            skipped += 1
            print(
                f"Skipping ledger {ledger.ledger_code} - {ledger.name}: account already exists"
            )
            continue

        values = {
            "name": ledger.ledger_code,
            "account_name": ledger.name,
            "account_type": "Internal",
            "account_status": "active",
            "state": "Active",
            "ledger_id": ledger.id,
        }

        try:
            account_model.create(values)
            created += 1
            print(f"Created account for ledger {ledger.ledger_code} - {ledger.name}")
        except Exception as exc:
            print(
                f"Error creating account for ledger {ledger.ledger_code} - {ledger.name}: {exc}"
            )

    print(f"Created {created} account(s); skipped {skipped} existing account(s)")


if __name__ == "__main__":
    create_internal_accounts()
