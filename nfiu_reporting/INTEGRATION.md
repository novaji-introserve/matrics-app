# NFIU Reporting Module — Integration Guide

## What This Module Does

The `nfiu_reporting` module enables the institution to flag suspicious or unusual
transactions and generate XML reports compliant with the **NFIU goAML 4.5 schema**
for submission to the Financial Intelligence Unit.

It extends the core `compliance_management` transaction model with:
- A suspicious/unusual workflow on individual transactions
- An audit history of every suspicious flagging event
- Full goAML XML report generation, validation, and submission

---

## Module Dependencies

```
nfiu_reporting
    ├── compliance_management   (base transaction + customer models)
    ├── regulatory_reports      (group_regulatory_report security group)
    ├── case_management_v2      (link cases to suspicious transactions)
    └── hr                      (reporting persons linked to hr.employee)
```

`transaction_screening` also explicitly depends on `nfiu_reporting` because its
screening rules use the `transaction_flag` field that this module adds.

---

## Data Models

### Core transaction extension — `NFIUTransaction`
Inherits `res.customer.transaction` and adds:

| Field | Type | Purpose |
|-------|------|---------|
| `suspicious_transaction` | Boolean | True once flagged suspicious |
| `report_nfiu` | Boolean | Include in next NFIU XML export |
| `state` | Selection | new → unusual → awaiting_approval → suspicious → done |
| `report_id` | Many2one → `nfiu.report` | The NFIU report this tx was included in |
| `transmode_code` | Selection | How funds moved: Cash/Check/Card/Wire/Electronic |
| `from_person_id / to_person_id` | Many2one → `res.partner` | Parties to the transaction |
| `from_entity_id / to_entity_id` | Many2one → `nfiu.entity` | Entity parties |
| `from_funds_code / to_funds_code` | Selection | Source/destination of funds codes |
| `suspicious_transaction_history_ids` | One2many → `nfiu.suspicious.transaction.hist` | Audit trail |
| `case_ids` | One2many → `case.manager` | Linked compliance cases |

### Suspicious transaction history — `nfiu.suspicious.transaction.hist`
Created every time a transaction is marked suspicious. Acts as an immutable audit trail.

| Field | Type | Purpose |
|-------|------|---------|
| `transaction_id` | Many2one → `res.customer.transaction` | The flagged transaction |
| `name` | Char (related) | Transaction reference number |
| `customer_id` | Many2one (related) | Customer on the transaction |
| `account_id` | Many2one (related) | Account on the transaction |
| `date_reported` | Datetime | When it was flagged |
| `reported_by` | Many2one → `res.users` | Who flagged it |
| `comments` | Text | Notes at time of flagging |

### Report — `nfiu.report`
The actual goAML report submitted to the NFIU.

| Field | Type | Purpose |
|-------|------|---------|
| `report_code` | Selection | CTR / STR / UTR / EFT / IFT etc. |
| `entity_id` | Many2one → `nfiu.entity` | Reporting institution |
| `reporting_person_id` | Many2one → `nfiu.person` | Officer submitting the report |
| `date_from / date_to` | Date | Transaction window |
| `indicator_ids` | Many2many → `nfiu.indicator` | goAML indicator codes |
| `xml_content` | Text | Generated XML |
| `state` | Selection | draft → generated → validated → submitted |

### Supporting models

| Model | Purpose |
|-------|---------|
| `nfiu.entity` | The reporting institution (bank/branch) |
| `nfiu.person` | Reporting officers — linked to `hr.employee` |
| `nfiu.address` | Addresses for entities and persons |
| `nfiu.entity.director` | Directors/signatories of the entity |
| `nfiu.indicator` | goAML indicator categories (THRESHOLDREPORT, STR, etc.) |
| `nfiu.currency.threshold` | Per-currency CTR thresholds |

---

## How Transaction Screening Connects

This is the primary integration point between screening and NFIU reporting.

### The flow

```
multi_screen()
    └── action_screen()                          [compliance_management]
            └── for each rule:
                    evaluate rule condition
                    if matched AND rule.transaction_flag == 'suspicious':
                        action_mark_as_suspicious()    [nfiu_reporting]
                            ├── creates nfiu.suspicious.transaction.hist record
                            └── writes: suspicious_transaction=True
                                        state='suspicious'
                                        report_nfiu=True
```

### The `transaction_flag` field

`nfiu_reporting` adds a `transaction_flag` field to `res.transaction.screening.rule`:

```
transaction_flag = Selection(['unusual', 'suspicious'])
```

- **suspicious** → calls `action_mark_as_suspicious()`, creates history, sets `report_nfiu=True`
- **unusual** → calls `action_mark_unusual()`, sets `state='unusual'` only (no NFIU history)

This means the screening rule itself controls whether a match results in an NFIU flagging.
Setting a rule to `transaction_flag='suspicious'` automatically creates NFIU audit records.

### Who can call `action_mark_as_suspicious()`

Requires create access on `nfiu.suspicious.transaction.hist`:

| Group | Access |
|-------|--------|
| `regulatory_reports.group_regulatory_report` | Full CRUD |
| `compliance_management.group_compliance_chief_compliance_officer` | Full CRUD |
| `compliance_management.group_tech_support` | Full CRUD |

---

## Case Management Connection

When a compliance case (`case.manager`) is created and linked to a transaction
(`transaction_id` is set), the NFIU module automatically advances that transaction
to `state='awaiting_approval'`. This bridges the suspicious flagging workflow
with the case investigation workflow.

```
Transaction flagged suspicious
    └── Analyst creates case (case_management_v2)
            └── Case.create() → transaction.state = 'awaiting_approval'
```

---

## Transaction State Lifecycle

```
new
 │
 ├─ action_mark_unusual()       → unusual
 │
 ├─ action_mark_as_suspicious() → suspicious ──┐
 │                                              │
 │   (case created)                            │
 └─ case linked                 → awaiting_approval
                                               │
                                  report included in NFIU XML
                                               │
                                              done
```

---

## NFIU Report Generation Workflow

1. **Setup** (one-time):
   - Create `nfiu.entity` for your institution
   - Create `nfiu.person` for reporting officers (linked to HR employees)
   - Configure `nfiu.currency.threshold` per currency
   - Set up `nfiu.indicator` codes

2. **Flagging** (ongoing, automated):
   - Transactions are screened by rules
   - Matched suspicious rules call `action_mark_as_suspicious()`
   - `report_nfiu=True` is set on those transactions

3. **Report creation**:
   - Create `nfiu.report` (select type: CTR/STR/etc., date range, entity, indicators)
   - Click **Generate XML** — queries `res.customer.transaction` where `report_nfiu=True`
     and `date_created` within the date range
   - Click **Validate XML** — validates against the embedded goAML 4.5 XSD schema
   - Click **Submit** — marks report as submitted

---

## Menu Locations

**Financial Intelligence** (under Regulatory Reports menu):
- Local Currency Transactions
- Foreign Currency Transactions
- Suspicious Transactions
- Financial Intelligence Reports

**Configuration** (under Compliance Configuration menu):
- Reporting Persons
- Reporting Entities
- Directors
- Report Indicators
- Currency Thresholds

---

## Key Files Reference

| File | What it does |
|------|-------------|
| `models/nfiu_transaction.py` | Transaction extension, `action_mark_as_suspicious()`, `nfiu.suspicious.transaction.hist` |
| `models/nfiu_report.py` | XML report generation, goAML schema, validation |
| `models/nfiu_entity.py` | Reporting institution model |
| `models/nfiu_person.py` | Reporting officer model (linked to HR) |
| `views/nfiu_transaction_views.xml` | Adds Mark Suspicious/Unusual buttons + history tab to transaction form |
| `views/nfiu_report_views.xml` | NFIU report form with Generate/Validate/Submit buttons |
| `security/ir.model.access.csv` | Access control — all models restricted to regulatory_report group + CCO + Tech Support |
| `data/NFIU_goAML_4_5_Schema.xsd` | Official NFIU schema used for XML validation |
