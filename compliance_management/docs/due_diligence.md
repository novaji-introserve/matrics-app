# iComply — Due Diligence: Complete Reference

---

## 1. Overview

The Due Diligence menu provides compliance officers with a structured way to manage and review different types of entities the bank has relationships with. Each category has its own regulatory obligations and risk profile.

All five categories — Customers, Vendors, Partners, Correspondents, and Respondents — are the **same underlying model** (`res.partner`). The only thing separating them is the `internal_category` field on each partner record. Each menu item is a filtered view of the same list.

```
Due Diligence
  ├── Customers       → internal_category = 'customer'
  ├── Vendors         → internal_category = 'vendor'
  ├── Partners        → internal_category = 'partner'
  ├── Respondents     → internal_category = 'respondent'
  └── Correspondents  → internal_category = 'correspondent'
```

---

## 2. The Five Categories Explained

### Customers

**Filter:** `internal_category = customer`

People or businesses that hold accounts at the bank. They are the primary subjects of all compliance activity — KYC, AML screening, risk scoring, watchlist checks, EDD, and transaction monitoring.

This is the largest and most active category. Every customer that comes through the ETL or data import lands here if their `internal_category` is set to `customer`.

**Regulatory obligations:**
- Full KYC verification (identity, address, source of funds)
- AML and sanctions screening (PEP, watchlist, sanction list)
- Risk scoring via plans and assessments
- Enhanced Due Diligence (EDD) for high-risk customers
- Ongoing transaction monitoring

---

### Vendors

**Filter:** `internal_category = vendor`

Suppliers or service providers the bank pays money to — contractors, technology vendors, consultants, facility managers. They are not account holders but they receive money from the bank.

Due diligence on vendors is required to ensure the bank is not inadvertently paying sanctioned parties or entities with links to financial crime.

**Regulatory obligations:**
- Sanctions and PEP screening before onboarding
- Ownership and beneficial owner verification
- Periodic review, especially for high-value contracts
- Source of funds not required (they receive funds, not deposit them)

---

### Partners

**Filter:** `internal_category = partner`

Business partners, affiliates, joint venture entities, or organisations the bank has a formal commercial or strategic relationship with — but who are neither account holders nor suppliers. Examples include insurance partners, fintech collaborators, or referral agents.

**Regulatory obligations:**
- Ownership structure and beneficial owner checks
- Sanctions screening
- Business purpose and nature of relationship documented
- Periodic review based on risk level of the partnership

---

### Correspondents

**Filter:** `internal_category = correspondent`

Other banks or financial institutions that your bank has a **correspondent banking relationship** with. A correspondent bank provides services — such as clearing USD transactions, holding nostro accounts, or processing international wire transfers — on behalf of another bank.

**Your bank is the correspondent** — you are providing the service to the foreign or smaller bank.

This is one of the most heavily regulated categories. FATF, the CBN, and most global regulators require deep due diligence on correspondent relationships because they can be exploited to move illicit funds across borders through layers of institutions.

**Regulatory obligations:**
- Full institutional due diligence on the foreign bank
- Assessment of the foreign bank's own AML/CFT programme
- Review of the foreign bank's regulatory environment and supervisory authority
- Identification of the foreign bank's beneficial owners
- No shell bank relationships permitted
- Senior management approval before establishing or renewing the relationship
- Ongoing monitoring of the relationship and transactions

---

### Respondents

**Filter:** `internal_category = respondent`

The reverse of a correspondent. A respondent is a bank that **uses your bank** to access the financial system — for example, a smaller domestic bank or foreign bank that relies on your bank to clear transactions or hold accounts on their behalf.

**Your bank is the correspondent; they are the respondent.**

The due diligence obligations are the same as for correspondents because the risk is symmetric — you are still exposed to the respondent bank's customer base and AML controls.

---

## 3. Correspondent vs Respondent — The Clearest Distinction

```
                  Correspondent Banking Relationship
                  
Your Bank  ←─────────────────────────────────────→  Foreign Bank
(Correspondent)                                      (Respondent)

Your bank provides:                  Foreign bank receives:
- Nostro account                     - Access to USD clearing
- USD clearing                       - Access to SWIFT network
- International wire processing      - Cross-border payment capability
```

A bank can be **both** at the same time — a correspondent to one bank and a respondent to another, depending on the direction of the relationship.

| | Correspondent | Respondent |
|--|--|--|
| **Who provides the service** | Your bank | The other bank |
| **Who receives the service** | The other bank | Your bank |
| **Who holds the nostro account** | Your bank | The other bank |
| **Regulatory focus** | You must vet them | You must be vetted |

---

## 4. Why They Are Separated in the Menu

Each category carries different regulatory obligations and a different due diligence workflow. Mixing them in a single list would make it impossible to apply category-specific screening logic or track completion status correctly.

| Category | Primary Risk | Key Obligation |
|----------|-------------|----------------|
| Customer | Money laundering, terrorism financing | KYC, AML screening, risk scoring |
| Vendor | Sanctions evasion, illicit payments | Sanctions screening, ownership check |
| Partner | Reputational, indirect exposure | Ownership structure, business purpose |
| Correspondent | Cross-border illicit fund flows | Institutional AML programme assessment |
| Respondent | Same as correspondent | Same as correspondent |

---

## 5. Access Control by Role

From the menu definitions, the Due Diligence menus are restricted as follows:

| Menu | BCO | CO | TMT | CCO |
|------|:---:|:--:|:---:|:---:|
| Customers | ✓ | ✓ | ✓ | ✓ |
| Vendors | ✓ | ✓ | | ✓ |
| Partners | ✓ | ✓ | | ✓ |
| Respondents | ✓ | ✓ | | ✓ |
| Correspondents | ✓ | ✓ | | ✓ |

TMT (Transaction Monitoring Team) can only see Customers — their focus is transaction surveillance, not relationship due diligence.

---

## 6. What CCO and CO See vs BCO

From the code in `customer.py`:

**CCO and CO** — see all records across all branches with the matching `internal_category`.

**BCO (Branch Compliance Officer)** — sees only records where `branch_id` is in their assigned branches. They cannot see entities outside their branch scope.

```python
# CCO / CO
domain = [('internal_category', '=', 'customer'), ('origin', 'in', ['demo', 'test', 'prod'])]

# BCO
domain = [
    ('branch_id.id', 'in', [user's assigned branch IDs]),
    ('internal_category', '=', 'customer'),
    ('origin', 'in', ['demo', 'test', 'prod'])
]
```

---

## 7. The `origin` Field

All five Due Diligence views also filter on `origin IN ('demo', 'test', 'prod')`. This field tracks where the data came from:

| Value | Meaning |
|-------|---------|
| `prod` | Real customer data from the live banking system |
| `demo` | Demo data loaded for testing purposes |
| `test` | Test data created during system testing |

Records without an `origin` set are excluded from all Due Diligence views. This prevents orphaned or incomplete partner records from polluting the compliance lists.

---

## 8. Enhanced Due Diligence (EDD)

EDD is a separate but related process accessed from `KYC → Enhanced Due Diligence`. It applies to **high-risk customers** (typically from the Customer category) who require deeper scrutiny beyond standard KYC.

EDD captures:
- Detailed customer profile and business nature
- Source of funds and source of wealth (with supporting documents)
- Residency status
- PEP association
- Cross-border transaction expectations
- Negative news / adverse media
- Third-party involvement
- Expected income and net worth

An approved EDD record carries its own `risk_score` which takes **second priority** in the customer risk scoring chain (after a direct risk assessment, before plan-based scoring).

```
Priority order for customer risk_score:
  1st → Direct res.risk.assessment linked to this customer
  2nd → Approved EDD risk_score
  3rd → Risk plan computation (SQL-based plans)
```

LOGin Page
if AD is active the normal form will be hidden as a support if there is need to toggle back th normal form ater activating AD

add support as param to the url i.e http://localhost:8069/web/login?support=1