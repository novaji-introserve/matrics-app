# AML Detection — Full Technical Reference

## Overview

The `transaction_screening` module adds a four-layer AML detection engine that runs
automatically every time a transaction is screened. After the rule-based screening
completes, `_run_aml_detection()` is called on the transaction and executes four
independent checks:

| Check | What it detects | Alert model |
|-------|----------------|-------------|
| **Velocity** | Too many or too large transactions in a short window | `res.aml.velocity.alert` |
| **Structuring** | Multiple sub-threshold deposits deliberately kept below the CTR limit | `res.aml.structuring.alert` |
| **Anomaly** | A single transaction that deviates statistically from the customer's normal behaviour | `res.aml.anomaly.alert` |
| **Dormant** | An account that was inactive for a long period suddenly making a transaction | `res.aml.dormant.alert` |

Each check returns a **risk score (0–100)**. The four scores are combined into a single
**composite AML risk score** using configurable weights. The composite score and a
comma-separated flags string (`VELOCITY,STRUCTURING,ANOMALY,DORMANT`) are written
back to the transaction.

---

## How It Connects to Transaction Screening

```
ETL / API / UI button
    └── multi_screen(self)                         [compliance_management]
            │  fetches all active rules once
            └── for each transaction:
                    action_screen(rules)            [compliance_management]
                        │  evaluates each rule (SQL / Python)
                        │  writes state='done', rule_id, transaction_risk_level
                        │
                        └── _run_aml_detection()   [transaction_screening]
                                ├── _check_velocity(config)
                                ├── _check_structuring(config)
                                ├── _check_anomaly(config)
                                └── _check_dormant(config)
                                writes: aml_risk_score, aml_flags
```

Rule-based screening and AML detection are independent. A transaction can:
- Pass all rules but still get an AML flag
- Fail a rule (marked suspicious) and also get an AML flag
- Both, neither, or any combination

---

## AML Configuration (`res.aml.config`)

One active config record controls all four checks. Access via:
**Configuration → Transaction Monitoring → AML Configuration**

### CTR Threshold

| Field | Default | Meaning |
|-------|---------|---------|
| `ctr_threshold` | ₦5,000,000 | Cash Transaction Report threshold. Transactions at or above this are direct CTR events, not structuring candidates. |
| `currency_id` | Company currency | Currency the monetary thresholds are expressed in. |

### Velocity

| Field | Default | Meaning |
|-------|---------|---------|
| `velocity_window_hours` | 24 | Rolling window in hours to count transactions |
| `velocity_max_count` | 10 | Flag if customer exceeds this many transactions in the window |
| `velocity_max_amount` | ₦10,000,000 | Flag if customer's total volume in the window exceeds this |

### Structuring

| Field | Default | Meaning |
|-------|---------|---------|
| `structuring_window_hours` | 72 | Rolling window in hours |
| `structuring_min_count` | 3 | Minimum number of sub-threshold transactions required |
| `structuring_approach_pct` | 80.0 | Flag when sub-threshold total reaches this % of the CTR threshold |

### Anomaly

| Field | Default | Meaning |
|-------|---------|---------|
| `anomaly_zscore_threshold` | 3.0 | Z-score at which an amount is flagged as anomalous |
| `anomaly_min_history` | 10 | Minimum transactions required before Z-score detection activates |

### Dormant Account

| Field | Default | Meaning |
|-------|---------|---------|
| `dormant_enabled` | True | Toggle dormant detection on/off |
| `dormant_min_days` | 180 | Days of inactivity that qualifies an account as dormant |

### Composite Risk Weights

The four weights must always sum to **1.0**.

| Field | Default | Role |
|-------|---------|------|
| `velocity_risk_weight` | 0.30 | Proportion of velocity score in composite |
| `structuring_risk_weight` | 0.35 | Proportion of structuring score |
| `anomaly_risk_weight` | 0.25 | Proportion of anomaly score |
| `dormant_risk_weight` | 0.10 | Proportion of dormant score |

```
composite = (velocity_score  × 0.30)
          + (structuring_score × 0.35)
          + (anomaly_score   × 0.25)
          + (dormant_score   × 0.10)
```

---

## Check 1 — Velocity Detection

### What it looks for

A customer performing an unusually high number of transactions, or an unusually large
total volume, within a short rolling window. Common in money mule and account takeover
scenarios where funds must be moved quickly.

### Algorithm

```python
window_start = now() - velocity_window_hours   # e.g. last 24 hours

recent = transactions where:
    customer = this customer
    date_created >= window_start
    id != this transaction          # exclude self
    state not in cancelled/rejected

count  = len(recent) + 1            # include current
total  = sum(recent.amount) + self.amount

if count > velocity_max_count OR total > velocity_max_amount:
    count_ratio  = count / velocity_max_count
    amount_ratio = total / velocity_max_amount
    risk_score   = min(100, max(count_ratio, amount_ratio) × 100)
    → create res.aml.velocity.alert
    → return risk_score
```

### Risk score interpretation

The score is the **worst ratio** (count vs limit, or total vs limit) scaled to 100.
A customer at exactly the limit scores 100. Two times the limit scores 200, capped to 100.

### Demo trigger

`DEMO/AML/VEL/001–003` — three ₦4M transactions within 2 hours.
Total = ₦12M > ₦10M limit → `amount_ratio = 1.2` → risk_score = 100 (capped).

---

## Check 2 — Structuring / Smurfing Detection

### What it looks for

A customer deliberately splitting a large deposit into multiple smaller amounts, each
kept below the CTR reporting threshold, so that no single transaction triggers mandatory
reporting. This is called smurfing or structuring and is a financial crime.

The check only considers transactions **below** the CTR threshold — a transaction at
or above the threshold is itself a CTR event, not a structuring candidate.

### Algorithm

```python
threshold     = ctr_threshold               # e.g. ₦5,000,000
approach_limit = threshold × (approach_pct / 100)   # e.g. ₦4,000,000

if self.amount >= threshold:
    return 0.0      # direct CTR event, not structuring

window_start = now() - structuring_window_hours

sub_threshold = transactions where:
    customer = this customer
    date_created >= window_start
    amount < threshold              # sub-threshold only
    id != self
    state not in cancelled/rejected

count = len(sub_threshold) + 1
total = sum(sub_threshold.amount) + self.amount

if count >= structuring_min_count AND total >= approach_limit:
    risk_score = min(100, (total / threshold) × 70 + (count / min_count) × 30)
    → create res.aml.structuring.alert
    → return risk_score
```

### Risk score formula breakdown

The score has two components:
- **70% weight on total amount** — how close is the total to the CTR threshold
- **30% weight on transaction count** — how many sub-threshold transactions

This means a customer with a very high total gets a higher score than one with many
small transactions that barely approach the limit.

### Demo trigger

`DEMO/AML/STR/001–004` — four ₦1.2M transactions within 48 hours.
Total = ₦4.8M ≥ approach_limit of ₦4M. Count = 4 ≥ min_count of 3.

---

## Check 3 — Statistical Anomaly Detection

### What it looks for

A single transaction whose amount is statistically inconsistent with the customer's
established spending behaviour. Uses a Z-score calculation against the customer's
historical mean and standard deviation.

This check requires a minimum history (`anomaly_min_history`, default 10) before it
activates. Until then, it only updates the profile and returns 0.

### What is a Z-score?

The Z-score measures how many standard deviations an observation is from the mean:

```
Z = (amount - mean) / stddev
```

A Z-score of 0 means the amount is exactly average.
A Z-score of 3.0 means the amount is 3 standard deviations above the mean — statistically
unusual (less than 0.3% probability under a normal distribution).

**Example:**
- Customer mean: ₦10,000 | stddev: ₦1,600
- Transaction: ₦800,000
- Z = (800,000 − 10,000) / 1,600 = **493.75** → extreme outlier

### Algorithm

```python
profile = get_or_create_profile(customer)

if profile.transaction_count < anomaly_min_history:
    profile.update_with_transaction(amount)   # build up history
    return 0.0                                # not enough data yet

zscore = (amount - profile.mean_amount) / profile.stddev_amount

if abs(zscore) >= anomaly_zscore_threshold:
    risk_score = min(100, (abs(zscore) / threshold) × 50)
    → create res.aml.anomaly.alert
    → return risk_score

profile.update_with_transaction(amount)   # always update after check
```

### Why `abs(zscore)`?

Both unusually **large** and unusually **small** amounts are flagged. An account that
normally receives ₦500,000 but suddenly receives ₦500 could indicate account compromise.

---

## The Customer Behavioral Profile (`res.aml.customer.profile`)

This is the statistical backbone of anomaly detection. One profile record exists per
customer and is updated incrementally each time a transaction is screened.

### Fields

| Field | Type | Meaning |
|-------|------|---------|
| `customer_id` | Many2one | The customer this profile belongs to |
| `transaction_count` | Integer | Number of transactions processed so far |
| `mean_amount` | Float | Running mean of all transaction amounts |
| `m2` | Float | Welford's M2 accumulator (sum of squared deviations) |
| `stddev_amount` | Float (computed) | Sample standard deviation, derived from M2 |
| `last_updated` | Datetime | When the profile was last updated |

### What is the Mean?

The **mean** (average) is the sum of all transaction amounts divided by the count.
It represents the customer's typical transaction size.

```
mean = (tx1 + tx2 + tx3 + ... + txN) / N
```

For a customer whose transactions are ₦8,000 / ₦10,000 / ₦12,000:
```
mean = (8,000 + 10,000 + 12,000) / 3 = 10,000
```

The profile uses a **running mean** — it updates after each new transaction without
storing all historical amounts:

```
new_mean = old_mean + (new_amount - old_mean) / new_count
```

### What is Standard Deviation?

The **standard deviation (stddev)** measures how spread out the transaction amounts are
around the mean. A low stddev means the customer is consistent; a high stddev means
they vary a lot.

```
stddev = sqrt( sum((xi - mean)²) / (N - 1) )
```

For the example above:
```
deviations:  8,000 − 10,000 = −2,000  → squared = 4,000,000
            10,000 − 10,000 =      0  → squared =         0
            12,000 − 10,000 = +2,000  → squared = 4,000,000

variance = (4,000,000 + 0 + 4,000,000) / (3 − 1) = 4,000,000
stddev   = sqrt(4,000,000) = 2,000
```

This means typical transactions are within ±₦2,000 of the ₦10,000 mean.
A transaction of ₦800,000 would be Z = (800,000 − 10,000) / 2,000 = **395** standard
deviations away — an extreme outlier.

### Welford's Online Algorithm

The profile does not store every transaction. Instead it uses **Welford's online
algorithm** which maintains a running mean and a variance accumulator (M2) that can
be updated with a single new value at a time:

```python
def update_with_transaction(self, amount):
    n        = self.transaction_count + 1
    delta    = amount - self.mean_amount        # deviation from OLD mean
    new_mean = self.mean_amount + delta / n     # update mean
    delta2   = amount - new_mean                # deviation from NEW mean
    new_m2   = self.m2 + delta * delta2         # accumulate variance

    self.write({
        'transaction_count': n,
        'mean_amount': new_mean,
        'm2': new_m2,
    })
```

The standard deviation is then:
```
stddev = sqrt(M2 / (N - 1))      # sample standard deviation
```

This approach is numerically stable (avoids floating-point errors from summing large
squared values) and requires no historical data to be stored — only three numbers:
`transaction_count`, `mean_amount`, and `m2`.

### Weekly Profile Rebuild

A weekly cron job (`AML: Rebuild Customer Behavioral Profiles`) recalculates all
profiles from scratch using a single SQL query:

```sql
SELECT customer_id,
       COUNT(*)              AS cnt,
       AVG(amount)           AS mean,
       VAR_POP(amount) * COUNT(*) AS m2_val
FROM res_customer_transaction
WHERE customer_id IS NOT NULL
  AND state NOT IN ('cancelled', 'rejected')
GROUP BY customer_id
```

This corrects any drift that may have accumulated from incremental updates and ensures
the profile reflects all historical data, not just transactions screened since install.

---

## Check 4 — Dormant Account Detection

### What it looks for

An account that has had no transaction activity for longer than the configured dormancy
threshold (`dormant_min_days`, default 180 days) suddenly making a transaction. This
pattern can indicate account takeover, identity fraud, or reactivation for money
laundering.

### Algorithm

```python
current_date = self.date_created or now()

last_txn = most recent transaction for this customer
           where date_created < current_date
           and id != self
           and state not in cancelled/rejected

if no last_txn:
    return 0.0      # no history to compare against

dormant_days = (current_date - last_txn.date_created).days

if dormant_days < dormant_min_days:
    return 0.0

risk_score = min(100, (dormant_days / dormant_min_days) × 60)
→ create res.aml.dormant.alert (records last_txn date, days dormant, amount)
→ return risk_score
```

### Risk score

Scaled proportionally to how long the account was dormant, capped at 100.
An account dormant for exactly 180 days scores 60. One dormant for 300 days scores 100.

### Demo trigger

`DEMO/AML/DOR/001` — reactivation after 927 days dormant (last tx: 2023-10-01).
risk_score = min(100, (927/180) × 60) = min(100, 308.9) = **100**.

---

## Alert Models

All four alert models share the same structure and workflow.

### Common fields

| Field | All alerts | Meaning |
|-------|-----------|---------|
| `name` | Char | Auto-generated sequence ref (VAL/2026/00001 etc.) |
| `transaction_id` | Many2one | The transaction that triggered the alert |
| `customer_id` | Many2one | Customer on the transaction |
| `risk_score` | Float | Score 0–100 for this specific check |
| `state` | Selection | open → reviewed → escalated → closed / false_positive |
| `created_at` | Datetime | When the alert was created |
| `notes` | Text | Analyst review notes |

### Sequence prefixes

| Alert | Prefix | Example |
|-------|--------|---------|
| Velocity | `VAL` | `VAL/2026/00001` |
| Structuring | `SAL` | `SAL/2026/00001` |
| Anomaly | `AAL` | `AAL/2026/00001` |
| Dormant | `DAL` | `DAL/2026/00001` |

### Alert-specific fields

**Velocity alert** additionally stores:
- `window_hours` — the window that was checked
- `txn_count` — how many transactions were in the window
- `total_amount` — total volume in the window

**Structuring alert** additionally stores:
- `window_hours`, `txn_count`, `total_amount` — same as velocity
- `ctr_threshold` — the CTR threshold at time of detection

**Anomaly alert** additionally stores:
- `transaction_amount` — the outlier amount
- `customer_mean` — the customer's mean at time of detection
- `customer_stddev` — the customer's stddev at time of detection
- `zscore` — the calculated Z-score

**Dormant alert** additionally stores:
- `last_transaction_date` — when the customer last transacted
- `dormant_days` — how many days the account was inactive
- `transaction_amount` — the reactivation transaction amount

---

## Transaction Fields Written by AML Detection

After `_run_aml_detection()` completes, two fields are written on the transaction:

| Field | Type | Example |
|-------|------|---------|
| `aml_risk_score` | Float | `73.50` |
| `aml_flags` | Char | `VELOCITY,DORMANT` |

These are visible on the transaction form under the **AML Detection** tab, along with
sub-tabs for each alert type showing all alerts linked to that transaction.

---

## Demo Data Summary

The `transaction_screening/demo/demo.xml` file contains purpose-built transactions to
trigger each AML check when `multi_screen()` is run on them.

| Customer | Transactions | Check triggered | Why |
|----------|-------------|-----------------|-----|
| Demo — AML Velocity | VEL/001–003 | VELOCITY | 3 × ₦4M = ₦12M total in 2 hrs, exceeds ₦10M max |
| Demo — AML Structuring | STR/001–004 | STRUCTURING | 4 × ₦1.2M = ₦4.8M in 48 hrs, above 80% of ₦5M CTR |
| Demo — AML Dormant | DOR/001 | DORMANT | 927 days since last transaction (2023-10-01) |
| Demo — AML Anomaly | ANO/000 (outlier) + ANO/001–010 (baselines) | ANOMALY | ₦800,000 vs mean ₦9,700 — Z-score ≈ 494 |

### Why anomaly needs the outlier created first

The model uses `_order = 'id desc'`, so `multi_screen()` processes the record with
the **highest id first**. The 10 baseline transactions (ANO/001–010) have higher ids
than the outlier (ANO/000) because they are created after it in the XML. This ensures
the baselines are screened first (building the profile to count=10), and the outlier
is screened last — at which point the profile has enough history to compute the Z-score.

---

## Access Control

All alert models are accessible to three groups:

| Group | Read | Write | Create | Delete |
|-------|------|-------|--------|--------|
| Chief Compliance Officer | ✓ | ✓ | ✓ | ✓ |
| Compliance Officer | ✓ | ✓ | ✓ | — |
| Transaction Monitoring Team | ✓ | ✓ | ✓ | — |

The AML Configuration is restricted to CCO and Compliance Officer only.

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `models/aml_config.py` | All detection thresholds and weights |
| `models/aml_alerts.py` | Four alert models (velocity, structuring, anomaly, dormant) |
| `models/customer_profile.py` | Welford's algorithm, mean/stddev, weekly rebuild cron |
| `models/transaction.py` | `_run_aml_detection()`, `_check_velocity/structuring/anomaly/dormant()` |
| `views/aml_config.xml` | AML Configuration form with all four tabs |
| `views/aml_alerts.xml` | Alert list/form views + AML Detection tab on transaction form |
| `data/aml_sequences.xml` | VAL/SAL/AAL/DAL sequence definitions + weekly rebuild cron |
| `security/ir.model.access.csv` | ACL for config and first three alert models |
| `security/aml_dormant_access.xml` | ACL for dormant alert (separate XML — new model upgrade-safe) |
| `demo/demo.xml` | Demo partners and transactions for all four AML checks |
