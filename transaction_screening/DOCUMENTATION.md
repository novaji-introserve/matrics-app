# Transaction Screening Module — Full Technical Documentation

**Module technical name:** `transaction_screening`  
**Author:** Novaji Introserve Limited  
**Version:** 0.1  
**License:** LGPL-3

---

## Table of Contents

1. [What This Module Does](#1-what-this-module-does)
2. [Module Dependencies](#2-module-dependencies)
3. [File Structure](#3-file-structure)
4. [How Odoo Inheritance Works Here](#4-how-odoo-inheritance-works-here)
5. [Models Reference](#5-models-reference)
   - [res.customer.transaction (extended)](#51-rescustomertransaction-extended)
   - [res.aml.config](#52-resamlconfig)
   - [res.aml.customer.profile](#53-resamlcustomerprofile)
   - [res.aml.velocity.alert](#54-resamlvelocityalert)
   - [res.aml.structuring.alert](#55-resamlstructuringalert)
   - [res.aml.anomaly.alert](#56-resamlanomalyalert)
6. [Detection Algorithms Explained](#6-detection-algorithms-explained)
   - [Velocity Detection](#61-velocity-detection)
   - [Structuring / Smurfing Detection](#62-structuring--smurfing-detection)
   - [Anomaly Detection (Z-Score)](#63-anomaly-detection-z-score)
   - [Composite AML Risk Score](#64-composite-aml-risk-score)
7. [Execution Flow: What Happens When a Transaction is Screened](#7-execution-flow-what-happens-when-a-transaction-is-screened)
8. [Customer Behavioral Profiles — How Baselines Are Built](#8-customer-behavioral-profiles--how-baselines-are-built)
9. [Sequences (Alert Reference Numbers)](#9-sequences-alert-reference-numbers)
10. [Cron Jobs](#10-cron-jobs)
11. [Access Groups and Permissions](#11-access-groups-and-permissions)
12. [Menu Structure](#12-menu-structure)
13. [Views Reference](#13-views-reference)
14. [How to Configure the CTR Threshold](#14-how-to-configure-the-ctr-threshold)
15. [Database Tables Created](#15-database-tables-created)
16. [AML Detection Default Values and Their Meaning](#16-aml-detection-default-values-and-their-meaning)

---

## 1. What This Module Does

The `transaction_screening` module has two layers of capability:

**Layer 1 — Rules-Based Screening (original)**  
When a transaction is screened, the system evaluates it against a list of compliance rules defined in `res.transaction.screening.rule`. If any rule matches and that rule is marked as "blocking", the transaction is placed on hold (`blocked = True`). This is pure if/then logic: the rules are either SQL queries or Python expressions that the compliance team configures.

**Layer 2 — AML Statistical Detection (added)**  
After rule-based screening completes, the system runs three additional statistical checks that no rule can replicate:

| Check | What It Detects |
|---|---|
| **Velocity** | A customer making too many transactions, or moving too much money, in a short rolling time window |
| **Structuring** | A customer deliberately breaking up one large payment into multiple smaller ones to stay below the Cash Transaction Report (CTR) threshold — also called "smurfing" |
| **Anomaly** | A single transaction that is statistically unusual compared to what that customer normally does, measured using a Z-score |

All three checks are governed by a single configuration record (`res.aml.config`) that can be changed at any time without touching code. The CTR threshold is part of this config, making it fully dynamic.

---

## 2. Module Dependencies

```
transaction_screening
  ├── base              (Odoo core — res.partner, res.currency, etc.)
  ├── mail              (Odoo mail — chatter and activity tracking on alerts)
  ├── compliance_management   (base module — provides res.customer.transaction,
  │                            res.transaction.screening.rule, access groups,
  │                            and all parent menus)
  ├── nfiu_reporting    (Nigerian Financial Intelligence Unit reporting)
  └── regulatory_reports
```

The most important dependency is `compliance_management`. That module defines:
- The base `res.customer.transaction` model that this module extends
- The `res.transaction.screening.rule` model
- The `res.transaction.screening.history` model
- All compliance access groups used for permissions
- The parent menu items that this module's menus attach to

This module does **not** modify any base model directly. It uses Odoo's inheritance system (`_inherit`) to extend the transaction model by adding new fields and overriding one method.

---

## 3. File Structure

```
transaction_screening/
│
├── __init__.py                   ← imports controllers and models package
├── __manifest__.py               ← module metadata, dependencies, data file list
│
├── models/
│   ├── __init__.py               ← imports all model files
│   ├── transaction_screening_rule.py   ← extends res.transaction.screening.rule
│   ├── transaction.py            ← extends res.customer.transaction
│   │                               (adds blocked, aml_risk_score, aml_flags,
│   │                                alert relations, and all detection methods)
│   ├── aml_config.py             ← defines res.aml.config
│   ├── customer_profile.py       ← defines res.aml.customer.profile
│   └── aml_alerts.py             ← defines all three alert models
│
├── views/
│   ├── transaction_screening_rule.xml  ← views for screening rules
│   ├── transaction.xml                 ← adds "Blocked" field to transaction views
│   ├── aml_config.xml                  ← AML config form + customer profile tree
│   ├── aml_alerts.xml                  ← tree/form views for all 3 alert types
│   │                                     + AML Detection tab on transaction form
│   └── menuitems.xml                   ← Blocked/On-Hold menu item
│
├── data/
│   └── aml_sequences.xml         ← IR sequences for alert refs + weekly cron
│
├── security/
│   └── ir.model.access.csv       ← access rights for all models
│
├── controllers/
│   └── controllers.py
│
└── demo/
    └── demo.xml
```

---

## 4. How Odoo Inheritance Works Here

Odoo has a concept called **model inheritance**. Instead of copying the base transaction model and modifying it, this module declares a class that says "I am extending an existing model". Odoo then merges the new class into the same database table.

### Extending res.customer.transaction

```python
class Transaction(models.Model):
    _inherit = 'res.customer.transaction'
```

The `_inherit = 'res.customer.transaction'` line means:
- No new database table is created for `Transaction`
- All new fields (`blocked`, `aml_risk_score`, etc.) are added as new **columns** to the existing `res_customer_transaction` table
- The `action_screen()` method defined here **replaces** the base version, but calls it first via `super().action_screen()`
- Every place in Odoo that uses `res.customer.transaction` now automatically has the new fields

### Extending action_screen()

The base `action_screen()` in `compliance_management` runs rules and creates screening history records. The override in this module:

```python
def action_screen(self):
    result = super().action_screen()   # ← runs base rule screening first
    ...check for blocked rules...
    self._run_aml_detection()           # ← then runs AML detection
    return result
```

The call to `super()` ensures base functionality is never lost. AML detection always runs after rule screening, never instead of it. Both results are independent: a transaction can be blocked by a rule AND also have a high AML risk score from velocity/structuring.

### Alert Models — A Different Kind of Inheritance

The three alert models (`res.aml.velocity.alert`, etc.) use a different inheritance pattern:

```python
_inherit = ['mail.thread', 'mail.activity.mixin']
```

This is **mixin inheritance** — it adds chatter (message log), follower management, and scheduled activities to the alert models. This is not related to extending a business model; it just gives the alert records their chat box and activity features.

---

## 5. Models Reference

### 5.1 res.customer.transaction (extended)

**File:** `models/transaction.py`  
**Base defined in:** `compliance_management`  
**Table:** `res_customer_transaction` (new columns added to existing table)

#### Added Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `blocked` | Boolean | False | True when a screening rule with `blocked=True` matched this transaction. Puts the transaction on hold. Tracked (audit log). Indexed. |
| `aml_risk_score` | Float(5,2) | 0.0 | Composite AML risk score from 0 to 100. Computed after every `action_screen()` call. Read-only to users. |
| `aml_flags` | Char | (empty) | Comma-separated string of which AML checks fired. Possible values: `VELOCITY`, `STRUCTURING`, `ANOMALY`. Example: `"VELOCITY,STRUCTURING"`. Read-only to users. |
| `aml_velocity_alert_ids` | One2many → res.aml.velocity.alert | — | All velocity alerts linked to this transaction. Read-only. |
| `aml_structuring_alert_ids` | One2many → res.aml.structuring.alert | — | All structuring alerts linked to this transaction. Read-only. |
| `aml_anomaly_alert_ids` | One2many → res.aml.anomaly.alert | — | All anomaly alerts linked to this transaction. Read-only. |

#### Methods

**`action_view_blocked_transactions()`** — Server action called from the "Blocked / On-Hold" menu. Returns a window action filtered to `blocked = True`. Compliance officers and CCOs see all branches; regular users only see their assigned branches.

**`action_unblock()`** — Sets `blocked = False` on the transaction. Only callable by Compliance Officers (enforced at the menu/button level via `groups=`).

**`action_screen()`** — The main screening method. Calls the base implementation, checks for blocking rules, then calls `_run_aml_detection()`. Returns whatever the base method returned.

**`_run_aml_detection()`** — Orchestrator for all AML checks. Skips if no `customer_id`. Loads active config, runs all three checks, assembles flags list and composite score, writes them to the transaction record. Returns the composite score.

**`_check_velocity(config)`** — Velocity check implementation. Returns a risk score (0–100), or 0 if no breach.

**`_check_structuring(config)`** — Structuring/smurfing check implementation. Returns a risk score (0–100), or 0 if no breach.

**`_check_anomaly(config)`** — Z-score anomaly check implementation. Always updates the customer profile (Welford's algorithm) regardless of whether it flags. Returns a risk score (0–100), or 0 if no breach.

---

### 5.2 res.aml.config

**File:** `models/aml_config.py`  
**Table:** `res_aml_config`  
**Description:** Stores all configurable parameters for AML detection. One active record drives the entire detection system.

#### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | Char | 'AML Configuration' | Label for this config record |
| `active` | Boolean | True | Only active records are considered by `get_active_config()`. Archiving a record deactivates it. |
| `currency_id` | Many2one → res.currency | Company currency | Currency used for all monetary thresholds |
| `ctr_threshold` | Monetary | 5,000,000 | **The CTR threshold.** Any transaction at or above this amount is a direct CTR event. Sub-threshold transactions that collectively approach this are flagged as structuring. The NFIU default for Nigeria is ₦5,000,000 for individuals. |
| `velocity_window_hours` | Integer | 24 | How many hours back to look when counting transactions for velocity check |
| `velocity_max_count` | Integer | 10 | Maximum number of transactions allowed within the velocity window before flagging |
| `velocity_max_amount` | Monetary | 10,000,000 | Maximum total transaction value allowed within the velocity window |
| `structuring_window_hours` | Integer | 72 | How many hours back to look for structuring patterns (3 days default) |
| `structuring_min_count` | Integer | 3 | Minimum number of sub-CTR-threshold transactions required before even checking for structuring |
| `structuring_approach_pct` | Float | 80.0 | The sub-threshold transactions must sum to at least this % of the CTR threshold to trigger a structuring alert. Default 80% means: if 3+ transactions total ≥ ₦4,000,000 (80% of ₦5M), flag it. |
| `anomaly_zscore_threshold` | Float | 3.0 | How many standard deviations from the customer's mean a transaction must be before it is flagged as anomalous. 3.0 is the conventional threshold (0.3% of normal transactions would naturally exceed it). |
| `anomaly_min_history` | Integer | 10 | The customer needs at least this many past transactions before anomaly detection activates. Prevents false positives for new customers with no history. |
| `velocity_risk_weight` | Float | 0.30 | Weight of velocity score in the composite AML risk score |
| `structuring_risk_weight` | Float | 0.40 | Weight of structuring score in the composite AML risk score |
| `anomaly_risk_weight` | Float | 0.30 | Weight of anomaly score in the composite AML risk score |

#### Constraints

- `_check_weights`: Raises a `ValidationError` if `velocity_risk_weight + structuring_risk_weight + anomaly_risk_weight` does not equal 1.0 (tolerance: 0.001).
- `_check_ctr_threshold`: Raises a `ValidationError` if the CTR threshold is zero or negative.

#### Class Method: `get_active_config()`

Called by `_run_aml_detection()` at screening time. Searches for the most recently written active config. If none exists, creates one with all defaults. This guarantees detection never fails because someone forgot to configure it.

---

### 5.3 res.aml.customer.profile

**File:** `models/customer_profile.py`  
**Table:** `res_aml_customer_profile`  
**Description:** Stores a statistical behavioral baseline for each customer. One record per customer (enforced by a UNIQUE constraint on `customer_id`). Used by anomaly detection to measure how unusual a transaction is.

#### Fields

| Field | Type | Description |
|---|---|---|
| `customer_id` | Many2one → res.partner | The customer this profile belongs to. Indexed. Cascade delete: if the partner is deleted, the profile goes too. |
| `transaction_count` | Integer | Total number of transactions this profile has been trained on |
| `mean_amount` | Float(20,4) | Running arithmetic mean (average) transaction amount for this customer |
| `m2` | Float(20,6) | **Welford's M2 accumulator** — the running sum of squared deviations from the mean. This is an internal number, not meaningful on its own. It is used to compute the standard deviation without having to store all historical amounts. |
| `stddev_amount` | Float(20,4) | Computed from `m2` and `transaction_count`. This is the sample standard deviation: `sqrt(m2 / (n - 1))`. Stored in the database (not recomputed on every read). |
| `last_updated` | Datetime | When this profile was last updated, either by a live transaction or the weekly cron |

#### SQL Constraint

```sql
UNIQUE(customer_id)
```

There can only be one profile per customer. Attempting to create a second profile for the same customer raises a database error.

#### Methods

**`_compute_stddev()`** — Computes sample standard deviation from `m2` and `transaction_count` using the formula `sqrt(m2 / (n-1))`. Returns 0 if fewer than 2 transactions.

**`update_with_transaction(amount)`** — Called every time a transaction passes through anomaly detection. Updates `mean_amount`, `m2`, and `transaction_count` using Welford's online algorithm (see Section 8 for details). This is what keeps the profile up to date without needing to re-read all historical transactions.

**`compute_zscore(amount)`** — Returns `(amount - mean) / stddev`. Returns `None` if fewer than 2 transactions or standard deviation is zero (all transactions were the same amount).

**`get_or_create_profile(customer_id)`** — Class-level lookup. Finds the profile for a customer, or creates a new blank one if it does not exist yet.

**`_cron_rebuild_profiles()`** — Weekly maintenance job. Recalculates all profiles from scratch using a single SQL query across the entire `res_customer_transaction` table. This corrects any drift that may have accumulated from incremental updates and catches any transactions that were processed without going through `action_screen()`.

---

### 5.4 res.aml.velocity.alert

**File:** `models/aml_alerts.py`  
**Table:** `res_aml_velocity_alert`  
**Inherits:** `mail.thread`, `mail.activity.mixin` (chatter + activities)  
**Description:** Created when a customer exceeds transaction count or volume limits within the velocity window.

#### Fields

| Field | Type | Description |
|---|---|---|
| `name` | Char | Auto-generated reference number. Format: `VAL/2025/00001`. Read-only after creation. |
| `transaction_id` | Many2one → res.customer.transaction | The specific transaction that triggered this alert. Cascade delete. Indexed. |
| `customer_id` | Many2one → res.partner | The customer involved. Indexed. |
| `window_hours` | Integer | The velocity window (hours) that was configured when this alert was raised — recorded at detection time so it stays accurate even if config changes later |
| `txn_count` | Integer | How many transactions were counted within the window (including the triggering transaction) |
| `total_amount` | Float(20,2) | Total value of all transactions counted in the window |
| `risk_score` | Float(5,2) | Velocity risk score (0–100) at time of detection |
| `state` | Selection | Lifecycle state (see alert states below) |
| `created_at` | Datetime | When this alert was raised. Auto-set, read-only. |
| `notes` | Text | Free-text notes field for the reviewing officer |

---

### 5.5 res.aml.structuring.alert

**File:** `models/aml_alerts.py`  
**Table:** `res_aml_structuring_alert`  
**Inherits:** `mail.thread`, `mail.activity.mixin`  
**Description:** Created when multiple sub-CTR-threshold transactions by the same customer collectively approach the CTR threshold within the structuring window.

#### Fields

| Field | Type | Description |
|---|---|---|
| `name` | Char | Auto-generated. Format: `SAL/2025/00001` |
| `transaction_id` | Many2one → res.customer.transaction | The transaction that triggered the threshold breach (the last one that pushed the total over the approach limit) |
| `customer_id` | Many2one → res.partner | The customer involved |
| `window_hours` | Integer | The structuring window in hours at time of detection |
| `txn_count` | Integer | Number of sub-threshold transactions in the window |
| `total_amount` | Float(20,2) | Sum of all sub-threshold transactions in the window |
| `ctr_threshold` | Float(20,2) | The CTR threshold value that was active at detection time — snapshot preserved so it is accurate even if config is later changed |
| `risk_score` | Float(5,2) | Structuring risk score (0–100) |
| `state` | Selection | Alert lifecycle state |
| `created_at` | Datetime | Detection timestamp |
| `notes` | Text | Reviewer notes |

---

### 5.6 res.aml.anomaly.alert

**File:** `models/aml_alerts.py`  
**Table:** `res_aml_anomaly_alert`  
**Inherits:** `mail.thread`, `mail.activity.mixin`  
**Description:** Created when a transaction's amount is statistically unusual compared to that customer's historical transaction amounts, measured by Z-score.

#### Fields

| Field | Type | Description |
|---|---|---|
| `name` | Char | Auto-generated. Format: `AAL/2025/00001` |
| `transaction_id` | Many2one → res.customer.transaction | The unusual transaction |
| `customer_id` | Many2one → res.partner | The customer involved |
| `transaction_amount` | Float(20,2) | The actual amount of this transaction |
| `customer_mean` | Float(20,2) | The customer's mean transaction amount at time of detection |
| `customer_stddev` | Float(20,2) | The customer's standard deviation at time of detection |
| `zscore` | Float(10,4) | The Z-score: how many standard deviations this transaction is from the customer's mean. Can be negative (unusually small transaction) or positive (unusually large). |
| `risk_score` | Float(5,2) | Anomaly risk score (0–100) |
| `state` | Selection | Alert lifecycle state |
| `created_at` | Datetime | Detection timestamp |
| `notes` | Text | Reviewer notes |

#### Alert States (All Three Models)

| State value | Label | Meaning |
|---|---|---|
| `open` | Open | Newly raised. Needs review. |
| `reviewed` | Under Review | A compliance officer has picked it up and is investigating |
| `escalated` | Escalated | Raised to senior compliance or CCO for decision |
| `closed` | Closed | Reviewed and resolved (either confirmed suspicious and acted upon, or ruled out) |
| `false_positive` | False Positive | Confirmed as not suspicious. Remains in history for audit trail. |

State changes are tracked (audit log in chatter) because all alert models inherit `mail.thread`.

---

## 6. Detection Algorithms Explained

### 6.1 Velocity Detection

**What it looks for:** A customer moving an unusually high number of transactions, or an unusually large total amount, in a short time window.

**How it works:**

1. Calculate the start of the rolling window: `now - velocity_window_hours`
2. Find all non-cancelled/rejected transactions by this customer within that window, excluding the current transaction
3. Add 1 to the count (for the current transaction) and add the current transaction's amount to the total
4. If `count > velocity_max_count` OR `total > velocity_max_amount`, create a velocity alert

**Risk score formula:**
```
count_ratio  = count / velocity_max_count
amount_ratio = total / velocity_max_amount
risk_score   = min(100, max(count_ratio, amount_ratio) * 100)
```

The score takes the worst of the two ratios. If a customer makes 20 transactions when the limit is 10, that's a ratio of 2.0, giving a score of 100 (capped). If a customer makes 12 transactions when the limit is 10, that's 120 — also capped at 100.

**Example with defaults (window=24h, max_count=10, max_amount=₦10M):**

A customer makes 11 transactions in 20 hours, totalling ₦3M.
- Count exceeds limit: 11 > 10 → `count_ratio = 11/10 = 1.1`
- Amount does not exceed limit: ₦3M < ₦10M → `amount_ratio = 0.3`
- Score: `min(100, max(1.1, 0.3) * 100) = min(100, 110) = 100`
- Result: Velocity alert raised with score 100, flag `VELOCITY` added to transaction

---

### 6.2 Structuring / Smurfing Detection

**What it looks for:** A customer deliberately breaking up what should be one large transaction into multiple smaller ones, each staying just below the CTR threshold, to avoid regulatory reporting obligations.

**How it works:**

1. If the current transaction is already at or above the CTR threshold, skip — that is a direct CTR event, not structuring.
2. Calculate the structuring window start: `now - structuring_window_hours`
3. Find all sub-threshold transactions (amount < CTR threshold) by this customer in that window, excluding the current transaction
4. Add 1 to the count and the current amount to the total
5. Calculate the approach limit: `CTR_threshold × (structuring_approach_pct / 100)`
6. If `count >= structuring_min_count` AND `total >= approach_limit`, create a structuring alert

**Risk score formula:**
```
risk_score = min(100, (total / ctr_threshold) * 70 + (count / structuring_min_count) * 30)
```

70% of the score comes from how close the total is to the CTR threshold. 30% comes from how many transactions are involved relative to the minimum required.

**Example with defaults (window=72h, min_count=3, approach=80%, CTR=₦5M):**

A customer makes 4 transactions in 60 hours: ₦1.2M, ₦1.5M, ₦1.3M, ₦1.1M. Total = ₦5.1M.
- All are below ₦5M threshold ✓
- Count: 4 ≥ 3 (structuring_min_count) ✓
- Approach limit: ₦5M × 80% = ₦4M
- Total ₦5.1M ≥ ₦4M ✓
- Score: `min(100, (5.1/5.0)*70 + (4/3)*30) = min(100, 71.4 + 40) = min(100, 111.4) = 100`
- Result: Structuring alert raised with score 100

---

### 6.3 Anomaly Detection (Z-Score)

**What it looks for:** A single transaction that is statistically out of character for this specific customer — not compared to a general population, but compared to that customer's own history.

**What a Z-score means:** If a customer's average transaction is ₦50,000 with a standard deviation of ₦10,000, and they suddenly send ₦200,000, the Z-score is `(200,000 - 50,000) / 10,000 = 15.0`. That is 15 standard deviations above their mean — extremely unusual.

**How it works:**

1. Load or create a behavioral profile for this customer
2. If the customer has fewer than `anomaly_min_history` transactions in their profile, update the profile (add this transaction to their baseline) and skip anomaly detection — not enough history to judge
3. Compute the Z-score: `(transaction_amount - customer_mean) / customer_stddev`
4. If `abs(zscore) >= anomaly_zscore_threshold`, create an anomaly alert
5. Regardless of whether an alert is raised, update the profile with this transaction's amount (Welford update)

**Risk score formula:**
```
risk_score = min(100, (abs(zscore) / anomaly_zscore_threshold) * 50)
```

At exactly the threshold (zscore = 3.0), the score is `(3.0/3.0)*50 = 50`. At twice the threshold (zscore = 6.0), the score is `(6.0/3.0)*50 = 100`. The 50 cap on the base means the score can reach 100 but grows gradually.

**Note on direction:** The check uses `abs(zscore)`, so it catches both directions — an unusually large transaction AND an unusually small transaction (which could indicate testing of a mule account or unusual withdrawal pattern).

---

### 6.4 Composite AML Risk Score

After all three checks run, the composite score is computed:

```python
composite = round(
    v_score * config.velocity_risk_weight +
    s_score * config.structuring_risk_weight +
    a_score * config.anomaly_risk_weight,
    2
)
```

With default weights (velocity=0.30, structuring=0.40, anomaly=0.30):

```
composite = (v_score × 0.30) + (s_score × 0.40) + (a_score × 0.30)
```

If only structuring fires with score 80:
```
composite = (0 × 0.30) + (80 × 0.40) + (0 × 0.30) = 32.0
```

If all three fire at 100:
```
composite = (100 × 0.30) + (100 × 0.40) + (100 × 0.30) = 100.0
```

The weights must always sum to 1.0. Odoo will reject a config save if they do not. Weights can be adjusted to make, for example, structuring matter more (increase its weight) at the cost of reducing another weight.

The `aml_risk_score` field on the transaction is updated every time `action_screen()` is called. It does not accumulate — each screening run overwrites the previous value.

---

## 7. Execution Flow: What Happens When a Transaction is Screened

When someone calls `transaction.action_screen()` (from the UI or from another module), this is the exact sequence of events:

```
action_screen() called
│
├─ 1. super().action_screen()
│       [compliance_management base method runs]
│       ├─ Load all active screening rules, ordered by priority
│       ├─ For each rule:
│       │    ├─ Evaluate SQL condition OR Python expression
│       │    ├─ If match: create res.transaction.screening.history record
│       │    └─ If rule.transaction_flag == 'suspicious': mark transaction suspicious
│       └─ Return result
│
├─ 2. Check screening history for blocking rules
│       ├─ Load all history records for this transaction
│       └─ If any history record's rule has rule.blocked == True:
│            └─ Set transaction.blocked = True
│
└─ 3. _run_aml_detection()
        ├─ Skip if no customer_id on transaction
        ├─ Load active AML config (or create default)
        │
        ├─ _check_velocity(config)
        │    ├─ Query: all transactions by this customer in last N hours
        │    ├─ If count > limit OR total > limit:
        │    │    └─ Create res.aml.velocity.alert
        │    └─ Return risk score (or 0)
        │
        ├─ _check_structuring(config)
        │    ├─ Skip if this transaction >= CTR threshold
        │    ├─ Query: all sub-threshold txns by this customer in last N hours
        │    ├─ If count >= min AND total >= approach_limit:
        │    │    └─ Create res.aml.structuring.alert
        │    └─ Return risk score (or 0)
        │
        ├─ _check_anomaly(config)
        │    ├─ Load/create customer behavioral profile
        │    ├─ If insufficient history:
        │    │    ├─ Update profile (Welford)
        │    │    └─ Return 0
        │    ├─ Compute Z-score
        │    ├─ If |Z-score| >= threshold:
        │    │    └─ Create res.aml.anomaly.alert
        │    ├─ Update profile (Welford) ← always runs
        │    └─ Return risk score (or 0)
        │
        └─ Write aml_risk_score and aml_flags to transaction
```

**Important: The two layers are independent.** A transaction can be:
- Blocked by a rule but have zero AML flags (straightforward rule breach)
- Not blocked by any rule but have a high AML risk score (statistical anomaly)
- Both blocked AND have AML flags
- Neither blocked nor flagged (clean transaction)

---

## 8. Customer Behavioral Profiles — How Baselines Are Built

### Welford's Online Algorithm

The anomaly detection system needs to know each customer's "normal" transaction amount. The naive approach would be to store all transaction amounts and compute the average and standard deviation every time. For 7 million customers this is impractical.

Instead, the module uses **Welford's online algorithm**, which updates the mean and variance incrementally with each new data point, storing only three numbers: count, mean, and M2 (variance accumulator). No history is needed.

Each time `_check_anomaly()` is called, this runs at the end regardless of whether an alert was raised:

```python
def update_with_transaction(self, amount):
    n = self.transaction_count + 1
    delta = amount - self.mean_amount         # difference from OLD mean
    new_mean = self.mean_amount + delta / n   # update mean
    delta2 = amount - new_mean                # difference from NEW mean
    self.write({
        'transaction_count': n,
        'mean_amount': new_mean,
        'm2': self.m2 + delta * delta2,       # update variance accumulator
    })
```

The standard deviation is then: `sqrt(m2 / (n - 1))` — this is the **sample** standard deviation (divides by n-1, not n), which is the statistically correct formula for inferring population variance from a sample.

### Why This Matters for 7 Million Customers

Each profile stores exactly 4 numbers (count, mean, m2, last_updated). Updating a profile is a single `UPDATE` statement on one row. This scales to 7 million customers without any performance degradation. There are no aggregation queries at screening time.

### Weekly Full Rebuild

Once per week, the cron job `_cron_rebuild_profiles()` recalculates all profiles from the entire transaction table using a single SQL query:

```sql
SELECT
    t.customer_id,
    COUNT(*)                               AS cnt,
    AVG(t.amount)                          AS mean,
    COALESCE(VAR_POP(t.amount) * COUNT(*), 0) AS m2_val
FROM res_customer_transaction t
WHERE t.customer_id IS NOT NULL
  AND t.state NOT IN ('cancelled', 'rejected')
GROUP BY t.customer_id
```

`VAR_POP(amount) * COUNT(*)` gives the M2 accumulator value (`sum of squared deviations from the mean`), which is exactly what Welford's algorithm stores. This ensures the profiles stay accurate even for transactions that were imported or processed outside of `action_screen()`.

---

## 9. Sequences (Alert Reference Numbers)

Three IR sequences are defined in `data/aml_sequences.xml`. They are created with `noupdate="1"`, which means they are only created on first install and are never overwritten by module updates.

| Sequence | Code | Format | Example |
|---|---|---|---|
| AML Velocity Alert | `res.aml.velocity.alert` | `VAL/YYYY/NNNNN` | `VAL/2025/00001` |
| AML Structuring Alert | `res.aml.structuring.alert` | `SAL/YYYY/NNNNN` | `SAL/2025/00042` |
| AML Anomaly Alert | `res.aml.anomaly.alert` | `AAL/YYYY/NNNNN` | `AAL/2025/00007` |

The year resets every January 1. The 5-digit padding restarts from 00001 each year. The `company_id` is set to `False` (global), meaning sequences are shared across all companies in a multi-company setup.

When an alert is created with `name = 'New'` (the default), the `create()` override calls `ir.sequence.next_by_code()` to assign the real reference. If for any reason the sequence does not exist, it falls back to `'VAL/NEW'`, `'SAL/NEW'`, or `'AAL/NEW'` so creation does not fail.

---

## 10. Cron Jobs

### AML: Rebuild Customer Behavioral Profiles

| Setting | Value |
|---|---|
| Technical name | `cron_rebuild_aml_profiles` |
| Model | `res.aml.customer.profile` |
| Method called | `_cron_rebuild_profiles()` |
| Frequency | Every 1 week |
| Repeats | Indefinitely (`numbercall = -1`) |
| Active | Yes (enabled by default on install) |

**Purpose:** Rebuilds all customer behavioral profiles from the complete transaction history. This is a safety net for accuracy — the live Welford updates keep profiles current, but this full rebuild catches any gaps or corrections.

**Performance note:** For 7 million customers, this query will aggregate a very large table. It should be scheduled during off-peak hours. You can change the schedule in: Settings → Technical → Scheduled Actions → "AML: Rebuild Customer Behavioral Profiles".

---

## 11. Access Groups and Permissions

The module uses access groups defined in `compliance_management`. There are three relevant groups:

| Group ID | Display Name | Role |
|---|---|---|
| `compliance_management.group_compliance_chief_compliance_officer` | Chief Compliance Officer (CCO) | Full authority over all compliance data |
| `compliance_management.group_compliance_compliance_officer` | Compliance Officer (CO) | Operational compliance work |
| `compliance_management.group_compliance_transaction_monitoring_team` | Transaction Monitoring Team (TMT) | Monitors transactions for suspicious activity |

### Model-Level Access (ir.model.access.csv)

The table below shows what each group can do with each model. Column headers: **R**=Read, **W**=Write, **C**=Create, **D**=Delete.

| Model | CCO | CO | TMT |
|---|---|---|---|
| `res.aml.config` | R+W+C+D | R+W only | — (no access) |
| `res.aml.customer.profile` | R+W+C+D | R+W+C only | R+W+C only |
| `res.aml.velocity.alert` | R+W+C+D | R+W+C only | R+W+C only |
| `res.aml.structuring.alert` | R+W+C+D | R+W+C only | R+W+C only |
| `res.aml.anomaly.alert` | R+W+C+D | R+W+C only | R+W+C only |

**Key points:**
- Only the **CCO** can delete AML configuration, customer profiles, or alerts. This preserves the audit trail.
- **CO** can read and update the AML config (e.g. change the CTR threshold) but cannot delete it. They cannot create a new config either — only the CCO can.
- **CO** and **TMT** can create alert notes and change alert states but cannot delete alert records.
- The TMT has no access to the AML Configuration model at all — they cannot see or change detection thresholds.
- Alert records are created programmatically by the detection engine, not manually by users, but the CSV gives CO and TMT create access because the code runs in their security context.

---

## 12. Menu Structure

All menus from this module attach to parent menus defined in `compliance_management`.

### New Menu Items

```
Compliance (root menu)
│
├── Transaction Monitoring                 [parent: compliance_management]
│   ├── Transactions To Review             [compliance_management]
│   ├── Transactions Reviewed              [compliance_management]
│   ├── All Transactions                   [compliance_management]
│   ├── Blocked / On-Hold                  [this module, menuitems.xml]  ← sequence 1
│   ├── AML Velocity Alerts                [this module, aml_alerts.xml] ← sequence 40
│   ├── AML Structuring Alerts             [this module, aml_alerts.xml] ← sequence 41
│   └── AML Anomaly Alerts                 [this module, aml_alerts.xml] ← sequence 42
│
└── Configuration                          [parent: compliance_management]
    ├── ...existing config items...
    ├── AML Configuration                  [this module, aml_config.xml] ← sequence 95
    └── AML Customer Profiles              [this module, aml_config.xml] ← sequence 96
```

### Who Sees What

| Menu Item | Groups that can see it |
|---|---|
| Blocked / On-Hold | Compliance Officer, Branch Compliance Officer, Transaction Monitoring Team |
| AML Velocity Alerts | Compliance Officer, Transaction Monitoring Team |
| AML Structuring Alerts | Compliance Officer, Transaction Monitoring Team |
| AML Anomaly Alerts | Compliance Officer, Transaction Monitoring Team |
| AML Configuration | Chief Compliance Officer, Compliance Officer |
| AML Customer Profiles | Chief Compliance Officer, Compliance Officer |

**The TMT cannot access AML Configuration** — they can see and work the alerts, but they cannot change the detection thresholds that produce those alerts. Only CO and CCO can do that.

---

## 13. Views Reference

### Views in aml_config.xml

| View ID | Type | Model | Description |
|---|---|---|---|
| `view_aml_config_form` | Form | `res.aml.config` | Full form with tabs for CTR threshold, Velocity, Structuring, Anomaly, and Risk Weights |
| `view_aml_config_tree` | Tree | `res.aml.config` | List view showing name, CTR threshold, currency, windows, active status |
| `action_aml_config` | Window Action | `res.aml.config` | Opens tree+form view |
| `view_aml_customer_profile_tree` | Tree | `res.aml.customer.profile` | List of all customer profiles sorted by last update |
| `action_aml_customer_profile` | Window Action | `res.aml.customer.profile` | Opens tree view |

### Views in aml_alerts.xml

| View ID | Type | Model | Description |
|---|---|---|---|
| `view_aml_velocity_alert_tree` | Tree | `res.aml.velocity.alert` | List with color decorations (red=escalated, grey=closed/false_positive) |
| `view_aml_velocity_alert_form` | Form | `res.aml.velocity.alert` | Detail view with status bar, chatter |
| `view_aml_velocity_alert_search` | Search | `res.aml.velocity.alert` | Quick filters for Open / Escalated, grouping by customer or state |
| `action_aml_velocity_alerts` | Window Action | `res.aml.velocity.alert` | Default filter: Open alerts |
| `view_aml_structuring_alert_tree` | Tree | `res.aml.structuring.alert` | Same decoration pattern, includes CTR threshold column |
| `view_aml_structuring_alert_form` | Form | `res.aml.structuring.alert` | Shows all structuring detection data + chatter |
| `view_aml_structuring_alert_search` | Search | `res.aml.structuring.alert` | Filters and grouping |
| `action_aml_structuring_alerts` | Window Action | `res.aml.structuring.alert` | Default filter: Open alerts |
| `view_aml_anomaly_alert_tree` | Tree | `res.aml.anomaly.alert` | Shows amount, mean, Z-score |
| `view_aml_anomaly_alert_form` | Form | `res.aml.anomaly.alert` | Shows full statistical context + chatter |
| `view_aml_anomaly_alert_search` | Search | `res.aml.anomaly.alert` | Filters and grouping |
| `action_aml_anomaly_alerts` | Window Action | `res.aml.anomaly.alert` | Default filter: Open alerts |
| `transaction_form_aml` | Form (inherit) | `res.customer.transaction` | Adds "AML Detection" tab to the transaction form with risk score, flags, and three alert sub-tables |

### Transaction Form — AML Detection Tab

This view inherits from `compliance_management.compliance_transaction_form` with priority 25 (higher than the base form's default, lower than others that may exist). It injects a new notebook page called "AML Detection" containing:
- AML Risk Score
- AML Flags
- A sub-notebook with three pages: Velocity Alerts, Structuring Alerts, Anomaly Alerts — each showing all alerts linked to this transaction as read-only list

---

## 14. How to Configure the CTR Threshold

The CTR threshold is controlled entirely through the UI. No code change is needed.

**Path:** Compliance → Configuration → AML Configuration

1. Open the active AML Configuration record (or create one if none exists)
2. The **CTR Threshold** field is on the main form, in the "CTR Threshold" group
3. Change the value to whatever NFIU has mandated
4. Save — the new threshold takes effect on the very next transaction screening

**Current default:** ₦5,000,000 (based on NFIU guidance for individual cash transactions in Nigeria)

**What the CTR threshold affects:**
- Structuring detection: only transactions **below** this amount are checked for structuring. If a transaction is at or above the threshold, it triggers a CTR filing directly and structuring detection is skipped.
- Structuring approach limit: the total of sub-threshold transactions is compared against `CTR_threshold × structuring_approach_pct%`

**What the CTR threshold does NOT affect:**
- Velocity detection: velocity uses its own separate `velocity_max_amount` limit
- Anomaly detection: anomaly is based on the customer's own history, not any absolute threshold

---

## 15. Database Tables Created

When this module is installed, Odoo creates the following new tables:

| Table | Model | Description |
|---|---|---|
| `res_aml_config` | `res.aml.config` | AML detection configuration parameters |
| `res_aml_customer_profile` | `res.aml.customer.profile` | One row per customer, behavioral baseline stats |
| `res_aml_velocity_alert` | `res.aml.velocity.alert` | Velocity alert records |
| `res_aml_structuring_alert` | `res.aml.structuring.alert` | Structuring/smurfing alert records |
| `res_aml_anomaly_alert` | `res.aml.anomaly.alert` | Statistical anomaly alert records |

Additionally, the following **new columns** are added to the existing `res_customer_transaction` table:

| Column | Type | Default |
|---|---|---|
| `blocked` | boolean | false |
| `aml_risk_score` | double precision | 0.0 |
| `aml_flags` | varchar | NULL |

The One2many fields (`aml_velocity_alert_ids`, etc.) are not columns — they are virtual relations that Odoo resolves by looking at the foreign key (`transaction_id`) on the alert tables.

---

## 16. AML Detection Default Values and Their Meaning

This table summarises every configurable default and the real-world reasoning behind it:

| Parameter | Default | Why |
|---|---|---|
| CTR Threshold | ₦5,000,000 | NFIU Cash Transaction Report threshold for individual accounts in Nigeria |
| Velocity Window | 24 hours | One business day. Catches rapid-fire transactions within a single day. |
| Max Count in Window | 10 transactions | 10 transactions in one day is unusual for most retail customers. Adjust upward for business/corporate accounts if needed. |
| Max Amount in Window | ₦10,000,000 | 2× the CTR threshold. Any customer moving double the CTR amount in a day via multiple transactions warrants review. |
| Structuring Window | 72 hours | 3 days. Structuring is typically executed over a few days to appear natural. |
| Min Sub-threshold Txns | 3 | At least 3 transactions below the threshold are needed before the pattern is considered intentional. 1 or 2 could easily be coincidence. |
| Structuring Approach % | 80% | Transactions summing to ≥ ₦4M (80% of ₦5M) in 3 days, all individually below ₦5M, is suspicious. Lower this if you want to catch smaller patterns. |
| Anomaly Z-score | 3.0 | The conventional statistical threshold. At 3.0 standard deviations, only about 0.3% of naturally-occurring transactions would be flagged as false positives. |
| Anomaly Min History | 10 transactions | A customer needs at least 10 past transactions to establish a reliable baseline. Below that, there is not enough data to judge what is "normal" for them. |
| Velocity Weight | 0.30 | 30% of composite score |
| Structuring Weight | 0.40 | 40% of composite score — structuring is weighted highest as it is the most deliberate AML typology |
| Anomaly Weight | 0.30 | 30% of composite score |
