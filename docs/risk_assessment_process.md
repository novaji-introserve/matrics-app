# Risk Assessment Process — iComply

## Overview

The risk assessment system evaluates every customer's risk level based on their
profile (account type, industry, delivery channel, jurisdiction). The result is
a `risk_score` (float) and `risk_level` (low / medium / high) stored directly
on the customer (`res.partner`).

---

## Data Model Chain

```
res.risk.assessment.line        ← individual risk criteria per assessment
        ↑ (line_ids)
res.risk.assessment             ← risk rating for a specific subject
        ↑ (risk_assessment)
res.compliance.risk.assessment.plan  ← SQL query that matches customers
        ↓ (Go service runs SQL per customer)
res_partner_risk_plan_line      ← mapping: customer ↔ matched plan + score
        ↓ (Go aggregates)
res.partner.risk_score          ← final score on customer
res.partner.risk_level          ← low / medium / high
```

---

## Step 1 — Risk Assessment Lines (`res.risk.assessment.line`)

The lowest level. Each line represents one risk category under an assessment.

| Field | Description |
|---|---|
| `name` | Risk category e.g. Money Laundering, Terrorism Financing |
| `inherent_risk_score` | Raw risk before any controls (e.g. 9) |
| `control_effectiveness_score` | How effective existing controls are (e.g. 8) |
| `residual_risk_score` | Risk after controls applied (e.g. 6) |
| `residual_risk_impact` | Financial / reputational impact (e.g. 24.00) |
| `residual_risk_probability` | Likelihood % (e.g. 75) |
| `category_id` | AML / CTF / PF etc. |
| `existing_controls` | Current controls in place |
| `planned_mitigation` | Future mitigation actions |
| `implication` | Consequence if risk materialises |

**Example — Sterling Alumni Account (CUR012):**
```
Money Laundering         → inherent=9, control=8, residual=6, impact=24, prob=75%
Terrorism Financing      → inherent=9, control=8, residual=6, impact=22, prob=75%
Proliferation Financing  → inherent=9, control=8, residual=6, impact=18, prob=75%
```

---

## Step 2 — Risk Assessment (`res.risk.assessment`)

Groups the lines together and produces a single `risk_rating`.

| Field | Description |
|---|---|
| `name` | Assessment name e.g. "STERLING ALUMNI ACCOUNT" |
| `risk_rating` | Computed average of line scores |
| `subject_id` | What is being assessed (account product / industry etc.) |
| `universe_id` | Risk universe e.g. Financial Crime |
| `assessment_type_id` | Institutional or Counter Party |
| `type_id` | Risk type classification |
| `line_ids` | One2many → res.risk.assessment.line |
| `partner_id` | Optional direct link to a customer |
| `internal_category` | `inst` (Institutional) or `cp` (Counter Party) |

**Four categories of assessments exist:**
- Account Products (Alumni Account, Savings, Current etc.)
- Delivery Channels (Branch, Mobile, Internet Banking etc.)
- Customer Industries (Agriculture, Manufacturing, Financial Services etc.)
- Jurisdictions (North, South, High-Risk Countries etc.)

**UI:** Compliance → Risk → FCRA Risk Assessment → Institutional / Counter Party

---

## Step 3 — Risk Assessment Plan (`res.compliance.risk.assessment.plan`)

The plan is the **bridge** between an assessment and a customer.
It contains a SQL query that determines which customers the assessment applies to.

| Field | Description |
|---|---|
| `name` | Plan name e.g. "Risk Analysis For STERLING ALUMNI ACCOUNT" |
| `code` | Unique code e.g. `STERLING_ALUMNI_ACCOUNT_CUR012` |
| `sql_query` | SQL returning 1 row if customer matches, empty if not |
| `risk_assessment` | Many2one → res.risk.assessment (the assessment to apply) |
| `risk_assessment_score` | Related field: risk_assessment.risk_rating |
| `compute_score_from` | `risk_assessment` or `risk_score` (fixed value) |
| `use_composite_calculation` | Whether to combine with other plan scores |
| `universe_id` | Risk universe for grouping |
| `priority` | Order of evaluation |
| `state` | `active` or `draft` |

**Example SQL query:**
```sql
SELECT 1
FROM res_partner_account a
INNER JOIN res_partner r ON r.id = a.customer_id
WHERE r.id = %s AND a.category = 'CUR012'
ORDER BY a.opening_date DESC LIMIT 1;
```

`%s` is replaced with the customer's ID at runtime.
- Returns 1 row → customer MATCHES this plan → apply the assessment score
- Returns 0 rows → customer does NOT match → skip this plan

**UI:** Compliance → Configuration → Risk Management → Customer Risk Analysis

---

## Step 4 — Processing Engine

Two engines exist. The Go service is used for production scale.

### A. Go Service (`risk_analysis/`)

Located at `/home/novaji/odoo/icomply_odoo/risk_analysis/`

**Designed for:** millions of customers

**How it works:**

```
1. Start → InitializeCache()
   └── Loads ALL active plans from res_compliance_risk_assessment_plan into memory
   └── Loads all settings (risk thresholds, aggregation method)
   └── Separates: composite plans vs regular plans

2. For each customer (in parallel batches via worker pool):
   └── DELETE FROM res_partner_risk_plan_line WHERE partner_id = customer.id
   └── For each active plan:
       └── Run plan.sql_query with customer.id
       └── If returns row → MATCH
           └── Get risk_rating from linked res.risk.assessment
           └── INSERT INTO res_partner_risk_plan_line (partner_id, plan_id, risk_score)
   └── Aggregate all matched plan scores:
       └── AVG / MAX / SUM (based on settings)
   └── Also check EDD score (approved EDD overrides plan score)
   └── UPDATE res_partner SET risk_score = ?, risk_level = ? WHERE id = customer.id

3. Commit batch → log progress → next batch
```

**Key files:**
| File | Purpose |
|---|---|
| `domain/services/batched_plan_risk_calculator.go` | Main engine |
| `workers/worker_pool.go` | Parallel batch processing |
| `api/handlers/risk_analysis_handler.go` | HTTP API to trigger |
| `infrastructure/repository/customer_repository.go` | DB read/write |
| `infrastructure/cache/` | Redis or file cache for plan data |
| `config/config.go` | DB connection, batch size, worker count |

**Trigger via API:**
```bash
POST http://<go-service-host>:<port>/api/risk-analysis/run
```

### B. Odoo Python Cron (`cron_run_risk_assessment`)

Located at: `compliance_management/models/customer.py:1126`

**Designed for:** ≤ 200 customers only (hardcoded limit)

**Schedule:** Every 6 hours (disabled by default)

**Enable:** Settings → Technical → Scheduled Actions → "Run Risk Assessment" → Active = True

**Limitation:** If customer count > 200, the cron does nothing. Use Go service instead.

---

## Step 5 — Intermediate Table (`res_partner_risk_plan_line`)

Written by the Go service. Shows exactly which plans matched each customer.

| Column | Description |
|---|---|
| `partner_id` | Customer (res.partner) |
| `plan_line_id` | The risk plan that matched |
| `risk_score` | Score from that plan's linked assessment |

**Check mapping:**
```sql
SELECT rp.name, rcrap.name AS plan, rprl.risk_score
FROM res_partner_risk_plan_line rprl
JOIN res_partner rp ON rp.id = rprl.partner_id
JOIN res_compliance_risk_assessment_plan rcrap ON rcrap.id = rprl.plan_line_id
ORDER BY rp.name
LIMIT 20;
```

---

## Step 6 — Final Result on Customer

Written directly to `res_partner` table.

| Column | Values |
|---|---|
| `risk_score` | Float e.g. 7.5 |
| `risk_level` | `low` / `medium` / `high` |
| `composite_risk_score` | Combined score across universes |

**Risk level thresholds** (configurable in system settings):
- 0 – 30 → `low`
- 31 – 60 → `medium`
- 61 – 100 → `high`

**UI location:** Due Diligence → Customers → open any customer
- **Risk Level** badge (green/orange/red) — top right stat button
- **Risk Score** number — top right stat button
- **Run Risk Analysis** button — triggers single customer assessment manually

---

## Step 7 — EDD Override

If a customer has an **approved Enhanced Due Diligence (EDD)** record, the EDD
risk score overrides the plan-calculated score.

```sql
SELECT risk_score
FROM res_partner_edd
WHERE customer_id = %s AND status = 'approved'
ORDER BY date_approved DESC LIMIT 1;
```

Priority order:
1. Approved EDD score (highest priority)
2. Plan-calculated composite score
3. Default score (0)

---

## Debugging Checklist

### 1. Are plans active?
```sql
SELECT COUNT(*) FROM res_compliance_risk_assessment_plan WHERE state = 'active';
```

### 2. Did Go write plan line matches?
```sql
SELECT COUNT(*), COUNT(DISTINCT partner_id) AS customers
FROM res_partner_risk_plan_line;
```

### 3. Did Go update customers?
```sql
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE risk_score > 0) AS has_score,
    COUNT(*) FILTER (WHERE risk_level IS NOT NULL) AS has_level
FROM res_partner
WHERE origin IN ('demo','test','prod');
```

### 4. Check a specific customer's matched plans:
```sql
SELECT rcrap.name AS plan, rprl.risk_score
FROM res_partner_risk_plan_line rprl
JOIN res_compliance_risk_assessment_plan rcrap ON rcrap.id = rprl.plan_line_id
WHERE rprl.partner_id = <customer_id>;
```

### 5. Go service not writing — check:
- Go service DB config points to correct database
- DB user has WRITE permission on `res_partner` and `res_partner_risk_plan_line`
- Go service logs for errors
- At least one plan has `state = 'active'`
- Customers have `origin IN ('demo','test','prod')`

---

## Summary Flow

```
Sterling Demo Data
    ↓ creates
res.risk.assessment (per product/industry/channel/jurisdiction)
    + res.risk.assessment.line (ML, TF, PF scores per assessment)
    ↓ linked via
res.compliance.risk.assessment.plan (SQL query + linked assessment)
    ↓ Go service runs
For every customer:
    SQL query → match? → take risk_rating → write to res_partner_risk_plan_line
    Aggregate all matched plan scores
    Write final risk_score + risk_level → res_partner
    ↓ visible in
Due Diligence → Customers → Customer Form → Risk Score / Risk Level
```
