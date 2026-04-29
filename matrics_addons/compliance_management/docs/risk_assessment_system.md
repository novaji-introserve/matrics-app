# iComply — Risk Assessment System: Complete Reference

---

## 1. Overview

The risk assessment system answers one question: **how risky is this customer?**

It works in layers:

```
Settings (thresholds & scoring rules)
    └── Risk Universe  (broad grouping, e.g. "Delivery Channel")
            └── Risk Subject  (specific entity, e.g. "USSD")
                    └── Risk Assessment Plan  (the rule that scores a customer)
                            └── Risk Assessment  (the actual score record for one customer)
                                    └── Risk Assessment Lines  (individual risk items)
                                            └── Controls  (what reduces the risk)
```

---

## 2. The Building Blocks

### 2.1 Risk Universe (`res.risk.universe`)

The **broadest grouping** of risk. Think of it as a risk domain.

Examples:
- Delivery Channel
- Customer Type
- Geography / Jurisdiction
- Product & Services

Key field: `weight_percentage` — used in composite scoring to decide how much this universe contributes to the final customer risk score.

---

### 2.2 Risk Subject (`res.risk.subject`)

A **specific item within a universe** that can be assessed.

Examples (under Delivery Channel universe):
- USSD
- Mobile Banking
- Internet Banking
- Branch

Each subject belongs to one universe via `universe_id`.

---

### 2.3 Risk Category (`res.risk.category`)

Used on individual **assessment lines** to classify what kind of risk the line represents.

Examples:
- Regulatory Risk
- Operational Risk
- Fraud Risk
- Reputational Risk

---

### 2.4 Risk Type (`res.risk.type`)

Classifies the **overall assessment** at a higher level.

Examples:
- Compliance Risk
- Market Risk
- Credit Risk

---

### 2.5 Risk Assessment Type (`res.risk.assessment.type`)

Describes the **methodology or trigger** for the assessment.

Examples:
- KYC Assessment
- Onboarding Assessment
- Periodic Review
- EDD Assessment

---

### 2.6 Risk Controls (`risk.assessment.control`)

Reusable **mitigation controls** that can be applied to any assessment line to reduce risk.

Key fields:
| Field | What It Means |
|-------|--------------|
| `control_type` | preventive / detective / corrective / directive |
| `control_nature` | manual / automated / semi-automated |
| `effectiveness_score_numeric` | A number (1 to max_score) — the higher, the more effective |
| `effectiveness_score` | Category: Not Effective → Highly Effective |
| `is_fcra_relevant` | Whether this control counts for FCRA compliance |

The `effectiveness_score_numeric` is the value used in all calculations.

---

## 3. The Risk Assessment Plan (`res.compliance.risk.assessment.plan`)

The plan is the **rule engine**. It decides what score to assign a customer and under what conditions.

### 3.1 Plan States

```
draft  →  active  →  inactive
```

Only **active** plans are evaluated.

### 3.2 How a Plan Scores a Customer

The `compute_score_from` field controls this. There are four modes:

| Mode | How It Works |
|------|-------------|
| `static` | Always returns the fixed `risk_score` value |
| `dynamic` | Runs the `sql_query` against the database; returns a number |
| `risk_assessment` | Links to a `res.risk.assessment` record; uses that record's `risk_rating` |
| `python` | Evaluates `condition_python` code; code must set a `result` variable |

### 3.3 The `risk_assessment` Mode (Most Common)

This is the most used mode. You create a **template assessment** with lines and controls configured, then the plan points to it.

```
Plan (USSD_DELIVERY_CHANNEL)
 └── compute_score_from = 'risk_assessment'
 └── risk_assessment → Assessment: "USSD Channel Risk Template"
                            └── risk_rating = 7.5  (computed from lines)
```

When a customer uses the USSD channel, the plan matches and pulls the 7.5 score.

### 3.4 Composite Calculation

When `use_composite_calculation = True`, the plan participates in weighted scoring across universes.

This is covered in section 6 below.

---

## 4. The Risk Assessment (`res.risk.assessment`)

This is the **actual scored record** — either a template used by a plan, or a direct assessment for a specific customer/partner.

### 4.1 Key Fields

| Field | Purpose |
|-------|---------|
| `partner_id` | The customer being assessed (if customer-specific) |
| `subject_id` | What is being assessed (e.g. USSD subject) |
| `universe_id` | Which risk domain this belongs to |
| `type_id` | Risk type classification |
| `assessment_type_id` | KYC / Onboarding / Periodic etc. |
| `risk_rating` | The final aggregated score (auto-computed) |
| `internal_category` | `inst` = Institutional, `cp` = Counterparty |
| `is_default` | Marks this as the default assessment for its subject |
| `line_ids` | The individual risk items (One2many) |

### 4.2 How `risk_rating` Is Computed

`risk_rating` = aggregate of all line `residual_risk_score` values.

The aggregation method is controlled by the setting with code `risk_assessment_computation`:
- **avg** (default) — average of all line scores
- **max** — highest line score
- **sum** — total of all line scores

This is recalculated automatically on every save.

---

## 5. Risk Assessment Lines (`res.risk.assessment.line`)

Each line represents **one individual risk item** within an assessment. The calculation flows like this:

```
inherent_risk_score
        │
        │   minus the effect of controls
        ▼
residual_risk_score   ──→  feeds into assessment's risk_rating
```

### 5.1 Key Fields

| Field | Purpose |
|-------|---------|
| `category_id` | What category of risk this line is |
| `inherent_risk_score` | The raw risk before any controls |
| `existing_controls` | Controls currently in place (Many2many) |
| `control_effectiveness_score` | Sum of all selected controls' effectiveness scores |
| `residual_risk_score` | Final risk after controls (computed) |
| `residual_risk_probability` | Probability expressed as % (computed) |
| `risk_level` | low / medium / high (computed from thresholds) |
| `implication` | Business implications of this risk |
| `planned_mitigation` | Actions planned to further reduce risk |
| `department_id` | Who is responsible for managing this risk |
| `implementation_date` | Timeline for mitigation (Immediate / 7 / 14 / 30 / 60 / 90 days) |

### 5.2 The Scoring Formulas

**Residual Risk Score:**
```
residual_risk_score = inherent_risk_score × (1 − (control_effectiveness_score ÷ maximum_risk_threshold))
```

**Residual Risk Probability:**
```
residual_risk_probability = (1 − (control_effectiveness_score ÷ maximum_risk_threshold)) × 100%
```

**Example:**
- `inherent_risk_score` = 10
- Controls applied with total `effectiveness_score_numeric` = 15
- `maximum_risk_threshold` (from settings) = 25

```
residual_risk_score     = 10 × (1 − (15 ÷ 25))  = 10 × 0.40  = 4.0
residual_risk_probability = (1 − (15 ÷ 25)) × 100 = 40%
```

**Interpretation:** Controls reduced the risk from 10 to 4.0 (60% reduction).

### 5.3 Risk Level Thresholds

The `risk_level` label is determined by comparing `residual_risk_impact` against thresholds in settings:

| Score Range | Level |
|------------|-------|
| < `low_risk_threshold` | Low |
| `low_risk_threshold` to < `medium_risk_threshold` | Medium |
| ≥ `medium_risk_threshold` | High |

---

## 6. Composite Scoring (Cross-Universe Weighted Score)

When a customer needs to be scored across **multiple risk universes**, composite scoring combines them into one final number.

### 6.1 How It Works

Each universe has a `weight_percentage`. Plans linked to a universe contribute a weighted score:

```
weighted_score = plan_risk_score × (universe.weight_percentage ÷ 100)
```

All weighted scores are summed across all matched plans to give the customer's composite risk score.

### 6.2 The Composite Plan Line (`res.partner.composite.plan.line`)

Every time a plan is evaluated against a customer, a composite plan line is created or updated:

| Field | Purpose |
|-------|---------|
| `partner_id` | The customer |
| `plan_id` | Which plan matched |
| `universe_id` | Which universe this plan belongs to |
| `subject_id` | Which subject matched |
| `matched` | Did the plan's condition match? |
| `risk_score` | The raw score from the plan |
| `assessment_id` | The assessment the plan pulled score from |
| `universe_weight_percentage` | Weight from the universe |
| `weighted_score` | `risk_score × (weight_percentage ÷ 100)` |

### 6.3 Example

Three universes, three plans:

| Universe | Weight | Plan Score | Weighted |
|----------|--------|------------|----------|
| Delivery Channel | 30% | 7.5 | 2.25 |
| Customer Type | 40% | 6.0 | 2.40 |
| Geography | 30% | 8.0 | 2.40 |
| **Total** | **100%** | | **7.05** |

Customer's composite risk score = **7.05**

---

## 7. Settings That Drive Everything

These settings in `res.compliance.settings` and `res.fcra.score` control the entire calculation:

| Setting Code | What It Controls |
|-------------|-----------------|
| `risk_assessment_computation` | avg / max / sum — how lines aggregate to assessment score |
| `maximum_risk_threshold` | The ceiling for control effectiveness scores |
| `low_risk_threshold` | Boundary between Low and Medium |
| `medium_risk_threshold` | Boundary between Medium and High |
| FCRA `min_score` / `max_score` | Valid range for a control's effectiveness_score_numeric |

Changing these settings changes every computed score in the system.

---

## 8. Full Data Model Relationships

```
res.risk.universe
├── weight_percentage
└── subjects (via res.risk.subject.universe_id)
        └── res.risk.subject

res.compliance.risk.assessment.plan
├── compute_score_from (static / dynamic / risk_assessment / python)
├── universe_id ──────────────────────> res.risk.universe
├── risk_assessment ───────────────────> res.risk.assessment
└── risk_assessment_score (related readonly)

res.risk.assessment
├── partner_id ────────────────────────> res.partner
├── subject_id ────────────────────────> res.risk.subject
├── universe_id ───────────────────────> res.risk.universe
├── type_id ───────────────────────────> res.risk.type
├── assessment_type_id ────────────────> res.risk.assessment.type
├── risk_rating (computed from lines)
└── line_ids (One2many)
        └── res.risk.assessment.line
                ├── category_id ───────> res.risk.category
                ├── department_id ─────> hr.department
                ├── existing_controls ─> risk.assessment.control (Many2many)
                ├── implication ───────> risk.assessment.implication (Many2many)
                ├── planned_mitigation > risk.assessment.mitigation (Many2many)
                ├── inherent_risk_score
                ├── control_effectiveness_score (sum of control scores)
                └── residual_risk_score (computed)

res.partner.composite.plan.line
├── partner_id ────────────────────────> res.partner
├── plan_id ───────────────────────────> res.compliance.risk.assessment.plan
├── universe_id ───────────────────────> res.risk.universe
├── subject_id ────────────────────────> res.risk.subject
├── assessment_id ─────────────────────> res.risk.assessment
├── risk_score
├── universe_weight_percentage (computed)
└── weighted_score (computed)
```

---

## 9. Step-by-Step: Creating a Full Risk Assessment Setup

### Step 1 — Configure settings
Set `maximum_risk_threshold`, `low_risk_threshold`, `medium_risk_threshold` in Compliance Settings.

### Step 2 — Create reference data
1. Create **Risk Universes** (e.g. Delivery Channel) with weight percentages
2. Create **Risk Subjects** under each universe (e.g. USSD, Mobile)
3. Create **Risk Categories** (e.g. Fraud Risk, Regulatory Risk)
4. Create **Risk Controls** with effectiveness scores
5. Create **Risk Types** and **Assessment Types**

### Step 3 — Create a template Risk Assessment
1. Go to Compliance → Risk → New Assessment
2. Set `subject_id`, `universe_id`, `type_id`
3. Add **lines**: set `inherent_risk_score`, `category_id`, attach `existing_controls`
4. The system auto-calculates `residual_risk_score` per line and `risk_rating` overall

### Step 4 — Create a Plan pointing to that Assessment
1. Go to Configuration → Risk Management → Customer Risk Analysis
2. Create a new plan
3. Set `compute_score_from = risk_assessment`
4. Link to the template assessment created in Step 3
5. Set `use_composite_calculation = True` if using weighted universes
6. Activate the plan

### Step 5 — The plan runs against customers
- When a customer matches the plan's conditions (SQL, Python, or static)
- A `res.partner.composite.plan.line` is created
- The plan's score (from the linked assessment's `risk_rating`) is pulled
- If composite: the `weighted_score` is calculated using universe weight
- All weighted scores are summed → customer's final risk score

---

## 10. Automatic Score Updates

| Trigger | What Happens |
|---------|-------------|
| Assessment line created/updated | `residual_risk_score` recalculated, parent `risk_rating` updated |
| Control effectiveness changed | All lines using that control need manual recalculation |
| Assessment saved | `action_update_risk_score()` fires, aggregates all lines |
| Cron job runs | `cron_update_all_risk_scores()` refreshes ALL assessments in batch |

---

## 11. End-to-End Walkthrough: Creating and Running a Risk Assessment

This section walks through the complete process — from building the assessment template, attaching it to a plan, and running it against a customer.

---

### The 3-Part Process

```
PART 1: Build the Assessment  →  the score template
PART 2: Build the Plan        →  the matching rule
PART 3: Screen the Customer   →  run it
```

---

### PART 1 — Create the Risk Assessment

**Where:** `Compliance → Risk → New`

The assessment is a **score template**. It holds lines and controls and produces a `risk_rating` number. Plans will read this number later. It is NOT tied to any specific customer at this stage.

#### Step 1 — Fill the header

| Field | What to put |
|-------|------------|
| Name | Descriptive name e.g. "USSD Channel Risk" |
| Universe | Which risk domain e.g. "Delivery Channel" |
| Subject | Specific subject e.g. "USSD" |
| Category | `inst` (Institutional) or `cp` (Counterparty) |
| Type | e.g. "Compliance Risk" |
| Is Default | Check this if it is the main assessment for this subject |

Leave `partner_id` **empty** — that field is only for assessments tied directly to one specific customer. A template assessment has no partner.

#### Step 2 — Add Assessment Lines (FCRA Risk Assessment tab)

Each line represents one individual risk item. Click **Add a line**:

| Field | What to put |
|-------|------------|
| Name | Description of the risk e.g. "Fraud via USSD channel" |
| Category | Risk category e.g. "Fraud Risk" |
| Inherent Risk Score | Slider — raw risk before any controls (1 to max) |
| Existing Controls | Pick controls already in place |
| Control Effectiveness Score | Auto-filled from selected controls, or adjust manually |
| Planned Mitigation | Actions to further reduce risk |
| Department | Who owns this risk |
| Implementation Date | Timeline: Immediate / 7 / 14 / 30 / 60 / 90 days |
| Implication | Business impact of this risk |

Once you save the line, `residual_risk_score` calculates automatically:
```
residual = inherent × (1 − controls_effectiveness ÷ max_threshold)
```

**Example with numbers:**
- Inherent Risk Score = 10
- Controls effectiveness total = 15
- Max threshold (from settings) = 25

```
residual_risk_score = 10 × (1 − 15 ÷ 25) = 10 × 0.40 = 4.0
```
Controls reduced the risk from 10 down to 4.0 — a 60% reduction.

#### Step 3 — Compute the Assessment Score

Click the **"Compute Risk Score"** button at the top of the form.

**Why you need to do this manually:**
Each line calculates its own `residual_risk_score` automatically when you save it. But the assessment itself — the parent record — does not roll those numbers up in real time. The button is your way of saying *"I am done adding lines, now produce the final number."*

```
Assessment: "USSD Channel Risk"
  ├── Line 1: Fraud via USSD       → residual = 4.0  (auto-calculated per line)
  ├── Line 2: SIM Swap exposure    → residual = 7.2  (auto-calculated per line)
  └── Line 3: Social engineering  → residual = 4.8  (auto-calculated per line)

  risk_rating = avg(4.0, 7.2, 4.8) = 5.33   ← this is what the button computes
```

The `risk_rating` is the single number the plan will read when a customer matches. Without clicking the button, the plan could be reading `0` or a stale old number — no error is shown, it just silently uses whatever the last computed value was.

**When you must click it:**
- After adding all lines for the first time
- After editing any line (changing inherent score or controls)
- After adding or removing lines on an existing assessment
- Any time you want the plan to reflect updated numbers

---

### PART 2 — Create the Plan

**Where:** `Configuration → Risk Management → Customer Risk Analysis → New`

The plan is the **matching rule**. It answers two questions:
1. Does this customer qualify? (the SQL query)
2. What score do they get if they do? (the linked assessment's `risk_rating`)

#### Step 1 — Fill the header

| Field | What to put |
|-------|------------|
| Name | e.g. "USSD Channel Plan" |
| Code | Unique code e.g. `USSD_DELIVERY` |
| Compute Risk Score From | Select **Related Risk Assessment** |
| Risk Assessment | Pick the assessment created in Part 1 |
| Priority | Lower number = evaluated first when multiple plans exist |
| Use Composite Calculation | Toggle ON if this plan contributes to weighted universe scoring |
| Universe | Required if composite is ON — must match the assessment's universe |

The **Risk Assessment Score** field will auto-display the assessment's current `risk_rating` (read-only, for reference only).

#### Step 2 — Write the SQL Query (Plan SQL Query tab)

This is the condition. It runs for each customer. If it returns a row → plan matches → customer gets the score.

```sql
-- Match customers who use the USSD channel
SELECT 1
FROM customer_channel_subscription
WHERE customer_id = %s
AND channel = 'USSD'
```

Rules for the SQL:
- `%s` is always replaced with the customer's database ID
- Must return **at least one row** to match — the actual value returned does not matter
- For `static` mode: returned value is ignored, `risk_score` slider is used instead
- For `dynamic` mode: the number returned IS the score
- For `risk_assessment` mode (most common): only whether a row exists matters; score comes from the linked assessment

More SQL examples:

```sql
-- Match PEP customers
SELECT 1 FROM res_partner WHERE id = %s AND is_pep = true

-- Match customers without BVN
SELECT 1 FROM res_partner WHERE id = %s AND bvn IS NULL

-- Match customers in a high-risk region
SELECT 1
FROM res_partner rp
JOIN res_partner_region rpr ON rp.region_id = rpr.id
WHERE rp.id = %s AND lower(rpr.name) = 'north east'
```

#### Step 3 — Activate the Plan

Click **"Activate Plan"** in the header statusbar.

Plans in `draft` or `inactive` state are **never evaluated**. Only `active` plans run when a customer is screened.

---

### PART 3 — Screen the Customer

**Where:** `Compliance → KYC → [open any customer]`

#### Step 1 — Open the customer record

Find the customer under KYC and open their form.

#### Step 2 — Click "Run Risk Analysis"

There is a button at the top of the customer form labelled **"Run Risk Analysis"**.

A confirmation dialog appears:
> *"Compute Risk Score: Using your defined Risk Analysis, the Customer risk score and level will be recomputed. This process cannot be reversed. Are you sure to proceed?"*

Click **OK**.

#### Step 3 — What happens internally

```
1. All previous risk plan lines for this customer are deleted
2. All active plans run in priority order
3. For each plan:
     SQL query runs with this customer's ID
     If SQL returns a row → matched = True
       score = plan.risk_assessment.risk_rating
       A plan line record is created (partner, plan, score)
     If SQL returns nothing → no line created
4. If composite plans exist → composite_risk_score calculated separately
     (weighted by universe weight_percentage)
5. Final score = plan_score + composite_score
6. Cap: if score > maximum_risk_threshold → cap at maximum_risk_threshold
7. Priority override:
     IF customer has a direct Risk Assessment (partner_id match) → use that score
     ELSE IF customer has an approved EDD record               → use EDD risk score
     ELSE                                                       → use plan score
8. customer.risk_score and customer.risk_level are updated
```

#### Step 4 — Read the result

After the button runs:
- `risk_score` — the final number on the customer record
- `risk_level` — low / medium / high / extreme (based on threshold settings)

---

### Full Example End to End

**Scenario:** You want all customers using the USSD channel to receive a risk score based on a pre-defined USSD risk profile.

**Step 1 — Create the Assessment**

Name: `USSD Channel Risk`
Universe: `Delivery Channel`
Subject: `USSD`

Lines:

| Line | Inherent | Controls Effectiveness | Max | Residual |
|------|----------|----------------------|-----|---------|
| Fraud via USSD | 10 | 15 | 25 | 4.0 |
| SIM Swap exposure | 9 | 5 | 25 | 7.2 |
| Social engineering | 8 | 10 | 25 | 4.8 |

`risk_rating = avg(4.0, 7.2, 4.8) = 5.33`

**Step 2 — Create the Plan**

Name: `USSD Channel Plan`
Code: `USSD_DELIVERY`
Compute Score From: `Related Risk Assessment`
Risk Assessment: `USSD Channel Risk` (score shows 5.33)
Use Composite: ON
Universe: `Delivery Channel` (weight = 30%)

SQL Query:
```sql
SELECT 1
FROM customer_channel_subscription
WHERE customer_id = %s
AND channel = 'USSD'
```

Activate the plan.

**Step 3 — Screen Customer A (uses USSD)**

Click Run Risk Analysis on Customer A:
```
SQL runs for Customer A → returns 1 row → matched
score = 5.33 (from linked assessment risk_rating)
Plan line created: Customer A | USSD Plan | score = 5.33

composite_risk_score = 5.33 × 30% (universe weight) = 1.60
final risk_score = 5.33 + 1.60 = 6.93
risk_level = medium (based on thresholds)
```

**Step 4 — Screen Customer B (does not use USSD)**

Click Run Risk Analysis on Customer B:
```
SQL runs for Customer B → returns nothing → not matched
No plan line created
risk_score = 0 (or from other matched plans)
```

---

### Common Mistakes

| Mistake | What Happens | Fix |
|---------|-------------|-----|
| Plan left in `draft` state | Plan never evaluates, customer always gets 0 | Click Activate Plan |
| Assessment has no lines | `risk_rating = 0`, plan always gives 0 | Add lines with inherent scores and controls |
| Assessment `risk_rating` not recomputed after editing lines | Plan reads the old stale score | Click "Compute Risk Score" button on assessment |
| SQL query has syntax error | Plan fails silently, score = 0, error logged | Check Odoo logs, fix SQL |
| `%s` missing from SQL | Plan crashes for every customer | Always include `WHERE customer_id = %s` or equivalent |
| Two plans with same universe and composite ON | Scores add up (can exceed max threshold) | System caps at `maximum_risk_threshold` automatically |
| Customer has direct Risk Assessment | Plan scores are ignored entirely | Priority override — direct assessment always wins |

---

## 12. Understanding Assessment Line Fields

---

### Inherent Risk Score

This is the risk **before anything is done about it**. The person creating the assessment judges: if we had zero controls in place, how bad would this be?

It is a subjective but informed score. You are asking yourself: *what is the raw damage potential of this risk?*

Example: Killing/violence in a region scores 9 out of 10 because even with no controls in place, the damage potential is catastrophic. A minor documentation gap might score 2 because even without controls the impact is low.

---

### Existing Controls

These are the measures **already in place today** that are actively working to reduce the risk.

A low effectiveness score (e.g. 1) means the controls exist but are weak — the threat is barely being contained. A high score means the controls are strong and doing their job well.

Examples of controls:
- Fraud detection alerts
- Transaction limits on USSD
- Two-factor authentication
- Manual review process for high-risk accounts

The system uses the total effectiveness of all selected controls to calculate how much the inherent risk has actually been reduced.

---

### Residual Risk Impact

This is exactly what you said — **the leftover risk after all existing controls have done their work**.

```
Residual = Inherent × (1 − controls_effectiveness ÷ max_threshold)
```

**Example — weak controls:**
- Inherent = 9 (violence in a region, high threat)
- Controls effectiveness = 1 (very little in place)
- Max threshold = 25

```
Residual = 9 × (1 − 1 ÷ 25) = 9 × 0.96 = 8.64
```

Controls barely helped. Risk is still 8.64 — almost as dangerous as before.

**Example — strong controls:**
- Inherent = 9
- Controls effectiveness = 20 (strong measures in place)
- Max threshold = 25

```
Residual = 9 × (1 − 20 ÷ 25) = 9 × 0.20 = 1.80
```

Strong controls reduced the risk from 9 down to 1.80 — an 80% reduction.

---

### Planned Mitigation — different from controls

Planned mitigation is further action to reduce the risk, but it is **not the same as existing controls** and it **does not affect the score calculation at all**.

The distinction:

| | Existing Controls | Planned Mitigation |
|--|--|--|
| **When** | Already in place today | Not yet done — future actions |
| **Affects score?** | Yes — directly reduces residual risk | No — purely informational |
| **Purpose** | Shows what is reducing the risk right now | Shows what is planned to reduce it further |
| **Example** | "Fraud detection alerts are enabled" | "We plan to add biometric verification by Q3" |

Planned mitigation is essentially a **to-do list** attached to the risk line. It tells reviewers: *"we know this residual risk is still high, and here is what we intend to do about it."*

It does not change the numbers. Once those planned actions are actually implemented, they should be added as controls on the line — at that point they will affect the score.

---

### Department

The department field answers: **who is responsible for managing this specific risk item?**

Different lines on the same assessment can belong to different departments. For example:

| Line | Residual Risk | Responsible Department |
|------|:---:|----------------------|
| Fraud via USSD | 4.0 | IT Security |
| KYC gap at onboarding | 7.1 | Compliance |
| Regional violence exposure | 8.6 | Risk Management |

This does not affect the score. It is an **ownership and accountability field** — when risk reports are produced, each department can see which lines they own and what their mitigation obligations are.

---

### Implementation Date

This is tied to the planned mitigation. It answers: **when does the responsible department plan to have the mitigation actions completed?**

Options: Immediate / 7 / 14 / 21 / 30 / 60 / 90 days — relative to when the assessment was done.

This does not affect the score. It is a management tracking tool to ensure planned actions are being followed through within a reasonable timeframe.

---

### What Actually Affects the Score vs What Is Informational

| Field | Affects Score | Purpose |
|-------|:---:|---------|
| Inherent Risk Score | Yes | Starting point — raw damage potential |
| Existing Controls (effectiveness) | Yes | How much risk is reduced today |
| Residual Risk Score | Yes — computed | What remains after controls |
| Residual Risk Probability | Yes — computed | Remaining risk expressed as a percentage |
| Planned Mitigation | No | Future actions — to-do list |
| Department | No | Who owns this risk line |
| Implementation Date | No | Deadline for completing planned actions |
| Implication | No | Documents the business impact |

---

## 13. The Risk Analysis Module — Automated Scoring and Breakdown

The `risk_analysis` module sits on top of the core risk assessment system and automates what the manual "Run Risk Analysis" button does — but for every customer at once, on a schedule, using optimized PostgreSQL materialized views.

---

### 13.1 What It Does

Instead of a compliance officer clicking a button per customer, the risk_analysis module:

1. Runs all active plans against every customer automatically every 6 hours
2. Detects which risk patterns each customer matches
3. Stores a per-customer breakdown of matched risk codes and scores in a materialized view
4. Writes `risk_score`, `risk_level`, and `composite_risk_score` back to each customer record

---

### 13.2 The `risk.analysis` Model

This model tracks the materialized views the module generates:

| Field | Purpose |
|-------|---------|
| `name` | View name e.g. `mv_risk_delivery_channel` |
| `code` | The full SQL used to create the view (readonly) |
| `universe` | Which risk universe this view covers |
| `last_refresh` | When the view was last refreshed |
| `pattern_stats` | Summary: "Patterns matched: 5, Unmatched: 2" |

---

### 13.3 How Materialized Views Work

A materialized view is a pre-computed table stored in PostgreSQL. Instead of running complex SQL joins against every customer on demand, the system builds the result once and stores it. Queries then hit the pre-built table instead of recomputing every time.

```
Without materialized view:
  Run Risk Analysis → runs SQL per plan × every customer → slow

With materialized view:
  Cron builds mv_risk_delivery_channel → stores results
  Query → reads pre-built table → fast
```

Each view produces one row per customer with a JSONB column showing all matched risk patterns:

```json
{
  "USSD_DELIVERY": 7.5,
  "MOBILE_BANKING": 5.0,
  "HIGH_RISK_REGION": 8.0
}
```

This is the **breakdown** — every matched plan code and its score, stored against the customer.

---

### 13.4 Pattern Types the Module Recognises

The module analyses each plan's SQL query and classifies it into a pattern type. This allows it to build optimised set-based SQL instead of running each query individually per customer.

| Pattern Type | What It Detects | Example SQL Pattern |
|-------------|----------------|-------------------|
| `account_category` | Customer's account category from `res_partner_account` | `WHERE a.category = 'SAVINGS'` |
| `industry` | Customer's industry classification | `WHERE customer_industry_id IN (SELECT id FROM customer_industry WHERE name = 'crypto')` |
| `region` | Customer's region | `JOIN res_partner_region WHERE name = 'abia'` |
| `channel` | Digital channel subscriptions | `JOIN customer_channel_subscription JOIN digital_delivery_channel` |
| `branch_region` | Branch's regional location | `JOIN res_branch WHERE region = 'north east'` |

In addition to SQL-pattern plans, the module also detects these **independent risk factors** directly from the customer record:

| Factor | Condition |
|--------|-----------|
| `invalid_bvn` | BVN is NULL or wrong format |
| `invalid_name` | Name is empty or contains only special characters |
| `missing_contact` | No phone, mobile, or customer_phone |
| `sanction` | `likely_sanction = TRUE` |
| `pep` | `is_pep = TRUE` |
| `watchlist` | `is_watchlist = TRUE` |
| `default_risk` | Customer has a default risk assessment linked |

---

### 13.5 The Cron Jobs

Three scheduled jobs run automatically:

| Job | Schedule | What It Does |
|-----|----------|-------------|
| `ir_cron_run_risk_assessment` | Every 6 hours | Runs `update_sanction_status()` then scores all customers in batches of 500. Updates `risk_score`, `risk_level`, `last_risk_calculation` on every customer |
| `ir_cron_generate_mv` | Every 6 hours | Rebuilds the materialized views — regenerates the `mv_risk_*` tables from scratch |
| `ir_cron_refresh_mv` | Every 1 hour | Refreshes (not rebuilds) the materialized views — faster incremental update. Currently disabled |

The sanction status check runs **before** risk scoring so that `is_watchlist`, `is_pep`, and `likely_sanction` flags are current when the scores are calculated.

---

### 13.6 Manual Button vs Cron Job

Both use the same underlying `_get_risk_score_from_plan()` logic. The differences are:

| | Manual "Run Risk Analysis" button | Cron job |
|--|--|--|
| Trigger | Compliance officer clicks the button | Automatic every 6 hours |
| Scope | One customer at a time | All customers, batches of 500 |
| Sanction check | Not run | Runs first before scoring |
| Speed | Immediate, synchronous | Background, commits between batches |
| Audit trail | ORM write — tracked in chatter | Bulk update — no chatter entry |
| Use case | Testing a specific customer, urgent re-score | Keeping everyone up to date automatically |

---

### 13.7 View Generation Process

When the cron fires to generate views, it goes through these steps:

```
1. _cron_generate_views()
      │
      ├── For each risk universe with active composite plans:
      │     _build_optimized_view(universe, plans)
      │     → Analyses each plan's SQL → classifies pattern type
      │     → Builds one large UNION ALL SQL covering all patterns
      │     → _setup_partitioned_view()  → CREATE MATERIALIZED VIEW WITH NO DATA
      │     → _populate_partitioned_view() → REFRESH MATERIALIZED VIEW
      │     → _create_partition_indexes() → UNIQUE, GIN, and filtered indexes
      │
      └── For independent risk factors:
            _build_independent_risk_view()
            → Single view covering PEP, watchlist, sanction, BVN, name, contact
```

The result is a set of `mv_risk_*` tables — one per universe plus one for independent factors — each indexed for fast lookups.

---

### 13.8 Full Automated Scoring Flow

```
Every 6 hours the cron fires:

STEP 1 — Rebuild materialized views
  Analyse all active plan SQL queries
  Classify into pattern types
  Build optimised UNION ALL SQL per universe
  CREATE / REFRESH mv_risk_* tables
  Each customer gets a JSONB row: { "PLAN_CODE": score, ... }

STEP 2 — Update sanction flags
  update_sanction_status()
  Sets is_pep, is_watchlist, likely_sanction on each customer

STEP 3 — Score all customers (batches of 500)
  For each customer:
    _get_risk_score_from_plan()
      → Run composite plans → write composite_plan_lines
      → Run regular plans  → write risk_plan_lines
      → Apply max/avg/sum setting
      → Add composite + regular score
      → Priority override: Assessment > EDD > Plans
    Write risk_score, risk_level, last_risk_calculation

STEP 4 — Result on customer record
  risk_score = final number
  risk_level = low / medium / high / extreme
  composite_risk_score = weighted universe total
  last_risk_calculation = timestamp
  Composite plan lines tab = breakdown per universe and subject
```

---

### 13.9 Indexing for Performance

After each materialized view is created, three indexes are built:

| Index Type | Column | Purpose |
|-----------|--------|---------|
| UNIQUE | `partner_id` | Fast single-customer lookups |
| GIN | `risk_data` (JSONB) | Fast search by risk code key |
| Filtered | `partner_id WHERE risk_data != '{}'` | Skips customers with no matched risks |

The connection is also configured with:
- `statement_timeout = 1 hour` — allows large view builds to complete
- `maintenance_work_mem = 1GB` — faster sort and hash operations during index creation

---

## 14. Quick Reference: What Each Role Can Do

| Action | CO | CRM | CCO |
|--------|:--:|:---:|:---:|
| View risk assessments | ✓ | ✓ | ✓ |
| Create/edit assessments | ✓ | ✓ | ✓ |
| Configure risk categories | ✓ | ✓ | ✓ |
| Configure risk universe | ✓ | ✓ | ✓ |
| Create/manage plans | ✓ | ✓ | ✓ |
| View slider/FCRA settings | ✓ | ✓ | ✓ |
| Modify compliance settings | | | ✓ |
