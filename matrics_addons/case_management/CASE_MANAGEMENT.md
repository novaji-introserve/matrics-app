# Case Management v2 — Module Guide

## What This Module Does

`case_management_v2` provides a structured workflow for compliance officers to
investigate flagged transactions and exceptions. A case wraps a suspicious or
unusual event, assigns it to a responsible officer, tracks investigative
responses, and produces an email audit trail at every stage.

It integrates with:
- `compliance_management` — cases can be linked to a `res.customer.transaction`
- `nfiu_reporting` — creating a case on a suspicious transaction moves it to
  `state = 'awaiting_approval'` on the transaction
- `alert_management` — every email sent is logged to `alert.history`

---

## Data Models

### `case.manager` — The Case

The core record. One case per investigation event.

| Field | Type | Notes |
|-------|------|-------|
| `case_ref` | Char | Auto-generated sequence (CM00001, CM00002…). Read-only after creation. |
| `case_status` | Selection | **draft → open → closed → archived** (+ overdue). See state machine below. |
| `case_rating` | Selection | low / medium / high. Drives `case_score` via settings thresholds. |
| `case_score` | Float | Risk score (0–10). Auto-set when rating changes. |
| `officer_responsible` | Many2one → `res.users` | The analyst assigned to investigate. Required. |
| `supervisors` | Many2many → `res.users` | Supervisors CC'd on all case emails. Required. |
| `transaction_id` | Many2one → `res.customer.transaction` | The transaction under investigation (optional but typical). Cascade-deletes if transaction is deleted. |
| `customer_id` | Many2one → `res.partner` | Customer involved. |
| `department_id` | Many2one → `hr.department` | Defaults to current user's department. |
| `process_category` | Many2one → `exception.category.` | Category of exception (e.g. AML, Fraud). Required. |
| `process` | Many2one → `exception.process.` | Specific process within the category. Required. |
| `cases_action` | Text | What action has been / should be taken. Required. |
| `narration` | Text | Free-text notes. |
| `close_remarks` | Text | Required from the case creator when closing. |
| `response_ids` | One2many → `case.response.` | Investigation log — each entry is a timestamped response. |
| `new_response` | Text | Convenience field: type here and save → auto-creates a response record. Only available to `officer_responsible` while case is open. |
| `document` | Binary | Attached supporting document. |
| `event_date` | Datetime | When the event occurred. |

### `case.response.` — Investigation Log

One record per response/update added to a case.

| Field | Notes |
|-------|-------|
| `case_id` | Parent case (cascade delete) |
| `response` | Text of the investigation update |
| `create_date` | Auto-set — immutable timestamp |
| `create_uid` | Who wrote it — auto-set from session user |

### `exception.category.` and `exception.process.`

Lookup tables for classifying cases.

```
exception.category.          exception.process.
─────────────────            ──────────────────
AML                    →     Large Cash Deposit
Fraud                  →     Structuring
Sanctions              →     Unusual Wire Transfer
KYC                    →     PEP Activity
…                            …
```

`process` is filtered by `process_category` on the case form.

### `case.settings`

Key-value configuration store for the module.

| Code | Purpose | Default |
|------|---------|---------|
| `case_overdue_period` | Time before an open case with no responses becomes overdue | 48 hours |
| `case_archive_period` | Time before a closed case is auto-archived | 180 days |
| `low_risk_threshold` | Max score for "low" rating | 3.9 |
| `medium_risk_threshold` | Max score for "medium" rating | 6.9 |
| `high_risk_threshold` | Max score for "high" rating | 9.0 |

Change these at **Case Management → Configuration → Settings**.

---

## State Machine

```
[draft]
   │
   │  action_open_case()
   │  → Email sent to officer_responsible
   ▼
[open]──────────────────────────────────────────┐
   │                                            │
   │  No responses after overdue_period         │
   │  (hourly cron)                             │
   ▼                                            │
[overdue]                                       │
   │  (add a response to return to open)        │
   │  action_close_case()                       │
   │  requires: ≥1 response                     │
   │            close_remarks (if creator)      │
   │  → Email sent to officer_responsible       │
   ▼                                            │
[closed]◄───────────────────────────────────────┘
   │
   │  action_archive_case()  (manual)
   │  OR auto-archive after archive_period (daily cron)
   ▼
[archived]  ← active=False, hidden from all lists
```

### Rules enforced per transition

| Transition | Who can trigger | Requirements |
|-----------|----------------|--------------|
| draft → open | Anyone with case access | None |
| open → closed | Case creator only | ≥1 response exists; close_remarks filled |
| closed → archived | Anyone with case access | status must be 'closed' |
| open → overdue | Cron only | No responses within `case_overdue_period` |
| closed → archived | Cron (auto) | Closed for longer than `case_archive_period` |

> **Note:** Only the **creator** (`create_uid`) sees the Close button. Other users
> (officer, supervisors) can add responses but cannot close the case.

---

## Creating and Working a Case

### Step 1 — Open a case from a transaction

On any suspicious transaction form click **Create Case**. The case form opens
pre-filled with the customer, risk level, and a narration from the transaction.
At the same time, the transaction state advances to `awaiting_approval`.

### Step 2 — Assign and open

Set `officer_responsible` and `supervisors`. Select `process_category` and
`process`. Click **Open Case** — this sends the assignment email.

### Step 3 — Investigate (add responses)

The `officer_responsible` types their investigation update in the
**New Response** field and saves. Each save creates an immutable
`case.response.` record with a timestamp. Other users cannot add responses.

### Step 4 — Close the case

When the investigation is complete, the **creator** fills in **Reason for
Closure** and clicks **Close Case**. A closure email is sent.

### Step 5 — Post-closure action on the transaction

Closing a case does **not** automatically update the linked transaction. The
officer must manually act on it:

| Outcome | What to do on the transaction |
|---------|------------------------------|
| Transaction confirmed suspicious — report to NFIU | Ensure `report_nfiu = True`, include in next NFIU XML export, then set state to `done` |
| Transaction cleared — not suspicious | Click **Unmark as Suspicious** → resets `report_nfiu = False`, state back to `new` |
| Manual case (no NFIU relevance) | Manually set state to `done` |

See [INTEGRATION.md](../nfiu_reporting/INTEGRATION.md) for the full NFIU
reporting workflow.

---

## Security and Visibility

Record-level rule: a user can see a case only if they are:
- The **creator** (`create_uid`)
- The **officer responsible** (`officer_responsible`)
- Listed in **supervisors**

No other users can see the case. There is no global admin override in the
module's own rules (though the Odoo Administrator / superuser bypasses all
record rules).

**Model-level access** (all users in `base.group_user`):

| Operation | Allowed |
|-----------|---------|
| Read | ✓ |
| Create | ✓ |
| Write | ✓ |
| Delete | ✗ |

Cases cannot be deleted through the UI.

---

## Email Alerts

Three emails are sent automatically:

| Trigger | To | CC |
|---------|----|----|
| Case opened / officer changed | `officer_responsible` | Creator, all supervisors |
| New response added | Case creator | All supervisors |
| Case closed | `officer_responsible` | Creator, all supervisors |

All sent emails are logged to `alert.history` with the case reference, status,
and recipient list. You can view them under **Alert Management → Alert History**.

---

## Cron Jobs

| Job | Interval | What it does |
|-----|----------|-------------|
| Check for Overdue Cases | Every hour | Finds `open` cases with no responses older than `case_overdue_period`; sets status = `overdue` |
| Archive Old Closed Cases | Every day | Finds `closed` cases last modified more than `case_archive_period` ago; sets status = `archived`, active = False |

---

## Menu Locations

**Case Management** (top-level app):
- My Cases
- All Cases
- Overdue Cases
- Closed Cases
- Exception Categories
- Exception Processes
- Configuration → Settings

---

## Key Files Reference

| File | Purpose |
|------|---------|
| [models/case.py](models/case.py) | `case.manager`, `case.response.`, `case.settings` — all core logic |
| [models/exception.py](models/exception.py) | `exception.category.`, `exception.process.` — classification lookups |
| [models/alert.py](models/alert.py) | `alert.history` extension — adds `user_in_emails_` search |
| [views/case.xml](views/case.xml) | Case form, list, and search views |
| [data/emails/case_template.xml](data/emails/case_template.xml) | Three email templates (creation, response, closure) |
| [data/schedules/case_schedules.xml](data/schedules/case_schedules.xml) | Two cron job definitions |
| [data/demo/settings.xml](data/demo/settings.xml) | Default values for `case.settings` |
| [security/security.xml](security/security.xml) | Record-level rule — restricts visibility to creator/officer/supervisors |
| [security/ir.model.access.csv](security/ir.model.access.csv) | Model-level ACL |
