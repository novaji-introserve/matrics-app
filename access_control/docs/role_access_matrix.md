# iComply — Role Access Control: Complete Reference

---

## 1. How View Access Rules Work

iComply uses a custom `view.access.rule` model (in the `access_control` module) on top of Odoo's standard group-based security.

A rule ties together four things:
- **Model** — the data model being protected (e.g. `alert.history`, `case.manager`)
- **Menus** — which menu items are visible
- **Actions** — which window actions (list/form views) can be opened
- **Groups** — which roles the rule applies to

When a user logs in, Odoo hides every menu and action that does not appear in at least one rule that covers their group. Rules are **additive** — if a user belongs to two groups, they see the union of everything both groups allow.

The demo data file is `noupdate="1"`, meaning it only loads on a **fresh install**. To reload after changes, delete the existing view access rule records in Settings → Technical → View Access Rules, then run `-u access_control`.

---

## 2. Role Descriptions

### BCO — Branch Compliance Officer
A compliance officer operating at the **branch level**. Responsible for day-to-day compliance checks at a specific branch: customer onboarding screening, basic KYC verification, watchlist checks, and escalating issues to the main compliance team. They work directly with customers and their accounts. They do not configure the system — they consume it.

### BCntO — Branch Control Officer
An **internal control and audit** role at the branch. Responsible for reviewing branch operations, ensuring procedures are followed, and managing system-level settings for the branch. This role does not handle customer screening or alerts directly — they deal with configuration, settings, and oversight. Think of them as the branch-level system admin.

### CO — Compliance Officer
The **full-grade compliance officer**, not limited to a branch. Handles the complete compliance lifecycle: PEP screening, sanctions, adverse media, risk assessment, alert management, case escalation, transaction monitoring, and configuration of compliance rules. They are the main day-to-day operators of the entire compliance system.

### CRM — Compliance Risk Manager
Focused exclusively on **risk management**. They design and maintain the risk framework: risk categories, universes, subjects, types, and risk assessments. They review compliance statistics and slider settings. They are not involved in case management, alerts, or direct customer screening — their job is to define and measure risk, not respond to it.

### RM — Relationship Manager
A **customer-facing role**. Their primary job is managing the client relationship. They have access to compliance data about their customers (KYC, EDD, watchlist checks) so they can support the onboarding and review process. They do not manage alerts, cases, or system configuration — they need to see compliance status on their customers, nothing more.

### TMT — Transaction Monitoring Team
Responsible for **real-time and retrospective transaction surveillance**. They monitor transactions for suspicious patterns, review screening matches from sanction/watchlist/PEP checks, manage alerts, and escalate to case management. They are not involved in risk framework configuration or system settings.

### CCO — Chief Compliance Officer
The **head of the compliance function**. Has access to everything — all compliance modules, configuration, user management, settings, employee records, and group/access rights management. Also inherits all CO access (and through that, all BCO access). This is the role given to the compliance department head.

---

## 3. Group Inheritance Explained

Odoo's group inheritance means a child group **automatically receives all model-level access rights** (read/write/create/delete on ir.model.access) that the parent group has. It does NOT automatically copy view access rules — those are explicit.

```
BCO  ←──── CO  ←──── CCO
                        └──── (also inherits system admin / erp_manager)
```

**CO inherits BCO** means:
- A CO user automatically has every model permission that BCO has
- For view access rules, CO is explicitly listed in the rules that BCO is in (Rule 1, 2, 4, 7, etc.), so the menus and actions are visible to both
- CO is also listed in many additional rules (Rules 3, 5, 8, 9, 12–31, 35–37, etc.) that BCO does NOT appear in — those are CO-exclusive

**CCO inherits CO** (which in turn inherits BCO) means:
- CCO gets all BCO model permissions, all CO model permissions, plus system/admin permissions
- In the view access rules, CCO appears in every single rule, so they see everything

**BCntO does NOT inherit from anyone** — they are a completely separate branch-level role with only Settings and Spreadsheet Dashboard access. They are deliberately isolated from compliance data.

**CRM, RM, TMT do NOT inherit from anyone** — each has a specific, non-overlapping scope.

---

## 4. Current Access Per Role

### BCO — Branch Compliance Officer

| Area | Menus | Actions |
|------|-------|---------|
| Dashboard | Compliance Dashboard | — |
| KYC | Compliance → KYC | — |
| Screening (root) | Compliance → Screening | — |
| Adverse Media (root) | Compliance → Adverse Media | — |
| Configuration (root) | Compliance → Configuration | — |
| Partner data | — | PEP Customers, FEP List, Greylist, Watchlist |
| Cases | Case Management (root) | All case views (open, draft, overdue, closed, archived, mine, created by me) |
| Watchlist | Screening → Screening List → Watchlist | Watchlist records |
| Password | — | Change Password wizard |

---

### BCntO — Branch Control Officer

| Area | Menus | Actions |
|------|-------|---------|
| Settings | Settings (General Config) | General Configuration |
| Password | — | Change Password wizard |

---

### CO — Compliance Officer
*Has everything BCO has, plus all of the following:*

| Area | Menus | Actions |
|------|-------|---------|
| PEP List | Screening → Screening List → PEP List | PEP listed entities |
| Global PEP | Screening → Screening List → Global PEP List | Global PEP database |
| Sanction List | Screening → Screening List → Sanction List | Sanction list |
| Screening Matches | Screening → Matches | Blacklist, FEP, Global PEP, Pending, PEP, Sanction, Watchlist matches |
| Risk | Compliance → Risk | Counterparty risk assessments, Institutional risk assessments |
| Transaction Screening | KYC → Transaction Screening, KYC → Transaction Screening → History | Transaction screening history |
| Adverse Media | Adverse Media → Media Watchlist | Adverse media watchlist |
| Adverse Media Logs | Adverse Media → Adverse Media Logs | Adverse media alert logs |
| Risk Management Config | Configuration → Risk Management | — |
| Risk Categories | Configuration → Risk Management → Risk Categories | Risk categories |
| Risk Universe | Configuration → Risk Management → Risk Universe | Risk universe |
| Risk Subjects | Configuration → Risk Management → Risk Subjects | Risk subjects |
| Risk Types | Configuration → Risk Management → Risk Types | Risk types |
| Customer Risk Analysis | Configuration → Risk Management → Customer Risk Analysis | Risk analysis / assessment plans |
| Dashboard Charts | Configuration → Dashboard Charts | Dashboard charts |
| PEP Config | Configuration → PEP Configurations, → PEP Sources | PEP sources |
| Adverse Media Config | Configuration → Adverse Media, → Media Configurations | Media keywords |
| Slider Settings | Configuration → Configuration | FCRA score / slider settings |
| Statistics | Configuration → Configuration → Statistics | Compliance statistics |
| Sanction Screening Config | Configuration → Sanction Screening, → Configurations | Sanction screening alert rules |
| Alerts | Alerts, Alert History, My Alert | All alerts, My alerts |
| EDD | KYC → Enhanced Due Diligence | EDD records |
| Model access only | — | Composite risk score, Risk assessment lines, Risk assessment types, Risk assessment controls, Risk analysis lines |

---

### CRM — Compliance Risk Manager

| Area | Menus | Actions |
|------|-------|---------|
| Dashboard | Compliance Dashboard | — |
| KYC (root) | Compliance → KYC | — |
| Screening (root) | Compliance → Screening | — |
| Adverse Media (root) | Compliance → Adverse Media | — |
| Configuration (root) | Compliance → Configuration | — |
| Risk | Compliance → Risk | Counterparty risk assessments, Institutional risk assessments |
| Risk Management Config | Configuration → Risk Management | — |
| Risk Categories | Configuration → Risk Management → Risk Categories | Risk categories |
| Risk Universe | Configuration → Risk Management → Risk Universe | Risk universe |
| Risk Subjects | Configuration → Risk Management → Risk Subjects | Risk subjects |
| Risk Types | Configuration → Risk Management → Risk Types | Risk types |
| Customer Risk Analysis | Configuration → Risk Management → Customer Risk Analysis | Risk analysis / assessment plans |
| Slider Settings | Configuration → Configuration | FCRA score / slider settings |
| Statistics | Configuration → Configuration → Statistics | Compliance statistics |
| Partner data | — | PEP Customers, FEP List, Greylist, Watchlist |
| Model access only | — | Customer accounts, Composite risk score, Risk assessment lines, Risk assessment types, Risk assessment controls, Risk analysis lines |
| Password | — | Change Password wizard |

---

### RM — Relationship Manager

| Area | Menus | Actions |
|------|-------|---------|
| Dashboard | Compliance Dashboard | — |
| KYC | Compliance → KYC | — |
| EDD | KYC → Enhanced Due Diligence | EDD records |
| Screening (root) | Compliance → Screening | — |
| Adverse Media (root) | Compliance → Adverse Media | — |
| Configuration (root) | Compliance → Configuration | — |
| Partner data | — | PEP Customers, FEP List, Greylist, Watchlist |
| Model access only | — | Customer accounts |
| Password | — | Change Password wizard |

---

### TMT — Transaction Monitoring Team

| Area | Menus | Actions |
|------|-------|---------|
| Dashboard | Compliance Dashboard | — |
| KYC (root) | Compliance → KYC | — |
| Transaction Screening | KYC → Transaction Screening, → History | Transaction screening history |
| Screening (root) | Compliance → Screening | — |
| Watchlist | Screening → Screening List → Watchlist | Watchlist records |
| Sanction List | Screening → Screening List → Sanction List | Sanction list |
| Screening Matches | Screening → Matches | All 7 screening result views |
| Adverse Media (root) | Compliance → Adverse Media | — |
| Adverse Media Logs | Adverse Media → Adverse Media Logs | Adverse media alert logs |
| Configuration (root) | Compliance → Configuration | — |
| Cases | Case Management (root) | All case views |
| Alerts | Alerts, Alert History, My Alert | All alerts, My alerts |
| Partner data | — | PEP Customers, FEP List, Greylist, Watchlist |
| Model access only | — | Customer accounts, Transactions |
| Password | — | Change Password wizard |

---

### CCO — Chief Compliance Officer
*Has everything CO has (which includes everything BCO has), plus all of the following:*

| Area | Menus | Actions |
|------|-------|---------|
| Account Types | Configuration → Accounts → Account Types | Account type configuration |
| Account Products | Configuration → Accounts → Account Products | Account product configuration |
| Branches | Configuration → Branches | Branch configuration |
| Customer Sectors | Configuration → Customer → Customer Sectors | Customer sector configuration |
| Compliance Settings | Configuration → Configuration → Settings | Compliance-specific settings |
| Users | — | System users (base) |
| Groups | Settings → Technical → Groups | Security groups |
| Model Access | — | ir.model.access records |
| Employee | — | HR employee records (model access) |
| Settings | Settings (General Config) | General Configuration (same as BCntO) |

---

## 5. Access Summary Matrix

| Feature | BCO | BCntO | CO | CRM | RM | TMT | CCO |
|---------|:---:|:-----:|:--:|:---:|:--:|:---:|:---:|
| Dashboard | ✓ | | ✓ | ✓ | ✓ | ✓ | ✓ |
| KYC / Due Diligence | ✓ | | ✓ | ✓ | ✓ | ✓ | ✓ |
| Enhanced Due Diligence | | | ✓ | | ✓ | | ✓ |
| Case Management | ✓ | | ✓ | | | ✓ | ✓ |
| PEP List | | | ✓ | | | | ✓ |
| Global PEP List | | | ✓ | | | | ✓ |
| Watchlist | ✓ | | ✓ | | | ✓ | ✓ |
| Sanction List | | | ✓ | | | ✓ | ✓ |
| Screening Matches | | | ✓ | | | ✓ | ✓ |
| Risk | | | ✓ | ✓ | | | ✓ |
| Transaction History | | | ✓ | | | ✓ | ✓ |
| Adverse Media | | | ✓ | | | | ✓ |
| Adverse Media Logs | | | ✓ | | | ✓ | ✓ |
| Account Types / Products | | | | | | | ✓ |
| Branches | | | | | | | ✓ |
| Risk Categories/Universe/Subject/Type | | | ✓ | ✓ | | | ✓ |
| Risk Analysis / Plans | | | ✓ | ✓ | | | ✓ |
| Dashboard Charts | | | ✓ | | | | ✓ |
| PEP / Sanction Config | | | ✓ | | | | ✓ |
| Adverse Media Config | | | ✓ | | | | ✓ |
| Slider Settings | | | ✓ | ✓ | | | ✓ |
| Compliance Statistics | | | ✓ | ✓ | | | ✓ |
| Compliance Settings | | | | | | | ✓ |
| Alerts | | | ✓ | | | ✓ | ✓ |
| Customer Sectors | | | | | | | ✓ |
| Users / Groups | | | | | | | ✓ |
| Settings (Odoo) | | ✓ | | | | | ✓ |
| Change Password | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

---

## 6. Recommended Access Adjustments (Advisory)

These are suggestions — not yet implemented in the XML. Review with stakeholders before applying.

### Additions worth considering

| Role | Feature | Reason |
|------|---------|--------|
| BCO | Greylist menu (`menu_greylist`) | BCO already has the greylist action via Rule 1 — exposing the menu makes it navigable directly |
| BCO | Sanction List menu (`menu_sanction`) | BCO handles screening at branch level; seeing the sanction list is part of that job |
| BCO | Adverse Media Logs | BCO deals with adverse media checks during onboarding at the branch |
| RM | Transaction Screening History | RMs often need to check if a customer's transactions triggered any flags |
| RM | Watchlist menu | RMs should be able to check if their customers are on a watchlist directly |
| TMT | Adverse Media Screening | TMT monitors suspicious activity — adverse media about a customer is directly relevant |
| TMT | EDD | When escalating a case, TMT may need to view or verify EDD records |
| CRM | Customer Accounts (menu) | CRM assesses risk against account data; currently they have model access but no menu |
| CRM | Risk Assessment menu | The `menu_compliance_risk_assessment` exists under Risk; CRM should see this |

### Restrictions worth considering

| Role | Feature | Reason |
|------|---------|--------|
| BCO | Configuration root menu | BCO sees the Configuration menu (from Rule 1) but has no actions inside it — this could be confusing. Consider removing `menu_compliance_configuration` from Rule 1 for BCO. |
| RM | Configuration root menu | Same issue as BCO — RM sees the Configuration root but cannot do anything inside it |
| RM | Screening root menu | RM does not perform screening themselves; seeing the Screening menu may be misleading |
| CRM | Case Management | CRM does not manage cases; they shouldn't see case menus (they currently don't — confirm this is intentional) |
| TMT | Configuration root menu | TMT sees Configuration root but has no actions inside it — same cosmetic issue as BCO/RM |
| BCntO | Isolated to Settings only | Currently BCntO only sees Settings. If they need to view compliance reports for audit, add read-only access to Statistics (`menu_compliance_stat`) |

### Role gap: No read-only viewer role
Currently there is no "read-only auditor" role. If external auditors or board members need to view the compliance dashboard and reports without editing anything, a new group (e.g. `group_compliance_auditor`) with access only to Dashboard, Statistics, and Risk reports would be appropriate.
