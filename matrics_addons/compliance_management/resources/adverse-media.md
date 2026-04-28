# Adverse Media Screening — Consolidated Requirements

**Last updated:** 2026-04-27  
**Module:** `compliance_management` → `adverse.media`, `adverse.media.alert`, `media.keyword`  
**Status:** Phase 1 in progress

---

## Implementation Status Key

| Symbol | Meaning |
|---|---|
| ✅ | Implemented and working |
| 🟡 | Partially implemented — gaps noted |
| ❌ | Not yet started |
| 🔧 | Bug fixed in current sprint |

---

## Defects Fixed (Sprint: 2026-04-27)

All eight defects below are resolved. No further action required on these.

| ID | Description | Fix |
|---|---|---|
| B1 | `_prepare_search_query()` returned a Python list; NewsAPI received a list object instead of a boolean query string. Keywords were collected but silently discarded — every query was just the partner name. | Method now returns `"Partner Name" AND ("keyword1" OR "keyword2" OR ...)` |
| B2 | `scan_news_articles()` hardcoded `next_scan_date = now + 1 day` regardless of `monitoring_frequency`; weekly and monthly records were re-scanned daily. | Removed explicit `next_scan_date` write; stored computed field `_compute_next_scan_date` now recalculates from updated `last_scan_date`. |
| B3 | `scan_adverse_media()` cron wrote `last_scan_date` a second time after `scan_news_articles()` already set it, overwriting the precise timestamp. | Removed redundant write from cron method. |
| B4 | `sudo(flag=True)` passed an invalid positional argument to `sudo()`. | Changed to `.sudo()`. |
| B5 | `AdverseMediaAlert.risk_score` was `fields.Integer` while keyword scores and `_calculate_risk_score()` return floats; precision silently lost on save. | Changed to `fields.Float`. |
| B6 | `_create_alert()` guarded on `key in article` (key existence, not value); silently dropped valid articles where NewsAPI legitimately returns `None` for `description` or `content`. | Guard now checks `.get('title')`, `.get('url')`, `.get('publishedAt')` for truthiness only; optional fields use `.get() or ''`. |
| B7 | Medium Risk search filter domain `[('risk_score', '>=', 19), ('risk_score', '<', 19)]` was a logical contradiction — matched nothing. | Fixed to `[('risk_score', '>=', 5), ('risk_score', '<', 20)]`. |
| B8 | `update_partner_risk()` found a `media.keyword` matching the alert's risk score and mutated that shared record's score, corrupting the global keyword configuration for all future records. | Rewired to compute new partner score directly from `partner.composite_risk_score + alert.risk_score`, capped at `maximum_risk_threshold`, then updates the partner via parameterised SQL. Keyword records are never modified. |

---

## Implementation Phases

### Phase 1 — Foundation (current)
Search query, deduplication, matched keywords, async scanning, scan history.  
*Unblocks all downstream features.*

### Phase 2 — Review Workflow
Assignment, SLA, escalation, false positive suppression, notification improvements, risk score integrity.  
*Required for regulatory compliance.*

### Phase 3 — Advanced Screening
Multi-source aggregation, per-partner configuration, source credibility, case management integration, dashboards.  
*Improves coverage and analyst productivity.*

---

## 1. Customer & Entity Screening

**Status:** 🟡 Partial  
**Phase:** 3

The system shall screen the following subjects:

- Individual customers
- Corporate customers
- Directors
- Shareholders
- Beneficial owners
- Authorised signatories
- Vendors, partners, agents, correspondents, and respondents
- Related parties and linked entities
- Existing customers during periodic review
- Customers triggered by risk events, onboarding changes, or transaction alerts

**Current implementation:** One `adverse.media` record per `res.partner`. Manual creation only. The partner domain is restricted to `origin in ['demo', 'test', 'prod']`.

**Gaps:**
- No automatic screening of directors, shareholders, or beneficial owners linked to a corporate customer
- No bulk enrolment of all partners of a given type into the watchlist
- No related-party traversal (e.g. screen a company and all its directors in one operation)

**Implementation notes:**
- Add a server action on `res.partner` list view: "Enrol in Adverse Media Monitoring" — bulk-creates `adverse.media` records for selected partners
- For related-party screening, iterate `partner_id.child_ids` and `partner_id.related_company_ids` at scan time; create child `adverse.media` records automatically when a corporate customer is enrolled

---

## 2. Screening Sources

**Status:** 🟡 Partial  
**Phase:** 3

The system shall search adverse media from:

- Licensed news databases
- Public news websites
- Regulatory enforcement publications
- Court or legal notices where available
- Government press releases
- Sanctions-related news
- Fraud, corruption, bribery, terrorism-financing, cybercrime, trafficking, tax evasion, money laundering, and financial crime sources
- Local, regional, and international media
- Multi-language sources
- Historical and recent news archives

**Current implementation:** NewsAPI only (`NewsApiKey`, `NewsApiUrl` environment variables). English language only. Look-back window defaults to 30 days from last scan.

**Gaps:**
- No RSS feed support
- No Nigerian local media sources (Punch, Vanguard, TheCable, BusinessDay)
- No regulatory feeds (EFCC press releases, CBN circulars, NFIU advisories)
- No multilingual sources
- No historical archive beyond NewsAPI's 30-day free-tier window

**Implementation notes (Phase 3):**
- Introduce `media.source` model with fields: `name`, `source_type` (Selection: `newsapi | rss | webhook`), `base_url`, `api_key`, `credibility_score` (Float 0–1), `active`, `language`
- `AdverseMedia` gains a Many2many to `media.source`; scan dispatches per-source via a common `_fetch_from_source(source)` interface
- Priority sources for Phase 3: NewsAPI (current), EFCC RSS, CBN circulars RSS, Punch RSS, Vanguard RSS

---

## 3. Search & Matching

**Status:** 🟡 Partial  
**Phase:** 1 (basic), 3 (advanced)

The system shall support:

- Exact name matching
- Fuzzy name matching
- Alias and former-name matching
- Phonetic matching
- Transliteration matching
- Date of birth matching
- Country and nationality filtering
- Company registration number matching
- Address/location matching
- Director/shareholder relationship matching
- Parent-subsidiary relationship matching
- Negative keyword (exclusion) matching to reduce false positives
- Configurable matching thresholds

**Current implementation (after B1 fix):** Exact name match combined with keyword boolean query: `"Partner Name" AND ("keyword1" OR "keyword2" OR ...)`. Article-level matching checks that the partner name appears in the article text alongside at least one keyword.

**Gaps:**
- No alias/AKA support — if a customer is known by a different name, it will not be found
- No exclusion keywords — common names produce high false-positive rates
- No fuzzy, phonetic, or transliteration matching
- No additional identifier matching (DOB, registration number, address)
- Match confidence score not computed or stored

**Implementation notes (Phase 1):**
- `res.partner` gains `alias_names = fields.Char` (comma-separated AKAs). `_prepare_search_query()` includes aliases: `("Primary Name" OR "Alias One" OR "Alias Two") AND (keywords)`
- `media.keyword` gains `exclusion_terms = fields.Char` (comma-separated). Words in this list, if found in the article, skip the alert. Store on keyword so each category has its own exclusion list (e.g. "fraud" excludes "insurance fraud" for banking-only scope)
- `AdverseMediaAlert` gains `match_confidence = fields.Float` (0–1). Initial implementation: 1.0 for exact match, lowered when exclusions are near-matched

---

## 4. Adverse Media Categories

**Status:** 🟡 Partial  
**Phase:** 1

The system shall classify results into one or more categories:

- Money laundering
- Terrorist financing
- Sanctions evasion
- Fraud
- Bribery and corruption
- Tax evasion
- Human trafficking
- Drug trafficking
- Cybercrime
- Organised crime
- Environmental crime
- Market abuse / insider trading
- Regulatory enforcement
- Bankruptcy / insolvency
- Civil litigation
- Criminal prosecution
- Political exposure controversy
- Reputation risk
- Other (configurable)

**Current implementation:** `media.keyword` records function as categories (Money Laundering, Fraud, Corruption, Sanctions, Terrorism Financing, Other Financial Crime). These are the same objects used for search terms and risk scoring.

**Gaps:**
- No structured category field on `adverse.media.alert` — category is inferred from which keywords matched, but not stored
- The 6 current keywords do not cover all categories above (missing: trafficking, cybercrime, tax evasion, bankruptcy, civil litigation, etc.)
- No multi-category assignment per alert

**Implementation notes (Phase 1):**
- Add `category_ids = fields.Many2many('media.keyword', ...)` on `AdverseMediaAlert` — this replaces the commented-out `matched_keyword` field and stores which keywords were matched (see §3 and REQ: Matched Keyword Tracking below)
- Extend demo keyword seed data to cover all categories above; set appropriate risk scores per category
- Populate `category_ids` at alert creation from the `matched_keywords` list passed to `_create_alert()`

---

## 5. Screening Triggers

**Status:** 🟡 Partial  
**Phase:** 2 (programmatic triggers), 1 (manual)

The module shall run adverse media checks:

- During customer onboarding
- During KYC refresh
- When customer risk rating changes
- When beneficial ownership changes
- When transaction monitoring raises an alert
- When sanctions/PEP screening produces a material match
- On scheduled periodic screening
- On manual request by a compliance user
- When new media is published about an existing customer
- Before high-risk account approval

**Current implementation:** Scheduled cron (`ir.cron` — disabled by default, runs `scan_adverse_media()`). Manual "Run Media Screening" button on the `adverse.media` form.

**Gaps:**
- No programmatic trigger from onboarding, KYC refresh, or risk rating changes
- No trigger from `res.customer.screening` (sanctions/PEP module)
- No trigger from `res.customer.transaction` (transaction monitoring)

**Implementation notes (Phase 2):**
- Override `res.partner.write()` in `compliance_management`: when `risk_level` changes to `high`, call `scan_news_articles()` on the partner's `adverse.media` record if one exists
- Add a method `trigger_adverse_media_scan(partner_id)` callable by the sanctions/PEP screening module post-match
- On `res.customer.screening` confirmation, automatically queue an adverse media scan for the same partner

---

## 6. Risk Scoring

**Status:** 🟡 Partial  
**Phase:** 2

The system shall calculate an adverse media risk score based on:

- Severity of allegation (keyword category weight)
- Recency of publication
- Source credibility
- Number of matching articles
- Repetition across independent sources
- Customer type and existing risk rating
- Jurisdiction risk
- Link strength between customer and article (match confidence)
- Article type: accusation, investigation, conviction, enforcement action, or opinion
- Resolution status: ongoing, pending, dismissed, or resolved
- Relevance to AML/CFT risk

**Current implementation (after B5 fix):** Simple Float sum of matched keyword risk scores. No weighting by recency, source credibility, or article type.

**Gaps:**
- No recency decay (a 10-year-old article carries the same weight as yesterday's)
- No source credibility weighting
- No article type field (conviction vs opinion have very different risk implications)
- No resolution status field
- Risk score on partner blindly adds alert score to composite; no decay over time

**Implementation notes (Phase 2):**
- Add `article_type = fields.Selection([('accusation', 'Accusation/Allegation'), ('investigation', 'Under Investigation'), ('conviction', 'Convicted/Sentenced'), ('enforcement', 'Regulatory Enforcement'), ('opinion', 'Opinion/Commentary'), ('other', 'Other')])` to `adverse.media.alert`
- Add `resolution_status = fields.Selection([('ongoing', 'Ongoing'), ('pending', 'Pending'), ('dismissed', 'Dismissed'), ('resolved', 'Resolved')])` to `adverse.media.alert`
- Scoring formula (Phase 2): `score = keyword_score × recency_factor × source_credibility × article_type_multiplier`
  - `recency_factor`: 1.0 if < 30 days, 0.8 if < 90 days, 0.6 if < 1 year, 0.4 otherwise
  - `article_type_multiplier`: conviction=1.5, enforcement=1.3, investigation=1.0, accusation=0.8, opinion=0.5
  - `source_credibility`: from `media.source.credibility_score` (default 1.0)
- Cap final score at `maximum_risk_threshold` from `res.compliance.settings` at creation time (not in post-processing)

---

## 7. Alert Generation

**Status:** 🟡 Partial  
**Phase:** 1

The system shall generate alerts when:

- A match exceeds the configured risk threshold
- A high-severity category is detected
- Multiple medium-risk articles are found for the same customer
- A customer previously cleared receives new adverse media
- A related party has adverse media
- The article involves financial crime or regulatory enforcement
- A customer appears in politically sensitive or reputational-risk media

**Current implementation:** Alert created for every article where both the partner name and at least one keyword appear in the article text. No threshold check before creation.

**Gaps:**
- No minimum risk threshold check before alert creation — low-scoring articles create noise
- No de-duplication by URL (only by title via SQL constraint) — same article from different syndicated sources creates duplicates
- No aggregation of multiple medium-risk articles into a single higher-priority alert
- No related-party alert generation

**Implementation notes (Phase 1):**
- Add `content_hash = fields.Char` to `adverse.media.alert` computed as `SHA-256(url + title)`. Check for existing hash before calling `_create_alert()`; skip if found
- Add `minimum_alert_threshold` setting to `res.compliance.settings`; skip alert creation if `risk_score < minimum_alert_threshold`
- SQL constraint: add `(media_id, content_hash)` unique index alongside existing `(media_id, name)`

---

## 8. Alert Prioritization

**Status:** 🟡 Partial  
**Phase:** 2

The system shall prioritize alerts using a five-level scale:

| Priority | Label | Meaning |
|---|---|---|
| 0 | Critical | Confirmed financial crime; enforcement action; conviction |
| 1 | High | Active investigation; regulatory warning; sanctions link |
| 2 | Medium | Allegation; civil litigation; reputation risk |
| 3 | Low | Opinion; indirect association; low-credibility source |
| 4 | Informational | Mention without adverse context |

**Current implementation:** A badge derived from `risk_score` using three thresholds (low/medium/high). The `status` field tracks workflow state, not alert severity — these are conflated in the current view.

**Gaps:**
- No separate `priority` field distinct from workflow `status`
- Only three levels (low/medium/high); spec requires five
- Priority not surfaced in the alert list view or email notification

**Implementation notes (Phase 2):**
- Add `priority = fields.Selection([('0','Critical'),('1','High'),('2','Medium'),('3','Low'),('4','Informational')], default='2')` to `adverse.media.alert`
- Auto-assign priority at creation based on `article_type` and `risk_score`
- Expose `priority` widget in list and form views alongside `status`

---

## 9. Case Management

**Status:** ❌ Not Started (within adverse media module)  
**Phase:** 3

The system shall allow compliance users to:

- Open a case directly from an adverse media alert
- Assign cases to analysts
- Escalate cases to supervisors
- Add investigation notes
- Upload supporting documents
- Link related alerts and related customers/accounts
- Mark results as: true match, false match, irrelevant, duplicate, or inconclusive
- Record final disposition
- Recommend: enhanced due diligence, account restriction, rejection, exit, or continued monitoring
- Close with mandatory rationale

**Current implementation:** `case_management` module exists as a separate Odoo module. No link from `adverse.media.alert` to it.

**Gaps:** All requirements above are missing from the adverse media module.

**Implementation notes (Phase 3):**
- Add `case_id = fields.Many2one('case.management', string='Linked Case', ondelete='set null')` to `adverse.media.alert`
- Add "Open Case" button on alert form: creates a `case.management` record pre-filled with partner, alert title, description, and source URL
- `disposition = fields.Selection([('true_match','True Match'), ('false_match','False Match'), ('inconclusive','Inconclusive'), ('duplicate','Duplicate'), ('irrelevant','Irrelevant')])` — required when status moves to `closed`
- `recommendation = fields.Selection([('edd','Recommend EDD'), ('restrict','Recommend Account Restriction'), ('exit','Recommend Customer Exit'), ('monitor','Continue Monitoring'), ('no_action','No Action Required')])`

---

## 10. Review Workflow

**Status:** 🟡 Partial  
**Phase:** 2

The module shall support:

- Maker-checker approval
- First-level analyst review
- Second-level compliance review
- MLRO/compliance officer escalation
- Configurable approval matrix
- SLA tracking and breach notification
- Reassignment between officers
- Reopening of closed cases
- Supervisor override with mandatory reason
- Full decision audit trail

**Current implementation:** Status bar `new → under_review → confirmed → closed`. No enforcement — any user can move to any state. No assignment, no SLA, no escalation.

**Gaps:** All requirements above except basic state tracking.

**Implementation notes (Phase 2):**
- Add `assigned_officer_id = fields.Many2one('res.users', tracking=True)` — populate from the responsible officer on the matched keyword, or allow manual assignment
- Add `review_notes = fields.Text(tracking=True)` — required (`states` constraint) when transitioning to `confirmed` or `closed`
- Add `review_deadline = fields.Datetime` — computed at creation: `create_date + SLA_hours_per_risk_level` from `res.compliance.settings`
- Add `days_open = fields.Integer(compute=...)` for SLA monitoring
- Add cron to find alerts where `review_deadline < now AND status not in ('confirmed','closed')`: post a `mail.activity` to `assigned_officer_id` and their manager
- `status` transitions enforce: `under_review` requires `assigned_officer_id`; `confirmed`/`closed` require `review_notes`
- Add "Reopen" button on closed alerts (sets status back to `under_review`, logs reason in chatter)

---

## 11. False Positive Management

**Status:** ❌ Not Started  
**Phase:** 2

The system shall allow users to:

- Suppress known false positives
- Create customer-specific whitelisting rules
- Add exclusion terms (per keyword category)
- Record reasons for false-positive dismissal
- Prevent repeated alerts for already-cleared articles
- Reopen suppressed matches if new risk information appears

**Current implementation:** None.

**Implementation notes (Phase 2):**
- Introduce `adverse.media.suppression` model:
  - `partner_id` Many2one `res.partner`
  - `url_domain` Char (e.g. `example.com`) — suppress all articles from this domain for this partner
  - `keyword_id` Many2one `media.keyword` — suppress this keyword category for this partner
  - `reason` Text (required)
  - `created_by` Many2one `res.users`
  - `expires_on` Date — suppressions auto-expire (default 90 days, configurable)
  - `active` Boolean
- Before `_create_alert()`: check active suppressions for `(partner_id, url_domain)` and `(partner_id, keyword_id)` — skip alert if suppression found
- "Mark as False Positive" button on alert: sets `disposition = 'false_match'`, closes alert, and creates a suppression rule
- Suppression expiry cron: marks expired suppressions inactive; logs in `alert.history`

---

## 12. Continuous Monitoring

**Status:** 🟡 Partial  
**Phase:** 1

The system shall:

- Re-screen customers on a configured schedule
- Monitor newly published media
- Alert only on new or materially changed adverse information (skip duplicates)
- Support daily, weekly, monthly, and risk-based monitoring schedules
- Apply more frequent monitoring to high-risk customers automatically

**Current implementation (after B2, B3 fixes):** Daily/weekly/monthly schedule respected via `_compute_next_scan_date`. Cron finds records where `next_scan_date <= now`. Deduplication by title via SQL constraint.

**Gaps:**
- No automatic frequency escalation for high-risk customers
- No content-hash deduplication (B2 fix: frequency; deduplication gaps addressed in §7 above)
- No API quota guard — the cron can exhaust the NewsAPI daily limit silently

**Implementation notes (Phase 1):**
- Add `scan_status = fields.Selection([('idle','Idle'),('queued','Queued'),('running','Running'),('error','Error')], default='idle')` and `last_scan_error = fields.Text` to `adverse.media`
- Add API quota fields to `media.source`: `api_requests_today`, `api_request_limit`, `api_reset_date`; decrement after each call; abort scan gracefully and log when limit reached
- Phase 2: when `partner_id.risk_level` is set to `high`, automatically set `monitoring_frequency = 'daily'` on the corresponding `adverse.media` record

---

## 13. Search Result Display

**Status:** 🟡 Partial  
**Phase:** 2

Each alert record shall display:

- Customer / entity name
- Matched name in article (which alias or name variant triggered the match)
- Match confidence score
- Article title
- Source name and credibility rating
- Publication date
- Article URL
- Country / jurisdiction of source
- Extract / snippet
- Detected adverse category (matched keywords)
- Sentiment / severity indicator
- Article language
- Translation summary (where available)
- Duplicate indicator

**Current implementation:** Alert form shows: partner name, article title, description, content, publication date, risk score (slider), source URL, status bar.

**Gaps:** No source name, credibility, country, matched name display, confidence score, language, duplicate flag. Matched keywords not shown (commented out).

**Implementation notes (Phase 2):**
- Add `matched_keyword_ids = fields.Many2many('media.keyword', string='Matched Keywords')` — restore and populate from `_create_alert()`
- Add `source_id = fields.Many2one('media.source')` once Phase 3 multi-source is implemented; for now store `source_name = fields.Char` (populated from NewsAPI's `source.name` field in article JSON)
- Add `article_language = fields.Char` (from API response)
- Display all new fields in the alert form view; add `matched_keyword_ids` as tags in the list view

---

## 14. Evidence Capture

**Status:** 🟡 Partial  
**Phase:** 2

The system shall preserve:

- Article title, source, URL, and publication date
- Screening date
- Extracted text / snippet
- Screenshot or archived copy (where legally permitted)
- Analyst review notes
- Decision rationale
- Case outcome
- Reviewer approval
- Full audit history

**Current implementation:** Stores title, URL, description, content, `source_date`, `create_date`. Chatter (`mail.thread`) captures field changes. No structured decision rationale field.

**Gaps:** No screenshot/archive; `review_notes` field not yet present; no structured disposition or rationale beyond chatter.

**Implementation notes (Phase 2):**
- `review_notes`, `disposition`, `recommendation` fields (see §9, §10)
- `scan_log_id = fields.Many2one('adverse.media.scan.log')` links each alert to the scan that created it (see Scan History below)
- Archive capability: Phase 3 — store a text snapshot of the article in `content` at scan time (already partially done — `content` field exists); consider adding `content_archived_at` datetime

---

## 15. Reporting & Dashboards

**Status:** ❌ Not Started  
**Phase:** 3

The system shall provide dashboards showing:

- Total adverse media alerts (open, closed, overdue)
- Alerts by risk level and priority
- Alerts by typology (matched keyword category)
- Alerts by customer type
- True positive rate vs false positive rate
- Analyst workload and productivity
- Average case resolution time
- High-risk customers with unresolved adverse media
- Repeat adverse media customers
- EDD recommendations pending
- Regulatory reporting referrals
- Screening volume over time

**Implementation notes (Phase 3):**
- OWL dashboard component under `static/src/components/adverse_media_dashboard/`
- Backed by a PostgreSQL materialized view (pattern already used in `models/res_materialized_views.py`)
- Refresh via scheduled cron alongside existing dashboard refresh job
- Weekly digest email to compliance manager: summary of new/open/overdue alerts for the week

---

## 16. Integration Requirements

**Status:** 🟡 Partial  
**Phase:** 2–3

The module shall integrate with:

| System | Module | Status |
|---|---|---|
| Customer risk assessment | `res.risk.assessment` | 🟡 Partial — confirmed alerts update partner risk score |
| PEP / sanctions screening | `res.customer.screening` | ❌ No trigger link |
| Transaction monitoring | `res.customer.transaction` | ❌ No trigger link |
| Case management | `case_management` | ❌ No alert-to-case link |
| Document management | Odoo `ir.attachment` | ❌ Not wired to alert |
| Regulatory reporting / STR | `nfiu_reporting` | ❌ No link |
| Audit logging | `alert.history` | 🟡 Partial — email notifications logged |
| Notification system | `mail.template` | ✅ Email template implemented |
| User access management | Odoo groups | 🟡 Partial — basic RBAC |
| KYC / onboarding | `res.customer.screening` | ❌ No onboarding trigger |

---

## 17. Configuration Requirements

**Status:** 🟡 Partial  
**Phase:** 2

Administrators shall be able to configure:

- Screening frequency (daily / weekly / monthly / risk-based)
- Risk score thresholds (low / medium / high / maximum)
- Minimum alert threshold (suppress alerts below this score)
- Match thresholds (future: fuzzy match sensitivity)
- Adverse media categories (keyword dictionary)
- Exclusion keyword dictionary (per category)
- Source weighting (credibility score per source)
- Country-risk weighting (future)
- SLA rules (hours to review per risk level)
- Escalation rules (threshold + escalation target)
- Alert suppression rules (false positive whitelist)
- Retention period (days before archived alerts are purged)
- Email notification templates

**Current implementation:** `res.compliance.settings` holds `low_risk_threshold`, `medium_risk_threshold`, `maximum_risk_threshold`. Keywords configurable via UI.

**Gaps:** No SLA config, no minimum alert threshold, no retention setting, no escalation rules, no API quota limit config.

**Implementation notes (Phase 2):**
- Extend `res.compliance.settings` with an "Adverse Media" configuration section:
  - `am_minimum_alert_score` Float
  - `am_sla_low_hours`, `am_sla_medium_hours`, `am_sla_high_hours`, `am_sla_critical_hours` Integer
  - `am_escalation_threshold` Float (auto-escalate alerts above this score)
  - `am_suppression_expiry_days` Integer (default 90)
  - `am_retention_days` Integer (default 2555 = 7 years, per CBN data retention rules)

---

## 18. User Roles

**Status:** 🟡 Partial  
**Phase:** 2

The system shall support the following roles:

| Role | Permissions |
|---|---|
| Compliance Analyst | View alerts; move to `under_review`; add notes; mark false positive |
| Senior Compliance Analyst | All analyst permissions; confirm or close alerts; assign to others |
| Compliance Manager / MLRO | All senior analyst permissions; override decisions; access all records; configure keywords |
| Risk Officer | Read-only access to alerts and risk scores |
| Auditor | Read-only access to all records, scan logs, and audit trail |
| System Administrator | Full configuration access |
| Regulator / External Auditor | Read-only, masked sensitive fields |

**Current implementation:** Basic Odoo RBAC via `ir.model.access.csv`. Officers stored as Many2many on `media.keyword`.

**Gaps:** No granular role separation; no read-only auditor role; no MLRO-specific override capability.

**Implementation notes (Phase 2):**
- Add adverse media groups to `security/groups.xml`:
  - `group_adverse_media_analyst`
  - `group_adverse_media_senior_analyst`
  - `group_adverse_media_manager`
  - `group_adverse_media_auditor`
- Scope view button visibility and `status` transition buttons to appropriate groups

---

## 19. Audit & Compliance

**Status:** 🟡 Partial  
**Phase:** 1–2

The system shall maintain a full audit trail of:

- Every screening scan (date, articles fetched, alerts created, errors)
- All user actions on alerts (status changes, assignments, notes)
- Match decisions and rationale
- Case assignments and escalations
- Configuration changes (keyword edits, threshold changes)
- Manual overrides
- Report downloads and data exports

**Current implementation:** `mail.thread` tracking on `adverse.media`, `adverse.media.alert`, `media.keyword`. Email notifications logged in `alert.history`.

**Gaps:** No structured scan-level audit log; no config change capture.

**Implementation notes (Phase 1):**
- Introduce `adverse.media.scan.log` model:
  - `media_id` Many2one `adverse.media`
  - `scan_date` Datetime
  - `triggered_by` Selection: `cron | manual | event`
  - `articles_fetched` Integer
  - `articles_skipped` Integer (duplicates + suppressed)
  - `alerts_created` Integer
  - `status` Selection: `success | partial | failed`
  - `error_message` Text
- `AdverseMedia` gains `scan_log_ids = fields.One2many('adverse.media.scan.log', 'media_id')`
- Each `scan_news_articles()` call creates one log record on completion regardless of outcome

---

## 20. Data Protection & Security

**Status:** 🟡 Partial  
**Phase:** 2–3

The system shall provide:

- Role-based access control (RBAC)
- Encryption in transit (HTTPS / TLS)
- Secure API key storage (environment variables, not database)
- Data retention controls
- Privacy controls (masking of sensitive data for read-only roles)
- Access logging
- Secure deletion after retention period

**Current implementation:** Standard Odoo RBAC. API keys stored in `.env` (not in DB). No retention automation. No data masking.

**Implementation notes (Phase 3):**
- Retention cron: archive (set `active = False`) alerts older than `am_retention_days`; optionally anonymise partner name in archived records
- API key rotation: `media.source` supports key update without service interruption
- For masked roles (auditor/regulator): override `fields_get()` or use `groups` attribute on sensitive fields

---

## 21. Regulatory Reporting Support

**Status:** 🟡 Partial  
**Phase:** 3

The system shall allow adverse media findings to feed into:

- Enhanced Due Diligence (EDD) workflows
- Suspicious transaction investigation
- Suspicious Transaction Report (STR) preparation
- Customer risk reclassification
- Account restriction decisions
- Customer exit recommendations
- Periodic compliance review evidence
- Regulatory examination evidence packages

**Current implementation:** Confirmed alerts update the partner's `risk_score` and `risk_level` via raw SQL. No link to `nfiu_reporting` or `case_management`.

**Implementation notes (Phase 3):**
- When an alert is confirmed with `priority = 'critical'` or `priority = 'high'`: auto-create an activity on the partner assigned to the MLRO recommending EDD review
- Add "Refer to STR" button on confirmed alert (Phase 3): creates a record in `nfiu_reporting` pre-filled with alert evidence
- Add "Recommend EDD" button: links to `res.customer.edd` module

---

## 22. Non-Functional Requirements

**Status:** 🟡 Partial  
**Phase:** 1 (async), 3 (advanced)

The module shall support:

- High-volume batch screening (hundreds of partners per cron run)
- Real-time single-customer screening (< 10 seconds for manual trigger)
- API-based screening (trigger scan via external call)
- Multi-language processing
- Scalable search infrastructure
- Low-latency response for onboarding flows
- High availability (no single point of failure in scan pipeline)
- Configurable data retention
- Explainable scoring (score breakdown visible to analyst)
- Exportable evidence pack (PDF/ZIP of all alert data for a partner)
- Disaster recovery

**Current implementation:** Synchronous scanning in Odoo HTTP worker. Large batches will timeout. No async processing.

**Implementation notes (Phase 1):**
- Wrap `scan_news_articles()` with OCA `queue_job` `@job` decorator
- `scan_adverse_media()` cron enqueues one job per record rather than calling inline
- Set `scan_status = 'queued'` when enqueued, `'running'` when started, `'idle'` on success, `'error'` on failure with `last_scan_error` populated
- Manual "Run Media Screening" button enqueues a job rather than running synchronously; shows a toast notification that the scan has been queued

---

## 23. Core Workflow

```
1.  Customer onboarded / KYC refreshed / risk event triggered
      └─► adverse.media record created (or already exists)

2.  Scan triggered (cron, manual button, or programmatic event)
      └─► scan_status = 'queued' → job enqueued via queue_job

3.  Job runs: _fetch_from_source(source) for each active media.source
      └─► NewsAPI query: "Partner Name" AND ("keyword1" OR "keyword2" OR ...)
      └─► AKA names included if partner has alias_names

4.  For each article returned:
      a. Check content_hash against existing alerts → skip if duplicate
      b. Check active suppression rules → skip if suppressed
      c. Check minimum_alert_threshold → skip if score too low
      d. Match keywords against article text
      e. Calculate weighted risk score (keyword score × recency × source credibility × article type multiplier)
      f. Create adverse.media.alert with status='new', matched_keyword_ids, priority, score
      g. Log to adverse.media.scan.log

5.  After all articles processed:
      └─► scan_log updated (articles_fetched, alerts_created, status)
      └─► last_scan_date = now (next_scan_date recomputed by _compute_next_scan_date)
      └─► scan_status = 'idle'

6.  If new alerts created:
      └─► _notify_officers(): email sent to responsible officers per matched keyword
      └─► High/Critical alerts: mail.activity created for MLRO

7.  Analyst receives email → opens alert in Odoo
      └─► Sets status = 'under_review'; assigned_officer_id assigned
      └─► Reviews article; adds review_notes

8.  Analyst marks disposition:
      └─► false_match → mark false positive → create suppression rule → close
      └─► true_match → status = 'confirmed'

9.  On confirmation:
      └─► update_partner_risk(): partner.risk_score updated (composite + alert score, capped at max)
      └─► Priority ≥ High: auto-escalate → create case in case_management
      └─► Priority = Critical: notify MLRO via activity; prompt EDD recommendation

10. Case investigated (case_management module)
      └─► Disposition recorded (EDD / restriction / exit / monitoring)
      └─► Evidence stored for regulatory review

11. Alert closed with mandatory review_notes
      └─► Full audit trail in chatter + scan_log + alert.history
```

---

## 24. Technical Implementation Requirements

These are Odoo-specific implementation requirements derived from the code review, ordered by phase.

### Phase 1

| ID | Requirement | Model / File |
|---|---|---|
| TIR-01 | `_prepare_search_query()` returns a NewsAPI boolean string including partner name, aliases, and keyword clause | `adverse_media.py` ✅ Fixed |
| TIR-02 | `content_hash = fields.Char` on `adverse.media.alert`; computed `SHA-256(url + title)`; SQL unique index `(media_id, content_hash)` | `adverse_media.py` |
| TIR-03 | `matched_keyword_ids = fields.Many2many('media.keyword')` on `adverse.media.alert`; populated in `_create_alert()` | `adverse_media.py` |
| TIR-04 | `source_name = fields.Char` on `adverse.media.alert`; populated from `article['source']['name']` in NewsAPI response | `adverse_media.py` |
| TIR-05 | `scan_news_articles()` decorated with `@job`; cron enqueues jobs; manual button enqueues and toasts | `adverse_media.py` |
| TIR-06 | `adverse.media.scan.log` model introduced; one record per scan run | `adverse_media.py` |
| TIR-07 | `scan_status`, `last_scan_error` fields on `adverse.media` | `adverse_media.py` |
| TIR-08 | Alias support: `alias_names = fields.Char` on `res.partner`; included in search query | `res_partner.py` |
| TIR-09 | Exclusion terms: `exclusion_terms = fields.Char` on `media.keyword`; checked in article matching | `adverse_media.py` |
| TIR-10 | API quota fields on `media.source` (once introduced); guard in `_fetch_from_source()` | `adverse_media.py` |

### Phase 2

| ID | Requirement | Model / File |
|---|---|---|
| TIR-11 | `assigned_officer_id`, `review_deadline`, `review_notes`, `days_open` fields on `adverse.media.alert` | `adverse_media.py` |
| TIR-12 | Status transition enforcement: `under_review` requires `assigned_officer_id`; `confirmed`/`closed` require `review_notes` | `adverse_media.py` |
| TIR-13 | SLA breach cron: finds overdue alerts, posts activity to officer and manager | `adverse_media.py` / cron XML |
| TIR-14 | `adverse.media.suppression` model with expiry cron | `adverse_media.py` |
| TIR-15 | `priority`, `article_type`, `resolution_status`, `disposition`, `recommendation` fields on `adverse.media.alert` | `adverse_media.py` |
| TIR-16 | Weighted risk score formula: `keyword_score × recency_factor × article_type_multiplier` | `adverse_media.py` |
| TIR-17 | Adverse media groups in `security/groups.xml` and `ir.model.access.csv` | `security/` |
| TIR-18 | `res.compliance.settings` extended with adverse media configuration section | `res_compliance_settings.py` |

### Phase 3

| ID | Requirement | Model / File |
|---|---|---|
| TIR-19 | `media.source` model with `source_type`, `credibility_score`, `api_requests_today`, `api_request_limit` | `adverse_media.py` |
| TIR-20 | `_fetch_from_source()` dispatcher replacing direct NewsAPI call | `adverse_media.py` |
| TIR-21 | RSS feed fetcher using `feedparser` | `adverse_media.py` |
| TIR-22 | `case_id` link and "Open Case" button on `adverse.media.alert` | `adverse_media.py` / `views/` |
| TIR-23 | Adverse media OWL dashboard component backed by materialized view | `static/src/components/` |
| TIR-24 | Weekly digest email cron to compliance manager | cron XML + mail template |
| TIR-25 | "Refer to STR" button linking to `nfiu_reporting` | `adverse_media.py` / `views/` |
