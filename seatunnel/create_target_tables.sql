-- ============================================================
-- ETL Target Tables — Standalone Schema
-- Creates all 12 tables needed for the 5 SeaTunnel ETL jobs.
--
-- Intended for testing against a fresh PostgreSQL DB that does
-- not have a full Odoo installation.
--
-- What is omitted vs the Odoo-managed DB:
--   - FKs to Odoo core tables (res_users, res_company,
--     res_country, res_currency, ir_attachment, etc.)
--   - Odoo ORM bookkeeping constraints on create_uid/write_uid
--
-- What is kept:
--   - All columns (exact types from production)
--   - PKs and UNIQUE constraints (required by SeaTunnel upserts)
--   - Inter-table FKs between the 12 ETL tables
--
-- Creation order respects FK dependencies:
--   1. account_officers, customer_industry,
--      res_partner_sector, res_partner_account_product, res_branch
--   2. pep_list, sanction_list, res_partner_watchlist (no FK yet)
--   3. res_partner  (refs all of the above)
--   4. ALTER res_partner_watchlist to add FK → res_partner
--   5. res_partner_account
--   6. res_customer_transaction
--   7. customer_digital_product
-- ============================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────
-- 1a. account_officers
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS account_officers_id_seq;

CREATE TABLE IF NOT EXISTS account_officers (
    id          INTEGER   NOT NULL DEFAULT nextval('account_officers_id_seq'),
    create_uid  INTEGER,
    write_uid   INTEGER,
    name        VARCHAR   NOT NULL,
    code        VARCHAR,
    area        VARCHAR,
    email       VARCHAR,
    create_date TIMESTAMP,
    write_date  TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT account_officers_uniq_account_code UNIQUE (code)
);

-- ─────────────────────────────────────────────────────────────
-- 1b. customer_industry
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS customer_industry_id_seq;

CREATE TABLE IF NOT EXISTS customer_industry (
    id          INTEGER   NOT NULL DEFAULT nextval('customer_industry_id_seq'),
    create_uid  INTEGER,
    write_uid   INTEGER,
    name        VARCHAR   NOT NULL,
    code        VARCHAR   NOT NULL,
    active      BOOLEAN,
    create_date TIMESTAMP,
    write_date  TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT customer_industry_uniq_industry_code UNIQUE (code)
);

-- ─────────────────────────────────────────────────────────────
-- 1c. res_partner_sector
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS res_partner_sector_id_seq;

CREATE TABLE IF NOT EXISTS res_partner_sector (
    id              INTEGER   NOT NULL DEFAULT nextval('res_partner_sector_id_seq'),
    risk_assessment INTEGER,
    create_uid      INTEGER,
    write_uid       INTEGER,
    name            VARCHAR   NOT NULL,
    code            VARCHAR   NOT NULL,
    status          VARCHAR,
    active          BOOLEAN,
    create_date     TIMESTAMP,
    write_date      TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT res_partner_sector_uniq_sector_code UNIQUE (code)
);

-- ─────────────────────────────────────────────────────────────
-- 1d. res_partner_account_product
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS res_partner_account_product_id_seq;

CREATE TABLE IF NOT EXISTS res_partner_account_product (
    id                  INTEGER   NOT NULL DEFAULT nextval('res_partner_account_product_id_seq'),
    risk_assessment     INTEGER,
    create_uid          INTEGER,
    write_uid           INTEGER,
    name                VARCHAR   NOT NULL,
    code                VARCHAR,
    product_id          VARCHAR,
    product_category    VARCHAR,
    description         VARCHAR,
    product_type        VARCHAR,
    customer_product_id VARCHAR,
    active              BOOLEAN,
    create_date         TIMESTAMP,
    write_date          TIMESTAMP,
    productclass        VARCHAR,
    PRIMARY KEY (id),
    CONSTRAINT res_partner_account_product_uniq_product_id UNIQUE (product_id)
);

-- ─────────────────────────────────────────────────────────────
-- 1e. res_branch
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS res_branch_id_seq;

CREATE TABLE IF NOT EXISTS res_branch (
    id            INTEGER   NOT NULL DEFAULT nextval('res_branch_id_seq'),
    region_id     INTEGER,
    create_uid    INTEGER,
    write_uid     INTEGER,
    name          VARCHAR,
    code          VARCHAR,
    co_code       VARCHAR,
    region        VARCHAR,
    zone          VARCHAR,
    address       VARCHAR,
    state_located VARCHAR,
    town_area     VARCHAR,
    active        BOOLEAN,
    create_date   TIMESTAMP,
    write_date    TIMESTAMP,
    branch_type   INTEGER,
    PRIMARY KEY (id),
    CONSTRAINT res_branch_uniq_branch_name UNIQUE (name)
);

-- ─────────────────────────────────────────────────────────────
-- 2a. pep_list
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS pep_list_id_seq;

CREATE TABLE IF NOT EXISTS pep_list (
    id          INTEGER   NOT NULL DEFAULT nextval('pep_list_id_seq'),
    create_uid  INTEGER,
    write_uid   INTEGER,
    firstname   VARCHAR,
    lastname    VARCHAR,
    name        VARCHAR,
    unique_id   VARCHAR,
    position    TEXT,
    create_date TIMESTAMP,
    write_date  TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT pep_list_uniq_unique_id UNIQUE (unique_id)
);

-- ─────────────────────────────────────────────────────────────
-- 2b. sanction_list
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS sanction_list_id_seq;

CREATE TABLE IF NOT EXISTS sanction_list (
    id          INTEGER   NOT NULL DEFAULT nextval('sanction_list_id_seq'),
    create_uid  INTEGER,
    write_uid   INTEGER,
    name        VARCHAR,
    sanction_id VARCHAR,
    nationality VARCHAR,
    surname     VARCHAR,
    first_name  VARCHAR,
    middle_name VARCHAR,
    source      VARCHAR,
    active      BOOLEAN,
    create_date TIMESTAMP,
    write_date  TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT sanction_list_sanction_id UNIQUE (sanction_id)
);

-- ─────────────────────────────────────────────────────────────
-- 2c. res_partner_watchlist
--     FK to res_partner is deferred — added after res_partner
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS res_partner_watchlist_id_seq;

CREATE TABLE IF NOT EXISTS res_partner_watchlist (
    id           INTEGER   NOT NULL DEFAULT nextval('res_partner_watchlist_id_seq'),
    customer_id  INTEGER,          -- FK → res_partner(id) added below
    create_uid   INTEGER,
    write_uid    INTEGER,
    name         VARCHAR,
    watchlist_id VARCHAR,
    nationality  VARCHAR,
    surname      VARCHAR,
    first_name   VARCHAR,
    middle_name  VARCHAR,
    bvn          VARCHAR,
    source       VARCHAR,
    create_date  TIMESTAMP,
    write_date   TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT res_partner_watchlist_bvn UNIQUE (bvn)
);

-- ─────────────────────────────────────────────────────────────
-- 3. res_partner  (customers)
--    Self-referential and cross-table FKs declared inline.
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS res_partner_id_seq;

CREATE TABLE IF NOT EXISTS res_partner (
    id                              INTEGER   NOT NULL DEFAULT nextval('res_partner_id_seq'),
    company_id                      INTEGER,
    create_date                     TIMESTAMP,
    name                            VARCHAR,
    title                           INTEGER,
    parent_id                       INTEGER,
    user_id                         INTEGER,
    state_id                        INTEGER,
    country_id                      INTEGER,
    industry_id                     INTEGER,
    color                           INTEGER,
    commercial_partner_id           INTEGER,
    create_uid                      INTEGER,
    write_uid                       INTEGER,
    display_name                    VARCHAR,
    ref                             VARCHAR,
    lang                            VARCHAR,
    tz                              VARCHAR,
    vat                             VARCHAR,
    company_registry                VARCHAR,
    website                         VARCHAR,
    function                        VARCHAR,
    type                            VARCHAR,
    street                          VARCHAR,
    street2                         VARCHAR,
    zip                             VARCHAR,
    city                            VARCHAR,
    email                           VARCHAR,
    phone                           VARCHAR,
    mobile                          VARCHAR,
    commercial_company_name         VARCHAR,
    company_name                    VARCHAR,
    date                            DATE,
    comment                         TEXT,
    partner_latitude                NUMERIC,
    partner_longitude               NUMERIC,
    active                          BOOLEAN,
    employee                        BOOLEAN,
    is_company                      BOOLEAN,
    partner_share                   BOOLEAN,
    write_date                      TIMESTAMP,
    message_main_attachment_id      INTEGER,
    message_bounce                  INTEGER,
    email_normalized                VARCHAR,
    signup_type                     VARCHAR,
    signup_expiration               TIMESTAMP,
    signup_token                    VARCHAR,
    partner_gid                     INTEGER,
    additional_info                 VARCHAR,
    phone_sanitized                 VARCHAR,
    branch_id                       INTEGER,
    education_level_id              INTEGER,
    kyc_limit_id                    INTEGER,
    tier_id                         INTEGER,
    identification_type_id          INTEGER,
    region_id                       INTEGER,
    sector_id                       INTEGER,
    customer_industry_id            INTEGER,
    sex_id                          INTEGER,
    account_officer_id              INTEGER,
    risk_level_id                   INTEGER,
    global_pep_id                   INTEGER,
    likely_pep_match_id             INTEGER,
    likely_watchlist_match_id       INTEGER,
    likely_sanction_match_id        INTEGER,
    likely_global_pep_match_id      INTEGER,
    customer_status                 INTEGER,
    customer_id                     VARCHAR,
    bvn                             VARCHAR,
    identification_number           VARCHAR,
    dob                             VARCHAR,
    firstname                       VARCHAR,
    short_name                      VARCHAR,
    lastname                        VARCHAR,
    middlename                      VARCHAR,
    othername                       VARCHAR,
    town                            VARCHAR,
    registration_date               VARCHAR,
    risk_level                      VARCHAR,
    internal_category               VARCHAR,
    anti_bribery_file_name          VARCHAR,
    data_protection_file_name       VARCHAR,
    whistle_blowing_file_name       VARCHAR,
    anti_money_laundering_file_name VARCHAR,
    address                         VARCHAR,
    customer_title                  VARCHAR,
    gender                          VARCHAR,
    marital_status                  VARCHAR,
    employment_status               VARCHAR,
    state_residence                 VARCHAR,
    nin                             VARCHAR,
    customer_rating                 VARCHAR,
    origin                          VARCHAR,
    first_risk_rating               VARCHAR,
    pep                             VARCHAR,
    customer_phone                  VARCHAR,
    branch_code                     VARCHAR,
    identification_expiry_date      DATE,
    company_reg_date                DATE,
    risk_score                      NUMERIC,
    composite_risk_score            NUMERIC,
    is_pep                          BOOLEAN,
    is_watchlist                    BOOLEAN,
    is_fep                          BOOLEAN,
    is_blacklist                    BOOLEAN,
    global_pep                      BOOLEAN,
    is_greylist                     BOOLEAN,
    likely_sanction                 BOOLEAN,
    likely_pep                      BOOLEAN,
    screening_needed                BOOLEAN,
    last_risk_calculation           TIMESTAMP,
    last_screening_date             TIMESTAMP,
    customertype                    INTEGER,
    nationality                     INTEGER,
    status                          INTEGER,
    town_id                         INTEGER,
    officer_code                    INTEGER,
    occupation                      VARCHAR,
    phone1                          VARCHAR,
    date_opened                     DATE,
    identification_issue_date       DATE,

    PRIMARY KEY (id),
    CONSTRAINT res_partner_uniq_customer_id UNIQUE (customer_id),
    CONSTRAINT res_partner_check_name CHECK (
        (((type)::text = 'contact'::text) AND (name IS NOT NULL))
        OR ((type)::text <> 'contact'::text)
    ),
    -- Self-referential
    CONSTRAINT res_partner_parent_id_fkey
        FOREIGN KEY (parent_id)             REFERENCES res_partner(id) ON DELETE SET NULL,
    CONSTRAINT res_partner_commercial_partner_id_fkey
        FOREIGN KEY (commercial_partner_id) REFERENCES res_partner(id) ON DELETE SET NULL,
    -- Inter-ETL-table
    CONSTRAINT res_partner_sector_id_fkey
        FOREIGN KEY (sector_id)             REFERENCES res_partner_sector(id)   ON DELETE SET NULL,
    CONSTRAINT res_partner_customer_industry_id_fkey
        FOREIGN KEY (customer_industry_id)  REFERENCES customer_industry(id)    ON DELETE SET NULL,
    CONSTRAINT res_partner_account_officer_id_fkey
        FOREIGN KEY (account_officer_id)    REFERENCES account_officers(id)     ON DELETE SET NULL,
    CONSTRAINT res_partner_branch_id_fkey
        FOREIGN KEY (branch_id)             REFERENCES res_branch(id)           ON DELETE SET NULL,
    CONSTRAINT res_partner_likely_pep_match_id_fkey
        FOREIGN KEY (likely_pep_match_id)       REFERENCES pep_list(id)              ON DELETE SET NULL,
    CONSTRAINT res_partner_likely_sanction_match_id_fkey
        FOREIGN KEY (likely_sanction_match_id)  REFERENCES sanction_list(id)         ON DELETE SET NULL,
    CONSTRAINT res_partner_likely_watchlist_match_id_fkey
        FOREIGN KEY (likely_watchlist_match_id) REFERENCES res_partner_watchlist(id) ON DELETE SET NULL
);

-- Resolve circular dependency: watchlist → partner
ALTER TABLE res_partner_watchlist
    ADD CONSTRAINT res_partner_watchlist_customer_id_fkey
    FOREIGN KEY (customer_id) REFERENCES res_partner(id) ON DELETE SET NULL;

-- ─────────────────────────────────────────────────────────────
-- 4. res_partner_account  (bank accounts)
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS res_partner_account_id_seq;

CREATE TABLE IF NOT EXISTS res_partner_account (
    id                          INTEGER   NOT NULL DEFAULT nextval('res_partner_account_id_seq'),
    message_main_attachment_id  INTEGER,
    customer_id                 INTEGER,
    account_officer_id          INTEGER,
    currency_id                 INTEGER,
    product_id                  INTEGER,
    ledger_id                   INTEGER,
    branch_id                   INTEGER,
    account_type_id             INTEGER,
    risk_assessment             INTEGER,
    num_credit_last6m           INTEGER,
    num_debit_last6m            INTEGER,
    num_credit_last1y           INTEGER,
    num_debit_last1y            INTEGER,
    create_uid                  INTEGER,
    write_uid                   INTEGER,
    name                        VARCHAR,
    account_name                VARCHAR,
    account_position            VARCHAR,
    account_type                VARCHAR,
    account_code                VARCHAR,
    account_status              VARCHAR,
    currency                    VARCHAR,
    category                    VARCHAR,
    category_description        VARCHAR,
    closure_status              VARCHAR,
    branch_code                 VARCHAR,
    state                       VARCHAR,
    customer                    VARCHAR,
    date_last_credit_customer   VARCHAR,
    amount_last_credit_customer VARCHAR,
    date_last_debit_customer    VARCHAR,
    last_transaction_date       DATE,
    opening_date                DATE,
    balance                     NUMERIC,
    avg_credit_last6m           NUMERIC,
    max_credit_last6m           NUMERIC,
    tot_credit_last6m           NUMERIC,
    avg_debit_last6m            NUMERIC,
    max_debit_last6m            NUMERIC,
    tot_debit_last6m            NUMERIC,
    avg_credit_last1y           NUMERIC,
    max_credit_last1y           NUMERIC,
    tot_credit_last1y           NUMERIC,
    avg_debit_last1y            NUMERIC,
    max_debit_last1y            NUMERIC,
    tot_debit_last1y            NUMERIC,
    max_debit_daily             NUMERIC,
    overdraft_limit             NUMERIC,
    uncleared_balance           NUMERIC,
    start_year_balance          NUMERIC,
    high_transactions_account   BOOLEAN,
    is_joint_account            BOOLEAN,
    create_date                 TIMESTAMP,
    write_date                  TIMESTAMP,
    officercode                 INTEGER,
    sectorcode                  INTEGER,
    product_type_id             INTEGER,
    account_tier                INTEGER,
    lnbalance                   VARCHAR,
    bkbalance                   VARCHAR,
    unclearedbal                VARCHAR,
    holdbal                     VARCHAR,
    totdebit                    VARCHAR,
    totcredit                   VARCHAR,
    last_month_balance          VARCHAR,
    lien                        VARCHAR,
    source_account_id           VARCHAR,
    accounttitle                VARCHAR,
    account_class               VARCHAR,
    freeze_code                 VARCHAR,
    cleared_balance             VARCHAR,
    bvn                         VARCHAR,
    date_closed                 DATE,
    "Status"                    BOOLEAN,

    PRIMARY KEY (id),
    CONSTRAINT res_partner_account_uniq_account_id UNIQUE (name),
    -- Inter-ETL-table
    CONSTRAINT res_partner_account_customer_id_fkey
        FOREIGN KEY (customer_id)       REFERENCES res_partner(id)              ON DELETE SET NULL,
    CONSTRAINT res_partner_account_account_officer_id_fkey
        FOREIGN KEY (account_officer_id) REFERENCES account_officers(id)        ON DELETE SET NULL,
    CONSTRAINT res_partner_account_branch_id_fkey
        FOREIGN KEY (branch_id)         REFERENCES res_branch(id)               ON DELETE SET NULL,
    CONSTRAINT res_partner_account_product_type_id_fkey
        FOREIGN KEY (product_type_id)   REFERENCES res_partner_account_product(id) ON DELETE SET NULL,
    CONSTRAINT res_partner_account_sectorcode_fkey
        FOREIGN KEY (sectorcode)        REFERENCES res_partner_sector(id)       ON DELETE SET NULL
);

-- ─────────────────────────────────────────────────────────────
-- 5. res_customer_transaction
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS res_customer_transaction_id_seq;

CREATE TABLE IF NOT EXISTS res_customer_transaction (
    id                         INTEGER   NOT NULL DEFAULT nextval('res_customer_transaction_id_seq'),
    message_main_attachment_id INTEGER,
    account_id                 INTEGER,
    currency_id                INTEGER,
    customer_id                INTEGER,
    branch_id                  INTEGER,
    tran_type                  INTEGER,
    rule_id                    INTEGER,
    account_officer_id         INTEGER,
    create_uid                 INTEGER,
    write_uid                  INTEGER,
    name                       VARCHAR,
    batch_code                 VARCHAR,
    state                      VARCHAR,
    trans_code                 VARCHAR,
    currency                   VARCHAR,
    inputter                   VARCHAR,
    authorizer                 VARCHAR,
    transaction_type           VARCHAR,
    branch_code                VARCHAR,
    narration                  TEXT,
    amount                     NUMERIC,
    date_created               TIMESTAMP,
    create_date                TIMESTAMP,
    write_date                 TIMESTAMP,
    created_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deptcode                   INTEGER,
    status                     INTEGER,
    tran_channel               INTEGER,
    refno                      VARCHAR,
    valuedate                  VARCHAR,
    actualdate                 VARCHAR,
    userid                     VARCHAR,
    reversal                   VARCHAR,
    accountmodule              VARCHAR,
    tellerno                   VARCHAR,
    request_id                 VARCHAR,
    trans_id                   VARCHAR,
    account_group              VARCHAR,
    transaction_mode           VARCHAR,
    parent_ledger_id           VARCHAR,
    sub_general_ledger_code    VARCHAR,
    source_branch_code         VARCHAR,
    posted_by                  VARCHAR,
    initiated_by               VARCHAR,
    account_name               VARCHAR,

    PRIMARY KEY (id),
    CONSTRAINT res_customer_transaction_uniq_trans_id   UNIQUE (trans_id),
    CONSTRAINT res_customer_transaction_uniq_trans_name UNIQUE (name),
    -- Inter-ETL-table
    CONSTRAINT res_customer_transaction_account_id_fkey
        FOREIGN KEY (account_id)         REFERENCES res_partner_account(id) ON DELETE SET NULL,
    CONSTRAINT res_customer_transaction_customer_id_fkey
        FOREIGN KEY (customer_id)        REFERENCES res_partner(id)         ON DELETE SET NULL,
    CONSTRAINT res_customer_transaction_account_officer_id_fkey
        FOREIGN KEY (account_officer_id) REFERENCES account_officers(id)   ON DELETE SET NULL,
    CONSTRAINT res_customer_transaction_branch_id_fkey
        FOREIGN KEY (branch_id)          REFERENCES res_branch(id)         ON DELETE SET NULL
);

-- ─────────────────────────────────────────────────────────────
-- 6. customer_digital_product  (no inter-table FKs)
-- ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS customer_digital_product_id_seq;

CREATE TABLE IF NOT EXISTS customer_digital_product (
    id               INTEGER   NOT NULL DEFAULT nextval('customer_digital_product_id_seq'),
    create_uid       INTEGER,
    write_uid        INTEGER,
    customer_name    VARCHAR,
    customer_segment VARCHAR,
    ussd             VARCHAR,
    carded_customer  VARCHAR,
    alt_bank         VARCHAR,
    customer_id      TEXT,
    create_date      TIMESTAMP,
    write_date       TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT customer_digital_product_customer_id_key UNIQUE (customer_id)
);

COMMIT;
