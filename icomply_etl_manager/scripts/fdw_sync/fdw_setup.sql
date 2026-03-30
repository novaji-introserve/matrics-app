-- ============================================
-- FDW Setup Script (Run Once Manually)
-- ============================================
-- This script sets up Foreign Data Wrappers for Postgres-to-Postgres syncs
-- Run this on the TARGET database (db that icomply App uses)
-- ============================================

-- Step 1: Create schema for foreign tables
CREATE SCHEMA IF NOT EXISTS seabaas;

-- Step 2: Install FDW extension (if not already installed)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- ============================================
-- SERVER 1: ActiveTransaction, Accounts, ActiveLien
-- (All 3 tables are in the same source database)
-- ============================================

-- Step 3: Create foreign server (connection to source DB 1)
CREATE SERVER txn_db_server 
FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS (
    host 'SOURCE_DB_1_IP',        -- Replace with actual source IP
    dbname 'SOURCE_DB_1_NAME',    -- Replace with actual database name
    port '5432/6432'
);

-- Step 4: Create user mapping (credentials for source DB 1)
CREATE USER MAPPING FOR current_user 
SERVER txn_db_server 
OPTIONS (
    user 'SOURCE_DB_1_USER',      -- Replace with actual username
    password 'SOURCE_DB_1_PASSWORD'  -- Replace with actual password
);

-- Step 5: Import foreign tables for Server 1
IMPORT FOREIGN SCHEMA public 
LIMIT TO (ActiveTransaction, Accounts, ActiveLiens) 
FROM SERVER txn_db_server 
INTO seabaas;

-- ============================================
-- SERVER 2: customer_profile
-- (Different source database for customer data)
-- ============================================

-- Step 6: Create foreign server (connection to source DB 2)
CREATE SERVER customer_db_server 
FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS (
    host 'SOURCE_DB_2_IP',        -- Replace with actual source IP
    dbname 'SOURCE_DB_2_NAME',    -- Replace with actual database name
    port '5432/6432'
);

-- Step 7: Create user mapping (credentials for source DB 2)
CREATE USER MAPPING FOR current_user 
SERVER customer_db_server 
OPTIONS (
    user 'SOURCE_DB_2_USER',      -- Replace with actual username
    password 'SOURCE_DB_2_PASSWORD'  -- Replace with actual password
);

-- Step 8: Import foreign table for Server 2
IMPORT FOREIGN SCHEMA public 
LIMIT TO (customer_profile) 
FROM SERVER customer_db_server 
INTO seabaas;

-- ============================================
-- SERVER 3: AuditLog
-- (Different source database)
-- ============================================

-- Step 9: Create foreign server (connection to source DB 3)
CREATE SERVER audit_db_server 
FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS (
    host 'SOURCE_DB_3_IP',        -- Replace with actual IP
    dbname 'SOURCE_DB_3_NAME',    -- Replace with actual database name
    port '5432/6432'
);

-- Step 10: Create user mapping (credentials for source DB 3)
CREATE USER MAPPING FOR current_user 
SERVER audit_db_server 
OPTIONS (
    user 'SOURCE_DB_3_USER',      -- Replace with actual username
    password 'SOURCE_DB_3_PASSWORD'  -- Replace with actual password
);

-- Step 11: Import foreign table for Server 3
IMPORT FOREIGN SCHEMA public 
LIMIT TO (audit_log) 
FROM SERVER audit_db_server 
INTO seabaas;

-- ============================================
-- Verification
-- ============================================
-- After running this script, verify foreign tables exist:
-- SELECT * FROM seabaas.ActiveTransaction LIMIT 1;
-- SELECT * FROM seabaas.Accounts LIMIT 1;
-- SELECT * FROM seabaas.ActiveLiens LIMIT 1;
-- SELECT * FROM seabaas.customer_profile LIMIT 1;
-- SELECT * FROM seabaas.audit_log LIMIT 1;

-- ============================================
-- Notes:
-- ============================================
-- 1. Replace all placeholder values (SOURCE_DB_X_IP, etc.) with actual values
-- 2. Ensure source databases allow connections from target database IP
-- 3. Ensure source database users have SELECT permissions on the tables
-- 4. Foreign tables will be created in the 'seabaas' schema
-- 5. You can query foreign tables like normal tables: SELECT * FROM seabaas.ActiveTransaction

