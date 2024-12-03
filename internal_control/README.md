# Internal Control System Setup Guide

## Overview

This document provides instructions for setting up the transaction monitoring system's database requirements, including importing transaction data and performing necessary column modifications.

## Prerequisites

- PostgreSQL installed and running
- Administrative access to the database
- Source CSV file: `tbl_transactions_202411212116.csv`

## Database Setup Instructions

### 1. Table Import and Column Modifications

First, we'll import the transactions table and modify the columns for proper data typing. Execute the following commands in sequence:

```sql
-- Import data from CSV file
\COPY tbl_transactions 
FROM '/users/user/tbl_transactions_202411212116.csv' 
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    ENCODING 'UTF8'
);

-- Modify column data types
ALTER TABLE tbl_transactions ALTER COLUMN "AccountNumber" TYPE BIGINT;
ALTER TABLE tbl_transactions ALTER COLUMN "chkNUm" TYPE BIGINT;
ALTER TABLE tbl_transactions ALTER COLUMN "Narration" TYPE character varying(255);

-- Rename ID column to lowercase
ALTER TABLE tbl_transactions RENAME COLUMN "ID" TO "id";
```

### 2. Data Modification

After importing and structuring the table, update specific records with crypto-related information:

```sql
-- Update specific transaction record with crypto prefix
UPDATE "tbl_transactions" 
SET "Narration" = CONCAT('crypto ', "Narration") 
WHERE "id" = 1029957;
```

## Verification Steps

After completing the setup, verify the following:

1. Check if the table was imported successfully:

```sql
SELECT COUNT(*) FROM tbl_transactions;
```

1. Verify column data types:

```sql
\d tbl_transactions
```

1. Confirm the crypto update:

```sql
SELECT "id", "Narration" 
FROM tbl_transactions 
WHERE "id" = 1029957;
```

## Troubleshooting

If you encounter any issues:

1. **Import Errors**
   - Verify the CSV file path is correct
   - Check file permissions
   - Ensure CSV formatting matches the specified delimiter and encoding

2. **Data Type Conversion Errors**
   - Check for non-numeric values in "AccountNumber" and "chkNUm" columns
   - Verify "Narration" field lengths don't exceed 255 characters

## Notes

- Backup your database before performing these operations
- Ensure sufficient disk space for the import operation
- Monitor the database logs for any errors during the process
- Do not alter the order in which views are called in the manifest.
- Always ensure that actions are loaded before menus.
- This means that if you're adding actions in your views, they must be loaded prior to the menus.
