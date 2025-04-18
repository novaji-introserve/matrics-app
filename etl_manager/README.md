# iComply ETL Manager

**Version:** 2.0  
**Author:** Olumide Awodeji (Synth corp)  
**Website:** <https://www.cybercraftsmen.tech>

## Overview

iComply ETL Manager is an enterprise-grade Extract, Transform, Load (ETL) solution for Odoo that enables seamless data integration across multiple database systems. It provides a robust framework for synchronizing data between different systems with high performance, reliability, and ease of use.

## Key Features

- **Multi-Database Support:** Connect to PostgreSQL, MySQL, MSSQL, Oracle, SQLite, Snowflake, Google BigQuery, IBM DB2, and more
- **Flexible Mapping:** Configure table mappings with column transformations and lookups
- **Scheduled Synchronization:** Automate data sync with configurable schedules
- **Performance Optimized:** Efficient batch operations and smart chunking for large datasets
- **Comprehensive Dashboard:** Monitor ETL processes with detailed statistics and logs
- **High Reliability:** Robust error handling and recovery mechanisms

## Installation

### Prerequisites

Before installing the module, ensure you have:

1. Odoo 16.0 or newer
2. Python 3.9 or newer
3. OCA's queue_job module

### Python Dependencies Installation

Install the core Python packages required by the module:

```bash
pip install pyodbc
pip install backoff
pip install mysql-connector-python
pip install psycopg2-binary
pip install cryptography==36.0.0  # Required version for compatibility
pip install pyOpenSSL==22.0.0     # Required version for compatibility
```

### Database Driver Installation

#### PostgreSQL

PostgreSQL support is typically included with `psycopg2`:

```bash
pip install psycopg2-binary
```

#### MySQL

MySQL support is provided by the MySQL connector:

```bash
pip install mysql-connector-python
```

#### Microsoft SQL Server (MSSQL)

MSSQL requires both the Python ODBC package and the appropriate ODBC drivers:

```bash
pip install pyodbc
```

##### MSSQL on macOS

```bash
# Install unixODBC
brew install unixodbc

# Verify it's installed
ls /usr/local/opt/unixodbc/lib/libodbc.2.dylib

# Install Microsoft ODBC driver for SQL Server
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18

# Verify driver is installed
ls -la /usr/local/Cellar/msodbcsql18/18.5.1.1/lib

# Verify driver is registered
odbcinst -q -d
```

##### MSSQL on CentOS/RHEL 8 or 9

```bash
# Check if your RHEL/CentOS version is supported
if ! [[ "8 9" == *"$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2 | cut -d '.' -f 1)"* ]];
then
 echo "RHEL $(grep VERSION_ID /etc/os-release | cut -d '"' -f 2 | cut -d '.' -f 1) is not currently supported.";
 exit;
fi

# Download the Microsoft repository configuration package
curl -sSL -O https://packages.microsoft.com/config/rhel/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2 | cut -d '.' -f 1)/packages-microsoft-prod.rpm

# Install the package
sudo yum install packages-microsoft-prod.rpm

# Delete the package file
rm packages-microsoft-prod.rpm

# Remove potential conflicting packages
sudo yum remove unixODBC-utf16 unixODBC-utf16-devel

# Install the ODBC driver
sudo ACCEPT_EULA=Y yum install -y msodbcsql18

# Optional: Install tools for bcp and sqlcmd
sudo ACCEPT_EULA=Y yum install -y mssql-tools18
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc

# Optional: Install unixODBC development headers
sudo yum install -y unixODBC-devel
```

##### MSSQL on Ubuntu 20.04, 22.04, 24.04, or 24.10

```bash
# Check if your Ubuntu version is supported
if ! [[ "20.04 22.04 24.04 24.10" == *"$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)"* ]];
then
 echo "Ubuntu $(grep VERSION_ID /etc/os-release | cut -d '"' -f 2) is not currently supported.";
 exit;
fi

# Download the Microsoft repository configuration package
curl -sSL -O https://packages.microsoft.com/config/ubuntu/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)/packages-microsoft-prod.deb

# Install the package
sudo dpkg -i packages-microsoft-prod.deb

# Delete the package file
rm packages-microsoft-prod.deb

# Update package lists and install the ODBC driver
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Optional: Install tools for bcp and sqlcmd
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc

# Optional: Install unixODBC development headers
sudo apt-get install -y unixodbc-dev
```

##### MSSQL on Windows

On Windows, download and install the Microsoft ODBC Driver directly from Microsoft:
[Download Microsoft ODBC Driver 18 for SQL Server](https://go.microsoft.com/fwlink/?linkid=2307162)

#### Oracle Database

Oracle requires both the Python connector and the Oracle Instant Client:

```bash
pip install cx_Oracle
```

Download and install Oracle Instant Client from the official Oracle website:
[Oracle Instant Client Downloads](https://www.oracle.com/database/technologies/instant-client.html)

Follow Oracle's installation instructions for your platform.

#### Google BigQuery

```bash
pip install google-cloud-bigquery
```

You'll also need to set up authentication - refer to Google's documentation for setting up service account credentials.

#### Snowflake

```bash
pip install snowflake-connector-python
```

#### IBM DB2

```bash
pip install ibm_db
```

IBM DB2 may require additional IBM CLI/ODBC drivers, which are platform-dependent. Refer to IBM's documentation for your specific platform.

### Odoo Module Installation

1. Download the module and place it in your Odoo addons directory
2. Install the OCA Queue Job module:

   ```bash
   git clone https://github.com/OCA/queue.git
   ```

3. Restart your Odoo server
4. Go to Apps and install "iComply ETL Manager"

## Configuration

### Initial Setup

1. Go to "ETL Manager" > "Configuration" > "Database Types"
   - Review the pre-configured database types
   - Add custom database types if needed

2. Go to "ETL Manager" > "Configuration" > "Database Connections"
   - Add connections to your source and target databases
   - Test connections to ensure they're working properly

3. Define synchronization frequencies at "ETL Manager" > "Configuration" > "Frequencies"
   - Configure hourly, daily, weekly, or custom schedules

### Configuring ETL Tables

1. Go to "ETL Manager" > "Configuration" > "ETL Tables"
2. Create a new record:
   - Set the source table name
   - Set the target table name
   - Select source and target database connections
   - Define primary key for synchronization
   - Set batch size (default 2000, adjust based on table size)
   - Specify category and synchronization frequency
   - Configure dependencies if this table depends on other tables being synchronized first

3. Configure Column Mappings:
   - Add mappings for each column from source to target
   - Use direct mappings for 1:1 transfers
   - Use lookup mappings for foreign key relationships

### Example Configuration

**Source Table:** `customer_data` (MSSQL)  
**Target Table:** `res_partner` (Odoo PostgreSQL)  
**Primary Key:** `customer_id`  
**Batch Size:** 2000  
**Frequency:** Daily

**Column Mappings:**

- `customer_id` (Direct) -> `id`
- `customer_name` (Direct) -> `name`
- `email` (Direct) -> `email`
- `phone` (Direct) -> `phone`
- `country_code` (Lookup) -> `country_id`
  - Lookup Table: `res_country`
  - Lookup Key: `code`
  - Lookup Value: `id`
- `state_code` (Lookup) -> `state_id`
  - Lookup Table: `res_country_state`
  - Lookup Key: `code`
  - Lookup Value: `id`

## Usage

### Manual Synchronization

1. Go to "ETL Manager" > "ETL Tables"
2. Select the table you want to synchronize
3. Click "Sync Now" to start the synchronization process
4. Monitor the progress in real-time
5. Review logs after completion

### Automatic Synchronization

Tables will be synchronized automatically based on their configured frequency. You can monitor these jobs in:

1. "ETL Manager" > "Dashboard" - For overall status
2. "ETL Manager" > "Sync Logs" - For detailed synchronization logs

### Dashboard

The dashboard provides an overview of:

- Synchronization status for all tables
- Success rates and statistics
- Last synchronization details
- Quick access to table configuration and logs

### Monitoring & Logs

1. Go to "ETL Manager" > "Sync Logs" to view detailed logs of all synchronization jobs
2. Filter logs by:
   - Table
   - Status (Success, Failed, Running)
   - Date
3. Drill down into individual logs to see:
   - Total records processed
   - New records created
   - Existing records updated
   - Error details (if any)

## Advanced Features

### Chunking Large Tables

For very large tables (>500,000 rows), the system automatically splits them into chunks for more efficient processing. You can configure:

1. **Batch Size:** Number of records to process in a single database operation
2. **Base Tables:** Mark tables with no dependencies to process them first

### Dependency Management

Tables often have dependencies on other tables. Configure these dependencies to ensure proper synchronization order:

1. Set dependencies in the "Dependencies" tab when configuring a table
2. The system will automatically process tables in the correct order

### Performance Optimization

Adjust these settings for optimal performance:

1. **Batch Size:** Default is 2000, increase for simple tables, decrease for complex ones
2. **Connection Pool:** The system automatically manages connection pools for optimal performance

## Troubleshooting

### Common Issues

1. **Connection Errors:**
   - Verify database credentials are correct
   - Ensure the database server is accessible from the Odoo server
   - Check that required database drivers are installed
   - Confirm driver registration: `odbcinst -q -d` (for ODBC drivers)

2. **Processing Errors:**
   - Review sync logs for specific error messages
   - Check column mappings for consistency
   - Verify data types are compatible between source and target columns

3. **Performance Issues:**
   - Adjust batch size for better performance
   - Ensure proper indexing on source and target tables
   - Consider chunking for very large tables

### Specific Errors and Solutions

#### ModuleNotFoundError: No module named 'cryptography.hazmat.backends.openssl.x509'

This error often occurs with newer versions of cryptography and pyOpenSSL. Solution:

```bash
pip install cryptography==36.0.0
pip install pyOpenSSL==22.0.0
```

#### ODBC Driver Manager not found

Make sure unixODBC is properly installed:

```bash
# On macOS
brew install unixodbc

# On Ubuntu
sudo apt-get install unixodbc unixodbc-dev

# On CentOS/RHEL
sudo yum install unixODBC unixODBC-devel
```

#### Connection timeout errors

Check if the database server is accessible from your Odoo server:

```bash
# For PostgreSQL
psql -h <host> -p <port> -U <username> -d <database>

# For MySQL
mysql -h <host> -P <port> -u <username> -p <database>

# For MSSQL
sqlcmd -S <host>,<port> -U <username> -P <password> -d <database>
```

### Support

For additional support, contact:

- Email: <olumide.awodeji@hotmail.com>
- Website: <https://www.cybercraftsmen.tech>

## License

iComply ETL Manager is licensed under LGPL-3.
