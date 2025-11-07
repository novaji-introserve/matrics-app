# Risk Analysis System

A high-performance, concurrent customer risk scoring system built in Go, designed to process millions of customers efficiently with full resumability and monitoring capabilities.

## Features

- **High Performance**: Process millions of customers in hours instead of days
- **Concurrent Processing**: Configurable worker pools with optimal parallelism
- **Resumability**: Checkpoint system allows resuming from interruptions
- **Incremental Updates**: Track processed customers to avoid reprocessing
- **Function-Based Scoring**: Dynamic risk function execution with caching
- **Composite Risk Analysis**: Multi-universe weighted risk calculations
- **Graceful Shutdown**: Safe interrupt handling with checkpoint saving
- **Comprehensive Logging**: Detailed progress tracking and error reporting

## Table of Contents

- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Performance Tuning](#performance-tuning)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Architecture

The system follows Clean Architecture principles with clear separation of concerns:

```bash
risk_analysis/
├── cmd/risk-processor/       # Application entry point
├── config/                   # Configuration management
├── application/              # Business orchestration layer
├── domain/
│   ├── models/              # Business entities
│   ├── services/            # Core business logic
│   └── repositories/        # Repository interfaces
├── infrastructure/
│   ├── database/            # Database connections
│   ├── cache/               # File-based caching
│   └── repository/          # Repository implementations
├── workers/                 # Concurrent processing
└── utils/                   # Shared utilities
```

### Key Components

1. **RiskProcessor**: Orchestrates the entire risk calculation workflow
2. **BatchedFunctionRiskCalculator**: Core calculation engine with caching
3. **CachedFunctionExecutor**: Executes database functions from memory cache
4. **WorkerPool**: Manages concurrent processing of customer batches
5. **CustomerIDCache**: File-based caching for incremental processing

## Installation

### Prerequisites

- Go 1.22.2 or higher
- PostgreSQL 13+
- Linux/Unix environment (for file paths in config)

### Build from Source

```bash
# Clone the repository
cd risk_analysis

# Download dependencies
go mod download

# Build the binary
go build -o risk-processor cmd/risk-processor/main.go

# Make executable
chmod +x risk-processor
```

### Verify Installation

```bash
./risk-processor --help
```

## Configuration

Configuration is managed through an INI file located at:

```
/data/odoo/ETL_script/update_script/settings.conf
```

### Configuration File Structure

```ini
[database]
host = localhost
port = 5432
dbname = your_database
user = your_username
password = your_password
ssl_mode = require
pool_min = 10
pool_max = 50
pool_max_idle_time = 300
pool_max_lifetime = 3600
connect_timeout = 10
query_timeout = 30

[risk_analysis]
# Processing Settings
batch_size = 1000
worker_count = 20
workers_per_batch = 2
chunk_size = 10000
enable_bulk_operations = true
bulk_insert_batch_size = 500
progress_checkpoint_interval = 10000

# Cache Settings
cache_directory = /tmp
customer_id_cache_file = /tmp/customer_ids.cache
processed_customers_file = /tmp/processed_customers.txt
risk_functions_cache_file = /tmp/risk_functions.json
risk_metadata_cache_file = /tmp/risk_calculator_metadata.json

# Logging Settings
log_level = INFO
log_format = json
log_output = both
log_file = /var/log/risk-processor.log
log_max_size = 100
log_max_backups = 5
log_max_age = 30

# Retry Settings
max_retries = 3
retry_initial_interval = 1
retry_max_interval = 10
retry_multiplier = 2

# Execution Control
dry_run = false
resume_from_checkpoint = false
checkpoint_file = /tmp/risk-processor-checkpoint.json
```

### Key Configuration Parameters

#### Database Connection Pool

- **pool_min**: Minimum connections (default: 10)
- **pool_max**: Maximum connections (default: 50)
- **pool_max_idle_time**: Idle timeout in seconds (default: 300)
- **pool_max_lifetime**: Connection lifetime in seconds (default: 3600)

#### Processing Performance

- **batch_size**: Customers per transaction batch (default: 1000)
- **worker_count**: Concurrent batch processors (default: 20)
- **workers_per_batch**: Workers within each batch (default: 2)
- **progress_checkpoint_interval**: Save checkpoint every N customers (default: 10000)

#### Caching

All cache files enable faster restarts and incremental processing:

- **customer_id_cache_file**: Cached list of all customer IDs
- **processed_customers_file**: Track completed customers
- **risk_functions_cache_file**: Cached database function definitions
- **risk_metadata_cache_file**: Cached plans and settings

## 📖 Usage

### Basic Usage

```bash
# Run with default settings from config file
./risk-processor

# Run with increased concurrency
./risk-processor --workers=50

# Process specific customers only
./risk-processor --customer-ids=1000,1001,1002
```

### Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `--dry-run` | Run without database updates | `--dry-run` |
| `--customer-ids` | Process specific customers | `--customer-ids=1,2,3` |
| `--workers` | Override worker count | `--workers=50` |
| `--batch-size` | Override batch size | `--batch-size=500` |
| `--resume-from-checkpoint` | Resume from last checkpoint | `--resume-from-checkpoint` |
| `--help` | Show help message | `--help` |

### Common Scenarios

#### Full Processing Run

```bash
./risk-processor
```

#### Resume After Interruption

```bash
./risk-processor --resume-from-checkpoint
```

#### Test Run Without Updates

```bash
./risk-processor --dry-run --customer-ids=1,2,3
```

#### High-Performance Processing

```bash
./risk-processor --workers=100 --batch-size=2000
```

### Graceful Shutdown

Press `Ctrl+C` once to trigger graceful shutdown:

- Completes current batches
- Saves checkpoint
- Logs final statistics

Press `Ctrl+C` twice to force immediate exit (not recommended).

## 🎯 Performance Tuning

### Worker Count Guidelines

| Total Customers | CPU Cores | Recommended Workers | Expected Duration |
|----------------|-----------|---------------------|-------------------|
| < 100,000 | 4 | 8-16 | < 5 minutes |
| 100,000 - 1M | 8 | 20-32 | 10-30 minutes |
| 1M - 5M | 16 | 32-64 | 1-3 hours |
| > 5M | 32+ | 64-128 | 3-8 hours |

### Memory Considerations

**Batch Size vs Memory Usage:**

- Small batches (500-1000): Lower memory, more DB overhead
- Medium batches (1000-2000): Balanced (recommended)
- Large batches (2000-5000): Higher memory, fewer DB calls

**Memory Requirements:**

- Base: ~500MB
- Per 1000 batch size: ~100MB
- Per 10 workers: ~50MB

### Database Connection Pool

**Formula:** `pool_max = worker_count * 2 + 10`

Example configurations:

- 20 workers → pool_max = 50
- 50 workers → pool_max = 110
- 100 workers → pool_max = 210

## 📊 Monitoring

### Log Output

The system provides detailed progress logging:

```
INFO    Processing progress     {"processed": 50000, "total": 1000000, 
         "progress_percent": 5.0, "elapsed": "2m30s", 
         "estimated_remaining": "47m30s", "success_count": 49950, 
         "failed_count": 50}
```

### Key Metrics

- **Processing Rate**: Customers per second
- **Success Rate**: Percentage of successful calculations
- **Worker Utilization**: Active workers / total workers
- **Batch Duration**: Average time per batch
- **Memory Usage**: Track with system tools

### Checkpoint Files

Checkpoints are saved at regular intervals:

```json
{
  "checkpoint_version": "1.0",
  "timestamp": "2025-10-28T10:30:00Z",
  "last_processed_customer_id": 50000,
  "total_processed": 50000,
  "total_success": 49950,
  "total_failed": 50,
  "batch_number": 50,
  "failed_customer_ids": [123, 456, 789]
}
```

## 🔍 Troubleshooting

### Common Issues

#### Database Connection Errors

**Symptoms:**

```
ERROR   Failed to connect to database
```

**Solutions:**

1. Verify database credentials in config file
2. Check PostgreSQL is running: `systemctl status postgresql`
3. Test connection: `psql -h localhost -U username -d dbname`
4. Check firewall rules

#### Out of Memory

**Symptoms:**

- Process killed by OOM
- High swap usage

**Solutions:**

1. Reduce `batch_size` to 500-1000
2. Reduce `worker_count`
3. Ensure adequate system RAM
4. Check for memory leaks in logs

#### Slow Performance

**Symptoms:**

- Processing rate < 100 customers/second
- High database wait times

**Solutions:**

1. Increase `worker_count` based on CPU cores
2. Optimize database indexes
3. Increase `pool_max` for more connections
4. Check database query performance
5. Ensure cache files are being used

#### High Failure Rate

**Symptoms:**

```
WARN    Failed to process customer   {"customer_id": 123, "error": "..."}
```

**Solutions:**

1. Check logs for error patterns
2. Run with `--dry-run` to identify issues
3. Verify database schema matches expectations
4. Check for NULL values in critical fields
5. Test with single customer: `--customer-ids=123`

### Debug Mode

Enable detailed logging:

1. Edit config file: `log_level = DEBUG`
2. Run with verbose output
3. Check `/var/log/risk-processor.log`

## 🔄 Deployment

### As a Cron Job

```bash
# Edit crontab
crontab -e

# Run daily at 2 AM
0 2 * * * /path/to/risk-processor >> /var/log/risk-processor-cron.log 2>&1
```

### As a Systemd Service

Create `/etc/systemd/system/risk-processor.service`:

```ini
[Unit]
Description=Risk Processor Service
After=postgresql.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/risk-processor
User=risk-processor
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable risk-processor.timer
```

## 📝 License

Copyright © 2025 Novaji Introserve. All rights reserved.

## 🤝 Support

For issues or questions, please check the troubleshooting section or contact your system administrator.
