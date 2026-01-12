# Risk Analysis System

A high-performance, concurrent customer risk scoring system built in Go, designed to process millions of customers efficiently with materialized views, full resumability, and Redis caching support.

## Features

- **Ultra-High Performance**: Uses materialized views for 10x faster processing
- **Redis Caching**: Optional Redis support for improved metadata caching
- **Concurrent Processing**: Configurable worker pools with optimal parallelism
- **Resumability**: Checkpoint system allows resuming from interruptions
- **Incremental Updates**: Track processed customers to avoid reprocessing
- **Automatic New Customer Detection**: Polls for customers with NULL risk scores
- **Composite Risk Analysis**: Multi-universe weighted risk calculations
- **Graceful Shutdown**: Safe interrupt handling with checkpoint saving
- **Comprehensive Logging**: Detailed progress tracking and error reporting
- **RESTful API**: Built-in API server with Swagger documentation

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
├── cmd/
│   ├── api-server/          # HTTP API server entry point
│   └── risk-processor/      # CLI processor entry point
├── config/                  # Configuration management
├── application/             # Business orchestration layer
├── domain/
│   ├── models/             # Business entities
│   ├── services/           # Core business logic
│   │   ├── mv_risk_calculator.go          # Materialized view calculator (current)
│   │   └── batched_function_risk_calculator.go  # Legacy function calculator
│   └── repositories/       # Repository interfaces
├── infrastructure/
│   ├── database/           # Database connections
│   ├── cache/              # Redis and file-based caching
│   └── repository/         # Repository implementations
├── api/                    # RESTful API layer
│   ├── handlers/           # HTTP request handlers
│   ├── middleware/         # CORS, logging, recovery
│   ├── responses/          # Standardized JSON responses
│   ├── routes/             # Route definitions
│   └── swagger.yaml        # OpenAPI specification
├── workers/                # Concurrent processing
└── utils/                  # Shared utilities
```

### Key Components

1. **RiskProcessor**: Orchestrates the entire risk calculation workflow
2. **MVRiskCalculator**: Core calculation engine using materialized views (10x faster)
3. **RedisCache**: Optional Redis caching for metadata and composite scores
4. **WorkerPool**: Manages concurrent processing of customer batches
5. **NewCustomerDetector**: Polls for customers with NULL risk_level/risk_score

## Installation

### Prerequisites

- Go 1.22.2 or higher
- PostgreSQL 13+ with materialized views
- Redis 6+ (optional, for caching)
- Linux/Unix environment (for file paths in config)

### Build from Source

```bash
# Clone the repository
cd risk_analysis

# Download dependencies
go mod download

# Build CLI processor
go build -o risk-processor cmd/risk-processor/main.go

# Build API server
go build -o risk-api-server cmd/api-server/main.go

# Make executable
chmod +x risk-processor risk-api-server
```

### Verify Installation

```bash
./risk-processor --help
./risk-api-server --help
```

## Configuration

Configuration is managed through `config.conf`:

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
workers_per_batch = 8
chunk_size = 10000
enable_bulk_operations = true
bulk_insert_batch_size = 500
progress_checkpoint_interval = 10000

# New Customer Detection (polls for NULL risk_level/risk_score)
new_customer_poll_interval = 60  # seconds

# Cache Settings
cache_directory = /tmp
customer_id_cache_file = /tmp/customer_ids.cache
processed_customers_file = /tmp/processed_customers.txt

# Redis Configuration (optional)
redis_enabled = true
redis_host = localhost
redis_port = 6379
redis_password =
redis_db = 0
redis_pool_size = 10

# API Server Configuration
api_port = 4567
api_log_level = info
api_log_output = both  # console, file, or both
api_log_file = log/risk-api-server.log

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

#### Materialized Views Performance

- **workers_per_batch**: Workers within each batch (default: 8, optimized for MVs)
- **batch_size**: Customers per transaction batch (default: 1000)
- **worker_count**: Concurrent batch processors (default: 20)

#### Redis Caching (Optional)

- **redis_enabled**: Enable Redis caching (default: true)
- **redis_host**: Redis server host (default: localhost)
- **redis_port**: Redis server port (default: 6379)
- **redis_pool_size**: Connection pool size (default: 10)

#### New Customer Detection

- **new_customer_poll_interval**: Poll interval in seconds (default: 60)
  - Queries for customers with NULL risk_level or risk_score
  - More efficient than tracking MAX(id)

## Usage

### CLI Processor

#### Basic Usage

```bash
# Run with default settings from config file
./risk-processor

# Run with increased concurrency
./risk-processor --workers=50

# Process specific customers only
./risk-processor --customer-ids=1000,1001,1002
```

#### Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `--dry-run` | Run without database updates | `--dry-run` |
| `--customer-ids` | Process specific customers | `--customer-ids=1,2,3` |
| `--workers` | Override worker count | `--workers=50` |
| `--batch-size` | Override batch size | `--batch-size=500` |
| `--resume-from-checkpoint` | Resume from last checkpoint | `--resume-from-checkpoint` |
| `--help` | Show help message | `--help` |

### API Server

#### Start the API Server

```bash
./risk-api-server
# Server starts on http://localhost:4567
# Visit http://localhost:4567 for Swagger documentation
```

#### API Endpoints

- **Health Check**: `GET /api/v1/health`
- **Risk Analysis (POST)**: `POST /api/v1/risk-analysis`
- **Risk Analysis (GET)**: `GET /api/v1/risk-analysis?customer_ids=1,2,3`
- **Swagger UI**: `http://localhost:4567` or `http://localhost:4567/docs`

See [api_readme.md](api_readme.md) or [api_documentation.md](api_documentation.md) for complete API documentation.

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

## Performance Tuning

### Materialized Views vs Functions

The system now uses materialized views for 10x performance improvement:

| Approach | Queries per Customer | Avg Time | Performance |
|----------|---------------------|----------|-------------|
| Functions (legacy) | 12+ function calls | ~100ms | Baseline |
| Materialized Views (current) | N MV queries | ~10ms | 10x faster |

### Worker Count Guidelines

| Total Customers | CPU Cores | Recommended Workers | Expected Duration |
|----------------|-----------|---------------------|-------------------|
| < 100,000 | 4 | 8-16 | < 5 minutes |
| 100,000 - 1M | 8 | 20-32 | 5-15 minutes |
| 1M - 5M | 16 | 32-64 | 30 minutes - 2 hours |
| > 5M | 32+ | 64-128 | 2-5 hours |

Note: With materialized views, processing is significantly faster than function-based approach.

### Redis Caching Benefits

When Redis is enabled:

- **Metadata Caching**: Settings, thresholds, universes cached in Redis
- **MV Data Caching**: Composite scores and MV results cached with database prefix
- **Performance**: 2-3x faster on subsequent runs
- **Scalability**: Multiple processors can share cache

### Memory Considerations

**Batch Size vs Memory Usage:**

- Small batches (500-1000): Lower memory, more DB overhead
- Medium batches (1000-2000): Balanced (recommended)
- Large batches (2000-5000): Higher memory, fewer DB calls

**Memory Requirements:**

- Base: ~500MB
- Per 1000 batch size: ~100MB
- Per 10 workers: ~50MB
- Redis (if enabled): ~200-500MB

### Database Connection Pool

**Formula:** `pool_max = worker_count * 2 + 10`

Example configurations:

- 20 workers → pool_max = 50
- 50 workers → pool_max = 110
- 100 workers → pool_max = 210

## Monitoring

### Log Output

The system provides detailed progress logging:

```
INFO    Processing progress     {"processed": 50000, "total": 1000000,
         "progress_percent": 5.0, "elapsed": "2m30s",
         "estimated_remaining": "12m30s", "success_count": 49950,
         "failed_count": 50}
```

### Key Metrics

- **Processing Rate**: Customers per second (100-300 with MVs)
- **Success Rate**: Percentage of successful calculations
- **Worker Utilization**: Active workers / total workers
- **Batch Duration**: Average time per batch (~10ms with MVs)
- **Memory Usage**: Track with system tools

### New Customer Detection

The processor automatically detects new customers:

```
INFO    New customer detection   {"poll_interval": "60s", "new_customers_found": 5}
INFO    Processing new customers {"customer_ids": [1001, 1002, 1003, 1004, 1005]}
```

Queries for customers where `risk_level IS NULL OR risk_score IS NULL`.

### Checkpoint Files

Checkpoints are saved at regular intervals:

```json
{
  "checkpoint_version": "1.0",
  "timestamp": "2025-11-08T18:30:00Z",
  "last_processed_customer_id": 50000,
  "total_processed": 50000,
  "total_success": 49950,
  "total_failed": 50,
  "batch_number": 50,
  "failed_customer_ids": [123, 456, 789]
}
```

## Troubleshooting

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
5. Verify materialized views exist

#### Redis Connection Errors

**Symptoms:**

```
ERROR   Failed to connect to Redis
```

**Solutions:**

1. Check Redis is running: `redis-cli ping`
2. Verify redis_host and redis_port in config
3. Set `redis_enabled = false` to disable Redis
4. Check Redis authentication if password is set

#### Slow Performance

**Symptoms:**

- Processing rate < 100 customers/second with MVs
- High database wait times

**Solutions:**

1. Refresh materialized views: `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_*`
2. Increase `workers_per_batch` (8-16 recommended for MVs)
3. Enable Redis caching if not already enabled
4. Check database query performance
5. Verify indexes on materialized views

#### High Failure Rate

**Symptoms:**

```
WARN    Failed to process customer   {"customer_id": 123, "error": "..."}
```

**Solutions:**

1. Check logs for error patterns
2. Run with `--dry-run` to identify issues
3. Verify materialized views are up to date
4. Check for NULL values in critical fields
5. Test with single customer: `--customer-ids=123`

### Debug Mode

Enable detailed logging:

1. Edit config file: `log_level = DEBUG`
2. Run with verbose output
3. Check `/var/log/risk-processor.log`

## Deployment

### As a Cron Job

```bash
# Edit crontab
crontab -e

# Run daily at 2 AM
0 2 * * * /path/to/risk-processor >> /var/log/risk-processor-cron.log 2>&1

# Refresh materialized views before processing (recommended)
0 1 * * * psql -d dbname -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_risk_data;"
```

### As a Systemd Service (API Server)

Create `/etc/systemd/system/risk-api-server.service`:

```ini
[Unit]
Description=Risk Analysis API Server
After=postgresql.service redis.service

[Service]
Type=simple
ExecStart=/usr/local/bin/risk-api-server
User=risk-processor
StandardOutput=journal
StandardError=journal
Restart=always

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable risk-api-server
sudo systemctl start risk-api-server
```

## Performance Comparison

### Before (Function-Based)

- **Processing Time**: 20-30 hours for 5 million customers
- **Queries per Customer**: 12+ function calls
- **Average Time**: ~100ms per customer
- **Throughput**: ~14 customers/second

### After (Materialized Views)

- **Processing Time**: 2-5 hours for 5 million customers
- **Queries per Customer**: N MV queries
- **Average Time**: ~10ms per customer
- **Throughput**: 100-300 customers/second
- **Speed Improvement**: 10x faster

## License

Copyright © 2025 Novaji Introserve. All rights reserved.

## Support

For issues or questions, please check the troubleshooting section or contact Olumide Awodeji.
