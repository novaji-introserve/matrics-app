 # Risk Analysis API Documentation

## Overview

The Risk Analysis API provides RESTful endpoints for calculating customer risk scores. It follows SOLID principles, DRY, and SRP while maintaining the existing architecture.

## Architecture

```bash
risk_analysis/
├── cmd/
│   ├── api-server/          # API server entry point
│   │   └── main.go
│   └── risk-processor/      # CLI tool entry point
│       └── main.go
├── api/
│   ├── handlers/            # HTTP handlers (SRP)
│   │   └── risk_analysis_handler.go
│   ├── middleware/          # HTTP middleware
│   │   └── middleware.go
│   ├── responses/           # Standardized responses
│   │   └── responses.go
│   └── routes/              # Route definitions
│       └── routes.go
└── ... (existing structure)
```

## API Endpoints

### 1. Health Check

**Endpoint:** `GET /api/v1/health`

**Description:** Check if the API service is running and database is connected.

**Response:**

```json
{
  "status": true,
  "message": "Service is healthy",
  "code": 0,
  "data": {
    "status": "healthy",
    "database": "connected",
    "cache": "initialized",
    "version": "1.0.0"
  },
  "timestamp": "2025-10-30T05:40:09.661Z",
  "path": "/api/v1/health"
}
```

### 2. Analyze Risk (POST)

**Endpoint:** `POST /api/v1/risk-analysis`

**Description:** Analyze risk scores for one or more customers.

**Request Body:**

```json
{
  "customer_ids": [3182038, 3182039],
  "dry_run": false
}
```

**Parameters:**

- `customer_ids` (required): Array of customer IDs to analyze
- `dry_run` (optional): If true, performs analysis without updating database (default: false)

**Response (Success):**

```json
{
  "status": true,
  "message": "Risk analysis computed successfully",
  "code": 0,
  "data": [
    {
      "customer_id": 3182038,
      "risk_score": 7.5,
      "risk_level": "high",
      "success": true
    },
    {
      "customer_id": 3182039,
      "risk_score": 3.2,
      "risk_level": "low",
      "success": true
    }
  ],
  "metadata": {
    "processing_time_ms": 450,
    "total_customers": 2,
    "success_count": 2,
    "failure_count": 0,
    "dry_run": false
  },
  "timestamp": "2025-10-30T05:40:09.661Z",
  "path": "/api/v1/risk-analysis"
}
```

**Response (Partial Failure):**

```json
{
  "status": true,
  "message": "Risk analysis completed with some failures",
  "code": 0,
  "data": [
    {
      "customer_id": 3182038,
      "risk_score": 7.5,
      "risk_level": "high",
      "success": true
    },
    {
      "customer_id": 9999999,
      "success": false,
      "error": "customer not found"
    }
  ],
  "metadata": {
    "processing_time_ms": 320,
    "total_customers": 2,
    "success_count": 1,
    "failure_count": 1,
    "dry_run": false
  },
  "timestamp": "2025-10-30T05:40:09.661Z",
  "path": "/api/v1/risk-analysis"
}
```

### 3. Analyze Risk (GET)

**Endpoint:** `GET /api/v1/risk-analysis?customer_ids=3182038,3182039&dry_run=false`

**Description:** Analyze risk scores using query parameters (alternative to POST).

**Query Parameters:**

- `customer_ids` (required): Comma-separated customer IDs
- `dry_run` (optional): true/false (default: false)

**Response:** Same format as POST endpoint

## Usage Examples

### Using cURL

#### Single Customer

```bash
# POST request
curl -X POST http://localhost:8080/api/v1/risk-analysis \
  -H "Content-Type: application/json" \
  -d '{"customer_ids":[3182038]}'

# GET request
curl "http://localhost:8080/api/v1/risk-analysis?customer_ids=3182038"
```

#### Multiple Customers

```bash
# POST request
curl -X POST http://localhost:8080/api/v1/risk-analysis \
  -H "Content-Type: application/json" \
  -d '{"customer_ids":[3182038,3182039,3182040]}'

# GET request
curl "http://localhost:8080/api/v1/risk-analysis?customer_ids=3182038,3182039,3182040"
```

#### Dry Run Mode

```bash
curl -X POST http://localhost:8080/api/v1/risk-analysis \
  -H "Content-Type: application/json" \
  -d '{"customer_ids":[3182038],"dry_run":true}'
```

### Using Python

```python
import requests
import json

# API endpoint
url = "http://localhost:8080/api/v1/risk-analysis"

# Request payload
payload = {
    "customer_ids": [3182038, 3182039],
    "dry_run": False
}

# Make request
response = requests.post(url, json=payload)

# Parse response
result = response.json()

if result["status"]:
    print(f"Success: {result['message']}")
    for customer in result["data"]:
        if customer["success"]:
            print(f"Customer {customer['customer_id']}: Score={customer['risk_score']}, Level={customer['risk_level']}")
        else:
            print(f"Customer {customer['customer_id']}: Error={customer['error']}")
else:
    print(f"Error: {result['message']}")
```

### Using JavaScript/Node.js

```javascript
const axios = require('axios');

async function analyzeRisk(customerIds, dryRun = false) {
    try {
        const response = await axios.post('http://localhost:8080/api/v1/risk-analysis', {
            customer_ids: customerIds,
            dry_run: dryRun
        });

        const result = response.data;
        
        if (result.status) {
            console.log(`Success: ${result.message}`);
            result.data.forEach(customer => {
                if (customer.success) {
                    console.log(`Customer ${customer.customer_id}: Score=${customer.risk_score}, Level=${customer.risk_level}`);
                } else {
                    console.log(`Customer ${customer.customer_id}: Error=${customer.error}`);
                }
            });
        } else {
            console.error(`Error: ${result.message}`);
        }
    } catch (error) {
        console.error('Request failed:', error.message);
    }
}

// Usage
analyzeRisk([3182038, 3182039]);
```

## Building and Running

### Build API Server

```bash
make build-api
# or
go build -o risk-api-server cmd/api-server/main.go
```

### Run API Server

```bash
./risk-api-server
# or
make run-api
```

### Run with Docker

```bash
docker build -t risk-api-server .
docker run -p 8080:8080 risk-api-server
```

## Configuration

API settings are configured in `config.conf`:

```ini
[api]
port = 8080
host = 0.0.0.0
```

## Design Principles

### SOLID Principles

1. **Single Responsibility Principle (SRP)**
   - Each handler manages one specific concern
   - Middleware components have single purposes
   - Response formatting is isolated

2. **Open/Closed Principle (OCP)**
   - New endpoints can be added without modifying existing code
   - Middleware can be composed

3. **Liskov Substitution Principle (LSP)**
   - Handlers implement consistent interfaces
   - Middleware is interchangeable

4. **Interface Segregation Principle (ISP)**
   - Clean separation between API and business logic
   - Handlers depend only on what they need

5. **Dependency Inversion Principle (DIP)**
   - Handlers depend on abstractions (interfaces)
   - Business logic is decoupled from HTTP layer

### DRY (Don't Repeat Yourself)

- Standardized response format reused across all endpoints
- Common middleware for logging, CORS, recovery
- Shared customer ID parsing logic

### Separation of Concerns

- API layer (`api/`) handles HTTP concerns
- Business logic (`domain/services/`) remains unchanged
- Configuration (`config/`) is centralized

## Performance

The API leverages the existing optimized batch processing:

- **Parallel Processing**: Uses worker pools within batches
- **Bulk Operations**: Efficient database updates using COPY
- **Cached Metadata**: Functions and settings loaded once at startup
- **Connection Pooling**: Reuses database connections

Expected performance: **~450ms** for single customer, scales linearly with batch size.

## Error Handling

All errors follow the standardized response format:

```json
{
  "status": false,
  "message": "Error description",
  "code": 1,
  "data": null,
  "timestamp": "2025-10-30T05:40:09.661Z",
  "path": "/api/v1/risk-analysis"
}
```

Common HTTP status codes:

- `200 OK`: Success
- `400 Bad Request`: Invalid input
- `500 Internal Server Error`: Server error
- `503 Service Unavailable`: Database connectivity issue

## Monitoring

The API includes:

- Request logging with duration tracking
- Panic recovery with stack traces
- Health check endpoint for monitoring
- Detailed metadata in responses

## Security Considerations

- Input validation on all endpoints
- SQL injection protection via parameterized queries
- CORS headers for cross-origin access
- Panic recovery to prevent crashes

## Comparison: CLI vs API

| Feature | CLI Tool | API Server |
|---------|----------|------------|
| Use Case | Batch processing, scheduled jobs | On-demand analysis, integrations |
| Interface | Command line | HTTP REST |
| Invocation | `./risk-processor --customer-ids=1,2,3` | `POST /api/v1/risk-analysis` |
| Output | Logs to stdout/file | JSON response |
| Integration | Shell scripts, cron | Any HTTP client |
| Checkpointing | Yes | No (stateless) |

Both use the same underlying business logic and share configuration.
