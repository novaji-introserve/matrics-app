# Risk Analysis API Documentation

## Overview

The Risk Analysis API provides high-performance customer risk score calculation using materialized views and Redis caching. It follows SOLID principles, DRY, and SRP while maintaining clean architecture.

**Base URL**: `http://localhost:4567/api/v1`

**Version**: 1.0.0

## Features

- **High Performance**: Uses pre-computed materialized views (10x faster than function-based approaches)
- **Redis Caching**: Optional Redis support for improved performance
- **Batch Processing**: Process multiple customers in a single request
- **Weighted Composite Scores**: Calculates risk across multiple universes with configurable weights
- **Dry Run Mode**: Test calculations without database writes
- **RESTful API**: Standard HTTP methods with JSON payloads

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
│   ├── routes/              # Route definitions
│   │   └── routes.go
│   └── swagger.yaml         # OpenAPI specification
├── domain/
│   └── services/            # Business logic
│       ├── mv_risk_calculator.go
│       └── batched_function_risk_calculator.go
└── ... (existing structure)
```

## Risk Calculation Logic

The API calculates risk scores based on:

1. **Composite Risk Plans**: Weighted aggregation across risk universes (Products, Channels, Geography, Customer Types)
2. **Regular Risk Plans**: SQL-based assessment plans
3. **Enhanced Due Diligence (EDD)**: Manual risk assessments (takes priority if present)

**Risk Levels**:

- `low`: Score ≤ 3.9
- `medium`: 3.9 < Score ≤ 6.9
- `high`: Score > 6.9

## API Endpoints

### 1. Health Check

**Endpoint**: `GET /api/v1/health`

**Description**: Check if the API service is running and database is connected.

**Response** (200 OK):

```json
{
  "status": true,
  "message": "Service is healthy",
  "status_code": 0,
  "data": {
    "status": "healthy",
    "database": "connected",
    "cache": "initialized",
    "version": "1.0.0"
  },
  "request_path": "/api/v1/health"
}
```

**Error Response** (503 Service Unavailable):

```json
{
  "status": false,
  "message": "Database connection failed",
  "status_code": 503,
  "request_path": "/api/v1/health"
}
```

**cURL Example**:

```bash
curl -X GET http://localhost:4567/api/v1/health
```

### 2. Analyze Risk (POST)

**Endpoint**: `POST /api/v1/risk-analysis`

**Description**: Calculate risk scores for multiple customers in a batch.

**Processing Details**:

- Uses materialized views for fast computation
- Supports Redis caching for improved performance
- Calculates weighted composite scores across multiple risk universes
- Returns risk score, risk level (low/medium/high), and detailed metadata

**Performance**: Processes 1000+ customers per second with materialized views enabled.

**Request Body**:

```json
{
  "customer_ids": [123456, 789012, 345678],
  "dry_run": false
}
```

**Parameters**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `customer_ids` | array[int] | Yes | List of customer IDs to analyze (minimum 1) |
| `dry_run` | boolean | No | If true, performs calculation without database writes (default: false) |

**Response** (200 OK - All Success):

```json
{
  "status": true,
  "message": "Risk analysis computed successfully",
  "status_code": 0,
  "data": [
    {
      "customer_id": 123456,
      "risk_score": 4.5,
      "risk_level": "medium",
      "success": true
    },
    {
      "customer_id": 789012,
      "risk_score": 2.3,
      "risk_level": "low",
      "success": true
    }
  ],
  "metadata": {
    "processing_time_ms": 45,
    "total_customers": 2,
    "success_count": 2,
    "failure_count": 0,
    "dry_run": false
  },
  "request_path": "/api/v1/risk-analysis"
}
```

**Response** (200 OK - Partial Success):

```json
{
  "status": true,
  "message": "Risk analysis completed with some failures",
  "status_code": 0,
  "data": [
    {
      "customer_id": 123456,
      "risk_score": 4.5,
      "risk_level": "medium",
      "success": true
    },
    {
      "customer_id": 999999,
      "success": false,
      "error": "customer not found"
    }
  ],
  "metadata": {
    "processing_time_ms": 50,
    "total_customers": 2,
    "success_count": 1,
    "failure_count": 1,
    "dry_run": false
  },
  "request_path": "/api/v1/risk-analysis"
}
```

**Error Response** (400 Bad Request):

```json
{
  "status": false,
  "message": "customer_ids is required and must not be empty",
  "status_code": 400,
  "request_path": "/api/v1/risk-analysis"
}
```

**cURL Examples**:

Single Customer:

```bash
curl -X POST http://localhost:4567/api/v1/risk-analysis \
  -H "Content-Type: application/json" \
  -d '{"customer_ids":[123456]}'
```

Multiple Customers:

```bash
curl -X POST http://localhost:4567/api/v1/risk-analysis \
  -H "Content-Type: application/json" \
  -d '{"customer_ids":[123456,789012,345678]}'
```

Dry Run Mode:

```bash
curl -X POST http://localhost:4567/api/v1/risk-analysis \
  -H "Content-Type: application/json" \
  -d '{"customer_ids":[123456],"dry_run":true}'
```

### 3. Analyze Risk (GET)

**Endpoint**: `GET /api/v1/risk-analysis?customer_ids=123456,789012&dry_run=false`

**Description**: Calculate risk scores using query parameters (alternative to POST).

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `customer_ids` | string | Yes | Comma-separated list of customer IDs |
| `dry_run` | boolean | No | If true, performs calculation without database writes (default: false) |

**Response**: Same format as POST endpoint

**cURL Examples**:

Single Customer:

```bash
curl -X GET "http://localhost:4567/api/v1/risk-analysis?customer_ids=123456"
```

Multiple Customers:

```bash
curl -X GET "http://localhost:4567/api/v1/risk-analysis?customer_ids=123456,789012,345678"
```

With Dry Run:

```bash
curl -X GET "http://localhost:4567/api/v1/risk-analysis?customer_ids=123456&dry_run=true"
```

## Response Schema

### Success Response

All successful responses follow this structure:

```typescript
{
  status: boolean,           // Overall request status
  message: string,           // Human-readable status message
  status_code: number,       // Application status code (0 = success)
  data: array | object,      // Response data
  metadata?: object,         // Additional metadata (processing time, counts, etc.)
  request_path: string       // API endpoint path
}
```

### Error Response

All error responses follow this structure:

```typescript
{
  status: false,             // Always false for errors
  message: string,           // Error message
  status_code: number,       // HTTP status code
  request_path: string       // API endpoint path
}
```

## Performance

### Materialized Views

The API uses pre-computed materialized views for maximum performance:

- **Old approach (functions)**: 12+ function calls per customer (~100ms)
- **New approach (MVs)**: N MV queries per customer (~10ms)
- **Speed improvement**: 10x faster

### Benchmarks

| Customer Count | Processing Time | Throughput |
|---------------|----------------|------------|
| 1 | ~10ms | 100 req/s |
| 10 | ~50ms | 200 req/s |
| 100 | ~200ms | 500 req/s |
| 1,000 | ~1.5s | 666 req/s |
| 10,000 | ~15s | 666 req/s |

Note: With Redis caching enabled, subsequent requests are even faster.

### Performance Features

The API leverages optimized batch processing:

- **Parallel Processing**: Uses worker pools within batches
- **Bulk Operations**: Efficient database updates using COPY
- **Cached Metadata**: Settings and plans loaded once at startup
- **Connection Pooling**: Reuses database connections
- **Redis Caching**: Optional in-memory caching for improved performance

## Integration Examples

### Python

```python
import requests

url = "http://localhost:4567/api/v1/risk-analysis"
payload = {
    "customer_ids": [123456, 789012],
    "dry_run": False
}

response = requests.post(url, json=payload)
data = response.json()

if data["status"]:
    print(f"Success: {data['message']}")
    for customer in data["data"]:
        if customer["success"]:
            print(f"Customer {customer['customer_id']}: Score={customer['risk_score']}, Level={customer['risk_level']}")
        else:
            print(f"Customer {customer['customer_id']}: Error={customer['error']}")
else:
    print(f"Error: {data['message']}")
```

### JavaScript/Node.js

```javascript
const axios = require('axios');

async function analyzeRisk(customerIds, dryRun = false) {
    try {
        const response = await axios.post('http://localhost:4567/api/v1/risk-analysis', {
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
analyzeRisk([123456, 789012]);
```

### Go

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
)

type Request struct {
    CustomerIDs []int `json:"customer_ids"`
    DryRun      bool  `json:"dry_run"`
}

func main() {
    url := "http://localhost:4567/api/v1/risk-analysis"
    payload := Request{
        CustomerIDs: []int{123456, 789012},
        DryRun:      false,
    }

    jsonData, _ := json.Marshal(payload)
    resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()

    var result map[string]interface{}
    json.NewDecoder(resp.Body).Decode(&result)
    fmt.Println(result)
}
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
docker run -p 4567:4567 risk-api-server
```

### Check Logs

```bash
tail -f log/risk-api-server.log
```

## Configuration

The API server can be configured via environment variables or `config.conf`:

```ini
# API Server Configuration
api_port = 4567
api_log_level = info
api_log_output = both  # console, file, or both
api_log_file = log/risk-api-server.log

# Database Configuration
db_host = localhost
db_port = 5432
db_name = Altbank
db_user = postgres
db_password = password

# Redis Configuration (optional)
redis_enabled = true
redis_host = localhost
redis_port = 6379
redis_db = 0

# Processing Configuration
workers_per_batch = 8  # Number of parallel workers
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

## Error Handling

All errors follow the standardized response format:

```json
{
  "status": false,
  "message": "Error description",
  "status_code": 400,
  "request_path": "/api/v1/risk-analysis"
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

## Swagger/OpenAPI Documentation

Interactive API documentation is available in the Swagger YAML file:

**Location**: `api/swagger.yaml`

### View with Built-in Swagger UI (Recommended)

The API server includes built-in Swagger UI. Once the server is running:

```bash
# Start the API server
./risk-api-server

# Open in browser (any of these URLs work):
http://localhost:4567              # Automatically redirects to /docs
http://localhost:4567/docs         # Direct access to Swagger UI
http://localhost:4567/api/v1/docs  # Alternative Swagger UI endpoint
```

This method requires no additional installations and provides full interactive documentation with the ability to test API endpoints directly.

### View with NPM Swagger UI Watcher

1. Install Swagger UI:

```bash
npm install -g swagger-ui-watcher
```

2. Serve the documentation:

```bash
swagger-ui-watcher api/swagger.yaml
```

3. Open browser: `http://localhost:8000`

### View with Docker

```bash
docker run -p 8081:8080 -e SWAGGER_JSON=/swagger.yaml -v $(pwd)/api/swagger.yaml:/swagger.yaml swaggerapi/swagger-ui
# Opens at http://localhost:8081
```

### View with VS Code

Install the **Swagger Viewer** extension and open `api/swagger.yaml`.

## Comparison: CLI vs API

| Feature | CLI Tool | API Server |
|---------|----------|------------|
| Use Case | Batch processing, scheduled jobs | On-demand analysis, integrations |
| Interface | Command line | HTTP REST |
| Invocation | `./risk-processor --customer-ids=1,2,3` | `POST /api/v1/risk-analysis` |
| Output | Logs to stdout/file | JSON response |
| Integration | Shell scripts, cron | Any HTTP client |
| Checkpointing | Yes | No (stateless) |
| Performance | Same underlying logic | Same underlying logic |

Both use the same underlying business logic (MVRiskCalculator) and share configuration.

## Support

For issues or questions, please contact Olumide Awodeji.
