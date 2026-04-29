# Risk Analysis System - Code Documentation

## Table of Contents

1. [Overview](#overview)
2. [Package Structure](#package-structure)
3. [Core Components](#core-components)
4. [Key Functions](#key-functions)
5. [Data Flow](#data-flow)
6. [Database Schema](#database-schema)

---

## Overview

The Risk Analysis System is a high-performance Go application designed to calculate risk scores for millions of customers concurrently. It uses **materialized views** for 10x faster processing, with optional **Redis caching** for distributed metadata storage. The system includes both a CLI processor and a RESTful API with Swagger documentation.

### Design Principles

- **Clean Architecture**: Clear separation between business logic and infrastructure
- **Materialized Views**: Pre-computed risk data for 10x query performance
- **Redis Caching**: Optional distributed caching for improved performance
- **Concurrency**: Parallel processing using goroutines and worker pools
- **Resumability**: Checkpoint system for fault tolerance
- **Observability**: Comprehensive logging and metrics
- **RESTful API**: Built-in HTTP API with Swagger documentation

### Performance

- **900 customers**: Processed in less than 3 minutes
- **5,000,000+ customers**: Estimated 27-30 hours (with materialized views)
- **Processing Rate**: 50+ customers/second with MVs, 5 customers/second without
- **Improvement**: 10x faster than function-based approach

---

## Package Structure

### `cmd/risk-processor`

**Purpose**: Application entry point

**Key Files**:

- `main.go`: Bootstrap application, parse CLI flags, setup graceful shutdown

**Responsibilities**:

- Parse command-line arguments
- Load configuration
- Initialize logger
- Setup database connection
- Create and run RiskProcessor
- Handle OS signals for graceful shutdown

### `config`

**Purpose**: Configuration management

**Key Files**:

- `config.go`: Load and parse INI configuration file

**Key Structures**:

```go
type Config struct {
    // Database settings
    DBHost, DBPort, DBName, DBUser, DBPassword, DBSSLMode string
    DBPoolMin, DBPoolMax int
    
    // Processing settings
    BatchSize, WorkerCount, WorkersPerBatch, ChunkSize int
    EnableBulkOperations bool
    ProgressCheckpointInterval int
    
    // Cache settings
    CacheDirectory string
    CustomerIDCacheFile string
    ProcessedCustomersFile string
    
    // Execution control
    DryRun bool
    ResumeFromCheckpoint bool
    CustomerIDs []int
}
```

### `application`

**Purpose**: Business orchestration layer

**Key Files**:

- `risk_processor.go`: Main orchestration of risk calculation workflow

**Key Structures**:

#### `RiskProcessor`

```go
type RiskProcessor struct {
    config            *config.Config
    db                *pgxpool.Pool
    logger            *zap.Logger
    mvCalculator      *services.MVRiskCalculator  // Using materialized views
    workerPool        *workers.WorkerPool
    customerCache     *cache.CustomerIDCache
    redisCache        *cache.RedisCache  // Optional Redis caching
    stats             struct {
        startTime, endTime  time.Time
        totalCustomers      int
        totalProcessed      int
        successCount        int
        failedCount         int
        failedCustomers     []int
        batchCount          int
        mu                  sync.Mutex
    }
}
```

**Key Methods**:

#### `NewRiskProcessor(config, db, logger) *RiskProcessor`

Creates a new risk processor with initialized components.

#### `InitializeCache(ctx) error`

Loads frequently accessed data into memory:

- Risk function definitions from database
- Composite plans and settings
- Processed customer tracking

#### `Run(ctx) error`

Main execution flow:

1. Load customer IDs (from config or database)
2. Resume from checkpoint if requested
3. Start worker pool
4. Process customers in parallel batches
5. Monitor progress and save checkpoints
6. Log final statistics

#### `processCustomersInBatches(ctx, customerIDs) error`

Processes customers using parallel batch transactions:

- Splits customers into batches
- Processes batches concurrently with semaphore
- Handles graceful shutdown
- Updates statistics

#### `processSingleBatch(ctx, batchNum, customerIDs, startIdx, totalCustomers) error`

Processes a single batch of customers:

- Calls BatchedFunctionRiskCalculator
- Counts successes/failures
- Marks customers as processed in cache
- Saves progress checkpoints

### `domain/models`

**Purpose**: Business entities and data structures

**Key Files**:

- `models.go`: Core domain entities
- `risk_function_result.go`: Risk function execution results

**Key Structures**:

```go
type Customer struct {
    ID                 int
    RiskScore          *float64
    RiskLevel          *string
    CompositeRiskScore *float64
    BranchID           *int
}

type RiskPlan struct {
    ID                      int
    Name                    string
    State                   string
    Priority                int
    ComputeScoreFrom        string
    SQLQuery                string
    RiskScore               float64
    RiskAssessmentID        *int
    UseCompositeCalculation bool
    UniverseID              *int
}

type RiskFunctionResult struct {
    FunctionName string
    Matches      map[string]float64
    HasMatch     bool
    Error        error
}
```

### `domain/services`

**Purpose**: Core business logic

**Key Files**:

- `batched_function_risk_calculator.go`: Main calculation engine
- `cached_function_executor.go`: Function execution with caching

**Key Structures**:

#### `BatchedFunctionRiskCalculator`

```go
type BatchedFunctionRiskCalculator struct {
    db                *pgxpool.Pool
    logger            *zap.Logger
    functionExecutor  *CachedFunctionExecutor
    cachedSettings    *CachedSettings
    compositePlans    []*RiskPlan
    cacheInitialized  bool
    cacheMu           sync.RWMutex
}
```

**Key Methods**:

##### `InitializeCache(ctx) error`

One-time initialization at startup:

- Loads all check_* functions from database into memory
- Caches risk thresholds and computation settings
- Loads composite plans for weighted scoring
- Saves metadata to disk for validation

##### `ProcessCustomerBatch(ctx, customerIDs, dryRun, workersPerBatch) []CustomerRiskResult`

Processes a batch of customers in parallel:

- Creates worker pool for the batch
- Processes each customer with `calculateSingleCustomer`
- Bulk updates database using PostgreSQL COPY
- Returns results for all customers

##### `calculateSingleCustomer(ctx, customerID) (score, level, compositePlanLines, error)`

Calculates risk score for a single customer:

**Priority Order:**

1. **Approved EDD**: If exists, use as-is (highest priority)
2. **Plan-Based Risk**: Execute risk plans and aggregate scores
3. **Composite Risk**: Calculate weighted scores across universes

**Algorithm:**

```bash
1. Clear previous composite plan lines
2. Calculate composite score if composite plans exist
   - For each composite plan:
     - Execute SQL query
     - If matched, collect plan line
     - Track scores per universe/subject
   - Aggregate scores using weighted computation
3. Check for Approved EDD (Priority 1)
   - If found: return EDD score (no composite added)
4. Get plan-based scores from res_partner_risk_plan_line (Priority 2)
5. Aggregate plan scores using configured method (max/avg/sum)
6. Add composite score to plan score
7. Apply maximum threshold cap
8. Classify into risk level (low/medium/high)
9. Return final score, level, and composite plan lines
```

##### `calculateCompositeScore(ctx, tx, customerID, compositePlans, compositeComputation) (float64, []CompositePlanLine, error)`

Implements composite risk calculation:

**Logic:**

```bash
1. Load universes with is_included_in_composite = true
2. For each composite plan:
   - Validate universe_id and risk_assessment_id
   - Get risk assessment with subject_id and risk_rating
   - Execute SQL query to check match
   - If matched:
     - Create composite plan line
     - Track score for universe/subject
3. Aggregate scores:
   - Group by universe → subject
   - Aggregate subject scores using method (max/avg/sum)
   - Apply universe weight percentage
   - Sum weighted scores
4. Return total composite score and plan lines
```

#### `CachedFunctionExecutor`

```go
type CachedFunctionExecutor struct {
    db               *pgxpool.Pool
    logger           *zap.Logger
    functions        []*RiskFunctionDefinition
    functionsMap     map[string]*RiskFunctionDefinition
    cacheInitialized bool
    cacheMu          sync.RWMutex
}
```

**Key Methods**:

##### `InitializeCache(ctx) error`

Loads all check_* functions from pg_proc:

```sql
SELECT p.oid, p.proname, ROW_NUMBER() OVER (ORDER BY p.proname)
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'public'
  AND p.proname LIKE 'check_%'
  AND p.prokind = 'f'
```

##### `ExecuteAllFunctions(ctx, tx, customerID) (map[string]float64, error)`

Executes all cached functions for a customer:

- Reads function definitions from memory (no DB lookup)
- Executes each function: `SELECT function_name($1)`
- Extracts numeric values from JSON results
- Returns map of function_name → score

### `infrastructure/cache`

**Purpose**: File-based caching for incremental processing

**Key Files**:

- `file_cache.go`: Customer ID caching

**Key Structures**:

```go
type CustomerIDCache struct {
    cacheFile      string
    processedFile  string
    db             *pgxpool.Pool
    logger         *zap.Logger
    mu             sync.RWMutex
    customerIDs    []int
    processedIDs   map[int]bool
    totalCount     int
}
```

**Key Methods**:

##### `LoadOrRefresh(ctx) ([]int, error)`

Loads customer IDs from cache or database:

- Checks current customer count in database
- If cache exists and count matches, load from file
- Otherwise, query database and save to cache
- Returns list of all customer IDs

##### `LoadProcessedCustomers() error`

Loads set of already processed customers from file:

- Reads processed_customers.txt line by line
- Builds in-memory set for fast lookup

##### `MarkBatchProcessed(customerIDs) error`

Marks multiple customers as processed:

- Appends customer IDs to processed_customers.txt
- Updates in-memory set
- Uses buffered writer for performance

##### `GetUnprocessedCustomers(allCustomers) []int`

Filters to only unprocessed customers:

- Checks each customer against processed set
- Returns only IDs not yet processed
- Enables incremental processing

### `infrastructure/database`

**Purpose**: Database connection management

**Key Files**:

- `connection.go`: PostgreSQL connection pool

**Key Structures**:

```go
type Connection struct {
    pool   *pgxpool.Pool
    logger *zap.Logger
    config ConnectionConfig
}

type ConnectionConfig struct {
    Host, Database, User, Password, SSLMode string
    Port                                   int
    PoolMinSize, PoolMaxSize               int
    MaxIdleTime, MaxLifetime               int
    ConnectTimeout, QueryTimeout           int
}
```

**Key Methods**:

##### `Connect(ctx) error`

Establishes connection pool:

- Parses connection string
- Creates pgxpool with configuration
- Tests connection with ping
- Returns ready-to-use pool

##### `GetStats() pgxpool.Stat`

Returns connection pool statistics:

- Total connections
- Idle connections
- Acquired connections
- Useful for monitoring

### `workers`

**Purpose**: Concurrent job processing

**Key Files**:

- `worker_pool.go`: Worker pool implementation

**Key Structures**:

```go
type WorkerPool struct {
    workerCount      int
    jobs             chan Job
    results          chan error
    wg               sync.WaitGroup
    logger           *zap.Logger
    activeWorkers    int32
    processedCount   int64
    successCount     int64
    failedCount      int64
    lastProcessedID  int64
    processingTimes  []time.Duration
}
```

**Key Methods**:

##### `Start(ctx)`

Starts worker goroutines:

- Creates workerCount goroutines
- Each worker waits on jobs channel
- Processes jobs until channel closed or context cancelled

##### `Submit(ctx, job) error`

Submits a job to the pool:

- Sends job to jobs channel
- Blocks if channel full (backpressure)
- Respects context cancellation

##### `GetStats() Stats`

Returns worker pool statistics:

- Active workers
- Total/success/failed counts
- Average/min/max processing times

---

## Key Functions

### Risk Calculation Flow

```bash
1. Load Configuration
   ↓
2. Initialize Database Connection
   ↓
3. Initialize Caches
   - Function definitions
   - Composite plans
   - Settings/thresholds
   - Processed customers
   ↓
4. Load Customer IDs
   - From config (specific IDs)
   - OR from cache/database (all IDs)
   - OR resume from checkpoint
   ↓
5. Filter to Unprocessed
   ↓
6. Split into Batches
   ↓
7. Process Batches in Parallel
   For each batch:
   ├─> Start Transaction
   ├─> Process Customers in Parallel
   │   For each customer:
   │   ├─> Calculate Composite Score
   │   ├─> Check Approved EDD (Priority 1)
   │   ├─> Get Plan-Based Scores (Priority 2)
   │   ├─> Aggregate Scores
   │   ├─> Apply Threshold
   │   └─> Classify Risk Level
   ├─> Bulk Update Database
   ├─> Mark as Processed
   └─> Save Checkpoint (periodic)
   ↓
8. Log Final Statistics
```

### Score Aggregation Methods

The system supports three aggregation methods:

#### 1. Maximum (max)

```go
max := 0.0
for _, score := range scores {
    if score > max {
        max = score
    }
}
return max
```

#### 2. Average (avg)

```go
sum := 0.0
for _, score := range scores {
    sum += score
}
return sum / float64(len(scores))
```

#### 3. Sum (sum)

```go
sum := 0.0
for _, score := range scores {
    sum += score
}
return sum
```

### Risk Level Classification

```go
func classifyRiskLevel(score float64, settings *CachedSettings) string {
    if score <= settings.LowRiskThreshold {
        return "low"
    } else if score <= settings.MediumRiskThreshold {
        return "medium"
    }
    return "high"
}
```

**Example Thresholds:**

- Low: ≤ 3.9
- Medium: 3.9 < score ≤ 6.9
- High: > 6.9

---

## Data Flow

### Customer Processing Pipeline

```bash
Customer IDs
    ↓
┌───────────────────────────────┐
│   Customer ID Cache           │
│   - Load from file/DB         │
│   - Filter processed          │
└───────────────────────────────┘
    ↓
┌───────────────────────────────┐
│   Batch Split                 │
│   - Size: 1000 (configurable) │
│   - Concurrent: 32 batches    │
└───────────────────────────────┘
    ↓
┌───────────────────────────────┐
│   Batch Processing            │
│   - Transaction per batch     │
│   - Workers per batch: 2-4    │
└───────────────────────────────┘
    ↓
┌───────────────────────────────┐
│   Customer Calculation        │
│   - Composite score           │
│   - Plan-based score          │
│   - Aggregation               │
└───────────────────────────────┘
    ↓
┌───────────────────────────────┐
│   Database Update             │
│   - Bulk COPY for plan lines  │
│   - UNNEST for risk scores    │
└───────────────────────────────┘
    ↓
┌───────────────────────────────┐
│   Progress Tracking           │
│   - Mark as processed         │
│   - Save checkpoint           │
└───────────────────────────────┘
```

---

## Database Schema

### Key Tables

#### `res_partner`

Customer table with risk scores

```sql
CREATE TABLE res_partner (
    id                  INTEGER PRIMARY KEY,
    risk_score          NUMERIC,
    risk_level          VARCHAR,
    composite_risk_score NUMERIC,
    branch_id           INTEGER
);
```

#### `res_compliance_risk_assessment_plan`

Risk assessment plans

```sql
CREATE TABLE res_compliance_risk_assessment_plan (
    id                      INTEGER PRIMARY KEY,
    name                    VARCHAR,
    state                   VARCHAR,
    priority                INTEGER,
    compute_score_from      VARCHAR,
    sql_query               TEXT,
    risk_score              NUMERIC,
    risk_assessment         INTEGER,
    universe_id             INTEGER,
    use_composite_calculation BOOLEAN
);
```

#### `res_partner_composite_plan_line`

Composite calculation results

```sql
CREATE TABLE res_partner_composite_plan_line (
    id           INTEGER PRIMARY KEY,
    partner_id   INTEGER,
    plan_id      INTEGER,
    universe_id  INTEGER,
    subject_id   INTEGER,
    assessment_id INTEGER,
    matched      BOOLEAN,
    risk_score   NUMERIC,
    name         VARCHAR
);
```

#### `res_partner_risk_plan_line`

Plan execution results

```sql
CREATE TABLE res_partner_risk_plan_line (
    id           INTEGER PRIMARY KEY,
    partner_id   INTEGER,
    plan_line_id INTEGER,
    risk_score   NUMERIC
);
```

#### `res_risk_universe`

Risk universes for composite calculation

```sql
CREATE TABLE res_risk_universe (
    id                     INTEGER PRIMARY KEY,
    name                   VARCHAR,
    is_included_in_composite BOOLEAN,
    weight_percentage      NUMERIC
);
```

#### `res_compliance_settings`

System configuration

```sql
CREATE TABLE res_compliance_settings (
    id   INTEGER PRIMARY KEY,
    code VARCHAR,
    val  TEXT
);
```

### Check Functions

All risk check functions follow this pattern:

```sql
CREATE OR REPLACE FUNCTION check_function_name(p_partner_id INTEGER)
RETURNS JSONB AS $$
BEGIN
    -- Check logic here
    IF condition THEN
        RETURN jsonb_build_object('criterion_key', risk_score);
    ELSE
        RETURN '{}'::jsonb;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

**Example:**

```sql
CREATE OR REPLACE FUNCTION check_cust_pep(p_partner_id INTEGER)
RETURNS JSONB AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM res_partner WHERE id = p_partner_id AND is_pep = true) THEN
        RETURN jsonb_build_object('cust_pep', 5.2);
    ELSE
        RETURN '{}'::jsonb;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

---

## Performance Optimizations

### 1. Function Caching

- Load all check_* functions once at startup
- Execute from memory (zero DB metadata lookups)
- Cache invalidation after 1 hour

### 2. Bulk Operations

- PostgreSQL COPY for composite plan lines (100x faster than INSERT)
- UNNEST for risk score updates (batched)
- Single transaction per batch

### 3. Parallel Processing

- Concurrent batch processing with semaphore
- Worker pools within batches
- Optimal concurrency: worker_count / 4 batches

### 4. File-Based Caching

- Customer IDs cached to disk (avoid full table scan)
- Processed customers tracked (incremental processing)
- Function definitions cached (faster restarts)

### 5. Connection Pooling

- Reusable database connections
- Configurable min/max connections
- Automatic connection health checks

### 6. Incremental Processing

- Track processed customers in file
- Skip already-processed on restart
- Resume from checkpoint on failure

---

## Error Handling

### Transaction Safety

Each batch is processed in a single transaction:

```go
tx, err := db.Begin(ctx)
if err != nil {
    return fmt.Errorf("failed to begin transaction: %w", err)
}
defer tx.Rollback(ctx)

// Process batch...

if err := tx.Commit(ctx); err != nil {
    return fmt.Errorf("failed to commit: %w", err)
}
```

### Graceful Shutdown

Signal handling for clean shutdown:

```go
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

go func() {
    <-sigChan
    logger.Warn("Shutdown signal received, finishing current batches...")
    cancel() // Cancel context
    
    // Second signal forces immediate exit
    go func() {
        <-sigChan
        logger.Error("Force shutdown - exiting immediately")
        os.Exit(1)
    }()
}()
```

### Error Recovery

**Context Cancellation:**

```go
select {
case <-ctx.Done():
    // Graceful shutdown
    saveCheckpoint()
    return ctx.Err()
default:
    // Continue processing
}
```

**Failed Customer Tracking:**

```go
if result.Error != nil {
    if result.Error == context.Canceled {
        // Graceful shutdown - don't count as failure
        cancelledCount++
    } else {
        // Actual error - log and track
        failedCustomers = append(failedCustomers, result.CustomerID)
        logger.Error("Failed to process customer", 
            zap.Int("customer_id", result.CustomerID),
            zap.Error(result.Error))
    }
}
```

---

## Checkpoint System

### Checkpoint Structure

```go
type Checkpoint struct {
    Version             string    `json:"checkpoint_version"`
    Timestamp           time.Time `json:"timestamp"`
    LastProcessedID     int       `json:"last_processed_customer_id"`
    TotalProcessed      int64     `json:"total_processed"`
    TotalSuccess        int64     `json:"total_success"`
    TotalFailed         int64     `json:"total_failed"`
    BatchNumber         int       `json:"batch_number"`
    FailedCustomerIDs   []int     `json:"failed_customer_ids"`
}
```

### Checkpoint Workflow

**Save Checkpoint:**

```go
func (p *RiskProcessor) saveCheckpoint() error {
    checkpoint := Checkpoint{
        Version:           "1.0",
        Timestamp:         time.Now(),
        LastProcessedID:   lastProcessedID,
        TotalProcessed:    int64(p.stats.totalProcessed),
        TotalSuccess:      int64(p.stats.successCount),
        TotalFailed:       int64(p.stats.failedCount),
        BatchNumber:       p.stats.batchCount,
        FailedCustomerIDs: p.stats.failedCustomers,
    }
    
    data, _ := json.MarshalIndent(checkpoint, "", "  ")
    os.WriteFile(p.config.CheckpointFile, data, 0644)
}
```

**Load Checkpoint:**

```go
func (p *RiskProcessor) loadCheckpoint() (Checkpoint, error) {
    data, err := os.ReadFile(p.config.CheckpointFile)
    if err != nil {
        return checkpoint, err
    }
    
    var checkpoint Checkpoint
    json.Unmarshal(data, &checkpoint)
    return checkpoint, nil
}
```

**Resume from Checkpoint:**

```go
if config.ResumeFromCheckpoint {
    checkpoint, _ := loadCheckpoint()
    
    // Load customers after last processed ID
    customerIDs, _ := loadCustomerIDsAfter(ctx, checkpoint.LastProcessedID)
    
    // Restore statistics
    stats.totalProcessed = int(checkpoint.TotalProcessed)
    stats.successCount = int(checkpoint.TotalSuccess)
    stats.failedCount = int(checkpoint.TotalFailed)
}
```

---

## Testing

### Unit Test Examples

**Test Risk Level Classification:**

```go
func TestClassifyRiskLevel(t *testing.T) {
    settings := &CachedSettings{
        LowRiskThreshold:    3.9,
        MediumRiskThreshold: 6.9,
        MaximumRiskThreshold: 9.0,
    }
    
    tests := []struct {
        score    float64
        expected string
    }{
        {2.0, "low"},
        {5.0, "medium"},
        {8.0, "high"},
    }
    
    for _, tt := range tests {
        result := classifyRiskLevel(tt.score, settings)
        if result != tt.expected {
            t.Errorf("classifyRiskLevel(%v) = %v, want %v", 
                tt.score, result, tt.expected)
        }
    }
}
```

**Test Score Aggregation:**

```go
func TestAggregateScores(t *testing.T) {
    scores := map[string]float64{
        "check1": 5.0,
        "check2": 10.0,
        "check3": 3.0,
    }
    
    // Test max
    max := aggregateScores(scores, "max")
    assert.Equal(t, 10.0, max)
    
    // Test avg
    avg := aggregateScores(scores, "avg")
    assert.Equal(t, 6.0, avg)
    
    // Test sum
    sum := aggregateScores(scores, "sum")
    assert.Equal(t, 18.0, sum)
}
```

### Integration Test

```go
func TestEndToEndProcessing(t *testing.T) {
    // Setup test database
    db := setupTestDB(t)
    defer db.Close()
    
    // Create test customers
    createTestCustomers(db, 100)
    
    // Create processor
    config := &Config{
        BatchSize:   10,
        WorkerCount: 2,
        DryRun:      true,
    }
    processor := NewRiskProcessor(config, db, logger)
    
    // Initialize cache
    processor.InitializeCache(context.Background())
    
    // Run processing
    err := processor.Run(context.Background())
    assert.NoError(t, err)
    
    // Verify statistics
    stats := processor.GetStats()
    assert.Equal(t, 100, stats.TotalProcessed)
    assert.Equal(t, 100, stats.SuccessCount)
    assert.Equal(t, 0, stats.FailedCount)
}
```

---

## Logging

### Log Levels

| Level | Usage |
|-------|-------|
| DEBUG | Detailed execution flow, function calls |
| INFO  | Progress updates, batch completion, statistics |
| WARN  | Recoverable errors, cache misses, retries |
| ERROR | Processing failures, database errors |

### Example Log Entries

**Application Startup:**

```json
{
  "level": "INFO",
  "timestamp": "2025-10-28T10:00:00Z",
  "message": "Risk processor starting with optimized settings",
  "version": "1.0.0",
  "cpu_cores": 16,
  "worker_count": 64,
  "batch_size": 1000
}
```

**Cache Initialization:**

```json
{
  "level": "INFO",
  "timestamp": "2025-10-28T10:00:05Z",
  "message": "Function definitions cached successfully from pg_proc",
  "function_count": 42,
  "optimization": "Functions will execute from memory cache"
}
```

**Progress Update:**

```json
{
  "level": "INFO",
  "timestamp": "2025-10-28T10:05:00Z",
  "message": "Processing progress",
  "processed": 50000,
  "total": 1000000,
  "progress_percent": 5.0,
  "elapsed": "5m0s",
  "estimated_remaining": "1h35m0s",
  "success_count": 49950,
  "failed_count": 50
}
```

**Batch Completion:**

```json
{
  "level": "INFO",
  "timestamp": "2025-10-28T10:00:15Z",
  "message": "Batch completed",
  "batch_number": 1,
  "success": 998,
  "failed": 2,
  "duration": "10s",
  "avg_ms_per_customer": 10
}
```

**Final Statistics:**

```json
{
  "level": "INFO",
  "timestamp": "2025-10-28T14:00:00Z",
  "message": "PROCESSING COMPLETED SUCCESSFULLY!",
  "duration": "4h0m0s",
  "total_customers": 1000000,
  "total_processed": 1000000,
  "success_count": 999500,
  "failed_count": 500,
  "success_rate": 99.95,
  "batches_processed": 1000
}
```

---

## Maintenance

### Cache Maintenance

**Clear Cache Files:**

```bash
rm -f /tmp/customer_ids.cache*
rm -f /tmp/processed_customers.txt
rm -f /tmp/risk_functions.json
rm -f /tmp/risk_calculator_metadata.json
```

**Validate Cache:**
Check cache metadata:

```bash
cat /tmp/risk_calculator_metadata.json | jq
```

### Database Maintenance

**Analyze Tables:**

```sql
ANALYZE res_partner;
ANALYZE res_compliance_risk_assessment_plan;
ANALYZE res_partner_risk_plan_line;
ANALYZE res_partner_composite_plan_line;
```

**Check Indexes:**

```sql
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE tablename IN ('res_partner', 'res_partner_risk_plan_line')
ORDER BY tablename, indexname;
```

**Vacuum:**

```sql
VACUUM ANALYZE res_partner;
VACUUM ANALYZE res_partner_risk_plan_line;
```

### Log Rotation

Configure logrotate for `/var/log/risk-processor.log`:

```bash
/var/log/risk-processor.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0644 risk-processor risk-processor
    postrotate
        systemctl reload risk-processor 2>/dev/null || true
    endscript
}
```

---

## Best Practices

### Configuration

1. **Start Conservative**: Begin with lower worker counts and increase gradually
2. **Monitor Resources**: Watch CPU, memory, and database connections
3. **Test with Dry Run**: Always test configuration changes with `--dry-run`
4. **Use Checkpoints**: Enable checkpoints for long-running jobs

### Database

1. **Connection Pool**: Set `pool_max = worker_count * 2 + 10`
2. **Indexes**: Ensure indexes on `res_partner.id`, `res_partner_risk_plan_line.partner_id`
3. **Maintenance**: Run VACUUM ANALYZE after large updates
4. **Monitoring**: Watch for long-running queries

### Performance

1. **Worker Tuning**: Match worker count to CPU cores
2. **Batch Size**: Use 1000-2000 for optimal balance
3. **Memory**: Monitor memory usage, reduce batch size if needed
4. **Cache**: Let cache files persist between runs

### Operations

1. **Scheduling**: Run during off-peak hours
2. **Monitoring**: Set up alerts for failures
3. **Logging**: Keep logs for at least 30 days
4. **Testing**: Test on subset before full run

---

## API Reference

### Command Line Interface

```bash
Usage: risk-processor [options]

Options:
  --dry-run                  Run without database updates
  --customer-ids=1,2,3       Process specific customers
  --workers=N                Override worker count
  --batch-size=N             Override batch size
  --resume-from-checkpoint   Resume from last checkpoint
  --help                     Show help message
```

### Configuration File (settings.conf)

**Database Section:**

```ini
[database]
host = string              # Database hostname
port = integer             # Database port (default: 5432)
dbname = string            # Database name
user = string              # Database username
password = string          # Database password
ssl_mode = string          # SSL mode (disable|require|verify-ca|verify-full)
pool_min = integer         # Min connections (default: 10)
pool_max = integer         # Max connections (default: 50)
```

**Risk Analysis Section:**

```ini
[risk_analysis]
batch_size = integer       # Customers per batch (default: 1000)
worker_count = integer     # Concurrent workers (default: 20)
workers_per_batch = integer # Workers within batch (default: 2)
chunk_size = integer       # Chunk size for queries (default: 10000)
log_level = string         # DEBUG|INFO|WARN|ERROR (default: INFO)
dry_run = boolean          # Dry run mode (default: false)
```

---

## Glossary

**Batch**: A group of customers processed together in a single transaction

**Checkpoint**: Saved state allowing resumption after interruption

**Composite Risk**: Weighted risk score across multiple universes

**Function-Based Risk**: Risk calculated by executing database check functions

**Incremental Processing**: Processing only unprocessed customers

**Plan-Based Risk**: Risk calculated by executing assessment plans

**Risk Level**: Classification of risk score (low/medium/high)

**Risk Universe**: Category of risk assessment with specific weight

**Worker Pool**: Set of goroutines processing jobs concurrently

---

## Version History

### Version 1.0.0 (Current)

- Initial release
- Function-based risk calculation
- Composite risk scoring
- Parallel batch processing
- Checkpoint system
- Incremental processing
- File-based caching

---

## Support and Contact

For technical issues or questions:

1. Check the troubleshooting section in README.md
2. Review logs in `/var/log/risk-processor.log`
3. Contact system administrator
4. Review code documentation (this file)

---

## License

Copyright © 2025 Novaji Introserve. All rights reserved.
