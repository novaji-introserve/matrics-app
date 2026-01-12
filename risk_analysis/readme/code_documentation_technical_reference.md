# Complete Code Documentation - Risk Analysis System

## Table of Contents

1. [cmd/risk-processor/main.go](#cmdrisk-processormain)
2. [config/config.go](#configconfiggo)
3. [application/risk_processor.go](#applicationrisk_processorgo)
4. [domain/models/models.go](#domainmodelsmodelsgo)
5. [domain/models/risk_function_result.go](#domainmodelsrisk_function_resultgo)
6. [domain/repositories/risk_function_repository.go](#domainrepositoriesrisk_function_repositorygo)
7. [domain/services/batched_function_risk_calculator.go](#domainservicesbatched_function_risk_calculatorgo)
8. [domain/services/batched_plan_risk_calculator.go](#domainservicesbatched_plan_risk_calculatorgo)
9. [domain/services/cached_function_executor.go](#domainservicescached_function_executorgo)
10. [infrastructure/cache/file_cache.go](#infrastructurecachefile_cachego)
11. [infrastructure/database/connection.go](#infrastructuredatabaseconnectiongo)
12. [infrastructure/repository/customer_repo.go](#infrastructurepositorycustomer_repogo)
13. [infrastructure/repository/postgres_risk_function_repo.go](#infrastructurerepositorypostgres_risk_function_repogo)
14. [workers/worker_pool.go](#workersworker_poolgo)
15. [utils/logger.go](#utilsloggergo)

---

# cmd/risk-processor/main.go

## Overview

Application entry point. Handles initialization, configuration, signal handling, and orchestration of the risk processing workflow.

## Functions

### `main()`

**Purpose**: Application entry point and main execution flow

**Algorithm**:

1. Configure GOMAXPROCS to use all CPU cores
2. Parse command-line flags
3. Load configuration from settings.conf
4. Initialize logger with file rotation
5. Set up database connection with pooling
6. Create cache directories
7. Initialize RiskProcessor
8. Initialize caches (functions, plans, settings)
9. Set up graceful shutdown handler
10. Run risk processor
11. Log final statistics

**Code Flow**:

```go
// Set CPU cores
numCPU := runtime.NumCPU()
runtime.GOMAXPROCS(numCPU)

// Parse flags
flag.Parse()

// Load config
cfg, err := config.LoadConfig()

// Initialize logger
logger, err := utils.NewLogger(logCfg)

// Setup graceful shutdown
ctx, cancel := context.WithCancel(context.Background())
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

go func() {
    sig := <-sigChan
    logger.Warn("Shutdown signal received")
    cancel()
    
    // Handle force shutdown on second signal
    go func() {
        <-sigChan
        logger.Error("Force shutdown")
        os.Exit(1)
    }()
}()

// Initialize database
dbConnection := database.NewConnection(dbConfig, logger)
err = dbConnection.Connect(ctx)

// Create processor
processor := application.NewRiskProcessor(cfg, dbConnection.GetPool(), logger)

// Initialize cache (CRITICAL!)
err = processor.InitializeCache(ctx)

// Run
err = processor.Run(ctx)

// Handle errors
if err == context.Canceled {
    // Graceful shutdown
    logger.Warn("Graceful shutdown completed")
    os.Exit(0)
} else if err != nil {
    logger.Error("Processing failed", zap.Error(err))
    os.Exit(1)
}
```

**Error Handling**:

- Configuration load errors → Fatal exit
- Logger initialization errors → Fatal exit
- Database connection errors → Fatal exit
- Cache initialization errors → Fatal exit
- Processing errors → Log and exit with code 1
- Context cancellation → Graceful shutdown with code 0

### `parseCustomerIDs(idStr string) ([]int, error)`

**Purpose**: Parse comma-separated customer ID string into slice of integers

**Parameters**:

- `idStr`: Comma-separated customer IDs (e.g., "1,2,3")

**Returns**:

- `[]int`: Slice of customer IDs
- `error`: Parsing error if invalid format

**Algorithm**:

```go
1. Split string by comma
2. For each ID string:
   - Trim whitespace
   - Skip if empty
   - Convert to integer
   - Return error if invalid
3. Return slice of IDs
```

**Example**:

```go
ids, err := parseCustomerIDs("1000, 1001, 1002")
// ids = []int{1000, 1001, 1002}
```

### `printHelp()`

**Purpose**: Display command-line help message

**Output**: Help text showing:

- Usage syntax
- Available options
- Configuration file location
- Examples

---

# config/config.go

## Overview

Configuration management using INI file format. Loads settings from `/data/odoo/ETL_script/update_script/settings.conf`.

## Types

### `Config`

**Purpose**: Hold all application configuration parameters

**Fields**:

```go
type RiskUniverse struct {
    ID                    int     `db:"id"`                        // Primary key
    Name                  string  `db:"name"`                      // Universe name
    IsIncludedInComposite bool    `db:"is_included_in_composite"`  // Include in composite calc
    WeightPercentage      float64 `db:"weight_percentage"`         // Weight in composite (0-100)
}
```

**Purpose**: Used in composite risk calculation with weighted scoring

### `CompositePlanLine`

**Purpose**: Represent result of composite plan execution in res_partner_composite_plan_line

**Fields**:

```go
type CompositePlanLine struct {
    ID           int     `db:"id"`            // Primary key
    PartnerID    int     `db:"partner_id"`    // Customer ID
    PlanID       int     `db:"plan_id"`       // Plan ID
    UniverseID   int     `db:"universe_id"`   // Universe ID
    SubjectID    *int    `db:"subject_id"`    // Subject ID
    Matched      bool    `db:"matched"`       // Did SQL query match
    RiskScore    float64 `db:"risk_score"`    // Risk rating from assessment
    AssessmentID *int    `db:"assessment_id"` // Assessment ID
}
```

**Purpose**: Stores composite calculation results for audit trail

### `Setting`

**Purpose**: Represent configuration setting from res_compliance_settings

**Fields**:

```go
type Setting struct {
    ID    int    `db:"id"`   // Primary key
    Code  string `db:"code"` // Setting code (e.g., "low_risk_threshold")
    Val   string `db:"val"`  // Setting value (string representation)
}
```

**Common Settings**:

- `low_risk_threshold`: Upper bound for low risk (e.g., "3.9")
- `medium_risk_threshold`: Upper bound for medium risk (e.g., "6.9")
- `maximum_risk_threshold`: Maximum possible score (e.g., "9.0")
- `risk_plan_computation`: Aggregation method (max/avg/sum)
- `risk_composite_computation`: Composite aggregation (max/avg/sum)

### `UniverseScoreData`

**Purpose**: Helper type for universe score calculation (not a database table)

**Fields**:

```go
type UniverseScoreData struct {
    Universe      RiskUniverse               // Universe metadata
    TotalScore    float64                    // Total score for universe
    Weight        float64                    // Weight percentage
    Name          string                     // Universe name
    SubjectScores map[int]SubjectScoreData   // Scores per subject
}
```

**Usage**: Internal data structure for composite calculations

### `SubjectScoreData`

**Purpose**: Helper type for subject within universe (not a database table)

**Fields**:

```go
type SubjectScoreData struct {
    Subject      interface{}      // Subject entity
    Score        float64          // Aggregated score
    MatchedPlans []int            // Plans that matched
    Assessment   *RiskAssessment  // Related assessment
}
```

**Usage**: Track scores per subject for aggregation

---

# domain/models/risk_function_result.go

## Overview

Represents results from executing database check_* functions.

## Types

### `RiskFunctionResult`

**Purpose**: Hold results from a check_* function execution

**Fields**:

```go
type RiskFunctionResult struct {
    FunctionName string             // Name of function (e.g., "check_cust_pep")
    Matches      map[string]float64 // Map of criterion -> score
    HasMatch     bool                // True if any criteria matched
    Error        error               // Execution error if any
}
```

**Example**:

```go
result := &RiskFunctionResult{
    FunctionName: "check_cust_pep",
    Matches: map[string]float64{
        "is_pep": 5.2,
        "high_risk_country": 10.0,
    },
    HasMatch: true,
    Error: nil,
}
```

## Functions

### `NewRiskFunctionResult(functionName string) *RiskFunctionResult`

**Purpose**: Create a new RiskFunctionResult with empty matches map

**Parameters**:

- `functionName`: Name of the check function

**Returns**: `*RiskFunctionResult` initialized with empty map

**Example**:

```go
result := NewRiskFunctionResult("check_jurisdiction")
```

### `AddMatch(key string, score float64)`

**Purpose**: Add a matched criterion with its score

**Parameters**:

- `key`: Criterion identifier (e.g., "cust_pep")
- `score`: Risk score for this criterion

**Side Effects**:

- Adds entry to Matches map
- Sets HasMatch to true

**Example**:

```go
result.AddMatch("is_pep", 5.2)
result.AddMatch("high_risk_country", 10.0)
```

### `GetScores() []float64`

**Purpose**: Return all positive scores from matched criteria

**Returns**: `[]float64` containing all scores > 0

**Algorithm**:

```go
1. Create slice with capacity = len(Matches)
2. For each score in Matches:
   IF score > 0:
      Append to slice
3. Return slice
```

**Example**:

```go
scores := result.GetScores()
// Returns: []float64{5.2, 10.0}
```

### `GetTotalScore() float64`

**Purpose**: Return sum of all scores

**Returns**: `float64` sum of all matched scores

**Algorithm**:

```go
total := 0.0
for _, score := range Matches {
    total += score
}
return total
```

**Example**:

```go
total := result.GetTotalScore()
// Returns: 15.2 (5.2 + 10.0)
```

### `GetMaxScore() float64`

**Purpose**: Return maximum score from all matches

**Returns**: `float64` maximum score

**Algorithm**:

```go
maxScore := 0.0
for _, score := range Matches {
    if score > maxScore {
        maxScore = score
    }
}
return maxScore
```

**Example**:

```go
max := result.GetMaxScore()
// Returns: 10.0
```

### `GetAverageScore() float64`

**Purpose**: Return average of all positive scores

**Returns**: `float64` average score, or 0.0 if no scores

**Algorithm**:

```go
scores := GetScores()
if len(scores) == 0 {
    return 0.0
}
total := sum(scores)
return total / float64(len(scores))
```

**Example**:

```go
avg := result.GetAverageScore()
// Returns: 7.6 ((5.2 + 10.0) / 2)
```

### `MatchCount() int`

**Purpose**: Return number of matched criteria

**Returns**: `int` count of entries in Matches map

**Example**:

```go
count := result.MatchCount()
// Returns: 2
```

---

# domain/repositories/risk_function_repository.go

## Overview

Repository interface for calling database check functions. Follows Dependency Inversion Principle.

## Interfaces

### `RiskFunctionRepository`

**Purpose**: Define contract for executing database check functions

**Methods**:

#### `CallCheckFunction(ctx context.Context, tx pgx.Tx, functionName string, partnerID int) (*models.RiskFunctionResult, error)`

**Purpose**: Call a specific check_* function for a customer

**Parameters**:

- `ctx`: Context for cancellation and timeout
- `tx`: Database transaction
- `functionName`: Name of function (e.g., "check_cust_pep")
- `partnerID`: Customer ID to check

**Returns**:

- `*models.RiskFunctionResult`: Result with matches and scores
- `error`: Execution error if any

**Example**:

```go
result, err := repo.CallCheckFunction(ctx, tx, "check_cust_pep", 1000)
if err != nil {
    // Handle error
}
if result.HasMatch {
    // Process matches
}
```

#### `CallAllCheckFunctions(ctx context.Context, tx pgx.Tx, partnerID int) ([]*models.RiskFunctionResult, error)`

**Purpose**: Call all check_* functions for a customer

**Parameters**:

- `ctx`: Context for cancellation and timeout
- `tx`: Database transaction
- `partnerID`: Customer ID to check

**Returns**:

- `[]*models.RiskFunctionResult`: Array of results from all functions
- `error`: Error during discovery or execution

**Benefits**:

- More efficient than calling functions individually
- Can batch operations
- Returns all results even if some functions fail

**Example**:

```go
results, err := repo.CallAllCheckFunctions(ctx, tx, 1000)
for _, result := range results {
    if result.Error != nil {
        log.Warn("Function failed", result.FunctionName)
        continue
    }
    if result.HasMatch {
        log.Info("Match found", result.FunctionName, result.Matches)
    }
}
```

#### `GetAvailableFunctions(ctx context.Context) ([]string, error)`

**Purpose**: Return list of all check_* functions in database

**Parameters**:

- `ctx`: Context for cancellation and timeout

**Returns**:

- `[]string`: Array of function names
- `error`: Discovery error

**Benefits**:

- Dynamic discovery of new functions without code changes
- Can be used to validate configuration
- Enables runtime function enumeration

**Example**:

```go
functions, err := repo.GetAvailableFunctions(ctx)
// Returns: ["check_cust_pep", "check_jurisdiction", "check_high_risk_country", ...]
```

---

# domain/services/batched_function_risk_calculator.go

## Overview

Core calculation engine with MAXIMUM performance optimizations. Processes customers using cached functions, settings, and composite plans.

## Types

### `CachedSettings`

**Purpose**: Hold frequently accessed settings loaded from database at startup

**Fields**:

```go
type CachedSettings struct {
    LowRiskThreshold     float64  // Upper bound for low risk (e.g., 3.9)
    MediumRiskThreshold  float64  // Upper bound for medium risk (e.g., 6.9)
    MaximumRiskThreshold float64  // Maximum possible score (e.g., 9.0)
    RiskPlanComputation  string   // Aggregation method (max/avg/sum)
    CompositeComputation string   // Composite aggregation (max/avg/sum)
}
```

### `CustomerRiskResult`

**Purpose**: Hold result of risk calculation for a single customer

**Fields**:

```go
type CustomerRiskResult struct {
    CustomerID         int                  // Customer ID
    RiskScore          float64              // Final calculated score
    RiskLevel          string               // Risk classification (low/medium/high)
    Error              error                // Calculation error if any
    CompositePlanLines []CompositePlanLine  // Plan lines for bulk insert
}
```

### `CompositePlanLine`

**Purpose**: Represent a composite plan line to be bulk inserted

**Fields**:

```go
type CompositePlanLine struct {
    PartnerID    int     // Customer ID
    PlanID       int     // Plan ID
    UniverseID   int     // Universe ID
    SubjectID    int     // Subject ID
    AssessmentID int     // Assessment ID
    Matched      bool    // Did SQL match
    RiskScore    float64 // Risk rating
    Name         string  // Plan name
}
```

### `BatchedFunctionRiskCalculator`

**Purpose**: Process customers with MAXIMUM performance optimizations

**Fields**:

```go
type BatchedFunctionRiskCalculator struct {
    db               *pgxpool.Pool                // Database pool
    logger           *zap.Logger                  // Logger
    functionExecutor *CachedFunctionExecutor      // Cached function executor
    cachedSettings   *CachedSettings              // Cached settings
    compositePlans   []*RiskPlan                  // Cached composite plans
    cacheInitialized bool                         // Cache ready flag
    cacheMu          sync.RWMutex                 // Cache lock
    cacheFilePath    string                       // Cache metadata file path
    
    // Performance metrics
    totalBatches      int64  // Total batches processed
    totalCustomers    int64  // Total customers processed
    totalProcessingMs int64  // Total processing time in ms
    metricsMu         sync.Mutex  // Metrics lock
}
```

**Performance Features**:

1. Function definitions cached at startup (ZERO DB lookups)
2. Settings cached at startup (ZERO DB lookups)
3. Composite plans cached at startup
4. Parallel processing within each batch
5. Bulk updates using PostgreSQL COPY

### `RiskPlan`

**Purpose**: Represent a risk assessment plan (domain service version)

**Fields**: Same as models.RiskPlan

### `RiskUniverse`

**Purpose**: Represent a risk universe (domain service version)

**Fields**: Same as models.RiskUniverse

### `RiskAssessment`

**Purpose**: Represent a risk assessment (domain service version)

**Fields**:

```go
type RiskAssessment struct {
    ID         int            // Assessment ID
    SubjectID  sql.NullInt64  // Subject ID (can be NULL)
    RiskRating float64        // Risk rating
}
```

## Functions

### `convertPythonToPostgresSQL(query string) (string, int)`

**Purpose**: Convert Python-style placeholders (%s) to PostgreSQL placeholders ($1, $2, etc.)

**Parameters**:

- `query`: SQL query with Python placeholders

**Returns**:

- `string`: Converted query with PostgreSQL placeholders
- `int`: Number of parameters found

**Algorithm**:

```go
paramCount := 1
result := ""
i := 0

FOR i < len(query):
    IF query[i] == '%' AND query[i+1] == 's':
        result += "$" + strconv.Itoa(paramCount)
        paramCount++
        i += 2  // Skip both % and s
    ELSE:
        result += string(query[i])
        i++

return result, paramCount - 1
```

**Example**:

```go
query := "SELECT * FROM res_partner WHERE id = %s AND active = %s"
converted, count := convertPythonToPostgresSQL(query)
// converted = "SELECT * FROM res_partner WHERE id = $1 AND active = $2"
// count = 2
```

### `NewBatchedFunctionRiskCalculator(db *pgxpool.Pool, logger *zap.Logger, riskFunctionsCachePath, riskMetadataCachePath string) *BatchedFunctionRiskCalculator`

**Purpose**: Create a new optimized batched function-based risk calculator

**Parameters**:

- `db`: Database connection pool
- `logger`: Structured logger
- `riskFunctionsCachePath`: Path to functions cache file
- `riskMetadataCachePath`: Path to metadata cache file

**Returns**: `*BatchedFunctionRiskCalculator` initialized but not cached

**Note**: Must call InitializeCache() before using

### `InitializeCache(ctx context.Context) error`

**Purpose**: Load all settings and function definitions into memory (CRITICAL for performance)

**Returns**: Error if initialization fails

**Algorithm**:

```go
1. Log cache initialization start
2. Initialize function executor cache:
   - Loads all check_* functions from pg_proc
   - Caches function definitions in memory
3. Start database transaction
4. Load settings:
   - low_risk_threshold (default: 3.9)
   - medium_risk_threshold (default: 6.9)
   - maximum_risk_threshold (default: 9.0)
   - risk_plan_computation (default: max)
   - risk_composite_computation (default: max)
5. Load composite plans:
   - SELECT from res_compliance_risk_assessment_plan
   - WHERE state = 'active' AND use_composite_calculation = true
   - Convert Python SQL to PostgreSQL
6. Count functions and plans in database
7. Commit transaction
8. Store cached data
9. Save cache metadata to disk
10. Set cacheInitialized = true
11. Log success with statistics
```

**Performance Impact**:

- **Before**: Every customer requires N+1 DB queries (N functions + 1 settings)
- **After**: ZERO DB queries for metadata per customer

**Example**:

```go
calculator := NewBatchedFunctionRiskCalculator(db, logger, funcCache, metaCache)
err := calculator.InitializeCache(ctx)
if err != nil {
    log.Fatal("Cache initialization failed:", err)
}
// Now ready to process millions of customers efficiently
```

### `saveCacheMetadata(functionCount, compositePlanCount int)`

**Purpose**: Save cache validation data to disk

**Parameters**:

- `functionCount`: Number of functions in database
- `compositePlanCount`: Number of composite plans

**Algorithm**:

```go
1. Create metadata map:
   - function_count
   - composite_plan_count
   - cached_at timestamp
   - composite_plans array
2. Marshal to JSON with indentation
3. Write to cacheFilePath
4. Log success
```

**File Example**:

```json
{
  "function_count": 42,
  "composite_plan_count": 8,
  "cached_at": "2025-10-28T10:00:00Z",
  "composite_plans": [...]
}
```

### `loadCompositePlans(ctx context.Context, tx pgx.Tx) ([]*RiskPlan, error)`

**Purpose**: Load composite risk assessment plans from database

**Parameters**:

- `ctx`: Context
- `tx`: Database transaction

**Returns**:

- `[]*RiskPlan`: Array of composite plans
- `error`: Query error

**Algorithm**:

```go
1. Query composite plans:
   SELECT id, name, state, priority, risk_score,
          compute_score_from, sql_query,
          risk_assessment, universe_id, use_composite_calculation
   FROM res_compliance_risk_assessment_plan
   WHERE state = 'active'
     AND use_composite_calculation = true
     AND compute_score_from = 'risk_assessment'
   ORDER BY priority

2. For each row:
   - Scan into RiskPlan struct
   - Convert Python SQL to PostgreSQL
   - Handle NULL values for risk_assessment and universe_id
   - Append to plans array

3. Return plans array
```

**Why Composite Plans**:

- Used for weighted scoring across universes
- Separate from regular plan-based risk
- Adds to final score

### `ProcessCustomerBatch(ctx context.Context, customerIDs []int, dryRun bool, workersPerBatch int) []CustomerRiskResult`

**Purpose**: Process multiple customers in parallel using cached functions

**Parameters**:

- `ctx`: Context for cancellation
- `customerIDs`: Customer IDs to process
- `dryRun`: If true, don't update database
- `workersPerBatch`: Number of parallel workers

**Returns**: `[]CustomerRiskResult` with result for each customer

**Algorithm**:

```go
1. Validate cache initialized
2. Return empty if no customers
3. Record start time
4. Create results slice
5. Create worker pool:
   - Create job channel
   - Determine number of workers (default: 4)
   - Start worker goroutines
6. Each worker:
   - Pull jobs from channel
   - Check context cancellation
   - Call calculateSingleCustomer()
   - Store result
7. Submit all jobs to channel
8. Close channel
9. Wait for workers to complete
10. If not dry run:
    - Update customer risk scores (bulk UNNEST)
    - Bulk insert composite plan lines (PostgreSQL COPY)
11. Calculate duration
12. Update metrics
13. Log batch statistics
14. Return results
```

**Worker Pattern**:

```go
type job struct {
    index      int
    customerID int
}

jobs := make(chan job, len(customerIDs))

// Start workers
for w := 0; w < numWorkers; w++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        for j := range jobs {
            // Check cancellation
            select {
            case <-ctx.Done():
                results[j.index] = CustomerRiskResult{
                    CustomerID: j.customerID,
                    Error: ctx.Err(),
                }
                continue
            default:
            }
            
            // Process customer
            score, level, planLines, err := calculateSingleCustomer(ctx, j.customerID)
            results[j.index] = CustomerRiskResult{...}
        }
    }()
}

// Submit jobs
for i, custID := range customerIDs {
    jobs <- job{index: i, customerID: custID}
}
close(jobs)
wg.Wait()
```

**Performance**:

- Parallel processing within batch
- No waiting between customers
- Bulk database updates
- Example: 1000 customers in 10 seconds = 100 customers/second

### `calculateSingleCustomer(ctx context.Context, customerID int) (float64, string, []CompositePlanLine, error)`

**Purpose**: Calculate risk score for a single customer using cached functions

**Parameters**:

- `ctx`: Context
- `customerID`: Customer ID to process

**Returns**:

- `float64`: Final risk score
- `string`: Risk level (low/medium/high)
- `[]CompositePlanLine`: Composite plan lines for bulk insert
- `error`: Calculation error

**Algorithm** (matches Python implementation):

```go
1. Read cached settings (thread-safe read lock)

2. Start database transaction

3. Clear previous composite plan lines:
   DELETE FROM res_partner_composite_plan_line WHERE partner_id = $1

4. Calculate composite score if composite plans exist:
   - Call calculateCompositeScore()
   - Returns: compositeScore, compositePlanLines
   - Store composite_risk_score in database immediately

5. Priority 1: Check Approved EDD (HIGHEST PRIORITY):
   SELECT risk_score FROM res_partner_edd
   WHERE customer_id = $1 AND status = 'approved'
   ORDER BY COALESCE(date_approved, write_date, create_date) DESC
   LIMIT 1
   
   IF found:
      finalScore = eddScore
      Apply maximum threshold cap
      Classify risk level
      Commit transaction
      RETURN finalScore, level, compositePlanLines
      (Note: EDD score used AS-IS, no composite added)

6. Priority 2: Get plan-based scores:
   SELECT plan_line_id, risk_score
   FROM res_partner_risk_plan_line
   WHERE partner_id = $1 AND risk_score > 0
   
   Store in map[string]float64

7. Aggregate plan scores using configured method (max/avg/sum):
   aggregatedScore = aggregateScores(planScores, settings.RiskPlanComputation)

8. Add composite score:
   finalScore = aggregatedScore + compositeScore

9. Apply maximum threshold cap:
   IF finalScore > settings.MaximumRiskThreshold:
      finalScore = settings.MaximumRiskThreshold

10. Classify risk level:
    level = classifyRiskLevel(finalScore, settings)

11. Commit transaction

12. RETURN finalScore, level, compositePlanLines
```

**Priority Order** (Critical):

1. **Approved EDD** → Use EDD score only (highest priority, overrides all)
2. **Plan-Based Risk** → Aggregate from res_partner_risk_plan_line + composite

**Composite Score Addition**:

- Added to plan-based score
- NOT added to EDD score (EDD is standalone)

**Example Execution**:

```go
// Customer 1000 with approved EDD
score, level, lines, err := calculateSingleCustomer(ctx, 1000)
// Returns: score=8.5 (from EDD), level="high", lines=[...composite lines], err=nil

// Customer 1001 without EDD, with plans
score, level, lines, err := calculateSingleCustomer(ctx, 1001)
// Returns: score=6.5 (plan=4.5 + composite=2.0), level="medium", lines=[...], err=nil
```

### `updateCustomerRiskScores(ctx context.Context, results []CustomerRiskResult) error`

**Purpose**: Update risk scores in database for a batch using bulk operation

**Parameters**:

- `ctx`: Context
- `results`: Array of customer results

**Returns**: Error if update fails

**Algorithm**:

```go
1. Filter out errors - only keep successful results
2. Return if no valid results
3. Extract arrays:
   - customerIDs []int
   - scores []float64
   - levels []string
4. Execute bulk update using UNNEST:
   UPDATE res_partner
   SET risk_score = updates.score,
       risk_level = updates.level,
       write_date = NOW()
   FROM (
       SELECT unnest($1::integer[]) AS id,
              unnest($2::numeric[]) AS score,
              unnest($3::text[]) AS level
   ) AS updates
   WHERE res_partner.id = updates.id
5. Log count updated
6. Return nil or error
```

**Performance**:

- Single query updates thousands of customers
- Much faster than individual UPDATEs
- Example: 1000 customers updated in <100ms

### `bulkInsertCompositePlanLines(ctx context.Context, results []CustomerRiskResult) error`

**Purpose**: Insert all composite plan lines using PostgreSQL COPY for maximum performance

**Parameters**:

- `ctx`: Context
- `results`: Array of customer results

**Returns**: Error if insert fails

**Algorithm**:

```go
1. Collect all composite plan lines from all customers:
   allPlanLines := []CompositePlanLine{}
   FOR each result:
      IF no error AND has plan lines:
         Append result.CompositePlanLines to allPlanLines

2. Return if no plan lines to insert

3. Use pgx CopyFrom for bulk insert:
   db.CopyFrom(
       ctx,
       pgx.Identifier{"res_partner_composite_plan_line"},
       []string{"partner_id", "plan_id", "universe_id", ...},
       pgx.CopyFromSlice(len(allPlanLines), func(i int) ([]interface{}, error) {
           line := allPlanLines[i]
           return []interface{}{
               line.PartnerID,
               line.PlanID,
               line.UniverseID,
               line.SubjectID,
               line.AssessmentID,
               line.Matched,
               line.RiskScore,
               line.Name,
               true,  // active
               1,     // create_uid
               time.Now(),  // create_date
               1,     // write_uid
               time.Now(),  // write_date
           }, nil
       }),
   )

4. Log rows inserted
5. Return nil or error
```

**Performance**:

- PostgreSQL COPY is 10-100x faster than individual INSERTs
- Can insert 10,000+ rows in < 1 second
- Critical for high-volume processing

### `getSetting(ctx context.Context, tx pgx.Tx, code string) (string, error)`

**Purpose**: Retrieve a setting value from database

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `code`: Setting code (e.g., "low_risk_threshold")

**Returns**:

- `string`: Setting value
- `error`: Error if not found or query fails

**Query**:

```sql
SELECT val FROM res_compliance_settings 
WHERE code = $1 
LIMIT 1
```

**Error Cases**:

- Setting not found → error with message
- Query fails → error with details

### `updateMetrics(customerCount int, duration time.Duration)`

**Purpose**: Update performance metrics (thread-safe)

**Parameters**:

- `customerCount`: Number of customers processed
- `duration`: Processing duration

**Algorithm**:

```go
metricsMu.Lock()
defer metricsMu.Unlock()

totalBatches++
totalCustomers += int64(customerCount)
totalProcessingMs += duration.Milliseconds()
```

### `GetMetrics() map[string]interface{}`

**Purpose**: Return performance metrics

**Returns**: Map with metrics:

- `total_batches`: Total batches processed
- `total_customers`: Total customers processed
- `total_processing_ms`: Total time in milliseconds
- `avg_ms_per_batch`: Average batch time
- `avg_ms_per_customer`: Average customer time
- `customers_per_second`: Processing rate
- `optimization_level`: Description

**Example Output**:

```go
{
    "total_batches": 1000,
    "total_customers": 1000000,
    "total_processing_ms": 3600000,
    "avg_ms_per_batch": 3600,
    "avg_ms_per_customer": 3.6,
    "customers_per_second": 277.8,
    "optimization_level": "MAXIMUM - Cached functions + parallel processing"
}
```

### `checkRiskAssessment(ctx context.Context, tx pgx.Tx, customerID int) (float64, bool, error)`

**Purpose**: Check if customer has risk assessment (currently unused)

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `customerID`: Customer ID

**Returns**:

- `float64`: Risk rating
- `bool`: True if found
- `error`: Query error

**Query**:

```sql
SELECT risk_rating FROM res_risk_assessment 
WHERE partner_id = $1 
ORDER BY create_date DESC 
LIMIT 1
```

**Note**: Marked as unused in code, kept for potential future use

### `checkApprovedEDD(ctx context.Context, tx pgx.Tx, customerID int) (float64, bool, error)`

**Purpose**: Check if customer has approved Enhanced Due Diligence

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `customerID`: Customer ID

**Returns**:

- `float64`: EDD risk score
- `bool`: True if found
- `error`: Query error

**Query**:

```sql
SELECT risk_score FROM res_partner_edd 
WHERE customer_id = $1 AND status = 'approved' 
ORDER BY COALESCE(date_approved, write_date, create_date) DESC 
LIMIT 1
```

**Priority**: HIGHEST - if EDD found, use its score and return immediately

**Example**:

```go
eddScore, found, err := checkApprovedEDD(ctx, tx, 1000)
if found {
    // Use EDD score, skip all other calculations
    return eddScore, classifyLevel(eddScore), nil
}
```

### `getCompositeScore(ctx context.Context, tx pgx.Tx, customerID int) (float64, error)`

**Purpose**: Get composite risk score from database (currently unused)

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `customerID`: Customer ID

**Returns**:

- `float64`: Composite score
- `error`: Query error

**Query**:

```sql
SELECT composite_risk_score FROM res_partner 
WHERE id = $1
```

**Note**: Marked as unused because composite score is calculated and stored directly

### `getPlanBasedScores(ctx context.Context, tx pgx.Tx, customerID int) (map[string]float64, error)`

**Purpose**: Get all plan-based risk scores from res_partner_risk_plan_line table

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `customerID`: Customer ID

**Returns**:

- `map[string]float64`: Map of plan_N -> score
- `error`: Query error

**Query**:

```sql
SELECT plan_line_id, risk_score
FROM res_partner_risk_plan_line
WHERE partner_id = $1 AND risk_score > 0
```

**Algorithm**:

```go
1. Query plan lines
2. For each row:
   - Scan plan_line_id and risk_score
   - Skip if plan_line_id is NULL
   - Add to map with key "plan_{id}"
3. Return map
```

**Example Return**:

```go
map[string]float64{
    "plan_1": 5.2,
    "plan_3": 3.8,
    "plan_7": 10.0,
}
```

### `aggregateScores(results map[string]float64, method string) float64`

**Purpose**: Aggregate scores based on configured method

**Parameters**:

- `results`: Map of scores to aggregate
- `method`: Aggregation method (max/avg/sum)

**Returns**: `float64` aggregated score

**Algorithm**:

```go
IF len(results) == 0:
    RETURN 0

SWITCH method:
CASE "max":
    max := 0.0
    FOR each score IN results:
        IF score > max:
            max = score
    RETURN max

CASE "avg":
    sum := 0.0
    FOR each score IN results:
        sum += score
    RETURN sum / len(results)

CASE "sum":
    sum := 0.0
    FOR each score IN results:
        sum += score
    RETURN sum

DEFAULT:
    // Default to max
    RETURN max(results)
```

**Example**:

```go
scores := map[string]float64{"plan_1": 5.2, "plan_2": 3.8, "plan_3": 10.0}

max := aggregateScores(scores, "max")     // Returns: 10.0
avg := aggregateScores(scores, "avg")     // Returns: 6.33
sum := aggregateScores(scores, "sum")     // Returns: 19.0
```

### `aggregateScoresSlice(scores []float64, method string) float64`

**Purpose**: Aggregate a slice of scores using specified method

**Parameters**:

- `scores`: Slice of scores
- `method`: Aggregation method (max/avg/sum)

**Returns**: `float64` aggregated score

**Algorithm**: Same as aggregateScores but operates on slice instead of map

**Example**:

```go
scores := []float64{5.2, 3.8, 10.0}

max := aggregateScoresSlice(scores, "max")  // Returns: 10.0
avg := aggregateScoresSlice(scores, "avg")  // Returns: 6.33
sum := aggregateScoresSlice(scores, "sum")  // Returns: 19.0
```

### `classifyRiskLevel(score float64, settings *CachedSettings) string`

**Purpose**: Classify risk score into level (low/medium/high)

**Parameters**:

- `score`: Risk score to classify
- `settings`: Cached settings with thresholds

**Returns**: `string` risk level

**Algorithm**:

```go
IF score <= settings.LowRiskThreshold:
    RETURN "low"
ELSE IF score <= settings.MediumRiskThreshold:
    RETURN "medium"
ELSE:
    RETURN "high"
```

**Example** (with default thresholds):

```go
classifyRiskLevel(2.5, settings)  // Returns: "low"    (≤ 3.9)
classifyRiskLevel(5.0, settings)  // Returns: "medium" (3.9 < score ≤ 6.9)
classifyRiskLevel(8.5, settings)  // Returns: "high"   (> 6.9)
```

### `calculateCompositeScore(ctx context.Context, tx pgx.Tx, customerID int, compositePlans []*RiskPlan, compositeComputation string) (float64, []CompositePlanLine, error)`

**Purpose**: Execute composite plans and calculate weighted composite score

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `customerID`: Customer ID
- `compositePlans`: Cached composite plans
- `compositeComputation`: Aggregation method (max/avg/sum)

**Returns**:

- `float64`: Composite score
- `[]CompositePlanLine`: Plan lines for bulk insert
- `error`: Calculation error

**Algorithm** (matches Python code lines 182-336):

```go
1. Return 0 if no composite plans

2. Load universes with is_included_in_composite = true:
   SELECT id, name, is_included_in_composite, weight_percentage
   FROM res_risk_universe
   WHERE is_included_in_composite = true
     AND weight_percentage > 0
   
   Store in map[int]*RiskUniverse

3. Return 0 if no universes

4. Initialize tracking structures:
   - universeSubjectScores: map[universeID]map[subjectID][]float64
   - compositePlanLines: []CompositePlanLine

5. FOR each composite plan:
   a. Validate plan has sql_query (skip if empty)
   
   b. Validate universe_id exists and is in loaded universes map:
      IF plan.UniverseID == nil:
         Skip plan
      IF universe not in map OR not included in composite:
         Skip plan
   
   c. Validate risk_assessment_id exists:
      IF plan.RiskAssessmentID == nil:
         Skip plan
   
   d. Get risk assessment details:
      SELECT id, subject_id, risk_rating
      FROM res_risk_assessment
      WHERE id = $1
      
      IF error OR subject_id is NULL OR risk_rating is NULL/≤0:
         Skip plan
   
   e. Execute SQL query to check match:
      EXECUTE plan.sql_query WITH customerID
      matched = (at least one row returned)
   
   f. IF matched:
      - Create CompositePlanLine:
        * PartnerID = customerID
        * PlanID = plan.ID
        * UniverseID = plan.UniverseID
        * SubjectID = assessment.SubjectID
        * AssessmentID = assessment.ID
        * Matched = true
        * RiskScore = assessment.RiskRating
        * Name = plan.Name
      
      - Append to compositePlanLines array
      
      - Track score for aggregation:
        universeSubjectScores[universeID][subjectID] = append(..., risk_rating)

6. Calculate weighted composite score:
   totalWeightedScore := 0.0
   totalWeight := 0.0
   
   FOR each universeID, subjectScores IN universeSubjectScores:
      universe := universes[universeID]
      
      // Aggregate scores per subject first
      universeScores := []float64{}
      FOR each subjectID, scores IN subjectScores:
         IF len(scores) > 0:
            aggregatedScore := aggregateScoresSlice(scores, compositeComputation)
            universeScores = append(universeScores, aggregatedScore)
      
      // Aggregate all subject scores for this universe
      IF len(universeScores) > 0:
         universeScore := aggregateScoresSlice(universeScores, compositeComputation)
         
         // Apply weight percentage
         weightedScore := universeScore * (universe.WeightPercentage / 100.0)
         totalWeightedScore += weightedScore
         totalWeight += universe.WeightPercentage

7. Log composite calculation summary

8. RETURN totalWeightedScore, compositePlanLines, nil
```

**Example Execution**:

```
Customer 1000 with 3 composite plans across 2 universes:

Plan 1 (Universe A, Weight 60%):
  - Matches
  - Subject 5, Score 8.0
  
Plan 2 (Universe A, Weight 60%):
  - Matches
  - Subject 5, Score 6.0
  
Plan 3 (Universe B, Weight 40%):
  - Matches
  - Subject 3, Score 7.0

Aggregation (method: avg):
  Universe A, Subject 5: avg(8.0, 6.0) = 7.0
  Universe A Score: 7.0
  Universe A Weighted: 7.0 * 0.60 = 4.2
  
  Universe B, Subject 3: 7.0
  Universe B Score: 7.0
  Universe B Weighted: 7.0 * 0.40 = 2.8
  
  Total Composite Score: 4.2 + 2.8 = 7.0
```

**Skip Reasons Tracked**:

- `no_sql_query`: Plan has no SQL query
- `no_universe_id`: Plan missing universe_id
- `universe_not_in_map`: Universe not in loaded map
- `universe_not_included`: Universe not included in composite
- `no_assessment_id`: Plan missing risk_assessment_id
- `null_subject_id`: Assessment has NULL subject_id
- `null_or_zero_rating`: Assessment has NULL or zero risk_rating
- `sql_error`: SQL query execution failed
- `sql_no_match`: SQL query returned no rows

---

# domain/services/batched_plan_risk_calculator.go

## Overview

Alternative calculator using plan-based approach (currently not actively used). Kept for potential future use.

## Types

Same types as BatchedFunctionRiskCalculator (CachedSettings, CustomerRiskResult, etc.)

### `BatchedPlanRiskCalculator`

**Purpose**: Process customers using plan-based approach

**Fields**:

```go
type BatchedPlanRiskCalculator struct {
    db               *pgxpool.Pool
    logger           *zap.Logger
    cachedSettings   *CachedSettings
    compositePlans   []*models.RiskPlan
    regularPlans     []*models.RiskPlan
    cacheInitialized bool
    cacheMu          sync.RWMutex
    
    // Performance metrics (same as function-based)
    totalBatches      int64
    totalCustomers    int64
    totalProcessingMs int64
    metricsMu         sync.Mutex
}
```

**Difference from Function-Based**:

- Uses plan SQL queries directly instead of check_* functions
- Separates composite plans from regular plans
- Does not use CachedFunctionExecutor

## Functions

Most functions are similar to BatchedFunctionRiskCalculator with key differences:

### `loadRegularPlans(ctx context.Context, tx pgx.Tx, excludeIDs []int) ([]*models.RiskPlan, error)`

**Purpose**: Load active plans excluding composite plans

**Query**:

```sql
SELECT id, name, state, priority, risk_score,
       compute_score_from, sql_query,
       risk_assessment, universe_id, use_composite_calculation
FROM res_compliance_risk_assessment_plan
WHERE state = 'active'
  AND (id != ALL($1) OR $1 = '{}')
ORDER BY priority
```

**Returns**: Regular (non-composite) plans only

### `calculateSingleCustomer(ctx context.Context, customerID int) (float64, string, error)`

**Purpose**: Calculate risk using plan-based approach

**Algorithm** (mirrors Python _get_risk_score_from_plan):

```go
1. Clear previous risk plan lines:
   DELETE FROM res_partner_risk_plan_line WHERE partner_id = $1

2. Clear previous composite plan lines:
   DELETE FROM res_partner_composite_plan_line WHERE partner_id = $1

3. Calculate composite score if plans exist

4. Priority 1: Check Risk Assessment:
   SELECT risk_rating FROM res_risk_assessment
   WHERE partner_id = $1
   ORDER BY create_date DESC
   LIMIT 1
   
   IF found: RETURN risk_rating (capped), level

5. Priority 2: Check Approved EDD:
   (Same as function-based)

6. Priority 3: Execute regular risk plans:
   FOR each plan IN regularPlans:
      score := 0.0
      
      SWITCH plan.ComputeScoreFrom:
      CASE "python":
         Skip (not supported in Go)
      
      CASE "dynamic", "static", "risk_assessment":
         Execute plan.SQLQuery with customerID
         
         IF query returns row (match):
            SWITCH plan.ComputeScoreFrom:
            CASE "dynamic":
               score = value from query
            CASE "static":
               score = plan.RiskScore
            CASE "risk_assessment":
               IF plan has RiskAssessmentID:
                  Get risk_rating from assessment
                  score = risk_rating
               ELSE:
                  score = plan.RiskScore
            
            Append score to scores array
            
            Create risk plan line:
            INSERT INTO res_partner_risk_plan_line
            (partner_id, plan_line_id, risk_score)
            VALUES ($1, $2, $3)

7. Aggregate plan scores:
   EXECUTE aggregation query:
   SELECT {AVG/MAX/SUM}(risk_score)
   FROM res_partner_risk_plan_line
   WHERE partner_id = $1 AND risk_score > 0

8. Add composite score:
   finalScore = planScore + compositeScore

9. Apply maximum threshold cap

10. Classify risk level

11. RETURN finalScore, level
```

**Note**: This calculator is NOT currently used. The function-based calculator is used instead.

---

# domain/services/cached_function_executor.go

## Overview

Caches database function definitions at startup and executes them from memory. Eliminates database metadata lookups.

## Types

### `RiskFunctionDefinition`

**Purpose**: Hold a cached function definition

**Fields**:

```go
type RiskFunctionDefinition struct {
    ID           int     // Function OID from pg_proc
    FunctionName string  // Function name (e.g., "check_cust_pep")
    QueryText    string  // SQL to execute function
    Sequence     int     // Order in list
    Active       bool    // Is active (always true)
    ParamCount   int     // Number of parameters
}
```

**Example**:

```go
{
    ID: 12345,
    FunctionName: "check_cust_pep",
    QueryText: "SELECT check_cust_pep($1)",
    Sequence: 1,
    Active: true,
    ParamCount: 1,
}
```

### `CachedFunctionExecutor`

**Purpose**: Cache function definitions and execute from memory

**Fields**:

```go
type CachedFunctionExecutor struct {
    db                *pgxpool.Pool                      // Database pool
    logger            *zap.Logger                        // Logger
    functions         []*RiskFunctionDefinition          // Array of functions
    functionsMap      map[string]*RiskFunctionDefinition // Map for lookup
    cacheInitialized  bool                               // Cache ready
    cacheMu           sync.RWMutex                       // Cache lock
    cacheFile         string                             // Cache file path
}
```

## Functions

### `NewCachedFunctionExecutor(db *pgxpool.Pool, logger *zap.Logger, cacheFilePath string) *CachedFunctionExecutor`

**Purpose**: Create a new cached function executor

**Parameters**:

- `db`: Database pool
- `logger`: Logger
- `cacheFilePath`: Path to save function cache

**Returns**: `*CachedFunctionExecutor` initialized but not cached

### `InitializeCache(ctx context.Context) error`

**Purpose**: Load all function definitions from pg_proc into memory

**Returns**: Error if loading fails

**Algorithm**:

```go
1. Log cache initialization

2. Query pg_proc for all check_* functions:
   SELECT p.oid::int AS id,
          p.proname AS function_name,
          ROW_NUMBER() OVER (ORDER BY p.proname) AS sequence
   FROM pg_proc p
   JOIN pg_namespace n ON p.pronamespace = n.oid
   WHERE n.nspname = 'public'
     AND p.proname LIKE 'check_%'
     AND p.prokind = 'f'
   ORDER BY p.proname

3. For each function:
   - Create RiskFunctionDefinition
   - Set Active = true
   - Set QueryText = "SELECT {function_name}($1)"
   - Set ParamCount = 1
   - Add to functions array
   - Add to functionsMap

4. Set cacheInitialized = true

5. Save to cache file

6. Log success with function count

7. Return nil
```

**Performance Impact**:

- **Without cache**: Query pg_proc for every customer
- **With cache**: ZERO database queries for function metadata

**Example**:

```go
executor := NewCachedFunctionExecutor(db, logger, "/tmp/functions.json")
err := executor.InitializeCache(ctx)
// Loaded 42 functions into cache
```

### `saveCacheToFile() error`

**Purpose**: Save function cache to JSON file

**Algorithm**:

```go
1. Marshal functions array to JSON with indentation
2. Write to cacheFile
3. Log success
4. Return nil or error
```

**File Format**:

```json
[
  {
    "ID": 12345,
    "FunctionName": "check_cust_pep",
    "QueryText": "SELECT check_cust_pep($1)",
    "Sequence": 1,
    "Active": true,
    "ParamCount": 1
  },
  ...
]
```

### `ExecuteAllFunctions(ctx context.Context, tx pgx.Tx, customerID int) (map[string]float64, error)`

**Purpose**: Execute all cached functions for a customer

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `customerID`: Customer ID

**Returns**:

- `map[string]float64`: Map of function_name -> score
- `error`: Execution error

**Algorithm**:

```go
1. Validate cache initialized

2. Acquire read lock on cache (allows concurrent reads)

3. Get functions array

4. Release read lock

5. Initialize results map

6. FOR each function IN functions:
   a. Execute cached query: tx.QueryRow(fn.QueryText, customerID)
   
   b. Scan result into rawResult interface{}
   
   c. Extract numeric value from result:
      SWITCH type of rawResult:
      CASE float64, float32, int64, int32, int:
         numericValue = convert to float64
      
      CASE string:
         IF empty or "{}":
            Continue (no match)
         TRY parse as JSON
         IF JSON object:
            Extract first numeric value
      
      CASE []byte (JSONB):
         Parse as JSON
         Extract first numeric value
      
      DEFAULT:
         Continue (unsupported type)
   
   d. IF numericValue > 0:
         results[fn.FunctionName] = numericValue

7. RETURN results map
```

**JSON Extraction Example**:

```go
// Function returns: {"cust_pep": 5.2, "high_risk": 10.0}
// Extracts: 5.2 (first numeric value)

// Function returns: {}
// Skips (no match)
```

**Example Output**:

```go
map[string]float64{
    "check_cust_pep": 5.2,
    "check_jurisdiction": 10.0,
    "check_high_risk_country": 3.5,
}
```

### `ExecuteFunction(ctx context.Context, tx pgx.Tx, functionName string, customerID int) (float64, error)`

**Purpose**: Execute a specific cached function

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `functionName`: Function to execute
- `customerID`: Customer ID

**Returns**:

- `float64`: Score from function
- `error`: Execution error

**Algorithm**:

```go
1. Validate cache initialized
2. Acquire read lock
3. Get function from functionsMap
4. Release lock
5. Return error if not found
6. Execute function query
7. Scan result
8. Return score or 0.0
```

### `GetFunctionCount() int`

**Purpose**: Return number of cached functions

**Returns**: `int` count

**Thread-Safe**: Uses read lock

### `GetFunctionNames() []string`

**Purpose**: Return all cached function names

**Returns**: `[]string` array of function names

**Example**:

```go
names := executor.GetFunctionNames()
// Returns: ["check_cust_pep", "check_jurisdiction", ...]
```

### `RefreshCache(ctx context.Context) error`

**Purpose**: Reload function definitions from database

**Returns**: Error if reload fails

**Usage**: Call if functions are added/modified at runtime

**Algorithm**:

```go
logger.Info("Refreshing function cache...")
return InitializeCache(ctx)
```

---

# infrastructure/cache/file_cache.go

## Overview

File-based caching system for customer IDs and processed customer tracking. Enables incremental processing.

## Types

### `CustomerIDCache`

**Purpose**: Manage customer IDs with file-based caching and processed tracking

**Fields**:

```go
type CustomerIDCache struct {
    cacheFile      string              // Path to customer IDs cache
    processedFile  string              // Path to processed customers file
    db             *pgxpool.Pool       // Database pool
    logger         *zap.Logger         // Logger
    mu             sync.RWMutex        // Read-write lock
    customerIDs    []int               // Cached customer IDs
    processedIDs   map[int]bool        // Set of processed IDs
    totalCount     int                 // Total customer count
}
```

### `CustomerIDCacheMetadata`

**Purpose**: Store cache metadata for validation

**Fields**:

```go
type CustomerIDCacheMetadata struct {
    TotalCount     int   `json:"total_count"`      // Total customers in DB
    CustomerCount  int   `json:"customer_count"`   // Customers in cache
    LastUpdated    int64 `json:"last_updated"`     // Unix timestamp
    ProcessedCount int   `json:"processed_count"`  // Processed customers
}
```

## Functions

### `NewCustomerIDCache(cacheFile, processedFile string, db *pgxpool.Pool, logger *zap.Logger) *CustomerIDCache`

**Purpose**: Create a new customer ID cache

**Parameters**:

- `cacheFile`: Path to cache file (e.g., /tmp/customer_ids.cache)
- `processedFile`: Path to processed file (e.g., /tmp/processed_customers.txt)
- `db`: Database pool
- `logger`: Logger

**Returns**: `*CustomerIDCache` initialized

### `LoadOrRefresh(ctx context.Context) ([]int, error)`

**Purpose**: Load customer IDs from cache or refresh from database

**Returns**:

- `[]int`: All customer IDs
- `error`: Load error

**Algorithm**:

```go
1. Lock cache

2. Get current count from database:
   SELECT COUNT(id) FROM res_partner

3. Set totalCount

4. Check if cache exists and is valid:
   IF cacheExists():
      metadata := loadMetadata()
      IF metadata.TotalCount == currentCount:
         // Cache valid
         customerIDs := loadFromFile()
         Unlock
         RETURN customerIDs

5. Cache miss or outdated:
   Log "loading from database"
   customerIDs := loadFromDatabase(ctx)
   saveToFile(customerIDs)
   
6. Store in memory

7. Unlock

8. RETURN customerIDs
```

**Cache Validation**:

- Compares customer count in database vs cache metadata
- If counts match: cache valid, load from file (fast)
- If counts differ: cache stale, reload from database

**Example**:

```go
cache := NewCustomerIDCache(cacheFile, processedFile, db, logger)
customerIDs, err := cache.LoadOrRefresh(ctx)
// First call: Loads from database, saves to cache
// Second call: Loads from cache (instant)
```

### `LoadProcessedCustomers() error`

**Purpose**: Load set of already processed customers from file

**Returns**: Error if load fails

**Algorithm**:

```go
1. Lock cache

2. Open processedFile

3. IF file doesn't exist:
      Log "starting fresh"
      Unlock
      RETURN nil

4. Create scanner for file

5. FOR each line:
      Trim whitespace
      Skip if empty
      Parse as integer
      IF invalid:
         Log warning
         Continue
      Add to processedIDs map

6. Close file

7. Unlock

8. Log count loaded

9. RETURN nil
```

**File Format** (processed_customers.txt):

```
1000
1001
1002
...
```

**Example**:

```go
cache.LoadProcessedCustomers()
// Loads processed IDs into memory set for fast lookup
```

### `MarkProcessed(customerID int) error`

**Purpose**: Mark a single customer as processed

**Parameters**:

- `customerID`: Customer ID to mark

**Returns**: Error if write fails

**Algorithm**:

```go
1. Lock cache
2. IF already marked:
      Unlock
      RETURN nil
3. Add to processedIDs set
4. Open processedFile in append mode
5. Write: "{customerID}\n"
6. Close file
7. Unlock
8. RETURN nil or error
```

**Thread-Safe**: Uses mutex lock

### `MarkBatchProcessed(customerIDs []int) error`

**Purpose**: Mark multiple customers as processed (more efficient)

**Parameters**:

- `customerIDs`: Array of customer IDs

**Returns**: Error if write fails

**Algorithm**:

```go
1. Lock cache
2. Open processedFile in append mode
3. Create buffered writer
4. FOR each customerID:
      IF not already marked:
         Add to processedIDs set
         Write to buffer: "{customerID}\n"
5. Flush buffer
6. Close file
7. Unlock
8. RETURN nil or error
```

**Performance**: Buffered writes are much faster than individual writes

**Example**:

```go
successfulIDs := []int{1000, 1001, 1002}
cache.MarkBatchProcessed(successfulIDs)
```

### `IsProcessed(customerID int) bool`

**Purpose**: Check if a customer has been processed

**Parameters**:

- `customerID`: Customer ID to check

**Returns**: `bool` true if processed

**Algorithm**:

```go
1. Acquire read lock
2. Check processedIDs map
3. Release lock
4. Return result
```

**Thread-Safe**: Uses read lock (allows concurrent reads)

**Example**:

```go
if cache.IsProcessed(1000) {
    // Skip, already processed
}
```

### `GetUnprocessedCustomers(allCustomers []int) []int`

**Purpose**: Filter to only unprocessed customers

**Parameters**:

- `allCustomers`: Array of all customer IDs

**Returns**: `[]int` unprocessed customer IDs only

**Algorithm**:

```go
1. Acquire read lock

2. Create unprocessed slice with capacity

3. FOR each customerID IN allCustomers:
      IF NOT processedIDs[customerID]:
         Append to unprocessed

4. Release lock

5. RETURN unprocessed
```

**Example**:

```go
allIDs := []int{1000, 1001, 1002, 1003}
// processed: 1000, 1002
unprocessed := cache.GetUnprocessedCustomers(allIDs)
// Returns: []int{1001, 1003}
```

### `GetProcessedCount() int`

**Purpose**: Return count of processed customers

**Returns**: `int` count

**Thread-Safe**: Uses read lock

### `cacheExists() bool`

**Purpose**: Check if cache file exists

**Returns**: `bool` true if exists

**Implementation**:

```go
_, err := os.Stat(cacheFile)
return err == nil
```

### `loadMetadata() (*CustomerIDCacheMetadata, error)`

**Purpose**: Load cache metadata from .meta file

**Returns**:

- `*CustomerIDCacheMetadata`: Loaded metadata
- `error`: Load error

**Algorithm**:

```go
1. Read {cacheFile}.meta
2. Unmarshal JSON
3. Return metadata
```

### `loadFromFile() ([]int, error)`

**Purpose**: Load customer IDs from cache file

**Returns**:

- `[]int`: Customer IDs
- `error`: Load error

**Algorithm**:

```go
1. Open cacheFile
2. Create scanner
3. FOR each line:
      Trim whitespace
      Skip if empty
      Parse as integer
      Append to customerIDs
4. Close file
5. RETURN customerIDs
```

**File Format** (customer_ids.cache):

```
1000
1001
1002
...
```

### `loadFromDatabase(ctx context.Context) ([]int, error)`

**Purpose**: Load customer IDs from database

**Returns**:

- `[]int`: Customer IDs
- `error`: Query error

**Algorithm**:

```go
1. Log "Loading from database"

2. Query: SELECT id FROM res_partner ORDER BY id

3. Pre-allocate slice with capacity = totalCount

4. FOR each row:
      Scan id
      Append to customerIDs

5. Close rows

6. Log count loaded

7. RETURN customerIDs
```

### `saveToFile(customerIDs []int) error`

**Purpose**: Save customer IDs to cache file

**Parameters**:

- `customerIDs`: Customer IDs to save

**Returns**: Error if save fails

**Algorithm**:

```go
1. Log "Saving to cache"

2. Create cacheFile

3. Create buffered writer

4. FOR each customerID:
      Write: "{customerID}\n"

5. Flush buffer

6. Close file

7. Create metadata:
   - TotalCount
   - CustomerCount
   - ProcessedCount

8. Marshal metadata to JSON

9. Write to {cacheFile}.meta

10. Log success

11. RETURN nil or error
```

---

# infrastructure/database/connection.go

## Overview

Database connection management with connection pooling.

## Types

### `ConnectionConfig`

**Purpose**: Hold database connection configuration

**Fields**:

```go
type ConnectionConfig struct {
    Host            string  // Database hostname
    Port            int     // Database port
    Database        string  // Database name
    User            string  // Database username
    Password        string  // Database password
    SSLMode         string  // SSL mode
    PoolMinSize     int     // Minimum connections
    PoolMaxSize     int     // Maximum connections
    MaxIdleTime     int     // Max idle time (seconds)
    MaxLifetime     int     // Max lifetime (seconds)
    ConnectTimeout  int     // Connect timeout (seconds)
    QueryTimeout    int     // Query timeout (seconds)
}
```

### `Connection`

**Purpose**: Manage database connection pool

**Fields**:

```go
type Connection struct {
    pool   *pgxpool.Pool      // Connection pool
    logger *zap.Logger        // Logger
    config ConnectionConfig   // Configuration
}
```

## Functions

### `NewConnection(cfg ConnectionConfig, logger *zap.Logger) *Connection`

**Purpose**: Create a new database connection manager

**Parameters**:

- `cfg`: Connection configuration
- `logger`: Logger

**Returns**: `*Connection`

### `Connect(ctx context.Context) error`

**Purpose**: Establish connection pool to database

**Parameters**:

- `ctx`: Context

**Returns**: Error if connection fails

**Algorithm**:

```go
1. Construct connection string:
   "host={host} port={port} dbname={database} user={user} password={password} 
    sslmode={sslmode} pool_max_conns={max} pool_min_conns={min} 
    pool_max_conn_idle_time={idletime}s pool_max_conn_lifetime={lifetime}s 
    connect_timeout={timeout}"

2. Parse connection string: pgxpool.ParseConfig(connString)

3. Create connection pool: pgxpool.NewWithConfig(ctx, poolConfig)

4. Ping database: pool.Ping(ctx)

5. IF ping fails:
      Close pool
      RETURN error

6. Store pool

7. Log success

8. RETURN nil
```

**Example**:

```go
cfg := ConnectionConfig{
    Host:        "localhost",
    Port:        5432,
    Database:    "production",
    User:        "risk_processor",
    Password:    "secret",
    SSLMode:     "require",
    PoolMinSize: 10,
    PoolMaxSize: 50,
}

conn := NewConnection(cfg, logger)
err := conn.Connect(ctx)
if err != nil {
    log.Fatal("Connection failed:", err)
}
```

### `GetPool() *pgxpool.Pool`

**Purpose**: Return the connection pool

**Returns**: `*pgxpool.Pool` for executing queries

**Example**:

```go
pool := conn.GetPool()
rows, err := pool.Query(ctx, "SELECT * FROM res_partner")
```

### `Close()`

**Purpose**: Close the database connection pool

**Side Effects**:

- Closes all connections in pool
- Logs closure

**Example**:

```go
defer conn.Close()
```

### `WithTimeout(ctx context.Context) (context.Context, context.CancelFunc)`

**Purpose**: Return a context with timeout for database operations

**Parameters**:

- `ctx`: Parent context

**Returns**:

- `context.Context`: Context with timeout
- `context.CancelFunc`: Cancel function

**Algorithm**:

```go
timeout := time.Duration(config.QueryTimeout) * time.Second
return context.WithTimeout(ctx, timeout)
```

**Example**:

```go
ctx, cancel := conn.WithTimeout(context.Background())
defer cancel()

// Query with timeout
rows, err := pool.Query(ctx, "SELECT * FROM large_table")
```

### `GetStats() pgxpool.Stat`

**Purpose**: Return connection pool statistics

**Returns**: `pgxpool.Stat` with pool metrics

**Fields**:

- AcquireCount: Total connections acquired
- AcquireDuration: Total time acquiring connections
- AcquiredConns: Currently acquired connections
- CanceledAcquireCount: Canceled acquire attempts
- ConstructingConns: Connections being constructed
- EmptyAcquireCount: Acquires from empty pool
- IdleConns: Idle connections
- MaxConns: Maximum connections
- TotalConns: Total connections

**Example**:

```go
stats := conn.GetStats()
logger.Info("Pool stats",
    zap.Int32("total_conns", stats.TotalConns()),
    zap.Int32("idle_conns", stats.IdleConns()),
    zap.Int32("acquired_conns", stats.AcquiredConns()))
```

---

# infrastructure/repository/customer_repo.go

## Overview

Repository for customer database operations.

## Types

### `CustomerRepository`

**Purpose**: Handle database operations for customers

**Fields**:

```go
type CustomerRepository struct {
    db     *pgxpool.Pool  // Database pool
    logger *zap.Logger    // Logger
}
```

## Functions

### `NewCustomerRepository(db *pgxpool.Pool, logger *zap.Logger) *CustomerRepository`

**Purpose**: Create a new customer repository

### `GetCustomerByID(ctx context.Context, customerID int) (*models.Customer, error)`

**Purpose**: Retrieve a customer by ID

**Parameters**:

- `ctx`: Context
- `customerID`: Customer ID

**Returns**:

- `*models.Customer`: Customer record
- `error`: Error if not found

**Query**:

```sql
SELECT id, risk_score, risk_level, composite_risk_score, branch_id
FROM res_partner
WHERE id = $1
```

### `GetAllCustomerIDs(ctx context.Context, batchSize int) ([]int, error)`

**Purpose**: Retrieve all customer IDs in batches

**Parameters**:

- `ctx`: Context
- `batchSize`: Batch size for pagination

**Returns**:

- `[]int`: All customer IDs
- `error`: Query error

**Algorithm**:

```go
1. Count total customers
2. Pre-allocate slice
3. FOR offset = 0; offset < total; offset += batchSize:
      Query batch with LIMIT and OFFSET
      Append to full list
4. Return all IDs
```

### `GetCustomerIDsAfter(ctx context.Context, afterID int, batchSize int) ([]int, error)`

**Purpose**: Retrieve customer IDs after a specific ID (for resume)

**Query**:

```sql
SELECT id FROM res_partner 
WHERE id > $1 
ORDER BY id 
LIMIT $2 OFFSET $3
```

### `UpdateCustomerRisk(ctx context.Context, customerID int, riskScore float64, riskLevel string) error`

**Purpose**: Update a single customer's risk score and level

**Query**:

```sql
UPDATE res_partner 
SET risk_score = $1, risk_level = $2 
WHERE id = $3
```

### `BatchUpdateCustomerRisk(ctx context.Context, updates map[int]struct{RiskScore float64; RiskLevel string}) error`

**Purpose**: Update multiple customers in a transaction

**Algorithm**:

```go
1. Begin transaction
2. Create batch
3. FOR each customerID, update IN updates:
      Queue: UPDATE res_partner SET risk_score=$1, risk_level=$2 WHERE id=$3
4. Execute batch
5. Commit transaction
```

**Example**:

```go
updates := map[int]struct{...}{
    1000: {RiskScore: 5.2, RiskLevel: "medium"},
    1001: {RiskScore: 8.5, RiskLevel: "high"},
}
repo.BatchUpdateCustomerRisk(ctx, updates)
```

### `GetCustomerCount(ctx context.Context) (int, error)`

**Purpose**: Return total number of customers

**Query**:

```sql
SELECT COUNT(id) FROM res_partner
```

---

# infrastructure/repository/postgres_risk_function_repo.go

## Overview

PostgreSQL implementation of RiskFunctionRepository interface.

## Types

### `PostgresRiskFunctionRepository`

**Purpose**: Implement RiskFunctionRepository for PostgreSQL

**Fields**:

```go
type PostgresRiskFunctionRepository struct {
    db                *pgxpool.Pool    // Database pool
    logger            *zap.Logger      // Logger
    functionCache     []string         // Cached function names
    functionCacheMu   sync.RWMutex     // Cache lock
    cacheInitialized  bool             // Cache ready
    cacheLastRefresh  time.Time        // Last refresh time
}
```

## Functions

### `NewPostgresRiskFunctionRepository(db *pgxpool.Pool, logger *zap.Logger) *PostgresRiskFunctionRepository`

**Purpose**: Create a new PostgreSQL risk function repository

### `InitializeCache(ctx context.Context) error`

**Purpose**: Load list of available check functions into cache

**Algorithm**:

```go
1. Call GetAvailableFunctions(ctx)
2. Lock cache
3. Store functions array
4. Set cacheInitialized = true
5. Set cacheLastRefresh = now
6. Unlock
7. Log success
```

### `GetAvailableFunctions(ctx context.Context) ([]string, error)`

**Purpose**: Return list of all check_* functions in database

**Returns**:

- `[]string`: Function names
- `error`: Query error

**Algorithm**:

```go
1. Check if cache valid (< 1 hour old):
      IF valid:
         Return cached functions

2. Query pg_proc:
   SELECT p.proname
   FROM pg_proc p
   JOIN pg_namespace n ON p.pronamespace = n.oid
   WHERE n.nspname = 'public'
     AND p.prokind = 'f'
     AND p.proname LIKE 'check_%'
   ORDER BY p.proname

3. Scan function names

4. Update cache

5. Return functions
```

**Cache Behavior**:

- Valid for 1 hour
- Automatically refreshes on expiry
- Thread-safe

### `CallCheckFunction(ctx context.Context, tx pgx.Tx, functionName string, partnerID int) (*models.RiskFunctionResult, error)`

**Purpose**: Call a specific check_* function

**Parameters**:

- `ctx`: Context
- `tx`: Transaction
- `functionName`: Function to call
- `partnerID`: Customer ID

**Returns**:

- `*models.RiskFunctionResult`: Function result
- `error`: Execution error

**Algorithm**:

```go
1. Create RiskFunctionResult

2. Build query: "SELECT {functionName}($1)"

3. Record start time

4. Execute query with partnerID

5. Scan result as JSON bytes

6. Record duration

7. Parse JSON result:
   var matches map[string]interface{}
   json.Unmarshal(jsonResult, &matches)

8. Extract scores:
   FOR each key, value IN matches:
      Convert value to float64
      IF successful:
         result.AddMatch(key, score)

9. Log execution

10. Return result
```

**Example**:

```go
result, err := repo.CallCheckFunction(ctx, tx, "check_cust_pep", 1000)
if err != nil {
    log.Error("Function failed", err)
}
if result.HasMatch {
    log.Info("Matches found", result.Matches)
}
```

### `CallAllCheckFunctions(ctx context.Context, tx pgx.Tx, partnerID int) ([]*models.RiskFunctionResult, error)`

**Purpose**: Call all check_* functions for a customer

**Algorithm**:

```go
1. Get available functions

2. Return empty if no functions

3. Create results array

4. FOR each function:
      result, err := CallCheckFunction(ctx, tx, function, partnerID)
      Append result to results
      IF err:
         Log warning
         Continue (don't fail entire operation)

5. Log summary

6. Return all results
```

**Error Handling**:

- Continues even if some functions fail
- Logs failures but doesn't stop processing
- Returns all results including failed ones

---

# workers/worker_pool.go

## Overview

Worker pool for concurrent job processing.

## Types

### `Job`

**Purpose**: Interface for a unit of work

**Methods**:

```go
type Job interface {
    Process(ctx context.Context) error  // Process the job
    ID() int                             // Get job ID
}
```

### `WorkerPool`

**Purpose**: Manage pool of workers for concurrent job processing

**Fields**:

```go
type WorkerPool struct {
    workerCount      int              // Number of workers
    jobs             chan Job         // Job queue
    results          chan error       // Result queue
    wg               sync.WaitGroup   // Wait group
    logger           *zap.Logger      // Logger
    activeWorkers    int32            // Active worker count (atomic)
    processedCount   int64            // Total processed (atomic)
    successCount     int64            // Success count (atomic)
    failedCount      int64            // Failure count (atomic)
    lastProcessedID  int64            // Last processed ID (atomic)
    processingTimes  []time.Duration  // Processing times
    processingTimesMu sync.Mutex      // Processing times lock
}
```

### `Stats`

**Purpose**: Hold worker pool statistics

**Fields**:

```go
type Stats struct {
    ActiveWorkers   int32  // Currently active workers
    TotalProcessed  int64  // Total jobs processed
    SuccessCount    int64  // Successful jobs
    FailedCount     int64  // Failed jobs
    LastProcessedID int64  // Last processed job ID
    AvgProcessingMs int64  // Average processing time
    MaxProcessingMs int64  // Maximum processing time
    MinProcessingMs int64  // Minimum processing time
}
```

## Functions

### `NewWorkerPool(workerCount int, jobBufferSize int, logger *zap.Logger) *WorkerPool`

**Purpose**: Create a new worker pool

**Parameters**:

- `workerCount`: Number of worker goroutines
- `jobBufferSize`: Size of job buffer
- `logger`: Logger

**Returns**: `*WorkerPool` initialized

**Example**:

```go
pool := NewWorkerPool(20, 100, logger)
```

### `Start(ctx context.Context)`

**Purpose**: Start the worker pool

**Parameters**:

- `ctx`: Context for cancellation

**Algorithm**:

```go
1. Log "Starting worker pool"

2. FOR i = 0 to workerCount:
      Add to wait group
      Start goroutine:
         runWorker(ctx, i)

3. Return (non-blocking)
```

### `runWorker(ctx context.Context, workerID int)`

**Purpose**: Main worker routine (goroutine)

**Algorithm**:

```go
1. Log "Worker started"

2. LOOP:
   SELECT:
      CASE ctx.Done():
         Log "Worker stopping due to cancellation"
         RETURN
      
      CASE job, ok := <-jobs:
         IF !ok:  // Channel closed
            Log "Worker stopping due to closed channel"
            RETURN
         
         Increment activeWorkers (atomic)
         
         start := time.Now()
         err := job.Process(ctx)
         duration := time.Since(start)
         
         Record processing time
         
         Update statistics (atomic):
            processedCount++
            IF err == nil:
               successCount++
            ELSE:
               failedCount++
            lastProcessedID = job.ID()
         
         Log job completion
         
         Send error to results channel
         
         Decrement activeWorkers (atomic)

3. Cleanup
```

**Goroutine Management**:

- Each worker runs in separate goroutine
- Workers pull jobs from shared channel
- Graceful shutdown on context cancellation
- Automatic cleanup on channel close

### `Submit(ctx context.Context, job Job) error`

**Purpose**: Submit a job to the worker pool

**Parameters**:

- `ctx`: Context
- `job`: Job to process

**Returns**: Error if submission fails

**Algorithm**:

```go
SELECT:
   CASE jobs <- job:
      RETURN nil
   CASE <-ctx.Done():
      RETURN ctx.Err()
```

**Blocking Behavior**:

- Blocks if job queue is full (backpressure)
- Returns immediately if context cancelled

### `Results() <-chan error`

**Purpose**: Return channel for receiving job results

**Returns**: `<-chan error` receive-only channel

**Example**:

```go
for err := range pool.Results() {
    if err != nil {
        log.Error("Job failed", err)
    }
}
```

### `Stop()`

**Purpose**: Stop worker pool and wait for all workers to finish

**Algorithm**:

```go
1. Close jobs channel (signals workers to stop)
2. Wait for all workers: wg.Wait()
3. Close results channel
4. Log "Worker pool stopped"
```

**Cleanup Order**:

1. Close jobs → workers stop pulling new jobs
2. Wait for workers → all current jobs complete
3. Close results → no more results

### `GetStats() Stats`

**Purpose**: Return worker pool statistics

**Returns**: `Stats` with current metrics

**Algorithm**:

```go
1. Calculate average, max, min processing times:
   Lock processingTimesMu
   IF len(processingTimes) > 0:
      Calculate stats
   Unlock

2. Create Stats struct:
   - Load atomic counters
   - Add calculated times
   
3. Return Stats
```

**Thread-Safe**: All counters are atomic or protected by mutex

---

# utils/logger.go

## Overview

Logger initialization and configuration using zap.

## Types

### `LogConfig`

**Purpose**: Hold logging configuration

**Fields**:

```go
type LogConfig struct {
    Level      string  // Log level (DEBUG/INFO/WARN/ERROR)
    Format     string  // Format (json/console)
    Output     string  // Output (stdout/file/both)
    File       string  // Log file path
    MaxSize    int     // Max file size in MB
    MaxBackups int     // Max backup files
    MaxAge     int     // Max age in days
}
```

## Functions

### `NewLogger(cfg LogConfig) (*zap.Logger, error)`

**Purpose**: Create a new configured logger

**Parameters**:

- `cfg`: Log configuration

**Returns**:

- `*zap.Logger`: Configured logger
- `error`: Initialization error

**Algorithm**:

```go
1. Parse log level:
   SWITCH cfg.Level:
      "DEBUG" → zapcore.DebugLevel
      "INFO"  → zapcore.InfoLevel
      "WARN"  → zapcore.WarnLevel
      "ERROR" → zapcore.ErrorLevel
      default → zapcore.InfoLevel

2. Configure encoder:
   IF cfg.Format == "json":
      encoder = JSON encoder (machine-readable)
   ELSE:
      encoder = Console encoder (human-readable with colors)

3. Configure output(s):
   cores := []zapcore.Core{}
   
   IF cfg.Output == "stdout" OR "both":
      stdoutSyncer = os.Stdout
      cores = append(cores, NewCore(encoder, stdoutSyncer, level))
   
   IF cfg.Output == "file" OR "both":
      IF cfg.File != "":
         Create log directory
         
         fileWriter = lumberjack.Logger{
            Filename:   cfg.File,
            MaxSize:    cfg.MaxSize,
            MaxBackups: cfg.MaxBackups,
            MaxAge:     cfg.MaxAge,
            Compress:   true,
         }
         
         fileEncoder = console encoder without colors
         cores = append(cores, NewCore(fileEncoder, fileWriter, level))

4. Create logger:
   core = NewTee(cores...)
   logger = New(core, AddCaller(), AddStacktrace(ErrorLevel))

5. Return logger
```

**Output Options**:

- `stdout`: Log to console only
- `file`: Log to file only
- `both`: Log to both console and file

**Example**:

```go
cfg := LogConfig{
    Level:      "INFO",
    Format:     "json",
    Output:     "both",
    File:       "/var/log/risk-processor.log",
    MaxSize:    100,
    MaxBackups: 5,
    MaxAge:     30,
}

logger, err := NewLogger(cfg)
if err != nil {
    log.Fatal("Logger init failed:", err)
}
defer logger.Sync()
```

### `CustomLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder)`

**Purpose**: Add color to log level for console output

**Parameters**:

- `l`: Log level
- `enc`: Encoder

**Algorithm**:

```go
SWITCH l:
   DebugLevel → enc.AppendString("\033[36mDEBUG\033[0m")  // Cyan
   InfoLevel  → enc.AppendString("\033[32mINFO\033[0m")   // Green
   WarnLevel  → enc.AppendString("\033[33mWARN\033[0m")   // Yellow
   ErrorLevel → enc.AppendString("\033[31mERROR\033[0m")  // Red
   DPanicLevel→ enc.AppendString("\033[31mDPANIC\033[0m") // Red
   PanicLevel → enc.AppendString("\033[31mPANIC\033[0m")  // Red
   FatalLevel → enc.AppendString("\033[35mFATAL\033[0m")  // Magenta
   default    → enc.AppendString(l.String())
```

**Colors**:

- DEBUG: Cyan
- INFO: Green
- WARN: Yellow
- ERROR: Red
- FATAL: Magenta

**Note**: Colors only applied to console output, not file output

---

## Summary

This documentation covers every function, type, and method in the risk analysis codebase:

### Key Components

1. **main.go**: Application entry point with graceful shutdown
2. **config.go**: INI-based configuration loading
3. **risk_processor.go**: Main orchestration with batching and checkpoints
4. **models.go**: Domain entities matching database schema
5. **batched_function_risk_calculator.go**: Core calculation engine with caching
6. **cached_function_executor.go**: Function execution from memory cache
7. **file_cache.go**: Customer ID and processed tracking
8. **connection.go**: Database connection pooling
9. **worker_pool.go**: Concurrent job processing
10. **logger.go**: Structured logging with rotation

### Performance Optimizations

- **Function Caching**: Zero DB lookups for metadata
- **Bulk Operations**: PostgreSQL COPY and UNNEST
- **Parallel Processing**: Concurrent batches and workers
- **File Caching**: Incremental processing
- **Connection Pooling**: Efficient database connections

### Error Handling

- Graceful shutdown with checkpoint saving
- Transaction safety for batches
- Failed customer tracking
- Context cancellation support

### Thread Safety

- Mutex locks for shared data
- Atomic counters for statistics
- Read-write locks for caches
- Channel-based communication

This complete documentation provides a comprehensive reference for understanding, maintaining, and extending the risk analysis system.
type Config struct {
    // Database connection settings
    DBHost     string  // Database hostname
    DBPort     int     // Database port (default: 5432)
    DBName     string  // Database name
    DBUser     string  // Database username
    DBPassword string  // Database password
    DBSSLMode  string  // SSL mode (disable, require, verify-ca, verify-full)

    // Connection pool settings
    DBPoolMin         int  // Minimum connections in pool (default: 10)
    DBPoolMax         int  // Maximum connections in pool (default: 50)
    DBPoolMaxIdleTime int  // Max idle time in seconds (default: 300)
    DBPoolMaxLifetime int  // Max connection lifetime in seconds (default: 3600)
    DBConnectTimeout  int  // Connect timeout in seconds (default: 10)
    DBQueryTimeout    int  // Query timeout in seconds (default: 30)

    // Processing settings
    BatchSize                  int   // Customers per batch (default: 1000)
    WorkerCount                int   // Number of concurrent workers (default: 20)
    WorkersPerBatch            int   // Workers within each batch (default: 2)
    ChunkSize                  int   // Records per memory chunk (default: 10000)
    EnableBulkOperations       bool  // Use bulk inserts/updates (default: true)
    BulkInsertBatchSize        int   // Bulk insert batch size (default: 500)
    ProgressCheckpointInterval int   // Checkpoint frequency (default: 10000)

    // Cache settings
    CacheDirectory           string  // Base cache directory (default: /tmp)
    CustomerIDCacheFile      string  // Customer ID cache file path
    ProcessedCustomersFile   string  // Processed customers file path
    RiskFunctionsCacheFile   string  // Risk functions cache file path
    RiskMetadataCacheFile    string  // Risk metadata cache file path

    // Logging settings
    LogLevel      string  // Log level (DEBUG, INFO, WARN, ERROR)
    LogFormat     string  // Log format (json, console)
    LogOutput     string  // Log output (stdout, file, both)
    LogFile       string  // Log file path
    LogMaxSize    int     // Max log file size in MB (default: 100)
    LogMaxBackups int     // Max log file backups (default: 5)
    LogMaxAge     int     // Max log file age in days (default: 30)

    // Retry settings
    MaxRetries          int  // Max retry attempts (default: 3)
    RetryInitialInterval int  // Initial retry interval in seconds (default: 1)
    RetryMaxInterval    int  // Max retry interval in seconds (default: 10)
    RetryMultiplier     int  // Retry backoff multiplier (default: 2)

    // Execution control
    DryRun              bool   // Dry run mode - no DB updates (default: false)
    ResumeFromCheckpoint bool   // Resume from checkpoint (default: false)
    CheckpointFile       string // Checkpoint file path
    CustomerIDs          []int  // Specific customer IDs to process
}

```

## Functions

### `LoadConfig() (*Config, error)`
**Purpose**: Load configuration from settings.conf file

**Returns**:
- `*Config`: Loaded configuration
- `error`: Load error if file not found or invalid

**Algorithm**:
```go
1. Check if settings.conf exists at /data/odoo/ETL_script/update_script/settings.conf
2. Load INI file using ini.Load()
3. Get [database] section
4. Get [risk_analysis] section
5. Create Config struct with defaults
6. Read database settings from [database] section
7. Read processing settings from [risk_analysis] section
8. Validate required fields:
   - dbname must be set
   - user must be set
   - password must be set
9. Parse customer_ids if provided
10. Return Config struct
```

**Validation**:

- Returns error if config file not found
- Returns error if required fields missing (dbname, user, password)
- Returns error if customer_ids format invalid

**Example**:

```go
cfg, err := config.LoadConfig()
if err != nil {
    log.Fatal("Failed to load config:", err)
}

fmt.Printf("Database: %s@%s:%d/%s\n", 
    cfg.DBUser, cfg.DBHost, cfg.DBPort, cfg.DBName)
fmt.Printf("Workers: %d, Batch Size: %d\n", 
    cfg.WorkerCount, cfg.BatchSize)
```

### `parseCustomerIDs(idStr string) ([]int, error)`

**Purpose**: Parse comma-separated customer ID string from config file

**Parameters**:

- `idStr`: Comma-separated customer IDs from config

**Returns**:

- `[]int`: Slice of customer IDs
- `error`: Parsing error

**Algorithm**:

```go
1. Return nil if idStr is empty
2. Split by comma
3. For each part:
   - Trim whitespace
   - Skip if empty
   - Parse as integer
   - Append to result
4. Return result slice
```

**Error Cases**:

- Invalid integer format → error with details
- Empty string → returns nil (no error)

---

# application/risk_processor.go

## Overview

Core orchestration layer for risk processing. Manages batches, workers, caching, checkpoints, and statistics.

## Types

### `CustomerJob`

**Purpose**: Represents a job for processing a single customer (DEPRECATED - not used)

**Fields**:

```go
type CustomerJob struct {
    customerID int  // Customer ID to process
}
```

**Note**: This type is kept for interface compatibility but is NOT USED. Batch processing is used instead.

### `Process(ctx context.Context) error`

**Purpose**: Interface method (not used)

**Returns**: Error indicating single customer processing not supported

### `ID() int`

**Purpose**: Return customer ID for the job

**Returns**: Customer ID

### `Checkpoint`

**Purpose**: Represent a saved processing state for resumability

**Fields**:

```go
type Checkpoint struct {
    Version             string    `json:"checkpoint_version"`      // Checkpoint format version
    Timestamp           time.Time `json:"timestamp"`               // When checkpoint was saved
    LastProcessedID     int       `json:"last_processed_customer_id"` // Last processed customer
    TotalProcessed      int64     `json:"total_processed"`         // Total customers processed
    TotalSuccess        int64     `json:"total_success"`           // Successful calculations
    TotalFailed         int64     `json:"total_failed"`            // Failed calculations
    BatchNumber         int       `json:"batch_number"`            // Current batch number
    FailedCustomerIDs   []int     `json:"failed_customer_ids"`     // List of failed customer IDs
}
```

**JSON Example**:

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

### `RiskProcessor`

**Purpose**: Orchestrate the entire risk calculation workflow

**Fields**:

```go
type RiskProcessor struct {
    config            *config.Config                           // Application configuration
    db                *pgxpool.Pool                            // Database connection pool
    logger            *zap.Logger                              // Structured logger
    batchedCalculator *services.BatchedFunctionRiskCalculator  // Risk calculator
    workerPool        *workers.WorkerPool                      // Worker pool (not actively used)
    customerCache     *cache.CustomerIDCache                   // Customer ID cache
    
    stats struct {
        startTime       time.Time    // Processing start time
        endTime         time.Time    // Processing end time
        totalCustomers  int          // Total customers to process
        totalProcessed  int          // Customers processed so far
        successCount    int          // Successful calculations
        failedCount     int          // Failed calculations
        failedCustomers []int        // List of failed customer IDs
        batchCount      int          // Number of batches processed
        mu              sync.Mutex   // Protects stats fields
    }
}
```

## Functions

### `NewRiskProcessor(config *config.Config, db *pgxpool.Pool, logger *zap.Logger) *RiskProcessor`

**Purpose**: Create a new risk processor with initialized components

**Parameters**:

- `config`: Application configuration
- `db`: Database connection pool
- `logger`: Structured logger

**Returns**: `*RiskProcessor` with initialized components

**Initialization**:

```go
1. Create BatchedFunctionRiskCalculator with cache paths from config
2. Create WorkerPool with worker count and buffer size
3. Create CustomerIDCache with cache file paths
4. Return RiskProcessor with all components
```

**Example**:

```go
processor := application.NewRiskProcessor(cfg, db, logger)
```

### `InitializeCache(ctx context.Context) error`

**Purpose**: Load frequently accessed data into memory for optimal performance

**Returns**: Error if initialization fails

**Algorithm**:

```go
1. Initialize batched calculator cache:
   - Load all check_* functions from database
   - Cache settings (thresholds, computation methods)
   - Load composite plans
   - Save metadata to disk
2. Load processed customers from file:
   - Read processed_customers.txt
   - Build in-memory set for fast lookup
3. Return nil if successful
```

**Critical**: This MUST be called before Run(). Without cache initialization, every customer would require database lookups for metadata.

**Performance Impact**:

- With cache: 0 DB lookups for function metadata per customer
- Without cache: N DB lookups per customer (where N = number of functions)

**Error Cases**:

- Function cache initialization failure → returns error
- Processed customers load failure → returns error

### `Run(ctx context.Context) error`

**Purpose**: Execute the risk processor - main entry point for processing

**Parameters**:

- `ctx`: Context for cancellation and timeout

**Returns**: Error if processing fails

**Algorithm**:

```go
1. Record start time
2. Initialize customer IDs:
   IF specific customer IDs provided in config:
      Use those IDs
   ELSE IF resume from checkpoint:
      Load checkpoint
      Load customer IDs after last processed ID
      Restore statistics from checkpoint
   ELSE:
      Load all customer IDs from cache/database
3. Start worker pool
4. Start monitoring goroutine (logs progress every 30 seconds)
5. Process customers in batches
6. Wait for completion or cancellation
7. Log final statistics
8. Save final checkpoint
9. Return error if any
```

**Checkpoint Resume Logic**:

```go
if config.ResumeFromCheckpoint {
    checkpoint, err := loadCheckpoint()
    if err != nil {
        // Start from beginning
    } else {
        // Resume from checkpoint
        customerIDs, _ = loadCustomerIDsAfter(ctx, checkpoint.LastProcessedID)
        stats.totalProcessed = int(checkpoint.TotalProcessed)
        stats.successCount = int(checkpoint.TotalSuccess)
        stats.failedCount = int(checkpoint.TotalFailed)
        stats.batchCount = checkpoint.BatchNumber
        stats.failedCustomers = checkpoint.FailedCustomerIDs
    }
}
```

**Cancellation Handling**:

- Respects context cancellation (Ctrl+C)
- Finishes current batches before stopping
- Saves checkpoint before returning
- Returns context.Canceled error

### `processCustomersInBatches(ctx context.Context, customerIDs []int) error`

**Purpose**: Process customers using parallel batch transactions for maximum performance

**Parameters**:

- `ctx`: Context for cancellation
- `customerIDs`: Slice of all customer IDs to process

**Returns**: Error if any batch fails

**Algorithm**:

```go
1. Calculate batch parameters:
   - transactionBatchSize = min(config.BatchSize, 200)  // Cap at 200 for safety
   - totalBatches = (totalCustomers + batchSize - 1) / batchSize
   - maxConcurrentBatches = config.WorkerCount / 4  // Optimal concurrency
   - Cap concurrent batches between 4 and 64

2. Create semaphore channel with size = maxConcurrentBatches
3. Create error channel for batch errors
4. Create WaitGroup for synchronization

5. FOR each batch:
   - Check if context cancelled → stop submitting new batches
   - Calculate batch bounds (startIdx, endIdx)
   - Get customer IDs for this batch
   - Launch goroutine:
     a. Acquire semaphore slot
     b. Check context again (may have cancelled while waiting)
     c. Call processSingleBatch()
     d. Release semaphore slot
     e. Send errors to error channel

6. Wait for all batches to complete
7. Close error channel
8. Collect first error (if any)
9. Log final statistics
10. Save final checkpoint
11. Return first error or nil
```

**Concurrency Control**:

```go
// Semaphore pattern for limiting concurrent batches
semaphore := make(chan struct{}, maxConcurrentBatches)

go func(batchNum int, custIDs []int) {
    semaphore <- struct{}{}        // Acquire
    defer func() { <-semaphore }() // Release
    
    // Process batch...
}(batchIdx+1, batchCustomerIDs)
```

**Performance Optimization**:

- **Parallelism**: Multiple batches process concurrently
- **Bounded Concurrency**: Semaphore prevents overwhelming database
- **Early Exit**: Stop submitting new batches on cancellation
- **Error Collection**: Continue processing other batches even if one fails

**Example Execution**:

- 1,000,000 customers
- Batch size: 1000
- Max concurrent: 32
- Result: 1000 batches, 32 processing at a time

### `processSingleBatch(ctx context.Context, batchNum int, customerIDs []int, startIdx int, totalCustomers int) error`

**Purpose**: Process a single batch of customers within a transaction

**Parameters**:

- `ctx`: Context for cancellation
- `batchNum`: Batch number for logging
- `customerIDs`: Customer IDs in this batch
- `startIdx`: Starting index in full customer list
- `totalCustomers`: Total customers being processed

**Returns**: Error if batch processing fails (nil on success)

**Algorithm**:

```go
1. Record batch start time
2. Log batch start with progress
3. Call batchedCalculator.ProcessCustomerBatch():
   - Process all customers in batch with configured workers
   - Returns results for each customer
4. Count successes, failures, and cancellations:
   FOR each result:
      IF error == context.Canceled:
         cancelledCount++  // Graceful shutdown
      ELSE IF error != nil:
         failedCount++
         Track failed customer ID
      ELSE:
         successCount++
         Track successful customer ID
5. Mark successful customers as processed in cache file
6. Calculate batch statistics (duration, avg time per customer)
7. Log batch completion (INFO if normal, WARN if cancelled)
8. Update global statistics (thread-safe with mutex)
9. Save checkpoint if interval reached
10. Return nil
```

**Error Handling**:

```go
// Distinguish between cancellation and actual errors
if result.Error == context.Canceled {
    // Don't count as failure - graceful shutdown
    cancelledCount++
    logger.Debug("Customer cancelled", ...)
} else {
    // Actual error - count as failure
    failedCount++
    logger.Error("Customer failed", ...)
    stats.failedCustomers = append(stats.failedCustomers, customerID)
}
```

**Statistics Update** (Thread-Safe):

```go
stats.mu.Lock()
stats.totalProcessed += successCount + failedCount + cancelledCount
stats.successCount += successCount
stats.failedCount += failedCount
stats.batchCount++
shouldCheckpoint := config.ProgressCheckpointInterval > 0 &&
    stats.totalProcessed % config.ProgressCheckpointInterval == 0
stats.mu.Unlock()

if shouldCheckpoint {
    saveCheckpoint()
}
```

**Logging**:

```go
// Normal completion
logger.Info("Batch completed",
    zap.Int("batch_number", batchNum),
    zap.Int("success", successCount),
    zap.Int("failed", failedCount),
    zap.Duration("duration", batchDuration),
    zap.Int64("avg_ms_per_customer", avgPerCustomer))

// Partial cancellation
logger.Warn("Batch partially cancelled due to shutdown",
    zap.Int("cancelled_by_shutdown", cancelledCount))
```

### `loadAllCustomerIDs(ctx context.Context) ([]int, error)`

**Purpose**: Load all customer IDs using file-based cache

**Returns**:

- `[]int`: All customer IDs
- `error`: Load error

**Algorithm**:

```go
1. Call customerCache.LoadOrRefresh(ctx):
   - Checks if cache file exists and is valid
   - If valid: loads from file
   - If invalid or missing: queries database and saves to cache
2. Get unprocessed customers:
   - Filter allCustomerIDs to exclude processed ones
   - Uses in-memory set for fast lookup
3. Log statistics
4. Return unprocessed customer IDs
```

**Cache Validation**:

- Compares customer count in database vs cache metadata
- If counts match: cache is valid
- If counts differ: cache is stale, reload from database

**Example Output**:

```
INFO  Loaded customer IDs from cache
      total_customers=1000000
      processed_customers=50000
      unprocessed_customers=950000
```

### `loadCustomerIDsAfter(ctx context.Context, afterID int) ([]int, error)`

**Purpose**: Load customer IDs after a specific ID (for checkpoint resume)

**Parameters**:

- `ctx`: Context
- `afterID`: Load customers with ID > afterID

**Returns**:

- `[]int`: Customer IDs after afterID
- `error`: Query error

**Algorithm**:

```go
1. Count remaining customers:
   SELECT COUNT(id) FROM res_partner WHERE id > $1
2. Pre-allocate slice with capacity = remainingCustomers
3. Process in chunks (config.ChunkSize):
   FOR offset = 0; offset < remainingCustomers; offset += chunkSize:
      Query chunk:
         SELECT id FROM res_partner 
         WHERE id > $1 
         ORDER BY id 
         LIMIT $2 OFFSET $3
      Scan IDs into chunk slice
      Append chunk to full list
      Log progress
4. Return full list of customer IDs
```

**Chunking Strategy**:

- Avoids loading all IDs into memory at once
- Default chunk size: 10,000
- Provides progress feedback for large result sets

**Example**:

```go
// Resume from checkpoint at customer ID 50000
customerIDs, err := loadCustomerIDsAfter(ctx, 50000)
// Returns all customer IDs > 50000
```

### `monitorProgress(ctx context.Context, done <-chan struct{})`

**Purpose**: Log progress and stats periodically (goroutine)

**Parameters**:

- `ctx`: Context for cancellation
- `done`: Channel signaling completion

**Algorithm**:

```go
1. Create ticker for 30 seconds
2. Loop:
   SELECT:
      CASE ticker fires:
         Call logProgress()
      CASE done channel closed:
         Return (processing complete)
      CASE context cancelled:
         Return (shutdown)
3. Cleanup ticker
```

**Runs as Background Goroutine**:

```go
done := make(chan struct{})
go monitorProgress(ctx, done)

// ... processing ...

close(done)  // Stop monitoring
```

### `logProgress()`

**Purpose**: Log current progress statistics

**Algorithm**:

```go
1. Lock stats mutex
2. Skip if totalCustomers == 0
3. Calculate elapsed time
4. Calculate progress percentage
5. Estimate remaining time:
   - progress = totalProcessed / totalCustomers
   - totalTime = elapsed / progress
   - remaining = totalTime - elapsed
6. Get worker pool stats
7. Log progress with all metrics
8. Unlock mutex
```

**Log Output**:

```
INFO  Processing progress
      processed=50000
      total=1000000
      progress_percent=5.0
      elapsed=5m0s
      estimated_remaining=1h35m0s
      success_count=49950
      failed_count=50
      active_workers=32
      avg_processing_ms=10
```

### `saveCheckpoint() error`

**Purpose**: Save current processing state to file

**Returns**: Error if save fails

**Algorithm**:

```go
1. Lock stats mutex
2. Determine last processed customer ID:
   IF specific customer IDs:
      Use last ID from config.CustomerIDs
   ELSE:
      Use lastProcessedID from worker pool stats
3. Create Checkpoint struct with current stats
4. Marshal to JSON with indentation
5. Ensure checkpoint directory exists
6. Write to checkpoint file
7. Log checkpoint saved
8. Unlock mutex
9. Return nil or error
```

**File Location**: Configured in `config.CheckpointFile` (default: `/tmp/risk-processor-checkpoint.json`)

**Checkpoint Triggers**:

- Every N customers (config.ProgressCheckpointInterval)
- On graceful shutdown
- On completion

### `loadCheckpoint() (Checkpoint, error)`

**Purpose**: Load processing state from checkpoint file

**Returns**:

- `Checkpoint`: Loaded checkpoint data
- `error`: Error if file doesn't exist or invalid

**Algorithm**:

```go
1. Check if checkpoint file exists
2. Read file contents
3. Unmarshal JSON into Checkpoint struct
4. Log checkpoint loaded with details
5. Return checkpoint
```

**Error Cases**:

- File not found → error
- Invalid JSON → error
- Read permission error → error

### `GetStats() struct{...}`

**Purpose**: Return current processing statistics (thread-safe)

**Returns**: Struct containing:

- TotalCustomers
- TotalProcessed
- SuccessCount
- FailedCount
- FailedCustomers
- BatchCount
- Duration

**Algorithm**:

```go
1. Lock stats mutex
2. Determine end time (now if still processing, else recorded endTime)
3. Calculate duration
4. Create result struct with all stats
5. Unlock mutex
6. Return struct
```

**Example Usage**:

```go
stats := processor.GetStats()
fmt.Printf("Processed: %d/%d (%.1f%%)\n",
    stats.TotalProcessed,
    stats.TotalCustomers,
    float64(stats.TotalProcessed)/float64(stats.TotalCustomers)*100)
```

---

# domain/models/models.go

## Overview

Core business entities representing database tables and domain concepts.

## Types

### `Customer`

**Purpose**: Represent a customer record from res_partner table

**Fields**:

```go
type Customer struct {
    ID                 int      `db:"id"`                    // Primary key
    RiskScore          *float64 `db:"risk_score"`            // Calculated risk score
    RiskLevel          *string  `db:"risk_level"`            // Risk classification (low/medium/high)
    CompositeRiskScore *float64 `db:"composite_risk_score"`  // Composite risk component
    BranchID           *int     `db:"branch_id"`             // Branch assignment
}
```

**Notes**:

- Uses pointers for nullable database fields
- RiskScore and RiskLevel are calculated by this system
- CompositeRiskScore is calculated separately and added

### `RiskPlan`

**Purpose**: Represent a risk assessment plan from res_compliance_risk_assessment_plan

**Fields**:

```go
type RiskPlan struct {
    ID                     int      `db:"id"`                        // Primary key
    Name                   string   `db:"name"`                      // Plan name
    State                  string   `db:"state"`                     // Plan state (active/inactive)
    Priority               int      `db:"priority"`                  // Execution priority
    ComputeScoreFrom       string   `db:"compute_score_from"`        // python/dynamic/static/risk_assessment
    SQLQuery               string   `db:"sql_query"`                 // SQL query to execute
    RiskScore              float64  `db:"risk_score"`                // Static risk score
    RiskAssessmentID       *int     `db:"risk_assessment_id"`        // Related assessment
    UseCompositeCalculation bool     `db:"use_composite_calculation"` // Is composite plan
    UniverseID             *int     `db:"universe_id"`               // Risk universe
}
```

**ComputeScoreFrom Values**:

- `python`: Execute Python code (not supported in Go)
- `dynamic`: Use score from SQL query result
- `static`: Use fixed risk_score value
- `risk_assessment`: Use risk_rating from assessment

### `RiskPlanLine`

**Purpose**: Represent result of risk plan execution in res_partner_risk_plan_line

**Fields**:

```go
type RiskPlanLine struct {
    ID          int     `db:"id"`           // Primary key
    PartnerID   int     `db:"partner_id"`   // Customer ID
    PlanLineID  int     `db:"plan_line_id"` // Plan ID
    RiskScore   float64 `db:"risk_score"`   // Score from plan execution
}
```

**Purpose**: Stores individual plan scores for aggregation

### `RiskAssessment`

**Purpose**: Represent customer risk assessment from res_risk_assessment

**Fields**:

```go
type RiskAssessment struct {
    ID         int       `db:"id"`          // Primary key
    PartnerID  int       `db:"partner_id"`  // Customer ID
    RiskRating *float64  `db:"risk_rating"` // Assessment rating
    SubjectID  *int      `db:"subject_id"`  // Assessment subject
    CreateDate time.Time `db:"create_date"` // When created
}
```

**Notes**:

- SubjectID can be NULL
- RiskRating can be NULL or 0
- CreateDate used for ordering (most recent)

### `EnhancedDueDiligence`

**Purpose**: Represent customer EDD record from res_partner_edd

**Fields**:

```go
type EnhancedDueDiligence struct {
    ID            int        `db:"id"`            // Primary key
    CustomerID    int        `db:"customer_id"`   // Customer ID
    Status        string     `db:"status"`        // EDD status (approved/pending/rejected)
    RiskScore     *float64   `db:"risk_score"`    // EDD risk score
    DateApproved  *time.Time `db:"date_approved"` // Approval date
}
```

**Priority**: EDD has HIGHEST priority - if approved EDD exists, use its score

### `RiskUniverse`

**Purpose**: Represent risk universe from res_risk_universe

**Fields**:

```go
