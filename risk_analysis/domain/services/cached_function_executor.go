package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// RiskFunctionDefinition holds a cached function definition
type RiskFunctionDefinition struct {
	ID           int
	FunctionName string
	QueryText    string
	Sequence     int
	Active       bool
	ParamCount   int // Number of parameters ($1, $2, etc.)
}

// CachedFunctionExecutor caches function definitions at startup
// and executes them from memory instead of querying the database each time
// This eliminates N database queries per customer down to 0!
type CachedFunctionExecutor struct {
	db               *pgxpool.Pool
	logger           *zap.Logger
	functions        []*RiskFunctionDefinition
	functionsMap     map[string]*RiskFunctionDefinition
	cacheInitialized bool
	cacheMu          sync.RWMutex
	cacheFile        string        // Path to cache file (for file-based caching)
	redisClient      *redis.Client // Redis client (for Redis-based caching)
	dbName           string        // Database name for Redis key prefixing
	useRedis         bool          // Flag to determine if Redis is enabled
}

// NewCachedFunctionExecutor creates a new cached function executor with file-based caching
func NewCachedFunctionExecutor(db *pgxpool.Pool, logger *zap.Logger, cacheFilePath string) *CachedFunctionExecutor {
	return &CachedFunctionExecutor{
		db:           db,
		logger:       logger,
		functionsMap: make(map[string]*RiskFunctionDefinition),
		cacheFile:    cacheFilePath,
		useRedis:     false,
	}
}

// NewRedisCachedFunctionExecutor creates a new cached function executor with Redis-based caching
func NewRedisCachedFunctionExecutor(db *pgxpool.Pool, logger *zap.Logger, redisClient *redis.Client, dbName string) *CachedFunctionExecutor {
	return &CachedFunctionExecutor{
		db:           db,
		logger:       logger,
		functionsMap: make(map[string]*RiskFunctionDefinition),
		redisClient:  redisClient,
		dbName:       dbName,
		useRedis:     true,
	}
}

// InitializeCache loads all function definitions from pg_proc into memory
// This is called ONCE at startup - queries pg_proc for all check_* functions
func (e *CachedFunctionExecutor) InitializeCache(ctx context.Context) error {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()

	e.logger.Info("Loading risk function definitions from pg_proc into cache...")

	// Query pg_proc directly to find all check_* functions
	query := `
		SELECT
			p.oid::int AS id,
			p.proname AS function_name,
			ROW_NUMBER() OVER (ORDER BY p.proname) AS sequence
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = 'public'
		  AND p.proname LIKE 'check_%'
		  AND p.prokind = 'f'
		ORDER BY p.proname
	`

	rows, err := e.db.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to load risk functions from pg_proc: %w", err)
	}
	defer rows.Close()

	functions := make([]*RiskFunctionDefinition, 0)

	for rows.Next() {
		var fn RiskFunctionDefinition

		err := rows.Scan(
			&fn.ID,
			&fn.FunctionName,
			&fn.Sequence,
		)

		if err != nil {
			e.logger.Error("Failed to scan function definition", zap.Error(err))
			continue
		}

		// Set as active by default
		fn.Active = true

		// Construct query: SELECT function_name($1)
		fn.QueryText = fmt.Sprintf("SELECT %s($1)", fn.FunctionName)
		fn.ParamCount = 1

		functions = append(functions, &fn)
		e.functionsMap[fn.FunctionName] = &fn

		e.logger.Debug("Loaded function definition",
			zap.String("function_name", fn.FunctionName),
			zap.String("query", fn.QueryText),
			zap.Int("sequence", fn.Sequence),
		)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating function rows: %w", err)
	}

	e.functions = functions
	e.cacheInitialized = true

	// Save to cache (Redis or file)
	if e.useRedis {
		if err := e.saveCacheToRedis(ctx); err != nil {
			e.logger.Warn("Failed to save function cache to Redis", zap.Error(err))
		}
		e.logger.Info("Function definitions cached successfully in Redis",
			zap.Int("function_count", len(functions)),
			zap.String("db_name", e.dbName),
			zap.String("optimization", "Functions will execute from memory cache - zero DB lookups!"),
		)
	} else {
		if err := e.saveCacheToFile(); err != nil {
			e.logger.Warn("Failed to save function cache to file", zap.Error(err))
		}
		e.logger.Info("Function definitions cached successfully in file",
			zap.Int("function_count", len(functions)),
			zap.String("cache_file", e.cacheFile),
			zap.String("optimization", "Functions will execute from memory cache - zero DB lookups!"),
		)
	}

	return nil
}

// saveCacheToFile saves the function cache to a JSON file
func (e *CachedFunctionExecutor) saveCacheToFile() error {
	data, err := json.MarshalIndent(e.functions, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal functions: %w", err)
	}

	if err := os.WriteFile(e.cacheFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	e.logger.Info("Saved function cache to file",
		zap.String("file", e.cacheFile),
		zap.Int("count", len(e.functions)),
	)

	return nil
}

// saveCacheToRedis saves the function cache to Redis
func (e *CachedFunctionExecutor) saveCacheToRedis(ctx context.Context) error {
	data, err := json.Marshal(e.functions)
	if err != nil {
		return fmt.Errorf("failed to marshal functions: %w", err)
	}

	key := fmt.Sprintf("%s_risk_functions", e.dbName)
	err = e.redisClient.Set(ctx, key, data, 0).Err() // 0 = no expiration (persistent)
	if err != nil {
		return fmt.Errorf("failed to save functions to Redis: %w", err)
	}

	e.logger.Info("Saved function cache to Redis",
		zap.String("key", key),
		zap.String("db_name", e.dbName),
		zap.Int("count", len(e.functions)),
	)

	return nil
}

// ExecuteAllFunctions executes all cached functions for a customer
// This runs entirely from the cached definitions - NO database lookups!
func (e *CachedFunctionExecutor) ExecuteAllFunctions(ctx context.Context, db *pgxpool.Pool, customerID int) (map[string]float64, error) {
	if !e.cacheInitialized {
		return nil, fmt.Errorf("cache not initialized - call InitializeCache() first")
	}

	// Read-lock the cache (allows concurrent reads from multiple goroutines)
	e.cacheMu.RLock()
	functions := e.functions
	e.cacheMu.RUnlock()

	results := make(map[string]float64, len(functions))

	// Execute each function from the cached definition
	for _, fn := range functions {
		var rawResult interface{}

		// PERFORMANCE DEBUG: Time each function execution to find slow queries
		funcStart := time.Now()

		// Execute the cached query - get raw result (NO TRANSACTION = FASTER!)
		err := db.QueryRow(ctx, fn.QueryText, customerID).Scan(&rawResult)

		funcDuration := time.Since(funcStart)

		// Log if function takes more than 100ms (should be <10ms normally)
		if funcDuration > 100*time.Millisecond {
			// e.logger.Warn("SLOW FUNCTION DETECTED",
			// 	zap.String("function_name", fn.FunctionName),
			// 	zap.Int("customer_id", customerID),
			// 	zap.Duration("duration", funcDuration),
			// 	zap.Float64("seconds", funcDuration.Seconds()),
			// 	zap.String("query", fn.QueryText),
			// )
		}

		if err != nil && err != pgx.ErrNoRows {
			// Log warning but continue with other functions
			e.logger.Warn("Function execution failed",
				zap.String("function_name", fn.FunctionName),
				zap.Int("customer_id", customerID),
				zap.Error(err),
			)
			continue
		}

		// // CRITICAL DEBUG: Log what the function returned
		// e.logger.Info("Function raw result",
		// 	zap.String("function_name", fn.FunctionName),
		// 	zap.Int("customer_id", customerID),
		// 	zap.Any("raw_result", rawResult),
		// 	zap.String("result_type", fmt.Sprintf("%T", rawResult)),
		// )

		if rawResult != nil {
			// Try to extract numeric value from result
			// The function might return a float, int, or JSON with numeric value
			var numericValue float64
			var extracted bool

			switch v := rawResult.(type) {
			case float64:
				numericValue = v
				extracted = true
			case float32:
				numericValue = float64(v)
				extracted = true
			case int64:
				numericValue = float64(v)
				extracted = true
			case int32:
				numericValue = float64(v)
				extracted = true
			case int:
				numericValue = float64(v)
				extracted = true
			case map[string]interface{}:
				// Handle JSONB that was already parsed by pgx as map
				// This is the MOST COMMON case for PostgreSQL functions returning JSONB
				// IMPORTANT: Functions return MULTIPLE key-value pairs, extract ALL of them
				// e.logger.Info("Processing map[string]interface{} result",
				// 	zap.String("function_name", fn.FunctionName),
				// 	zap.Int("customer_id", customerID),
				// 	zap.Any("map_contents", v))

				// Extract ALL numeric values from the JSONB map
				for key, val := range v {
					var numVal float64
					var ok bool

					switch num := val.(type) {
					case float64:
						numVal = num
						ok = true
					case int:
						numVal = float64(num)
						ok = true
					case int64:
						numVal = float64(num)
						ok = true
					case float32:
						numVal = float64(num)
						ok = true
					case int32:
						numVal = float64(num)
						ok = true
					}

					if ok && numVal > 0 {
						// Store each key-value pair in results
						results[key] = numVal
						// e.logger.Info("Extracted value from map",
						// 	zap.String("function_name", fn.FunctionName),
						// 	zap.Int("customer_id", customerID),
						// 	zap.String("json_key", key),
						// 	zap.Float64("value", numVal),
						// )
					}
				}
				continue // Skip the old single-value logic below
			case string:
				// Parse JSON to extract numeric value
				// Format: {"value": score} - look for "value" key first
				if v == "{}" || v == "" {
					e.logger.Debug("Function returned empty JSON",
						zap.String("function_name", fn.FunctionName),
						zap.Int("customer_id", customerID))
					continue // Empty result, skip
				}

				// Try to parse as JSON
				var jsonResult map[string]interface{}
				if err := json.Unmarshal([]byte(v), &jsonResult); err == nil {
					// First, try to extract "value" key (standard format)
					if val, ok := jsonResult["value"]; ok {
						switch num := val.(type) {
						case float64:
							numericValue = num
							extracted = true
						case int:
							numericValue = float64(num)
							extracted = true
						case int64:
							numericValue = float64(num)
							extracted = true
						}
					}

					// If "value" key not found, try first numeric value
					if !extracted {
						for key, val := range jsonResult {
							switch num := val.(type) {
							case float64:
								numericValue = num
								extracted = true
							case int:
								numericValue = float64(num)
								extracted = true
							case int64:
								numericValue = float64(num)
								extracted = true
							}
							if extracted {
								e.logger.Debug("Extracted value from JSON (non-standard key)",
									zap.String("function_name", fn.FunctionName),
									zap.Int("customer_id", customerID),
									zap.String("json_key", key),
									zap.Float64("value", numericValue),
								)
								break
							}
						}
					}
				} else {
					e.logger.Warn("Failed to parse JSON result",
						zap.String("function_name", fn.FunctionName),
						zap.Int("customer_id", customerID),
						zap.String("raw_value", v),
						zap.Error(err))
				}
			case []byte:
				// Handle JSONB type
				var jsonResult map[string]interface{}
				if err := json.Unmarshal(v, &jsonResult); err == nil {
					// First, try to extract "value" key (standard format)
					if val, ok := jsonResult["value"]; ok {
						switch num := val.(type) {
						case float64:
							numericValue = num
							extracted = true
						case int:
							numericValue = float64(num)
							extracted = true
						case int64:
							numericValue = float64(num)
							extracted = true
						}
					}

					// If "value" key not found, try first numeric value
					if !extracted {
						for key, val := range jsonResult {
							switch num := val.(type) {
							case float64:
								numericValue = num
								extracted = true
							case int:
								numericValue = float64(num)
								extracted = true
							case int64:
								numericValue = float64(num)
								extracted = true
							}
							if extracted {
								e.logger.Debug("Extracted value from JSONB (non-standard key)",
									zap.String("function_name", fn.FunctionName),
									zap.Int("customer_id", customerID),
									zap.String("json_key", key),
									zap.Float64("value", numericValue),
								)
								break
							}
						}
					}
				}
			default:
				e.logger.Debug("Unknown result type",
					zap.String("function_name", fn.FunctionName),
					zap.Int("customer_id", customerID),
					zap.String("type", fmt.Sprintf("%T", rawResult)))
				continue
			}

			// IMPORTANT: Store ALL extracted values, including 0
			// A function returning 0 might still be meaningful (e.g., "no violations found" vs "not applicable")
			if extracted {
				results[fn.FunctionName] = numericValue
				if numericValue > 0 {
					e.logger.Debug("Function returned positive score",
						zap.String("function_name", fn.FunctionName),
						zap.Int("customer_id", customerID),
						zap.Float64("score", numericValue),
					)
				}
			} else {
				e.logger.Debug("Could not extract numeric value from function result",
					zap.String("function_name", fn.FunctionName),
					zap.Int("customer_id", customerID),
					zap.Any("raw_result", rawResult))
			}
		}
	}

	return results, nil
}

// ExecuteFunction executes a specific cached function
// NOTE: Functions return JSONB format like {"function_name": score}
func (e *CachedFunctionExecutor) ExecuteFunction(ctx context.Context, tx pgx.Tx, functionName string, customerID int) (float64, error) {
	if !e.cacheInitialized {
		return 0, fmt.Errorf("cache not initialized - call InitializeCache() first")
	}

	// Get function definition from cache
	e.cacheMu.RLock()
	fn, found := e.functionsMap[functionName]
	e.cacheMu.RUnlock()

	if !found {
		return 0, fmt.Errorf("function %s not found in cache", functionName)
	}

	// Execute function - result will be JSONB
	var rawResult interface{}
	err := tx.QueryRow(ctx, fn.QueryText, customerID).Scan(&rawResult)

	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to execute function %s: %w", functionName, err)
	}

	if rawResult == nil {
		return 0, nil
	}

	// Extract numeric value from JSONB result
	var numericValue float64
	var extracted bool

	switch v := rawResult.(type) {
	case float64:
		numericValue = v
		extracted = true
	case float32:
		numericValue = float64(v)
		extracted = true
	case int64:
		numericValue = float64(v)
		extracted = true
	case int32:
		numericValue = float64(v)
		extracted = true
	case int:
		numericValue = float64(v)
		extracted = true
	case map[string]interface{}:
		// Handle JSONB that was already parsed by pgx as map
		// This is the MOST COMMON case for PostgreSQL functions returning JSONB
		// Format: {"function_name": score} or {"value": score}

		// First, try to extract "value" key (standard format)
		if val, ok := v["value"]; ok {
			switch num := val.(type) {
			case float64:
				numericValue = num
				extracted = true
			case int:
				numericValue = float64(num)
				extracted = true
			case int64:
				numericValue = float64(num)
				extracted = true
			}
		}

		// If "value" key not found, extract first numeric value from any key
		if !extracted {
			for _, val := range v {
				switch num := val.(type) {
				case float64:
					numericValue = num
					extracted = true
				case int:
					numericValue = float64(num)
					extracted = true
				case int64:
					numericValue = float64(num)
					extracted = true
				}
				if extracted {
					break
				}
			}
		}
	case []byte:
		// Handle JSONB type as bytes
		var jsonResult map[string]interface{}
		if err := json.Unmarshal(v, &jsonResult); err == nil {
			// Try "value" key first
			if val, ok := jsonResult["value"]; ok {
				switch num := val.(type) {
				case float64:
					numericValue = num
					extracted = true
				case int:
					numericValue = float64(num)
					extracted = true
				case int64:
					numericValue = float64(num)
					extracted = true
				}
			}

			// If "value" key not found, try first numeric value
			if !extracted {
				for _, val := range jsonResult {
					switch num := val.(type) {
					case float64:
						numericValue = num
						extracted = true
					case int:
						numericValue = float64(num)
						extracted = true
					case int64:
						numericValue = float64(num)
						extracted = true
					}
					if extracted {
						break
					}
				}
			}
		}
	case string:
		// Parse JSON string
		if v != "{}" && v != "" {
			var jsonResult map[string]interface{}
			if err := json.Unmarshal([]byte(v), &jsonResult); err == nil {
				// Try "value" key first
				if val, ok := jsonResult["value"]; ok {
					switch num := val.(type) {
					case float64:
						numericValue = num
						extracted = true
					case int:
						numericValue = float64(num)
						extracted = true
					case int64:
						numericValue = float64(num)
						extracted = true
					}
				}

				// If "value" key not found, try first numeric value
				if !extracted {
					for _, val := range jsonResult {
						switch num := val.(type) {
						case float64:
							numericValue = num
							extracted = true
						case int:
							numericValue = float64(num)
							extracted = true
						case int64:
							numericValue = float64(num)
							extracted = true
						}
						if extracted {
							break
						}
					}
				}
			}
		}
	}

	if !extracted {
		e.logger.Debug("Could not extract numeric value from function result",
			zap.String("function_name", functionName),
			zap.Int("customer_id", customerID),
			zap.Any("raw_result", rawResult),
			zap.String("result_type", fmt.Sprintf("%T", rawResult)))
		return 0, nil
	}

	return numericValue, nil
}

// GetFunctionCount returns the number of cached functions
func (e *CachedFunctionExecutor) GetFunctionCount() int {
	e.cacheMu.RLock()
	defer e.cacheMu.RUnlock()
	return len(e.functions)
}

// GetFunctionNames returns all cached function names
func (e *CachedFunctionExecutor) GetFunctionNames() []string {
	e.cacheMu.RLock()
	defer e.cacheMu.RUnlock()

	names := make([]string, len(e.functions))
	for i, fn := range e.functions {
		names[i] = fn.FunctionName
	}
	return names
}

// RefreshCache reloads function definitions from database
// Call this if functions are added/modified at runtime
func (e *CachedFunctionExecutor) RefreshCache(ctx context.Context) error {
	e.logger.Info("Refreshing function cache...")
	return e.InitializeCache(ctx)
}
