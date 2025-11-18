package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"risk_analysis/domain/models"
	"risk_analysis/domain/repositories"
)

// PostgresRiskFunctionRepository implements RiskFunctionRepository for PostgreSQL
// This concrete implementation follows the Dependency Inversion Principle (DIP)
type PostgresRiskFunctionRepository struct {
	db                *pgxpool.Pool
	logger            *zap.Logger
	functionCache     []string
	functionCacheMu   sync.RWMutex
	cacheInitialized  bool
	cacheLastRefresh  time.Time
}

// Ensure PostgresRiskFunctionRepository implements RiskFunctionRepository
var _ repositories.RiskFunctionRepository = (*PostgresRiskFunctionRepository)(nil)

// NewPostgresRiskFunctionRepository creates a new PostgreSQL risk function repository
func NewPostgresRiskFunctionRepository(db *pgxpool.Pool, logger *zap.Logger) *PostgresRiskFunctionRepository {
	return &PostgresRiskFunctionRepository{
		db:               db,
		logger:           logger,
		functionCache:    make([]string, 0),
		cacheInitialized: false,
	}
}

// InitializeCache loads the list of available check functions into cache
// This should be called once at application startup
func (r *PostgresRiskFunctionRepository) InitializeCache(ctx context.Context) error {
	functions, err := r.GetAvailableFunctions(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize function cache: %w", err)
	}

	r.functionCacheMu.Lock()
	r.functionCache = functions
	r.cacheInitialized = true
	r.cacheLastRefresh = time.Now()
	r.functionCacheMu.Unlock()

	r.logger.Info("Function cache initialized",
		zap.Int("function_count", len(functions)),
		zap.Strings("functions", functions),
	)

	return nil
}

// GetAvailableFunctions returns a list of all check_* functions in the database
func (r *PostgresRiskFunctionRepository) GetAvailableFunctions(ctx context.Context) ([]string, error) {
	// Check if cache is valid (less than 1 hour old)
	r.functionCacheMu.RLock()
	if r.cacheInitialized && time.Since(r.cacheLastRefresh) < 1*time.Hour {
		cached := make([]string, len(r.functionCache))
		copy(cached, r.functionCache)
		r.functionCacheMu.RUnlock()
		return cached, nil
	}
	r.functionCacheMu.RUnlock()

	// Query to find all check_* functions in the public schema
	query := `
		SELECT p.proname
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = 'public'
		  AND p.prokind = 'f'
		  AND p.proname LIKE 'check_%'
		ORDER BY p.proname
	`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query available functions: %w", err)
	}
	defer rows.Close()

	functions := make([]string, 0)
	for rows.Next() {
		var functionName string
		if err := rows.Scan(&functionName); err != nil {
			return nil, fmt.Errorf("failed to scan function name: %w", err)
		}
		functions = append(functions, functionName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating function rows: %w", err)
	}

	// Update cache
	r.functionCacheMu.Lock()
	r.functionCache = functions
	r.cacheInitialized = true
	r.cacheLastRefresh = time.Now()
	r.functionCacheMu.Unlock()

	return functions, nil
}

// CallCheckFunction calls a specific check_* function for a customer
func (r *PostgresRiskFunctionRepository) CallCheckFunction(ctx context.Context, tx pgx.Tx, functionName string, partnerID int) (*models.RiskFunctionResult, error) {
	result := models.NewRiskFunctionResult(functionName)

	// Build the SQL query to call the function
	// All check functions have signature: function_name(p_partner_id integer) RETURNS jsonb
	query := fmt.Sprintf("SELECT %s($1)", functionName)

	startTime := time.Now()
	var jsonResult []byte

	// Execute the function
	err := tx.QueryRow(ctx, query, partnerID).Scan(&jsonResult)
	if err != nil {
		result.Error = fmt.Errorf("failed to call function %s: %w", functionName, err)
		r.logger.Error("Failed to call check function",
			zap.String("function", functionName),
			zap.Int("partner_id", partnerID),
			zap.Error(err),
		)
		return result, result.Error
	}

	duration := time.Since(startTime)

	// Parse the JSON result
	var matches map[string]interface{}
	if err := json.Unmarshal(jsonResult, &matches); err != nil {
		result.Error = fmt.Errorf("failed to parse JSON from function %s: %w", functionName, err)
		r.logger.Error("Failed to parse function result",
			zap.String("function", functionName),
			zap.Int("partner_id", partnerID),
			zap.String("raw_json", string(jsonResult)),
			zap.Error(err),
		)
		return result, result.Error
	}

	// Extract scores from the JSON
	for key, value := range matches {
		// Try to convert value to float64
		var score float64
		switch v := value.(type) {
		case float64:
			score = v
		case int:
			score = float64(v)
		case int64:
			score = float64(v)
		default:
			r.logger.Warn("Unexpected value type in function result",
				zap.String("function", functionName),
				zap.String("key", key),
				zap.Any("value", value),
			)
			continue
		}

		// Add the match
		result.AddMatch(key, score)
	}

	r.logger.Debug("Check function executed",
		zap.String("function", functionName),
		zap.Int("partner_id", partnerID),
		zap.Int("match_count", result.MatchCount()),
		zap.Duration("duration", duration),
	)

	return result, nil
}

// CallAllCheckFunctions calls all check_* functions for a customer
func (r *PostgresRiskFunctionRepository) CallAllCheckFunctions(ctx context.Context, tx pgx.Tx, partnerID int) ([]*models.RiskFunctionResult, error) {
	// Get list of available functions
	functions, err := r.GetAvailableFunctions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get available functions: %w", err)
	}

	if len(functions) == 0 {
		r.logger.Warn("No check functions found in database")
		return []*models.RiskFunctionResult{}, nil
	}

	// Call each function
	results := make([]*models.RiskFunctionResult, 0, len(functions))
	successCount := 0
	errorCount := 0

	for _, functionName := range functions {
		result, err := r.CallCheckFunction(ctx, tx, functionName, partnerID)

		// Always add the result, even if there was an error
		// The result will have the Error field set
		results = append(results, result)

		if err != nil {
			errorCount++
			// Log but continue - we want to execute all functions
			r.logger.Warn("Function call failed, continuing with others",
				zap.String("function", functionName),
				zap.Int("partner_id", partnerID),
				zap.Error(err),
			)
		} else {
			successCount++
		}
	}

	r.logger.Info("Completed calling all check functions",
		zap.Int("partner_id", partnerID),
		zap.Int("total_functions", len(functions)),
		zap.Int("success_count", successCount),
		zap.Int("error_count", errorCount),
	)

	return results, nil
}
