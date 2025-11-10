package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"risk_analysis/api/responses"
	"risk_analysis/config"
	"risk_analysis/domain/services"
)

// RiskAnalysisHandler handles risk analysis API requests
type RiskAnalysisHandler struct {
	config       *config.Config
	db           *pgxpool.Pool
	logger       *zap.Logger
	mvCalculator *services.MVRiskCalculator
	useRedis     bool
}

// NewRiskAnalysisHandler creates a new risk analysis handler (DEPRECATED: now uses MV calculator)
func NewRiskAnalysisHandler(cfg *config.Config, db *pgxpool.Pool, logger *zap.Logger) *RiskAnalysisHandler {
	// Use MV calculator without Redis
	mvCalculator := services.NewMVRiskCalculator(db, logger, nil, cfg.DBName)

	return &RiskAnalysisHandler{
		config:       cfg,
		db:           db,
		logger:       logger,
		mvCalculator: mvCalculator,
		useRedis:     false,
	}
}

// NewRedisRiskAnalysisHandler creates a new risk analysis handler with Redis caching (using MV calculator)
func NewRedisRiskAnalysisHandler(cfg *config.Config, db *pgxpool.Pool, redisClient *redis.Client, logger *zap.Logger) *RiskAnalysisHandler {
	// Use MV calculator with Redis
	mvCalculator := services.NewMVRiskCalculator(db, logger, redisClient, cfg.DBName)

	return &RiskAnalysisHandler{
		config:       cfg,
		db:           db,
		logger:       logger,
		mvCalculator: mvCalculator,
		useRedis:     true,
	}
}

// InitializeCache initializes the handler's cache
func (h *RiskAnalysisHandler) InitializeCache(ctx context.Context) error {
	return h.mvCalculator.InitializeCache(ctx)
}

// AnalyzeRisk handles POST /api/v1/risk-analysis
func (h *RiskAnalysisHandler) AnalyzeRisk(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	startTime := time.Now()

	// Parse request body
	var req RiskAnalysisRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Error("Failed to parse request body", zap.Error(err))
		responses.Error(w, http.StatusBadRequest, "Invalid request body", r.URL.Path)
		return
	}

	// Validate request
	if len(req.CustomerIDs) == 0 {
		responses.Error(w, http.StatusBadRequest, "customer_ids is required and must not be empty", r.URL.Path)
		return
	}

	// Validate customer IDs
	for _, id := range req.CustomerIDs {
		if id <= 0 {
			responses.Error(w, http.StatusBadRequest, "All customer_ids must be positive integers", r.URL.Path)
			return
		}
	}

	h.logger.Info("Processing risk analysis request",
		zap.Int("customer_count", len(req.CustomerIDs)),
		zap.Bool("dry_run", req.DryRun),
	)

	// Process customers in batch using MV calculator
	results := h.mvCalculator.ProcessCustomerBatch(
		ctx,
		req.CustomerIDs,
		req.DryRun,
		h.config.WorkersPerBatch,
	)

	// Transform results to API response format
	responseData := make([]RiskAnalysisResult, 0, len(results))
	successCount := 0
	failureCount := 0

	for _, result := range results {
		if result.Error != nil {
			failureCount++
			responseData = append(responseData, RiskAnalysisResult{
				CustomerID: result.CustomerID,
				Success:    false,
				Error:      result.Error.Error(),
			})
		} else {
			successCount++
			responseData = append(responseData, RiskAnalysisResult{
				CustomerID: result.CustomerID,
				RiskScore:  result.RiskScore,
				RiskLevel:  result.RiskLevel,
				Success:    true,
			})
		}
	}

	duration := time.Since(startTime)

	// Prepare metadata
	metadata := map[string]interface{}{
		"processing_time_ms": duration.Milliseconds(),
		"total_customers":    len(req.CustomerIDs),
		"success_count":      successCount,
		"failure_count":      failureCount,
		"dry_run":            req.DryRun,
	}

	// Determine overall status
	status := true
	message := "Risk analysis computed successfully"
	statusCode := 0

	if failureCount > 0 {
		if successCount == 0 {
			status = false
			message = "Risk analysis failed for all customers"
			statusCode = 1
		} else {
			message = "Risk analysis completed with some failures"
		}
	}

	responses.Success(w, status, message, statusCode, responseData, metadata, r.URL.Path)
}

// AnalyzeRiskByQuery handles GET /api/v1/risk-analysis?customer_ids=1,2,3
func (h *RiskAnalysisHandler) AnalyzeRiskByQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	startTime := time.Now()

	// Parse customer IDs from query parameter
	customerIDsStr := r.URL.Query().Get("customer_ids")
	if customerIDsStr == "" {
		responses.Error(w, http.StatusBadRequest, "customer_ids query parameter is required", r.URL.Path)
		return
	}

	// Parse comma-separated customer IDs
	customerIDs, err := parseCustomerIDs(customerIDsStr)
	if err != nil {
		h.logger.Error("Failed to parse customer IDs", zap.Error(err))
		responses.Error(w, http.StatusBadRequest, "Invalid customer_ids format. Expected comma-separated integers", r.URL.Path)
		return
	}

	if len(customerIDs) == 0 {
		responses.Error(w, http.StatusBadRequest, "customer_ids must not be empty", r.URL.Path)
		return
	}

	// Parse dry_run parameter (optional, defaults to false)
	dryRun := false
	if dryRunStr := r.URL.Query().Get("dry_run"); dryRunStr != "" {
		dryRun, _ = strconv.ParseBool(dryRunStr)
	}

	h.logger.Info("Processing risk analysis request",
		zap.Int("customer_count", len(customerIDs)),
		zap.Bool("dry_run", dryRun),
	)

	// Process customers in batch using MV calculator
	results := h.mvCalculator.ProcessCustomerBatch(
		ctx,
		customerIDs,
		dryRun,
		h.config.WorkersPerBatch,
	)

	// Transform results to API response format
	responseData := make([]RiskAnalysisResult, 0, len(results))
	successCount := 0
	failureCount := 0

	for _, result := range results {
		if result.Error != nil {
			failureCount++
			responseData = append(responseData, RiskAnalysisResult{
				CustomerID: result.CustomerID,
				Success:    false,
				Error:      result.Error.Error(),
			})
		} else {
			successCount++
			responseData = append(responseData, RiskAnalysisResult{
				CustomerID: result.CustomerID,
				RiskScore:  result.RiskScore,
				RiskLevel:  result.RiskLevel,
				Success:    true,
			})
		}
	}

	duration := time.Since(startTime)

	// Prepare metadata
	metadata := map[string]interface{}{
		"processing_time_ms": duration.Milliseconds(),
		"total_customers":    len(customerIDs),
		"success_count":      successCount,
		"failure_count":      failureCount,
		"dry_run":            dryRun,
	}

	// Determine overall status
	status := true
	message := "Risk analysis computed successfully"
	statusCode := 0

	if failureCount > 0 {
		if successCount == 0 {
			status = false
			message = "Risk analysis failed for all customers"
			statusCode = 1
		} else {
			message = "Risk analysis completed with some failures"
		}
	}

	responses.Success(w, status, message, statusCode, responseData, metadata, r.URL.Path)
}

// HealthCheck handles GET /api/v1/health
func (h *RiskAnalysisHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	// Check database connectivity
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		h.logger.Error("Database health check failed", zap.Error(err))
		responses.Error(w, http.StatusServiceUnavailable, "Database connection failed", r.URL.Path)
		return
	}

	// Return health status
	healthData := map[string]interface{}{
		"status":     "healthy",
		"database":   "connected",
		"cache":      "initialized",
		"version":    "1.0.0",
	}

	responses.Success(w, true, "Service is healthy", 0, healthData, nil, r.URL.Path)
}

// RiskAnalysisRequest represents the API request body
type RiskAnalysisRequest struct {
	CustomerIDs []int `json:"customer_ids"`
	DryRun      bool  `json:"dry_run,omitempty"`
}

// RiskAnalysisResult represents a single customer's risk analysis result
type RiskAnalysisResult struct {
	CustomerID int     `json:"customer_id"`
	RiskScore  float64 `json:"risk_score,omitempty"`
	RiskLevel  string  `json:"risk_level,omitempty"`
	Success    bool    `json:"success"`
	Error      string  `json:"error,omitempty"`
}

// parseCustomerIDs parses comma-separated customer IDs
func parseCustomerIDs(idStr string) ([]int, error) {
	var customerIDs []int
	idStrings := strings.Split(idStr, ",")

	for _, idStr := range idStrings {
		idStr = strings.TrimSpace(idStr)
		if idStr == "" {
			continue
		}

		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, err
		}

		if id <= 0 {
			return nil, strconv.ErrRange
		}

		customerIDs = append(customerIDs, id)
	}

	return customerIDs, nil
}
