package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"risk_analysis/api/handlers"
	"risk_analysis/api/middleware"
)

// SetupRouter configures and returns the HTTP router
func SetupRouter(riskHandler *handlers.RiskAnalysisHandler, logger *zap.Logger) *mux.Router {
	router := mux.NewRouter()

	// API v1 routes
	api := router.PathPrefix("/api/v1").Subrouter()

	// Apply middleware
	api.Use(middleware.CORS)
	api.Use(middleware.Recovery(logger))

	// Health check endpoint
	api.HandleFunc("/health", riskHandler.HealthCheck).Methods(http.MethodGet)

	// Risk analysis endpoints
	api.HandleFunc("/risk-analysis", riskHandler.AnalyzeRisk).Methods(http.MethodPost)
	api.HandleFunc("/risk-analysis", riskHandler.AnalyzeRiskByQuery).Methods(http.MethodGet)

	// Root endpoint
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"Risk Analysis API v1.0.0","status":"running"}`))
	}).Methods(http.MethodGet)

	return router
}
