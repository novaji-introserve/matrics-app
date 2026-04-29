package routes

import (
	"net/http"

	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"
	"go.uber.org/zap"

	"risk_analysis/api/handlers"
	"risk_analysis/api/middleware"
)

const (
	swaggerYAMLPath = "/api/swagger.yaml"
	docsPath        = "/docs/"
)

// SetupRouter configures and returns the HTTP router
func SetupRouter(riskHandler *handlers.RiskAnalysisHandler, logger *zap.Logger) *mux.Router {
	router := mux.NewRouter()

	// Serve swagger.yaml file (must be registered early)
	router.HandleFunc(swaggerYAMLPath, func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./api/swagger.yaml")
	}).Methods(http.MethodGet)

	// Redirect /docs to /docs/ for Swagger UI
	router.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, docsPath, http.StatusMovedPermanently)
	}).Methods(http.MethodGet)

	// Swagger docs at /docs/ (main router level - must be registered before API subrouter)
	router.PathPrefix(docsPath).Handler(httpSwagger.Handler(
		httpSwagger.URL(swaggerYAMLPath),
		httpSwagger.DeepLinking(true),
		httpSwagger.DocExpansion("list"),
		httpSwagger.DomID("swagger-ui"),
	))

	// Root endpoint - redirect to /docs/
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, docsPath, http.StatusMovedPermanently)
	}).Methods(http.MethodGet)

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

	// Swagger documentation endpoint at /api/v1/docs
	api.PathPrefix("/docs").Handler(httpSwagger.Handler(
		httpSwagger.URL(swaggerYAMLPath),
		httpSwagger.DeepLinking(true),
		httpSwagger.DocExpansion("list"),
		httpSwagger.DomID("swagger-ui"),
	))

	return router
}
