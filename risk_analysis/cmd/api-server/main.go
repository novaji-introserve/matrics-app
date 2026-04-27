package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"go.uber.org/zap"

	"risk_analysis/api/handlers"
	"risk_analysis/api/middleware"
	"risk_analysis/api/routes"
	"risk_analysis/config"
	"risk_analysis/infrastructure/cache"
	"risk_analysis/infrastructure/database"
	"risk_analysis/utils"
)

func main() {
	// Set GOMAXPROCS to use all available CPU cores
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	fmt.Printf("Configuring Go runtime: GOMAXPROCS=%d (utilizing all %d CPU cores)\n", numCPU, numCPU)

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger (separate API logging configuration)
	logCfg := utils.LogConfig{
		Level:      cfg.APILogLevel,
		Format:     cfg.APILogFormat,
		Output:     cfg.APILogOutput,
		File:       cfg.APILogFile,
		MaxSize:    cfg.APILogMaxSize,
		MaxBackups: cfg.APILogMaxBackups,
		MaxAge:     cfg.APILogMaxAge,
	}

	logger, err := utils.NewLogger(logCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("API Server logger initialized",
		zap.String("log_level", cfg.APILogLevel),
		zap.String("log_output", cfg.APILogOutput),
		zap.String("log_file", cfg.APILogFile),
		zap.String("log_format", cfg.APILogFormat),
	)

	// Initialize database connection
	dbConfig := database.ConnectionConfig{
		Host:            cfg.DBHost,
		Port:            cfg.DBPort,
		Database:        cfg.DBName,
		User:            cfg.DBUser,
		Password:        cfg.DBPassword,
		SSLMode:         cfg.DBSSLMode,
		PoolMinSize:     cfg.DBPoolMin,
		PoolMaxSize:     cfg.DBPoolMax,
		MaxIdleTime:     cfg.DBPoolMaxIdleTime,
		MaxLifetime:     cfg.DBPoolMaxLifetime,
		ConnectTimeout:  cfg.DBConnectTimeout,
		QueryTimeout:    cfg.DBQueryTimeout,
	}

	dbConnection := database.NewConnection(dbConfig, logger)
	ctx := context.Background()
	if err := dbConnection.Connect(ctx); err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer dbConnection.Close()

	// Initialize risk analysis handler (with Redis or file-based caching)
	var riskHandler *handlers.RiskAnalysisHandler
	if cfg.RedisEnabled {
		logger.Info("API Server: Redis caching enabled",
			zap.String("host", cfg.RedisHost),
			zap.Int("port", cfg.RedisPort),
			zap.String("db_name", cfg.DBName),
		)

		// Create Redis client
		redisClient, err := cache.NewRedisCacheClient(
			cfg.RedisHost,
			cfg.RedisPort,
			cfg.RedisPassword,
			cfg.RedisDB,
			cfg.RedisPoolSize,
			cfg.DBName,
			logger,
		)
		if err != nil {
			logger.Fatal("Failed to connect to Redis", zap.Error(err))
		}

		// Create handler with Redis caching
		riskHandler = handlers.NewRedisRiskAnalysisHandler(cfg, dbConnection.GetPool(), redisClient.GetClient(), logger)
		logger.Info("API handler initialized with Redis caching")

		// Close Redis on server shutdown (moved to defer below)
		defer redisClient.Close()
	} else {
		logger.Info("API Server: File-based caching enabled",
			zap.String("cache_directory", cfg.CacheDirectory),
		)

		// Create cache directory
		if err := os.MkdirAll(cfg.CacheDirectory, 0755); err != nil {
			logger.Warn("Failed to create cache directory", zap.Error(err))
		}

		// Create handler with file-based caching
		riskHandler = handlers.NewRiskAnalysisHandler(cfg, dbConnection.GetPool(), logger)
		logger.Info("API handler initialized with file-based caching")
	}

	// Initialize cache
	if err := riskHandler.InitializeCache(ctx); err != nil {
		logger.Fatal("Failed to initialize cache", zap.Error(err))
	}

	// Setup router
	router := routes.SetupRouter(riskHandler, logger)

	// Get API port from config or environment
	apiPort := cfg.APIPort
	if apiPort == "" {
		apiPort = "8080"
	}

	// Create HTTP server
	server := &http.Server{
		Addr:         ":" + apiPort,
		Handler:      middleware.RequestLogger(logger)(router),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		logger.Info("Starting API server",
			zap.String("port", apiPort),
			zap.String("version", "1.0.0"),
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Failed to start server", zap.Error(err))
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("Server forced to shutdown", zap.Error(err))
	}

	logger.Info("Server exited gracefully")
}
