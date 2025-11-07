package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"time"

	"go.uber.org/zap"

	"risk_analysis/application"
	"risk_analysis/config"
	"risk_analysis/infrastructure/cache"
	"risk_analysis/infrastructure/database"
	"risk_analysis/utils"
)

func main() {
	// Set GOMAXPROCS to use all available CPU cores for maximum parallelism
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	fmt.Printf("Configuring Go runtime: GOMAXPROCS=%d (utilizing all %d CPU cores)\n", numCPU, numCPU)

	// Parse command line flags
	var (
		dryRun           = flag.Bool("dry-run", false, "Run in dry-run mode (no database updates)")
		customerIDsFlag  = flag.String("customer-ids", "", "Process only specific customers (comma-separated)")
		workerCountFlag  = flag.Int("workers", 0, "Override number of concurrent workers")
		batchSizeFlag    = flag.Int("batch-size", 0, "Override batch size")
		resumeCheckpoint = flag.Bool("resume-from-checkpoint", false, "Resume from last checkpoint")
		showHelp         = flag.Bool("help", false, "Show help message")
	)

	flag.Parse()

	if *showHelp {
		printHelp()
		os.Exit(0)
	}

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger with file output support
	logCfg := utils.LogConfig{
		Level:      cfg.LogLevel,
		Format:     cfg.LogFormat,
		Output:     cfg.LogOutput,
		File:       cfg.LogFile,
		MaxSize:    cfg.LogMaxSize,
		MaxBackups: cfg.LogMaxBackups,
		MaxAge:     cfg.LogMaxAge,
	}

	logger, err := utils.NewLogger(logCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Override with command line flags if provided
	if *dryRun {
		cfg.DryRun = true
	}

	if *workerCountFlag > 0 {
		cfg.WorkerCount = *workerCountFlag
	}

	if *batchSizeFlag > 0 {
		cfg.BatchSize = *batchSizeFlag
	}

	if *resumeCheckpoint {
		cfg.ResumeFromCheckpoint = true
	}

	// Parse customer IDs if provided
	if *customerIDsFlag != "" {
		customerIDs, err := parseCustomerIDs(*customerIDsFlag)
		if err != nil {
			logger.Fatal("Invalid customer IDs", zap.Error(err))
		}
		cfg.CustomerIDs = customerIDs
	}

	// Log startup information with detailed performance configuration
	logger.Info("Risk processor starting with optimized settings",
		zap.String("version", "1.0.0"),
		zap.Int("cpu_cores", numCPU),
		zap.Int("gomaxprocs", runtime.GOMAXPROCS(0)),
		zap.Int("worker_count", cfg.WorkerCount),
		zap.Int("workers_per_batch", cfg.WorkersPerBatch),
		zap.Int("batch_size", cfg.BatchSize),
		zap.Int("db_pool_max", cfg.DBPoolMax),
		zap.Int("db_pool_min", cfg.DBPoolMin),
		zap.Bool("dry_run", cfg.DryRun),
		zap.String("optimization_note", "Configured for maximum parallelism on 16-core server"),
	)

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Graceful shutdown handler
	go func() {
		sig := <-sigChan
		logger.Warn("========================================")
		logger.Warn("GRACEFUL SHUTDOWN INITIATED")
		logger.Warn("========================================")
		logger.Warn("Received shutdown signal",
			zap.String("signal", sig.String()),
			zap.String("action", "Finishing current batches and saving checkpoint..."),
		)
		logger.Warn("Please wait for current operations to complete...")
		logger.Warn("Press Ctrl+C again to force immediate shutdown (NOT RECOMMENDED)")

		// Cancel the context to stop processing new batches
		cancel()

		// Handle force shutdown on second signal
		go func() {
			sig := <-sigChan
			logger.Error("FORCE SHUTDOWN REQUESTED",
				zap.String("signal", sig.String()),
				zap.String("warning", "Exiting immediately - data may be lost!"),
			)
			os.Exit(1)
		}()
	}()

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
	err = dbConnection.Connect(ctx)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer dbConnection.Close()

	// Initialize Redis if enabled
	var processor *application.RiskProcessor
	if cfg.RedisEnabled {
		logger.Info("Redis caching enabled",
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
		defer redisClient.Close()

		// Create risk processor with Redis caching
		processor = application.NewRedisRiskProcessor(cfg, dbConnection.GetPool(), redisClient.GetClient(), logger)

		logger.Info("Risk processor initialized with Redis caching",
			zap.String("db_name", cfg.DBName),
			zap.String("cache_prefix", cfg.DBName),
		)
	} else {
		logger.Info("File-based caching enabled",
			zap.String("cache_directory", cfg.CacheDirectory),
		)

		// Create cache directories if they don't exist (from config)
		if err := os.MkdirAll(cfg.CacheDirectory, 0755); err != nil {
			logger.Warn("Failed to create cache directory",
				zap.String("directory", cfg.CacheDirectory),
				zap.Error(err),
			)
		} else {
			logger.Info("Cache directory ready", zap.String("directory", cfg.CacheDirectory))
		}

		// Create risk processor with file-based caching
		processor = application.NewRiskProcessor(cfg, dbConnection.GetPool(), logger)

		logger.Info("Risk processor initialized with file-based caching")
	}

	// Initialize cache before processing customers (CRITICAL for performance!)
	logger.Info("Initializing cache for optimal performance...")
	err = processor.InitializeCache(ctx)
	if err != nil {
		logger.Fatal("Failed to initialize cache", zap.Error(err))
	}

	// Run risk processor
	startTime := time.Now()
	err = processor.Run(ctx)

	// Get final statistics
	stats := processor.GetStats()

	if err != nil {
		if err == context.Canceled {
			// Graceful shutdown
			logger.Warn("========================================")
			logger.Warn("GRACEFUL SHUTDOWN COMPLETED")
			logger.Warn("========================================")
			logger.Warn("Processing interrupted by shutdown signal",
				zap.Duration("duration", time.Since(startTime)),
				zap.Int("total_customers", stats.TotalCustomers),
				zap.Int("total_processed", stats.TotalProcessed),
				zap.Int("success_count", stats.SuccessCount),
				zap.Int("failed_count", stats.FailedCount),
				zap.Float64("progress_percent", float64(stats.TotalProcessed)/float64(stats.TotalCustomers)*100),
			)
			logger.Warn("Checkpoint saved - you can resume with --resume-from-checkpoint flag")
			os.Exit(0) // Exit cleanly on graceful shutdown
		} else {
			// Error occurred
			logger.Error("Processing failed",
				zap.Error(err),
				zap.Duration("duration", time.Since(startTime)),
				zap.Int("total_processed", stats.TotalProcessed),
				zap.Int("success_count", stats.SuccessCount),
				zap.Int("failed_count", stats.FailedCount),
			)
			os.Exit(1)
		}
	}

	// Log successful completion
	logger.Info("========================================")
	logger.Info("PROCESSING COMPLETED SUCCESSFULLY!")
	logger.Info("========================================")
	logger.Info("Final statistics",
		zap.Duration("duration", time.Since(startTime)),
		zap.Int("total_customers", stats.TotalCustomers),
		zap.Int("total_processed", stats.TotalProcessed),
		zap.Int("success_count", stats.SuccessCount),
		zap.Int("failed_count", stats.FailedCount),
		zap.Float64("success_rate", float64(stats.SuccessCount)/float64(stats.TotalProcessed)*100),
		zap.Int("batches_processed", stats.BatchCount),
	)

	if stats.FailedCount > 0 {
		logger.Warn("Some customers failed processing",
			zap.Int("failed_count", stats.FailedCount),
			zap.String("note", "Check logs for details"),
		)
	}
}

// Helper to parse comma-separated customer IDs
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
			return nil, fmt.Errorf("invalid customer ID: %s", idStr)
		}
		customerIDs = append(customerIDs, id)
	}
	
	return customerIDs, nil
}

func printHelp() {
	// Use fmt.Print instead of fmt.Println to avoid redundant newline
	fmt.Print(`
Risk Processor - High-Performance Customer Risk Analysis

Usage:
  risk-processor [options]

Options:
  --dry-run                  Run in dry-run mode (no database updates)
  --customer-ids=1,2,3       Process only specific customers (comma-separated)
  --workers=50               Override number of concurrent workers
  --batch-size=500           Override batch size
  --resume-from-checkpoint   Resume from the last checkpoint
  --help                     Show this help message

Configuration:
  All configuration is loaded from config.conf file (INI format).
  The file contains two main sections:
    [database]       - Database connection and pool settings
    [risk_analysis]  - Risk processing, logging, and execution settings

  See config.conf for all available options and defaults.

Examples:
  # Run with default settings from config.conf
  risk-processor

  # Run with 50 concurrent workers (overrides config.conf)
  risk-processor --workers=50

  # Process only specific customers
  risk-processor --customer-ids=1000,1001,1002

  # Resume from last checkpoint
  risk-processor --resume-from-checkpoint

  # Run in dry-run mode (no database updates)
  risk-processor --dry-run
`)
}
