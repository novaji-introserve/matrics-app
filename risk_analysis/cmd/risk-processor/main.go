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
		configFile       = flag.String("config", "", "Path to config file (default: config.conf or $CONFIG_FILE)")
		dryRun           = flag.Bool("dry-run", false, "Run in dry-run mode (no database updates)")
		customerIDsFlag  = flag.String("customer-ids", "", "Process only specific customers (comma-separated)")
		workerCountFlag  = flag.Int("workers", 0, "Override number of concurrent workers")
		batchSizeFlag    = flag.Int("batch-size", 0, "Override batch size")
		resumeCheckpoint = flag.Bool("resume-from-checkpoint", false, "Resume from last checkpoint")
		monitorMode      = flag.Bool("monitor", false, "Run in monitoring mode (continuously poll for MV refresh)")
		pollInterval     = flag.Int("poll-interval", 5, "Polling interval in minutes for monitor mode (default: 5)")
		showHelp         = flag.Bool("help", false, "Show help message")
	)

	flag.Parse()

	if *showHelp {
		printHelp()
		os.Exit(0)
	}

	// Set config file path if specified via flag (highest priority)
	if *configFile != "" {
		os.Setenv("CONFIG_FILE", *configFile)
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

	// Monitor mode can be enabled via flag or config
	if *monitorMode {
		cfg.MonitorMode = true
	}

	// Poll interval: command-line flag overrides config (convert minutes to seconds)
	if *pollInterval > 0 {
		cfg.MonitorPollInterval = *pollInterval * 60 // Convert minutes to seconds
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
		Host:           cfg.DBHost,
		Port:           cfg.DBPort,
		Database:       cfg.DBName,
		User:           cfg.DBUser,
		Password:       cfg.DBPassword,
		SSLMode:        cfg.DBSSLMode,
		PoolMinSize:    cfg.DBPoolMin,
		PoolMaxSize:    cfg.DBPoolMax,
		MaxIdleTime:    cfg.DBPoolMaxIdleTime,
		MaxLifetime:    cfg.DBPoolMaxLifetime,
		ConnectTimeout: cfg.DBConnectTimeout,
		QueryTimeout:   cfg.DBQueryTimeout,
	}

	dbConnection := database.NewConnection(dbConfig, logger)
	err = dbConnection.Connect(ctx)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer dbConnection.Close()

	// Initialize Redis if enabled
	var processor *application.RiskProcessor
	var redisClient *cache.RedisCacheClient // Declare at function scope for monitor mode
	if cfg.RedisEnabled {
		logger.Info("Redis caching enabled",
			zap.String("host", cfg.RedisHost),
			zap.Int("port", cfg.RedisPort),
			zap.String("db_name", cfg.DBName),
		)

		// Create Redis client
		var err error
		redisClient, err = cache.NewRedisCacheClient(
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

	// Run risk processor based on mode
	var startTime time.Time
	if cfg.MonitorMode {
		// Monitor mode: continuously poll for MV refresh and trigger processing
		if !cfg.RedisEnabled {
			logger.Fatal("Monitor mode requires Redis to be enabled")
		}

		logger.Info("Starting in MONITOR mode",
			zap.Int("poll_interval_seconds", cfg.MonitorPollInterval),
			zap.String("poll_interval_human", (time.Duration(cfg.MonitorPollInterval)*time.Second).String()),
			zap.String("mode", "Continuous monitoring for MV refresh"),
		)

		// Create MV refresh monitor
		monitor := application.NewMVRefreshMonitor(
			dbConnection.GetPool(),
			redisClient.GetClient(),
			cfg.DBName,
			logger,
			time.Duration(cfg.MonitorPollInterval)*time.Second,
		)

		// Set callback to run risk processing when MV refresh is detected
		monitor.SetRefreshCallback(func(ctx context.Context, since time.Time) error {
			logger.Info("========================================")
			if since.IsZero() {
				logger.Info("MV REFRESH DETECTED - STARTING FULL PROCESSING")
			} else {
				logger.Info("MV REFRESH DETECTED - STARTING INCREMENTAL PROCESSING",
					zap.Time("since", since),
				)
			}
			logger.Info("========================================")

			startTime := time.Now()
			err := processor.Run(ctx)

			if err != nil {
				logger.Error("Processing failed after MV refresh",
					zap.Error(err),
					zap.Duration("duration", time.Since(startTime)),
				)
				return err
			}

			stats := processor.GetStats()
			logger.Info("========================================")
			logger.Info("PROCESSING COMPLETED AFTER MV REFRESH")
			logger.Info("========================================")
			logger.Info("Final statistics",
				zap.Duration("duration", time.Since(startTime)),
				zap.Int("total_customers", stats.TotalCustomers),
				zap.Int("total_processed", stats.TotalProcessed),
				zap.Int("success_count", stats.SuccessCount),
				zap.Int("failed_count", stats.FailedCount),
			)

			return nil
		})

		// Start monitoring
		monitor.Start(ctx)
		defer monitor.Stop()

		// Wait for shutdown signal
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		logger.Info("Monitor running... Press Ctrl+C to stop")
		<-sigChan
		logger.Info("Shutdown signal received, stopping monitor...")

		return
	}

	// Standard mode: run once and exit
	startTime = time.Now()
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
  --config=/path/to/file     Path to config file (default: config.conf or $CONFIG_FILE)
  --dry-run                  Run in dry-run mode (no database updates)
  --customer-ids=1,2,3       Process only specific customers (comma-separated)
  --workers=50               Override number of concurrent workers
  --batch-size=500           Override batch size
  --resume-from-checkpoint   Resume from the last checkpoint
  --monitor                  Run in continuous monitoring mode (polls for MV refresh)
  --poll-interval=5          Polling interval in minutes for monitor mode (default: 5)
  --help                     Show this help message

Configuration:
  Config file can be specified in three ways (in order of priority):
  1. --config flag:           risk-processor --config=/etc/risk-analysis/config.conf
  2. CONFIG_FILE env var:     export CONFIG_FILE=/etc/risk-analysis/config.conf
  3. Default location:        ./config.conf (current directory)

  The config file uses INI format with sections:
    [database]       - Database connection and pool settings
    [risk_analysis]  - Risk processing, logging, and execution settings
    [redis]          - Redis cache settings (required for monitor mode)

  See config.conf for all available options and defaults.

Monitor Mode:
  When --monitor flag is used, the processor runs continuously as a service,
  polling the risk_analysis table's last_refresh column for changes.
  When a change is detected (indicating the materialized view has been refreshed),
  it automatically triggers risk analysis processing.
  This mode requires Redis to be enabled in config.conf.

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

  # Run in monitor mode (continuously poll for MV refresh every 5 minutes)
  risk-processor --monitor

  # Run in monitor mode with custom poll interval (10 minutes)
  risk-processor --monitor --poll-interval=10
`)
}
