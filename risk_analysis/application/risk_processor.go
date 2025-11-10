package application

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"risk_analysis/config"
	"risk_analysis/domain/services"
	"risk_analysis/infrastructure/cache"
	"risk_analysis/workers"
)

// CustomerJob represents a job for processing a customer's risk score
type CustomerJob struct {
	customerID int
	processor  *RiskProcessor
}

// Process implements the Job interface for CustomerJob
// NOTE: This is NOT USED anymore - we use batch processing instead
func (j *CustomerJob) Process(ctx context.Context) error {
	// This method is kept for interface compatibility but is not used
	// The actual processing happens in processSingleBatch via batchedCalculator
	return fmt.Errorf("single customer processing not supported - use batch processing")
}

// ID returns the customer ID for the job
func (j *CustomerJob) ID() int {
	return j.customerID
}

// Checkpoint represents a saved processing state
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

// RiskProcessor orchestrates the risk score calculation process
type RiskProcessor struct {
	config              *config.Config // Use the config.Config type
	db                  *pgxpool.Pool
	logger              *zap.Logger
	batchedCalculator   *services.BatchedFunctionRiskCalculator // Function-based batched calculator (slower, uses functions)
	mvCalculator        *services.MVRiskCalculator              // MV-based calculator (faster, uses pre-computed materialized views)
	useMVCalculator     bool                                    // Flag to use MV calculator instead of function calculator
	workerPool          *workers.WorkerPool
	customerCache       *cache.CustomerIDCache       // File-based cache for customer IDs
	redisCustomerCache  *cache.RedisCustomerIDCache  // Redis-based cache for customer IDs
	redisClient         *redis.Client                // Redis client for checkpoint storage
	useRedis            bool                         // Flag to determine if Redis is enabled
	stats          struct {
		startTime       time.Time
		endTime         time.Time
		totalCustomers  int
		totalProcessed  int
		successCount    int
		failedCount     int
		failedCustomers []int
		batchCount      int
		mu              sync.Mutex // Protects stats fields
	}
}

// NewRiskProcessor creates a new risk processor with file-based caching
func NewRiskProcessor(config *config.Config, db *pgxpool.Pool, logger *zap.Logger) *RiskProcessor {
	// Initialize BATCHED FUNCTION risk calculator with cache paths from config
	batchedCalculator := services.NewBatchedFunctionRiskCalculator(
		db,
		logger,
		config.RiskFunctionsCacheFile,
		config.RiskMetadataCacheFile,
	)

	// Create worker pool for parallel processing
	workerPool := workers.NewWorkerPool(
		config.WorkerCount,
		config.BatchSize*2, // Buffer size twice the batch size to keep workers busy
		logger,
	)

	// Initialize customer ID cache for file-based caching
	customerCache := cache.NewCustomerIDCache(
		config.CustomerIDCacheFile,
		config.ProcessedCustomersFile,
		db,
		logger,
	)

	return &RiskProcessor{
		config:            config,
		db:                db,
		logger:            logger,
		batchedCalculator: batchedCalculator,
		workerPool:        workerPool,
		customerCache:     customerCache,
		useRedis:          false,
	}
}

// NewRedisRiskProcessor creates a new risk processor with Redis-based caching
// Uses materialized views for 10x faster performance
func NewRedisRiskProcessor(config *config.Config, db *pgxpool.Pool, redisClient *redis.Client, logger *zap.Logger) *RiskProcessor {
	// Initialize MV-based risk calculator with Redis caching (FASTEST)
	mvCalculator := services.NewMVRiskCalculator(
		db,
		logger,
		redisClient,
		config.DBName,
	)

	// Create worker pool for parallel processing
	workerPool := workers.NewWorkerPool(
		config.WorkerCount,
		config.BatchSize*2, // Buffer size twice the batch size to keep workers busy
		logger,
	)

	// Initialize Redis customer ID cache
	redisCustomerCache := cache.NewRedisCustomerIDCache(
		redisClient,
		db,
		config.DBName,
		logger,
	)

	return &RiskProcessor{
		config:             config,
		db:                 db,
		logger:             logger,
		mvCalculator:       mvCalculator,
		useMVCalculator:    true,
		workerPool:         workerPool,
		redisCustomerCache: redisCustomerCache,
		redisClient:        redisClient,
		useRedis:           true,
	}
}

// NewRedisRiskProcessorWithFunctions creates a new risk processor with Redis-based caching using functions (legacy)
func NewRedisRiskProcessorWithFunctions(config *config.Config, db *pgxpool.Pool, redisClient *redis.Client, logger *zap.Logger) *RiskProcessor {
	// Initialize BATCHED FUNCTION risk calculator with Redis caching (SLOWER - uses functions)
	batchedCalculator := services.NewRedisBatchedFunctionRiskCalculator(
		db,
		logger,
		redisClient,
		config.DBName,
	)

	// Create worker pool for parallel processing
	workerPool := workers.NewWorkerPool(
		config.WorkerCount,
		config.BatchSize*2, // Buffer size twice the batch size to keep workers busy
		logger,
	)

	// Initialize Redis customer ID cache
	redisCustomerCache := cache.NewRedisCustomerIDCache(
		redisClient,
		db,
		config.DBName,
		logger,
	)

	return &RiskProcessor{
		config:             config,
		db:                 db,
		logger:             logger,
		batchedCalculator:  batchedCalculator,
		useMVCalculator:    false,
		workerPool:         workerPool,
		redisCustomerCache: redisCustomerCache,
		redisClient:        redisClient,
		useRedis:           true,
	}
}

// InitializeCache loads frequently accessed data into memory for optimal performance
func (p *RiskProcessor) InitializeCache(ctx context.Context) error {
	// Initialize calculator cache (settings, plans, MVs, etc.)
	if p.useMVCalculator {
		p.logger.Info("Initializing MV-based calculator cache (using materialized views)...")
		if err := p.mvCalculator.InitializeCache(ctx); err != nil {
			return fmt.Errorf("failed to initialize MV calculator cache: %w", err)
		}

		// Start new customer monitor for real-time priority processing
		// Poll every 10 seconds for new customers
		p.logger.Info("Starting new customer monitor for priority processing...")
		p.mvCalculator.StartNewCustomerMonitor(ctx, 10*time.Second)
	} else {
		p.logger.Info("Initializing function-based calculator cache (using database functions)...")
		if err := p.batchedCalculator.InitializeCache(ctx); err != nil {
			return fmt.Errorf("failed to initialize batched calculator cache: %w", err)
		}
	}

	// Load processed customers to enable incremental processing
	if p.useRedis {
		if err := p.redisCustomerCache.LoadProcessedCustomers(ctx); err != nil {
			return fmt.Errorf("failed to load processed customers from Redis: %w", err)
		}
	} else {
		if err := p.customerCache.LoadProcessedCustomers(); err != nil {
			return fmt.Errorf("failed to load processed customers: %w", err)
		}
	}

	return nil
}

// Run executes the risk processor
func (p *RiskProcessor) Run(ctx context.Context) error {
	p.stats.startTime = time.Now()

	// Initialize customer IDs to process
	var customerIDs []int
	var err error
	
	// If specific customer IDs are provided, use them
	if len(p.config.CustomerIDs) > 0 {
		customerIDs = p.config.CustomerIDs
		p.stats.totalCustomers = len(customerIDs)
		p.logger.Info("Using specific customer IDs",
			zap.Int("count", len(customerIDs)),
		)
	} else {
		// Otherwise, load customer IDs from database
		if p.config.ResumeFromCheckpoint {
			// Load checkpoint and resume from last processed ID
			checkpoint, err := p.loadCheckpoint()
			if err != nil {
				p.logger.Warn("Failed to load checkpoint, starting from beginning",
					zap.Error(err),
				)
			} else {
				p.logger.Info("Resuming from checkpoint",
					zap.Int("last_processed_id", checkpoint.LastProcessedID),
					zap.Int64("total_processed", checkpoint.TotalProcessed),
				)
				
				// Load customer IDs from last processed ID
				customerIDs, err = p.loadCustomerIDsAfter(ctx, checkpoint.LastProcessedID)
				if err != nil {
					return fmt.Errorf("failed to load customer IDs after checkpoint: %w", err)
				}
				
				// Update stats from checkpoint
				p.stats.mu.Lock()
				p.stats.totalProcessed = int(checkpoint.TotalProcessed)
				p.stats.successCount = int(checkpoint.TotalSuccess)
				p.stats.failedCount = int(checkpoint.TotalFailed)
				p.stats.batchCount = checkpoint.BatchNumber
				p.stats.failedCustomers = checkpoint.FailedCustomerIDs
				p.stats.mu.Unlock()
			}
		}
		
		// If no checkpoint or checkpoint loading failed, load all customer IDs
		if customerIDs == nil {
			customerIDs, err = p.loadAllCustomerIDs(ctx)
			if err != nil {
				return fmt.Errorf("failed to load customer IDs: %w", err)
			}
		}
		
		p.stats.totalCustomers = len(customerIDs)
		p.logger.Info("Loaded customer IDs",
			zap.Int("count", len(customerIDs)),
		)
	}
	
	// Start worker pool
	p.workerPool.Start(ctx)
	defer p.workerPool.Stop()
	
	// Create a monitoring goroutine to log progress and save checkpoints
	done := make(chan struct{})
	defer close(done)
	go p.monitorProgress(ctx, done)
	
	// Process customers in batches
	return p.processCustomersInBatches(ctx, customerIDs)
}

// processCustomersInBatches processes customers using parallel batch transactions for maximum performance
func (p *RiskProcessor) processCustomersInBatches(ctx context.Context, customerIDs []int) error {
	totalCustomers := len(customerIDs)

	// Use smaller batch size for transactions (configurable via BATCH_SIZE env var)
	transactionBatchSize := p.config.BatchSize
	if transactionBatchSize > 500 {
		transactionBatchSize = 200 // Cap at 200 for transaction safety
	}

	batches := (totalCustomers + transactionBatchSize - 1) / transactionBatchSize

	// Determine optimal concurrency based on worker count and available CPU cores
	// For 16-core server with 128 workers: allow up to 32 concurrent batches
	// This ensures maximum CPU and I/O utilization
	maxConcurrentBatches := p.config.WorkerCount / 4
	if maxConcurrentBatches < 4 {
		maxConcurrentBatches = 4
	}
	if maxConcurrentBatches > 64 {
		maxConcurrentBatches = 64 // Cap at 64 for very high worker counts
	}

	p.logger.Info("Starting parallel batch transaction processing",
		zap.Int("total_customers", totalCustomers),
		zap.Int("transaction_batch_size", transactionBatchSize),
		zap.Int("total_batches", batches),
		zap.Int("concurrent_batches", maxConcurrentBatches),
	)

	// Create semaphore to limit concurrent batches
	semaphore := make(chan struct{}, maxConcurrentBatches)
	errChan := make(chan error, batches)
	var wg sync.WaitGroup

	p.logger.Info("About to enter batch processing loop",
		zap.Int("batches", batches),
	)

	// Process batches in parallel
	for batchIdx := 0; batchIdx < batches; batchIdx++ {
		// Check if context is cancelled - stop submitting new batches
		select {
		case <-ctx.Done():
			p.logger.Info("Context cancelled, stopping batch submission",
				zap.Int("batches_submitted", batchIdx),
				zap.Int("total_batches", batches),
			)
			// Wait for already submitted batches to complete
			wg.Wait()
			close(errChan)

			// Save checkpoint before returning
			if err := p.saveCheckpoint(); err != nil {
				p.logger.Warn("Failed to save checkpoint during shutdown", zap.Error(err))
			}
			return ctx.Err()
		default:
		}

		// Determine batch bounds
		startIdx := batchIdx * transactionBatchSize
		endIdx := (batchIdx + 1) * transactionBatchSize
		if endIdx > totalCustomers {
			endIdx = totalCustomers
		}

		// Get customer IDs for this batch
		batchCustomerIDs := customerIDs[startIdx:endIdx]

		// Log first few goroutine launches for debugging
		if batchIdx < 5 {
			p.logger.Info("Launching batch goroutine",
				zap.Int("batch_number", batchIdx+1),
				zap.Int("batch_size", len(batchCustomerIDs)),
			)
		}

		wg.Add(1)
		go func(batchNum int, custIDs []int, start int) {
			defer wg.Done()

			// Log that goroutine started
			if batchNum <= 5 {
				p.logger.Info("Batch goroutine started, acquiring semaphore",
					zap.Int("batch_number", batchNum),
				)
			}

			// Acquire semaphore slot
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Log semaphore acquired
			if batchNum <= 5 {
				p.logger.Info("Semaphore acquired, starting batch processing",
					zap.Int("batch_number", batchNum),
				)
			}

			// Check context again before processing (in case cancelled while waiting for semaphore)
			select {
			case <-ctx.Done():
				p.logger.Debug("Skipping batch due to context cancellation",
					zap.Int("batch_number", batchNum),
				)
				return
			default:
			}

			// Process batch
			if err := p.processSingleBatch(ctx, batchNum, custIDs, start, totalCustomers); err != nil {
				// Only log non-context-cancelled errors
				if err != context.Canceled {
					errChan <- err
				}
			}
		}(batchIdx+1, batchCustomerIDs, startIdx)
	}

	// Wait for all batches to complete
	wg.Wait()
	close(errChan)

	// Check for errors
	var firstError error
	for err := range errChan {
		if firstError == nil {
			firstError = err
		}
	}
	
	p.stats.endTime = time.Now()

	// Log final statistics
	p.logger.Info("Processing completed",
		zap.Int("total_processed", p.stats.totalProcessed),
		zap.Int("success_count", p.stats.successCount),
		zap.Int("failed_count", p.stats.failedCount),
		zap.Duration("duration", p.stats.endTime.Sub(p.stats.startTime)),
	)

	// Save final checkpoint
	if firstError != nil {
		p.logger.Error("Some batches failed during processing", zap.Error(firstError))
	}
	return p.saveCheckpoint()
}

// processSingleBatch processes a single batch of customers
func (p *RiskProcessor) processSingleBatch(ctx context.Context, batchNum int, customerIDs []int, startIdx int, totalCustomers int) error {
	batchStartTime := time.Now()

	p.logger.Debug("Processing batch",
		zap.Int("batch_number", batchNum),
		zap.Int("batch_size", len(customerIDs)),
		zap.Int("progress", startIdx),
		zap.Float64("progress_percent", float64(startIdx)/float64(totalCustomers)*100),
	)

	// Process entire batch with configured workers per batch
	var results []services.CustomerRiskResult
	if p.useMVCalculator {
		results = p.mvCalculator.ProcessCustomerBatch(ctx, customerIDs, p.config.DryRun, p.config.WorkersPerBatch)
	} else {
		results = p.batchedCalculator.ProcessCustomerBatch(ctx, customerIDs, p.config.DryRun, p.config.WorkersPerBatch)
	}

	// Count successes and failures
	batchSuccessCount := 0
	batchFailedCount := 0
	cancelledCount := 0
	successfulCustomerIDs := make([]int, 0, len(results))

	for _, result := range results {
		if result.Error != nil {
			// Check if error is due to context cancellation (graceful shutdown)
			if result.Error == context.Canceled {
				cancelledCount++
				// Don't log as error - this is expected during graceful shutdown
				p.logger.Debug("Customer processing cancelled due to shutdown",
					zap.Int("batch_number", batchNum),
					zap.Int("customer_id", result.CustomerID),
				)
			} else {
				batchFailedCount++
				p.logger.Error("Failed to process customer in batch",
					zap.Int("batch_number", batchNum),
					zap.Int("customer_id", result.CustomerID),
					zap.Error(result.Error),
				)

				p.stats.mu.Lock()
				p.stats.failedCustomers = append(p.stats.failedCustomers, result.CustomerID)
				p.stats.mu.Unlock()
			}
		} else {
			batchSuccessCount++
			successfulCustomerIDs = append(successfulCustomerIDs, result.CustomerID)
		}
	}

	// Mark successfully processed customers in the cache (file-based or Redis)
	if len(successfulCustomerIDs) > 0 {
		if p.useRedis {
			if err := p.redisCustomerCache.MarkBatchProcessed(ctx, successfulCustomerIDs); err != nil {
				p.logger.Warn("Failed to mark batch as processed in Redis cache",
					zap.Int("batch_number", batchNum),
					zap.Int("success_count", len(successfulCustomerIDs)),
					zap.Error(err),
				)
			}
		} else {
			if err := p.customerCache.MarkBatchProcessed(successfulCustomerIDs); err != nil {
				p.logger.Warn("Failed to mark batch as processed in file cache",
					zap.Int("batch_number", batchNum),
					zap.Int("success_count", len(successfulCustomerIDs)),
					zap.Error(err),
				)
			}
		}
	}

	batchDuration := time.Since(batchStartTime)
	avgPerCustomer := batchDuration.Milliseconds() / int64(len(customerIDs))

	// Log batch completion with cancellation info if applicable
	logFields := []zap.Field{
		zap.Int("batch_number", batchNum),
		zap.Int("success", batchSuccessCount),
		zap.Int("failed", batchFailedCount),
		zap.Duration("duration", batchDuration),
		zap.Int64("avg_ms_per_customer", avgPerCustomer),
	}

	if cancelledCount > 0 {
		logFields = append(logFields, zap.Int("cancelled_by_shutdown", cancelledCount))
		p.logger.Warn("Batch partially cancelled due to shutdown", logFields...)
	} else {
		p.logger.Info("Batch completed", logFields...)
	}

	// Update statistics (thread-safe)
	// Note: We count cancelled customers as "processed" since they were attempted
	// but we track them separately to not count them as failures
	p.stats.mu.Lock()
	p.stats.totalProcessed += batchSuccessCount + batchFailedCount + cancelledCount
	p.stats.successCount += batchSuccessCount
	p.stats.failedCount += batchFailedCount
	p.stats.batchCount++
	shouldCheckpoint := p.config.ProgressCheckpointInterval > 0 &&
		p.stats.totalProcessed%p.config.ProgressCheckpointInterval == 0
	p.stats.mu.Unlock()

	// Create checkpoint if needed
	if shouldCheckpoint {
		if err := p.saveCheckpoint(); err != nil {
			p.logger.Warn("Failed to save checkpoint",
				zap.Int("batch_number", batchNum),
				zap.Error(err),
			)
		}
	}

	return nil
}

// loadAllCustomerIDs loads all customer IDs using cache (file-based or Redis)
func (p *RiskProcessor) loadAllCustomerIDs(ctx context.Context) ([]int, error) {
	var allCustomerIDs []int
	var unprocessedCustomerIDs []int
	var processedCount int
	var err error

	if p.useRedis {
		// Load from Redis cache
		allCustomerIDs, err = p.redisCustomerCache.LoadOrRefresh(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load customer IDs from Redis cache: %w", err)
		}

		// Filter to only unprocessed customers for incremental processing
		unprocessedCustomerIDs = p.redisCustomerCache.GetUnprocessedCustomers(allCustomerIDs)
		processedCount = p.redisCustomerCache.GetProcessedCount()
	} else {
		// Load from file cache
		allCustomerIDs, err = p.customerCache.LoadOrRefresh(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load customer IDs from file cache: %w", err)
		}

		// Filter to only unprocessed customers for incremental processing
		unprocessedCustomerIDs = p.customerCache.GetUnprocessedCustomers(allCustomerIDs)
		processedCount = p.customerCache.GetProcessedCount()
	}

	p.logger.Info("Loaded customer IDs from cache",
		zap.Bool("redis", p.useRedis),
		zap.Int("total_customers", len(allCustomerIDs)),
		zap.Int("processed_customers", processedCount),
		zap.Int("unprocessed_customers", len(unprocessedCustomerIDs)),
	)

	return unprocessedCustomerIDs, nil
}

// loadCustomerIDsAfter loads customer IDs after a specific ID
func (p *RiskProcessor) loadCustomerIDsAfter(ctx context.Context, afterID int) ([]int, error) {
	var customerIDs []int
	
	// Count remaining customers
	var remainingCustomers int
	err := p.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner WHERE id > $1", afterID).Scan(&remainingCustomers)
	if err != nil {
		return nil, fmt.Errorf("failed to count remaining customers: %w", err)
	}
	
	p.logger.Info("Remaining customers to process",
		zap.Int("after_id", afterID),
		zap.Int("count", remainingCustomers),
	)
	
	// Pre-allocate slice for customer IDs
	customerIDs = make([]int, 0, remainingCustomers)
	
	// Process in chunks to avoid loading all IDs at once
	chunkSize := p.config.ChunkSize
	for offset := 0; offset < remainingCustomers; offset += chunkSize {
		// Use limit and offset to get a chunk of customer IDs
		rows, err := p.db.Query(ctx, 
			"SELECT id FROM res_partner WHERE id > $1 ORDER BY id LIMIT $2 OFFSET $3", 
			afterID, chunkSize, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to query customer IDs: %w", err)
		}
		
		// Scan customer IDs from rows
		chunkIDs := make([]int, 0, chunkSize)
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan customer ID: %w", err)
			}
			chunkIDs = append(chunkIDs, id)
		}
		rows.Close()
		
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating customer rows: %w", err)
		}
		
		// Append chunk IDs to full list
		customerIDs = append(customerIDs, chunkIDs...)
		
		p.logger.Info("Loaded customer ID chunk",
			zap.Int("offset", offset),
			zap.Int("chunk_size", len(chunkIDs)),
			zap.Int("total_loaded", len(customerIDs)),
		)
	}
	
	return customerIDs, nil
}

// monitorProgress logs progress and stats periodically
func (p *RiskProcessor) monitorProgress(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			p.logProgress()
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

// logProgress logs current progress statistics
func (p *RiskProcessor) logProgress() {
	p.stats.mu.Lock()
	defer p.stats.mu.Unlock()
	
	if p.stats.totalCustomers == 0 {
		return
	}
	
	elapsed := time.Since(p.stats.startTime)
	var remainingTime time.Duration
	
	if p.stats.totalProcessed > 0 {
		// Calculate progress and estimate remaining time
		progress := float64(p.stats.totalProcessed) / float64(p.stats.totalCustomers)
		if progress > 0 {
			totalTime := elapsed.Seconds() / progress
			remainingTime = time.Duration(totalTime-elapsed.Seconds()) * time.Second
		}
	}
	
	// Get worker pool stats
	poolStats := p.workerPool.GetStats()
	
	p.logger.Info("Processing progress",
		zap.Int("processed", p.stats.totalProcessed),
		zap.Int("total", p.stats.totalCustomers),
		zap.Float64("progress_percent", float64(p.stats.totalProcessed)/float64(p.stats.totalCustomers)*100),
		zap.Duration("elapsed", elapsed),
		zap.Duration("estimated_remaining", remainingTime),
		zap.Int("success_count", p.stats.successCount),
		zap.Int("failed_count", p.stats.failedCount),
		zap.Int32("active_workers", poolStats.ActiveWorkers),
		zap.Int64("avg_processing_ms", poolStats.AvgProcessingMs),
	)
}

// saveCheckpoint saves the current processing state
func (p *RiskProcessor) saveCheckpoint() error {
	p.stats.mu.Lock()
	defer p.stats.mu.Unlock()

	// Determine last processed customer ID
	lastProcessedID := 0
	if len(p.config.CustomerIDs) > 0 && p.stats.totalProcessed > 0 {
		if p.stats.totalProcessed <= len(p.config.CustomerIDs) {
			lastProcessedID = p.config.CustomerIDs[p.stats.totalProcessed-1]
		}
	} else {
		// No specific customer IDs, use workerPool stats
		lastProcessedID = int(p.workerPool.GetStats().LastProcessedID)
	}

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

	if p.useRedis {
		// Save checkpoint to Redis
		ctx := context.Background()
		data, err := json.Marshal(checkpoint)
		if err != nil {
			return fmt.Errorf("failed to marshal checkpoint: %w", err)
		}

		key := fmt.Sprintf("%s_checkpoint", p.config.DBName)
		if err := p.redisClient.Set(ctx, key, data, 0).Err(); err != nil {
			return fmt.Errorf("failed to save checkpoint to Redis: %w", err)
		}

		p.logger.Info("Checkpoint saved to Redis",
			zap.String("key", key),
			zap.String("db_name", p.config.DBName),
			zap.Int("last_processed_id", lastProcessedID),
			zap.Int("total_processed", p.stats.totalProcessed),
		)
	} else {
		// Save checkpoint to file
		data, err := json.MarshalIndent(checkpoint, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal checkpoint: %w", err)
		}

		// Ensure checkpoint directory exists
		checkpointDir := filepath.Dir(p.config.CheckpointFile)
		if err := os.MkdirAll(checkpointDir, 0755); err != nil {
			return fmt.Errorf("failed to create checkpoint directory: %w", err)
		}

		// Write checkpoint to file
		if err := os.WriteFile(p.config.CheckpointFile, data, 0644); err != nil {
			return fmt.Errorf("failed to write checkpoint file: %w", err)
		}

		p.logger.Info("Checkpoint saved to file",
			zap.String("file", p.config.CheckpointFile),
			zap.Int("last_processed_id", lastProcessedID),
			zap.Int("total_processed", p.stats.totalProcessed),
		)
	}

	return nil
}

// loadCheckpoint loads the processing state from a checkpoint
func (p *RiskProcessor) loadCheckpoint() (Checkpoint, error) {
	var checkpoint Checkpoint

	if p.useRedis {
		// Load checkpoint from Redis
		ctx := context.Background()
		key := fmt.Sprintf("%s_checkpoint", p.config.DBName)

		data, err := p.redisClient.Get(ctx, key).Result()
		if err != nil {
			if err == redis.Nil {
				return checkpoint, fmt.Errorf("checkpoint not found in Redis: %s", key)
			}
			return checkpoint, fmt.Errorf("failed to load checkpoint from Redis: %w", err)
		}

		// Unmarshal checkpoint data
		if err := json.Unmarshal([]byte(data), &checkpoint); err != nil {
			return checkpoint, fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
		}

		p.logger.Info("Checkpoint loaded from Redis",
			zap.String("key", key),
			zap.String("db_name", p.config.DBName),
			zap.Int("last_processed_id", checkpoint.LastProcessedID),
			zap.Int64("total_processed", checkpoint.TotalProcessed),
			zap.Time("timestamp", checkpoint.Timestamp),
		)
	} else {
		// Load checkpoint from file
		// Check if checkpoint file exists
		if _, err := os.Stat(p.config.CheckpointFile); os.IsNotExist(err) {
			return checkpoint, fmt.Errorf("checkpoint file not found: %s", p.config.CheckpointFile)
		}

		// Read checkpoint file
		data, err := os.ReadFile(p.config.CheckpointFile)
		if err != nil {
			return checkpoint, fmt.Errorf("failed to read checkpoint file: %w", err)
		}

		// Unmarshal checkpoint data
		if err := json.Unmarshal(data, &checkpoint); err != nil {
			return checkpoint, fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
		}

		p.logger.Info("Checkpoint loaded from file",
			zap.String("file", p.config.CheckpointFile),
			zap.Int("last_processed_id", checkpoint.LastProcessedID),
			zap.Int64("total_processed", checkpoint.TotalProcessed),
			zap.Time("timestamp", checkpoint.Timestamp),
		)
	}

	return checkpoint, nil
}

// func (p *RiskProcessor) saveCheckpoint() error {
// 	p.stats.mu.Lock()
// 	defer p.stats.mu.Unlock()
	
// 	// Determine last processed customer ID
// 	lastProcessedID := 0
// 	if len(p.config.CustomerIDs) > 0 && p.stats.totalProcessed > 0 {
// 		if p.stats.totalProcessed <= len(p.config.CustomerIDs) {
// 			lastProcessedID = p.config.CustomerIDs[p.stats.totalProcessed-1]
// 		}
// 	} else {
// 		// No specific customer IDs, use workerPool stats
// 		lastProcessedID = int(p.workerPool.GetStats().LastProcessedID)
// 	}
	
// 	checkpoint := Checkpoint{
// 		Version:           "1.0",
// 		Timestamp:         time.Now(),
// 		LastProcessedID:   lastProcessedID,
// 		TotalProcessed:    int64(p.stats.totalProcessed),
// 		TotalSuccess:      int64(p.stats.successCount),
// 		TotalFailed:       int64(p.stats.failedCount),
// 		BatchNumber:       p.stats.batchCount,
// 		FailedCustomerIDs: p.stats.failedCustomers,
// 	}
	
// 	// Marshal checkpoint to JSON
// 	data, err := json.MarshalIndent(checkpoint, "", "  ")
// 	if err != nil {
// 		return fmt.Errorf("failed to marshal checkpoint: %w", err)
// 	}
	
// 	// Write checkpoint to file
// 	if err := os.WriteFile(p.config.CheckpointFile, data, 0644); err != nil {
// 		return fmt.Errorf("failed to write checkpoint file: %w", err)
// 	}
	
// 	p.logger.Info("Checkpoint saved",
// 		zap.String("file", p.config.CheckpointFile),
// 		zap.Int("last_processed_id", lastProcessedID),
// 		zap.Int("total_processed", p.stats.totalProcessed),
// 	)
	
// 	return nil
// }

// // loadCheckpoint loads the processing state from a checkpoint file
// func (p *RiskProcessor) loadCheckpoint() (Checkpoint, error) {
// 	var checkpoint Checkpoint
	
// 	// Check if checkpoint file exists
// 	if _, err := os.Stat(p.config.CheckpointFile); os.IsNotExist(err) {
// 		return checkpoint, fmt.Errorf("checkpoint file not found: %s", p.config.CheckpointFile)
// 	}
	
// 	// Read checkpoint file
// 	data, err := os.ReadFile(p.config.CheckpointFile)
// 	if err != nil {
// 		return checkpoint, fmt.Errorf("failed to read checkpoint file: %w", err)
// 	}
	
// 	// Unmarshal checkpoint data
// 	if err := json.Unmarshal(data, &checkpoint); err != nil {
// 		return checkpoint, fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
// 	}
	
// 	p.logger.Info("Checkpoint loaded",
// 		zap.String("file", p.config.CheckpointFile),
// 		zap.Int("last_processed_id", checkpoint.LastProcessedID),
// 		zap.Int64("total_processed", checkpoint.TotalProcessed),
// 		zap.Time("timestamp", checkpoint.Timestamp),
// 	)
	
// 	return checkpoint, nil
// }

// GetStats returns the current processing statistics
func (p *RiskProcessor) GetStats() struct {
	TotalCustomers int
	TotalProcessed int
	SuccessCount   int
	FailedCount    int
	FailedCustomers []int
	BatchCount     int
	Duration       time.Duration
} {
	p.stats.mu.Lock()
	defer p.stats.mu.Unlock()
	
	var endTime time.Time
	if p.stats.endTime.IsZero() {
		endTime = time.Now()
	} else {
		endTime = p.stats.endTime
	}
	
	return struct {
		TotalCustomers int
		TotalProcessed int
		SuccessCount   int
		FailedCount    int
		FailedCustomers []int
		BatchCount     int
		Duration       time.Duration
	}{
		TotalCustomers: p.stats.totalCustomers,
		TotalProcessed: p.stats.totalProcessed,
		SuccessCount:   p.stats.successCount,
		FailedCount:    p.stats.failedCount,
		FailedCustomers: p.stats.failedCustomers,
		BatchCount:     p.stats.batchCount,
		Duration:       endTime.Sub(p.stats.startTime),
	}
}


// package application

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"os"
// 	"sync"
// 	"time"

// 	"github.com/jackc/pgx/v5/pgxpool"
// 	"go.uber.org/zap"

// 	"risk_analysis/domain/services"
// 	"risk_analysis/workers"
// )

// // CustomerJob represents a job for processing a customer's risk score
// type CustomerJob struct {
// 	customerID int
// 	processor  *RiskProcessor
// }

// // Process implements the Job interface for CustomerJob
// func (j *CustomerJob) Process(ctx context.Context) error {
// 	// Calculate risk score
// 	score, level, err := j.processor.riskCalculator.CalculateRiskScore(ctx, j.customerID)
// 	if err != nil {
// 		return fmt.Errorf("error calculating risk score for customer %d: %w", j.customerID, err)
// 	}

// 	// If not in dry run mode, update the customer record
// 	if !j.processor.config.DryRun {
// 		if err := j.processor.riskCalculator.UpdateCustomerRiskScore(ctx, j.customerID, score, level); err != nil {
// 			return fmt.Errorf("error updating risk score for customer %d: %w", j.customerID, err)
// 		}
// 	}

// 	return nil
// }

// // ID returns the customer ID for the job
// func (j *CustomerJob) ID() int {
// 	return j.customerID
// }

// // Checkpoint represents a saved processing state
// type Checkpoint struct {
// 	Version             string    `json:"checkpoint_version"`
// 	Timestamp           time.Time `json:"timestamp"`
// 	LastProcessedID     int       `json:"last_processed_customer_id"`
// 	TotalProcessed      int64     `json:"total_processed"`
// 	TotalSuccess        int64     `json:"total_success"`
// 	TotalFailed         int64     `json:"total_failed"`
// 	BatchNumber         int       `json:"batch_number"`
// 	FailedCustomerIDs   []int     `json:"failed_customer_ids"`
// }

// // RiskProcessor orchestrates the risk score calculation process
// type RiskProcessor struct {
// 	config         *Config
// 	db             *pgxpool.Pool
// 	logger         *zap.Logger
// 	riskCalculator *services.RiskCalculator
// 	workerPool     *workers.WorkerPool
// 	stats          struct {
// 		startTime       time.Time
// 		endTime         time.Time
// 		totalCustomers  int
// 		totalProcessed  int
// 		successCount    int
// 		failedCount     int
// 		failedCustomers []int
// 		batchCount      int
// 		mu              sync.Mutex // Protects stats fields
// 	}
// }

// // Config holds configuration for the risk processor
// type Config struct {
// 	// Processing settings
// 	BatchSize                  int
// 	WorkerCount                int
// 	ChunkSize                  int
// 	EnableBulkOperations       bool
// 	BulkInsertBatchSize        int
// 	ProgressCheckpointInterval int

// 	// Business rules
// 	LowRiskThreshold         float64
// 	MediumRiskThreshold      float64
// 	MaximumRiskThreshold     float64
// 	RiskPlanComputation      string
// 	RiskCompositeComputation string

// 	// Execution control
// 	DryRun              bool
// 	ResumeFromCheckpoint bool
// 	CheckpointFile       string
// 	CustomerIDs          []int // Optional specific customer IDs to process
// }

// // NewRiskProcessor creates a new risk processor
// func NewRiskProcessor(config *Config, db *pgxpool.Pool, logger *zap.Logger) *RiskProcessor {
// 	// Initialize risk calculator with settings
// 	riskCalculator := services.NewRiskCalculator(
// 		db,
// 		logger,
// 		config.MaximumRiskThreshold,
// 		config.RiskPlanComputation,
// 		config.RiskCompositeComputation,
// 	)

// 	// Create worker pool for parallel processing
// 	workerPool := workers.NewWorkerPool(
// 		config.WorkerCount,
// 		config.BatchSize*2, // Buffer size twice the batch size to keep workers busy
// 		logger,
// 	)

// 	return &RiskProcessor{
// 		config:         config,
// 		db:             db,
// 		logger:         logger,
// 		riskCalculator: riskCalculator,
// 		workerPool:     workerPool,
// 	}
// }

// // Run executes the risk processor
// func (p *RiskProcessor) Run(ctx context.Context) error {
// 	p.stats.startTime = time.Now()

// 	// Initialize customer IDs to process
// 	var customerIDs []int
// 	var err error
	
// 	// If specific customer IDs are provided, use them
// 	if len(p.config.CustomerIDs) > 0 {
// 		customerIDs = p.config.CustomerIDs
// 		p.stats.totalCustomers = len(customerIDs)
// 		p.logger.Info("Using specific customer IDs",
// 			zap.Int("count", len(customerIDs)),
// 		)
// 	} else {
// 		// Otherwise, load customer IDs from database
// 		if p.config.ResumeFromCheckpoint {
// 			// Load checkpoint and resume from last processed ID
// 			checkpoint, err := p.loadCheckpoint()
// 			if err != nil {
// 				p.logger.Warn("Failed to load checkpoint, starting from beginning",
// 					zap.Error(err),
// 				)
// 			} else {
// 				p.logger.Info("Resuming from checkpoint",
// 					zap.Int("last_processed_id", checkpoint.LastProcessedID),
// 					zap.Int64("total_processed", checkpoint.TotalProcessed),
// 				)
				
// 				// Load customer IDs from last processed ID
// 				customerIDs, err = p.loadCustomerIDsAfter(ctx, checkpoint.LastProcessedID)
// 				if err != nil {
// 					return fmt.Errorf("failed to load customer IDs after checkpoint: %w", err)
// 				}
				
// 				// Update stats from checkpoint
// 				p.stats.mu.Lock()
// 				p.stats.totalProcessed = int(checkpoint.TotalProcessed)
// 				p.stats.successCount = int(checkpoint.TotalSuccess)
// 				p.stats.failedCount = int(checkpoint.TotalFailed)
// 				p.stats.batchCount = checkpoint.BatchNumber
// 				p.stats.failedCustomers = checkpoint.FailedCustomerIDs
// 				p.stats.mu.Unlock()
// 			}
// 		}
		
// 		// If no checkpoint or checkpoint loading failed, load all customer IDs
// 		if customerIDs == nil {
// 			customerIDs, err = p.loadAllCustomerIDs(ctx)
// 			if err != nil {
// 				return fmt.Errorf("failed to load customer IDs: %w", err)
// 			}
// 		}
		
// 		p.stats.totalCustomers = len(customerIDs)
// 		p.logger.Info("Loaded customer IDs",
// 			zap.Int("count", len(customerIDs)),
// 		)
// 	}
	
// 	// Start worker pool
// 	p.workerPool.Start(ctx)
// 	defer p.workerPool.Stop()
	
// 	// Create a monitoring goroutine to log progress and save checkpoints
// 	done := make(chan struct{})
// 	defer close(done)
// 	go p.monitorProgress(ctx, done)
	
// 	// Process customers in batches
// 	return p.processCustomersInBatches(ctx, customerIDs)
// }

// // processCustomersInBatches processes customers in batches
// func (p *RiskProcessor) processCustomersInBatches(ctx context.Context, customerIDs []int) error {
// 	totalCustomers := len(customerIDs)
// 	batchSize := p.config.BatchSize
// 	batches := (totalCustomers + batchSize - 1) / batchSize // Ceiling division
	
// 	p.logger.Info("Starting customer processing",
// 		zap.Int("total_customers", totalCustomers),
// 		zap.Int("batch_size", batchSize),
// 		zap.Int("total_batches", batches),
// 		zap.Int("worker_count", p.config.WorkerCount),
// 	)
	
// 	// Process each batch
// 	for batchIdx := 0; batchIdx < batches; batchIdx++ {
// 		// Check if context is cancelled
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		default:
// 			// Continue processing
// 		}
		
// 		// Determine batch bounds
// 		startIdx := batchIdx * batchSize
// 		endIdx := (batchIdx + 1) * batchSize
// 		if endIdx > totalCustomers {
// 			endIdx = totalCustomers
// 		}
		
// 		// Get customer IDs for this batch
// 		batchCustomerIDs := customerIDs[startIdx:endIdx]
		
// 		p.logger.Info("Processing batch",
// 			zap.Int("batch_number", batchIdx+1),
// 			zap.Int("batch_size", len(batchCustomerIDs)),
// 			zap.Int("progress", startIdx),
// 			zap.Int("total", totalCustomers),
// 		)
		
// 		// Submit jobs for this batch
// 		for _, customerID := range batchCustomerIDs {
// 			job := &CustomerJob{
// 				customerID: customerID,
// 				processor:  p,
// 			}
			
// 			if err := p.workerPool.Submit(ctx, job); err != nil {
// 				return fmt.Errorf("failed to submit job for customer %d: %w", customerID, err)
// 			}
// 		}
		
// 		// Collect results for this batch
// 		batchSuccessCount := 0
// 		batchFailedCount := 0
		
// 		for i := 0; i < len(batchCustomerIDs); i++ {
// 			select {
// 			case err := <-p.workerPool.Results():
// 				if err != nil {
// 					batchFailedCount++
// 					p.logger.Error("Failed to process customer",
// 						zap.Error(err),
// 					)
					
// 					// Extract customer ID from error if possible
// 					var customerID int
// 					if jobErr, ok := err.(interface{ JobID() int }); ok {
// 						customerID = jobErr.JobID()
// 						p.stats.mu.Lock()
// 						p.stats.failedCustomers = append(p.stats.failedCustomers, customerID)
// 						p.stats.mu.Unlock()
// 					}
// 				} else {
// 					batchSuccessCount++
// 				}
// 			case <-ctx.Done():
// 				return ctx.Err()
// 			}
// 		}
		
// 		// Update statistics
// 		p.stats.mu.Lock()
// 		p.stats.totalProcessed += len(batchCustomerIDs)
// 		p.stats.successCount += batchSuccessCount
// 		p.stats.failedCount += batchFailedCount
// 		p.stats.batchCount++
// 		p.stats.mu.Unlock()
		
// 		// Create checkpoint if needed
// 		if p.config.ProgressCheckpointInterval > 0 && 
// 		   p.stats.totalProcessed % p.config.ProgressCheckpointInterval == 0 {
// 			if err := p.saveCheckpoint(); err != nil {
// 				p.logger.Warn("Failed to save checkpoint",
// 					zap.Error(err),
// 				)
// 			}
// 		}
// 	}
	
// 	p.stats.endTime = time.Now()
	
// 	// Log final statistics
// 	p.logger.Info("Processing completed",
// 		zap.Int("total_processed", p.stats.totalProcessed),
// 		zap.Int("success_count", p.stats.successCount),
// 		zap.Int("failed_count", p.stats.failedCount),
// 		zap.Duration("duration", p.stats.endTime.Sub(p.stats.startTime)),
// 	)
	
// 	// Save final checkpoint
// 	return p.saveCheckpoint()
// }

// // loadAllCustomerIDs loads all customer IDs from the database
// func (p *RiskProcessor) loadAllCustomerIDs(ctx context.Context) ([]int, error) {
// 	var customerIDs []int
	
// 	// Count total customers
// 	var totalCustomers int
// 	err := p.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner").Scan(&totalCustomers)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to count customers: %w", err)
// 	}
	
// 	p.logger.Info("Total customers in database", zap.Int("count", totalCustomers))
	
// 	// Pre-allocate slice for all customer IDs
// 	customerIDs = make([]int, 0, totalCustomers)
	
// 	// Process in chunks to avoid loading all IDs at once
// 	chunkSize := p.config.ChunkSize
// 	for offset := 0; offset < totalCustomers; offset += chunkSize {
// 		// Use limit and offset to get a chunk of customer IDs
// 		rows, err := p.db.Query(ctx, 
// 			"SELECT id FROM res_partner ORDER BY id LIMIT $1 OFFSET $2", 
// 			chunkSize, offset)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to query customer IDs: %w", err)
// 		}
		
// 		// Scan customer IDs from rows
// 		chunkIDs := make([]int, 0, chunkSize)
// 		for rows.Next() {
// 			var id int
// 			if err := rows.Scan(&id); err != nil {
// 				rows.Close()
// 				return nil, fmt.Errorf("failed to scan customer ID: %w", err)
// 			}
// 			chunkIDs = append(chunkIDs, id)
// 		}
// 		rows.Close()
		
// 		if err := rows.Err(); err != nil {
// 			return nil, fmt.Errorf("error iterating customer rows: %w", err)
// 		}
		
// 		// Append chunk IDs to full list
// 		customerIDs = append(customerIDs, chunkIDs...)
		
// 		p.logger.Info("Loaded customer ID chunk",
// 			zap.Int("offset", offset),
// 			zap.Int("chunk_size", len(chunkIDs)),
// 			zap.Int("total_loaded", len(customerIDs)),
// 		)
// 	}
	
// 	return customerIDs, nil
// }

// // loadCustomerIDsAfter loads customer IDs after a specific ID
// func (p *RiskProcessor) loadCustomerIDsAfter(ctx context.Context, afterID int) ([]int, error) {
// 	var customerIDs []int
	
// 	// Count remaining customers
// 	var remainingCustomers int
// 	err := p.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner WHERE id > $1", afterID).Scan(&remainingCustomers)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to count remaining customers: %w", err)
// 	}
	
// 	p.logger.Info("Remaining customers to process",
// 		zap.Int("after_id", afterID),
// 		zap.Int("count", remainingCustomers),
// 	)
	
// 	// Pre-allocate slice for customer IDs
// 	customerIDs = make([]int, 0, remainingCustomers)
	
// 	// Process in chunks to avoid loading all IDs at once
// 	chunkSize := p.config.ChunkSize
// 	for offset := 0; offset < remainingCustomers; offset += chunkSize {
// 		// Use limit and offset to get a chunk of customer IDs
// 		rows, err := p.db.Query(ctx, 
// 			"SELECT id FROM res_partner WHERE id > $1 ORDER BY id LIMIT $2 OFFSET $3", 
// 			afterID, chunkSize, offset)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to query customer IDs: %w", err)
// 		}
		
// 		// Scan customer IDs from rows
// 		chunkIDs := make([]int, 0, chunkSize)
// 		for rows.Next() {
// 			var id int
// 			if err := rows.Scan(&id); err != nil {
// 				rows.Close()
// 				return nil, fmt.Errorf("failed to scan customer ID: %w", err)
// 			}
// 			chunkIDs = append(chunkIDs, id)
// 		}
// 		rows.Close()
		
// 		if err := rows.Err(); err != nil {
// 			return nil, fmt.Errorf("error iterating customer rows: %w", err)
// 		}
		
// 		// Append chunk IDs to full list
// 		customerIDs = append(customerIDs, chunkIDs...)
		
// 		p.logger.Info("Loaded customer ID chunk",
// 			zap.Int("offset", offset),
// 			zap.Int("chunk_size", len(chunkIDs)),
// 			zap.Int("total_loaded", len(customerIDs)),
// 		)
// 	}
	
// 	return customerIDs, nil
// }

// // monitorProgress logs progress and stats periodically
// func (p *RiskProcessor) monitorProgress(ctx context.Context, done <-chan struct{}) {
// 	ticker := time.NewTicker(30 * time.Second)
// 	defer ticker.Stop()
	
// 	for {
// 		select {
// 		case <-ticker.C:
// 			p.logProgress()
// 		case <-done:
// 			return
// 		case <-ctx.Done():
// 			return
// 		}
// 	}
// }

// // logProgress logs current progress statistics
// func (p *RiskProcessor) logProgress() {
// 	p.stats.mu.Lock()
// 	defer p.stats.mu.Unlock()
	
// 	if p.stats.totalCustomers == 0 {
// 		return
// 	}
	
// 	elapsed := time.Since(p.stats.startTime)
// 	var remainingTime time.Duration
	
// 	if p.stats.totalProcessed > 0 {
// 		// Calculate progress and estimate remaining time
// 		progress := float64(p.stats.totalProcessed) / float64(p.stats.totalCustomers)
// 		if progress > 0 {
// 			totalTime := elapsed.Seconds() / progress
// 			remainingTime = time.Duration(totalTime-elapsed.Seconds()) * time.Second
// 		}
// 	}
	
// 	// Get worker pool stats
// 	poolStats := p.workerPool.GetStats()
	
// 	p.logger.Info("Processing progress",
// 		zap.Int("processed", p.stats.totalProcessed),
// 		zap.Int("total", p.stats.totalCustomers),
// 		zap.Float64("progress_percent", float64(p.stats.totalProcessed)/float64(p.stats.totalCustomers)*100),
// 		zap.Duration("elapsed", elapsed),
// 		zap.Duration("estimated_remaining", remainingTime),
// 		zap.Int("success_count", p.stats.successCount),
// 		zap.Int("failed_count", p.stats.failedCount),
// 		zap.Int32("active_workers", poolStats.ActiveWorkers),
// 		zap.Int64("avg_processing_ms", poolStats.AvgProcessingMs),
// 	)
// }

// // saveCheckpoint saves the current processing state
// func (p *RiskProcessor) saveCheckpoint() error {
// 	p.stats.mu.Lock()
// 	defer p.stats.mu.Unlock()
	
// 	// Determine last processed customer ID
// 	lastProcessedID := 0
// 	if len(p.config.CustomerIDs) > 0 && p.stats.totalProcessed > 0 {
// 		if p.stats.totalProcessed <= len(p.config.CustomerIDs) {
// 			lastProcessedID = p.config.CustomerIDs[p.stats.totalProcessed-1]
// 		}
// 	} else {
// 		// No specific customer IDs, use workerPool stats
// 		lastProcessedID = int(p.workerPool.GetStats().LastProcessedID)
// 	}
	
// 	checkpoint := Checkpoint{
// 		Version:           "1.0",
// 		Timestamp:         time.Now(),
// 		LastProcessedID:   lastProcessedID,
// 		TotalProcessed:    int64(p.stats.totalProcessed),
// 		TotalSuccess:      int64(p.stats.successCount),
// 		TotalFailed:       int64(p.stats.failedCount),
// 		BatchNumber:       p.stats.batchCount,
// 		FailedCustomerIDs: p.stats.failedCustomers,
// 	}
	
// 	// Marshal checkpoint to JSON
// 	data, err := json.MarshalIndent(checkpoint, "", "  ")
// 	if err != nil {
// 		return fmt.Errorf("failed to marshal checkpoint: %w", err)
// 	}
	
// 	// Write checkpoint to file
// 	if err := os.WriteFile(p.config.CheckpointFile, data, 0644); err != nil {
// 		return fmt.Errorf("failed to write checkpoint file: %w", err)
// 	}
	
// 	p.logger.Info("Checkpoint saved",
// 		zap.String("file", p.config.CheckpointFile),
// 		zap.Int("last_processed_id", lastProcessedID),
// 		zap.Int("total_processed", p.stats.totalProcessed),
// 	)
	
// 	return nil
// }

// // loadCheckpoint loads the processing state from a checkpoint file
// func (p *RiskProcessor) loadCheckpoint() (Checkpoint, error) {
// 	var checkpoint Checkpoint
	
// 	// Check if checkpoint file exists
// 	if _, err := os.Stat(p.config.CheckpointFile); os.IsNotExist(err) {
// 		return checkpoint, fmt.Errorf("checkpoint file not found: %s", p.config.CheckpointFile)
// 	}
	
// 	// Read checkpoint file
// 	data, err := os.ReadFile(p.config.CheckpointFile)
// 	if err != nil {
// 		return checkpoint, fmt.Errorf("failed to read checkpoint file: %w", err)
// 	}
	
// 	// Unmarshal checkpoint data
// 	if err := json.Unmarshal(data, &checkpoint); err != nil {
// 		return checkpoint, fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
// 	}
	
// 	p.logger.Info("Checkpoint loaded",
// 		zap.String("file", p.config.CheckpointFile),
// 		zap.Int("last_processed_id", checkpoint.LastProcessedID),
// 		zap.Int64("total_processed", checkpoint.TotalProcessed),
// 		zap.Time("timestamp", checkpoint.Timestamp),
// 	)
	
// 	return checkpoint, nil
// }

// // GetStats returns the current processing statistics
// func (p *RiskProcessor) GetStats() struct {
// 	TotalCustomers int
// 	TotalProcessed int
// 	SuccessCount   int
// 	FailedCount    int
// 	FailedCustomers []int
// 	BatchCount     int
// 	Duration       time.Duration
// } {
// 	p.stats.mu.Lock()
// 	defer p.stats.mu.Unlock()
	
// 	var endTime time.Time
// 	if p.stats.endTime.IsZero() {
// 		endTime = time.Now()
// 	} else {
// 		endTime = p.stats.endTime
// 	}
	
// 	return struct {
// 		TotalCustomers int
// 		TotalProcessed int
// 		SuccessCount   int
// 		FailedCount    int
// 		FailedCustomers []int
// 		BatchCount     int
// 		Duration       time.Duration
// 	}{
// 		TotalCustomers: p.stats.totalCustomers,
// 		TotalProcessed: p.stats.totalProcessed,
// 		SuccessCount:   p.stats.successCount,
// 		FailedCount:    p.stats.failedCount,
// 		FailedCustomers: p.stats.failedCustomers,
// 		BatchCount:     p.stats.batchCount,
// 		Duration:       endTime.Sub(p.stats.startTime),
// 	}
// }
