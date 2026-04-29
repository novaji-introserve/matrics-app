package application

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// Redis key suffix for storing the last known refresh timestamp
	lastRefreshKey = "mv_last_refresh_timestamp"

	// Default polling interval (check every 5 minutes)
	defaultPollingInterval = 5 * time.Minute
)

// MVRefreshMonitor monitors the materialized view refresh timestamp
// and triggers risk analysis processing only when the MV has been refreshed
type MVRefreshMonitor struct {
	db              *pgxpool.Pool
	redisClient     *redis.Client
	dbName          string
	logger          *zap.Logger
	pollingInterval time.Duration

	// Callback receives the previous refresh time so the processor
	// can run incrementally (only changed customers) instead of all customers.
	// since == time.Time{} (zero) on first run → full processing.
	onRefreshCallback func(ctx context.Context, since time.Time) error

	// Control channels
	stopChan chan struct{}
	doneChan chan struct{}
}

// NewMVRefreshMonitor creates a new MV refresh monitor
func NewMVRefreshMonitor(
	db *pgxpool.Pool,
	redisClient *redis.Client,
	dbName string,
	logger *zap.Logger,
	pollingInterval time.Duration,
) *MVRefreshMonitor {
	if pollingInterval == 0 {
		pollingInterval = defaultPollingInterval
	}

	return &MVRefreshMonitor{
		db:              db,
		redisClient:     redisClient,
		dbName:          dbName,
		logger:          logger,
		pollingInterval: pollingInterval,
		stopChan:        make(chan struct{}),
		doneChan:        make(chan struct{}),
	}
}

// SetRefreshCallback sets the callback to execute when MV refresh is detected.
// since is the previous refresh timestamp — pass it to RunIncremental so only
// customers whose data changed after that time are reprocessed.
// On first run since is time.Time{} (zero value) → full processing.
func (m *MVRefreshMonitor) SetRefreshCallback(callback func(ctx context.Context, since time.Time) error) {
	m.onRefreshCallback = callback
}

// Start begins monitoring the MV refresh timestamp
func (m *MVRefreshMonitor) Start(ctx context.Context) {
	m.logger.Info("Starting materialized view refresh monitor",
		zap.Duration("polling_interval", m.pollingInterval),
		zap.String("db_name", m.dbName),
	)

	go m.monitorLoop(ctx)
}

// Stop gracefully stops the monitoring goroutine
func (m *MVRefreshMonitor) Stop() {
	m.logger.Info("Stopping materialized view refresh monitor")
	close(m.stopChan)
	<-m.doneChan
	m.logger.Info("Materialized view refresh monitor stopped")
}

// monitorLoop is the main monitoring loop that runs in a goroutine
func (m *MVRefreshMonitor) monitorLoop(ctx context.Context) {
	defer close(m.doneChan)

	ticker := time.NewTicker(m.pollingInterval)
	defer ticker.Stop()

	// Perform initial check immediately
	if err := m.checkAndProcess(ctx); err != nil {
		m.logger.Error("Initial MV refresh check failed", zap.Error(err))
	}

	for {
		select {
		case <-ticker.C:
			if err := m.checkAndProcess(ctx); err != nil {
				m.logger.Error("MV refresh check failed", zap.Error(err))
			}
		case <-m.stopChan:
			m.logger.Info("Monitor loop received stop signal")
			return
		case <-ctx.Done():
			m.logger.Info("Monitor loop context cancelled")
			return
		}
	}
}

// checkAndProcess checks if the MV has been refreshed and triggers processing if needed
func (m *MVRefreshMonitor) checkAndProcess(ctx context.Context) error {
	// Get the current last_refresh timestamp from the database
	currentRefresh, err := m.getCurrentRefreshTimestamp(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current refresh timestamp: %w", err)
	}

	// If no refresh timestamp exists yet, skip processing
	if currentRefresh == nil {
		m.logger.Info("No refresh timestamp found in risk_analysis table, skipping processing")
		return nil
	}

	// Get the last known refresh timestamp from Redis
	lastKnownRefresh, err := m.getLastKnownRefreshFromRedis(ctx)
	if err != nil {
		m.logger.Warn("Failed to get last known refresh from Redis, treating as first run",
			zap.Error(err),
		)
		// First run - save current timestamp and trigger processing
		if err := m.saveLastRefreshToRedis(ctx, *currentRefresh); err != nil {
			m.logger.Error("Failed to save timestamp to Redis", zap.Error(err))
		}

		// First run — full processing (since = zero time)
		m.logger.Info("First run detected, triggering full risk analysis processing",
			zap.Time("current_refresh", *currentRefresh),
		)
		return m.triggerProcessing(ctx, time.Time{})
	}

	// Compare timestamps
	if currentRefresh.After(lastKnownRefresh) {
		m.logger.Info("Materialized view refresh detected!",
			zap.Time("previous_refresh", lastKnownRefresh),
			zap.Time("current_refresh", *currentRefresh),
			zap.Duration("time_since_last", currentRefresh.Sub(lastKnownRefresh)),
		)

		// Update Redis with new timestamp before triggering so a crash
		// during processing doesn't cause a repeated full-reprocess.
		if err := m.saveLastRefreshToRedis(ctx, *currentRefresh); err != nil {
			m.logger.Error("Failed to update timestamp in Redis", zap.Error(err))
			return err
		}

		// Incremental processing — pass previous refresh as the since boundary
		return m.triggerProcessing(ctx, lastKnownRefresh)
	}

	m.logger.Debug("No MV refresh detected, skipping processing",
		zap.Time("last_refresh", lastKnownRefresh),
		zap.Time("current_refresh", *currentRefresh),
	)

	return nil
}

// getCurrentRefreshTimestamp queries the database for the most recent last_refresh timestamp
func (m *MVRefreshMonitor) getCurrentRefreshTimestamp(ctx context.Context) (*time.Time, error) {
	query := `SELECT last_refresh FROM risk_analysis ORDER BY last_refresh DESC LIMIT 1`

	var lastRefresh sql.NullTime
	err := m.db.QueryRow(ctx, query).Scan(&lastRefresh)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query last_refresh: %w", err)
	}

	if !lastRefresh.Valid {
		return nil, nil
	}

	return &lastRefresh.Time, nil
}

// getLastKnownRefreshFromRedis retrieves the last known refresh timestamp from Redis
func (m *MVRefreshMonitor) getLastKnownRefreshFromRedis(ctx context.Context) (time.Time, error) {
	key := m.getRedisKey(lastRefreshKey)

	timestampStr, err := m.redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return time.Time{}, fmt.Errorf("no last refresh timestamp in Redis")
		}
		return time.Time{}, fmt.Errorf("failed to get timestamp from Redis: %w", err)
	}

	// Parse timestamp (stored as RFC3339 format)
	timestamp, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp from Redis: %w", err)
	}

	return timestamp, nil
}

// saveLastRefreshToRedis saves the refresh timestamp to Redis
func (m *MVRefreshMonitor) saveLastRefreshToRedis(ctx context.Context, timestamp time.Time) error {
	key := m.getRedisKey(lastRefreshKey)

	// Store timestamp as RFC3339 string for easy parsing
	timestampStr := timestamp.Format(time.RFC3339)

	err := m.redisClient.Set(ctx, key, timestampStr, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to save timestamp to Redis: %w", err)
	}

	m.logger.Debug("Saved refresh timestamp to Redis",
		zap.Time("timestamp", timestamp),
		zap.String("key", key),
	)

	return nil
}

// triggerProcessing executes the callback function to trigger risk analysis
func (m *MVRefreshMonitor) triggerProcessing(ctx context.Context, since time.Time) error {
	if m.onRefreshCallback == nil {
		m.logger.Warn("No refresh callback set, skipping processing trigger")
		return nil
	}

	logFields := []zap.Field{}
	if since.IsZero() {
		logFields = append(logFields, zap.String("mode", "full"))
	} else {
		logFields = append(logFields,
			zap.String("mode", "incremental"),
			zap.Time("since", since),
		)
	}

	m.logger.Info("Triggering risk analysis processing...", logFields...)
	startTime := time.Now()

	err := m.onRefreshCallback(ctx, since)

	duration := time.Since(startTime)
	if err != nil {
		m.logger.Error("Risk analysis processing failed",
			zap.Error(err),
			zap.Duration("duration", duration),
		)
		return err
	}

	m.logger.Info("Risk analysis processing completed successfully",
		zap.Duration("duration", duration),
	)

	return nil
}

// getRedisKey generates a Redis key with database name prefix
func (m *MVRefreshMonitor) getRedisKey(suffix string) string {
	return fmt.Sprintf("%s_%s", m.dbName, suffix)
}

// GetCurrentStatus returns the current monitoring status (for debugging/health checks)
func (m *MVRefreshMonitor) GetCurrentStatus(ctx context.Context) (map[string]interface{}, error) {
	currentRefresh, err := m.getCurrentRefreshTimestamp(ctx)
	if err != nil {
		return nil, err
	}

	var currentRefreshStr string
	if currentRefresh != nil {
		currentRefreshStr = currentRefresh.Format(time.RFC3339)
	}

	lastKnownRefresh, err := m.getLastKnownRefreshFromRedis(ctx)
	var lastKnownRefreshStr string
	if err == nil {
		lastKnownRefreshStr = lastKnownRefresh.Format(time.RFC3339)
	}

	status := map[string]interface{}{
		"db_name":             m.dbName,
		"polling_interval":    m.pollingInterval.String(),
		"current_refresh":     currentRefreshStr,
		"last_known_refresh":  lastKnownRefreshStr,
		"callback_configured": m.onRefreshCallback != nil,
	}

	if currentRefresh != nil && lastKnownRefreshStr != "" {
		status["refresh_detected"] = currentRefresh.After(lastKnownRefresh)
	}

	return status, nil
}
