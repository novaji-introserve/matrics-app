// infrastructure/cache/file_cache.go
package cache

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// CustomerIDCache manages customer IDs with file-based caching
type CustomerIDCache struct {
	cacheFile      string
	processedFile  string
	db             *pgxpool.Pool
	logger         *zap.Logger
	mu             sync.RWMutex
	customerIDs    []int
	processedIDs   map[int]bool
	totalCount     int
	// lastTotalCount int
}

// CustomerIDCacheMetadata stores cache metadata
type CustomerIDCacheMetadata struct {
	TotalCount     int   `json:"total_count"`
	CustomerCount  int   `json:"customer_count"`
	LastUpdated    int64 `json:"last_updated"`
	ProcessedCount int   `json:"processed_count"`
}

// NewCustomerIDCache creates a new customer ID cache
func NewCustomerIDCache(cacheFile, processedFile string, db *pgxpool.Pool, logger *zap.Logger) *CustomerIDCache {
	return &CustomerIDCache{
		cacheFile:     cacheFile,
		processedFile: processedFile,
		db:            db,
		logger:        logger,
		processedIDs:  make(map[int]bool),
	}
}

// LoadOrRefresh loads customer IDs from cache or refreshes from database
func (c *CustomerIDCache) LoadOrRefresh(ctx context.Context) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get current count from database
	var currentCount int
	err := c.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner").Scan(&currentCount)
	if err != nil {
		return nil, fmt.Errorf("failed to count customers: %w", err)
	}

	c.totalCount = currentCount

	// Check if cache exists and is valid
	if c.cacheExists() {
		metadata, err := c.loadMetadata()
		if err == nil && metadata.TotalCount == currentCount {
			c.logger.Info("Loading customer IDs from cache",
				zap.String("file", c.cacheFile),
				zap.Int("count", currentCount),
			)
			return c.loadFromFile()
		}
	}

	// Cache miss or outdated - load from database
	c.logger.Info("Cache miss or outdated, loading from database",
		zap.Int("current_count", currentCount),
	)

	customerIDs, err := c.loadFromDatabase(ctx)
	if err != nil {
		return nil, err
	}

	// Save to cache
	if err := c.saveToFile(customerIDs); err != nil {
		c.logger.Warn("Failed to save cache", zap.Error(err))
		// Continue anyway with in-memory IDs
	}

	c.customerIDs = customerIDs
	return customerIDs, nil
}

// LoadProcessedCustomers loads the set of already processed customers
func (c *CustomerIDCache) LoadProcessedCustomers() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	file, err := os.Open(c.processedFile)
	if err != nil {
		if os.IsNotExist(err) {
			c.logger.Info("No processed customers file found, starting fresh")
			return nil
		}
		return fmt.Errorf("failed to open processed file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		id, err := strconv.Atoi(line)
		if err != nil {
			c.logger.Warn("Invalid customer ID in processed file", zap.String("line", line))
			continue
		}
		c.processedIDs[id] = true
		count++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading processed file: %w", err)
	}

	c.logger.Info("Loaded processed customers",
		zap.Int("count", count),
		zap.String("file", c.processedFile),
	)

	return nil
}

// MarkProcessed marks a customer as processed
func (c *CustomerIDCache) MarkProcessed(customerID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.processedIDs[customerID] {
		return nil // Already marked
	}

	c.processedIDs[customerID] = true

	// Append to file
	file, err := os.OpenFile(c.processedFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open processed file: %w", err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d\n", customerID)
	return err
}

// MarkBatchProcessed marks multiple customers as processed
func (c *CustomerIDCache) MarkBatchProcessed(customerIDs []int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	file, err := os.OpenFile(c.processedFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open processed file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, id := range customerIDs {
		if !c.processedIDs[id] {
			c.processedIDs[id] = true
			fmt.Fprintf(writer, "%d\n", id)
		}
	}

	return writer.Flush()
}

// IsProcessed checks if a customer has been processed
func (c *CustomerIDCache) IsProcessed(customerID int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.processedIDs[customerID]
}

// GetUnprocessedCustomers returns only customers that haven't been processed
func (c *CustomerIDCache) GetUnprocessedCustomers(allCustomers []int) []int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	unprocessed := make([]int, 0, len(allCustomers))
	for _, id := range allCustomers {
		if !c.processedIDs[id] {
			unprocessed = append(unprocessed, id)
		}
	}

	return unprocessed
}

// GetProcessedCount returns the count of processed customers
func (c *CustomerIDCache) GetProcessedCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.processedIDs)
}

// cacheExists checks if cache file exists
func (c *CustomerIDCache) cacheExists() bool {
	_, err := os.Stat(c.cacheFile)
	return err == nil
}

// loadMetadata loads cache metadata
func (c *CustomerIDCache) loadMetadata() (*CustomerIDCacheMetadata, error) {
	metaFile := c.cacheFile + ".meta"
	data, err := os.ReadFile(metaFile)
	if err != nil {
		return nil, err
	}

	var metadata CustomerIDCacheMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// loadFromFile loads customer IDs from cache file
func (c *CustomerIDCache) loadFromFile() ([]int, error) {
	file, err := os.Open(c.cacheFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open cache file: %w", err)
	}
	defer file.Close()

	var customerIDs []int
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		id, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid customer ID in cache: %s", line)
		}
		customerIDs = append(customerIDs, id)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading cache file: %w", err)
	}

	return customerIDs, nil
}

// loadFromDatabase loads customer IDs from database
func (c *CustomerIDCache) loadFromDatabase(ctx context.Context) ([]int, error) {
	c.logger.Info("Loading customer IDs from database")

	rows, err := c.db.Query(ctx, "SELECT id FROM res_partner ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("failed to query customer IDs: %w", err)
	}
	defer rows.Close()

	customerIDs := make([]int, 0, c.totalCount)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan customer ID: %w", err)
		}
		customerIDs = append(customerIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	c.logger.Info("Loaded customer IDs from database",
		zap.Int("count", len(customerIDs)),
	)

	return customerIDs, nil
}

// saveToFile saves customer IDs to cache file
func (c *CustomerIDCache) saveToFile(customerIDs []int) error {
	c.logger.Info("Saving customer IDs to cache",
		zap.String("file", c.cacheFile),
		zap.Int("count", len(customerIDs)),
	)

	// Write customer IDs
	file, err := os.Create(c.cacheFile)
	if err != nil {
		return fmt.Errorf("failed to create cache file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, id := range customerIDs {
		fmt.Fprintf(writer, "%d\n", id)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to write cache: %w", err)
	}

	// Write metadata
	metadata := CustomerIDCacheMetadata{
		TotalCount:     c.totalCount,
		CustomerCount:  len(customerIDs),
		ProcessedCount: len(c.processedIDs),
	}

	metaData, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metaFile := c.cacheFile + ".meta"
	if err := os.WriteFile(metaFile, metaData, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	c.logger.Info("Cache saved successfully")
	return nil
}
