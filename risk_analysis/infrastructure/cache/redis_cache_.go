// infrastructure/cache/redis_cache.go
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// RedisCustomerIDCache manages customer IDs with Redis-based caching
type RedisCustomerIDCache struct {
	redisClient    *redis.Client
	db             *pgxpool.Pool
	logger         *zap.Logger
	dbName         string // Database name used as key prefix
	mu             sync.RWMutex
	customerIDs    []int
	processedIDs   map[int]bool
	totalCount     int
	lastTotalCount int
}

// NewRedisCustomerIDCache creates a new Redis-based customer ID cache
func NewRedisCustomerIDCache(redisClient *redis.Client, db *pgxpool.Pool, dbName string, logger *zap.Logger) *RedisCustomerIDCache {
	return &RedisCustomerIDCache{
		redisClient:  redisClient,
		db:           db,
		dbName:       dbName,
		logger:       logger,
		processedIDs: make(map[int]bool),
	}
}

// getKey generates a Redis key with database name prefix
func (c *RedisCustomerIDCache) getKey(suffix string) string {
	return fmt.Sprintf("%s_%s", c.dbName, suffix)
}

// LoadOrRefresh loads customer IDs from Redis cache or refreshes from database
func (c *RedisCustomerIDCache) LoadOrRefresh(ctx context.Context) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First check if Redis cache exists
	metadataKey := c.getKey("customer_ids_metadata")
	metadataJSON, err := c.redisClient.Get(ctx, metadataKey).Result()

	if err == nil {
		// Cache exists, check if valid
		var metadata CustomerIDCacheMetadata
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err == nil {
			// Only hit the database to verify count if cache is older than 1 hour
			cacheAge := time.Now().Unix() - metadata.LastUpdated
			if cacheAge < 3600 { // Cache is fresh (less than 1 hour old)
				c.logger.Info("Loading customer IDs from Redis cache (no DB check needed)",
					zap.String("db", c.dbName),
					zap.Int("count", metadata.TotalCount),
					zap.Int64("cache_age_seconds", cacheAge),
				)
				c.totalCount = metadata.TotalCount
				return c.loadFromRedis(ctx)
			}

			// Cache is older than 1 hour, verify count with database
			var currentCount int
			err := c.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner").Scan(&currentCount)
			if err != nil {
				// If DB query fails, use cached data anyway
				c.logger.Warn("Failed to verify count from database, using cached data",
					zap.String("db", c.dbName),
					zap.Error(err),
				)
				c.totalCount = metadata.TotalCount
				return c.loadFromRedis(ctx)
			}

			c.totalCount = currentCount

			if metadata.TotalCount == currentCount {
				c.logger.Info("Redis cache validated with database",
					zap.String("db", c.dbName),
					zap.Int("count", currentCount),
				)
				return c.loadFromRedis(ctx)
			}

			c.logger.Info("Redis cache outdated (count mismatch), refreshing from database",
				zap.String("db", c.dbName),
				zap.Int("cached_count", metadata.TotalCount),
				zap.Int("current_count", currentCount),
			)
		}
	} else if err != redis.Nil {
		c.logger.Warn("Failed to load Redis cache metadata", zap.Error(err))
	}

	// Cache miss or outdated - get count from database
	var currentCount int
	err = c.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner").Scan(&currentCount)
	if err != nil {
		return nil, fmt.Errorf("failed to count customers: %w", err)
	}
	c.totalCount = currentCount

	c.logger.Info("Redis cache miss, loading from database",
		zap.String("db", c.dbName),
		zap.Int("current_count", currentCount),
	)

	customerIDs, err := c.loadFromDatabase(ctx)
	if err != nil {
		return nil, err
	}

	// Save to Redis cache
	if err := c.saveToRedis(ctx, customerIDs); err != nil {
		c.logger.Warn("Failed to save to Redis cache", zap.Error(err))
		// Continue anyway with in-memory IDs
	}

	c.customerIDs = customerIDs
	return customerIDs, nil
}

// LoadProcessedCustomers loads the set of already processed customers from Redis
func (c *RedisCustomerIDCache) LoadProcessedCustomers(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	processedKey := c.getKey("processed_customers")

	// Use SMEMBERS to get all processed customer IDs from Redis Set
	processedIDs, err := c.redisClient.SMembers(ctx, processedKey).Result()
	if err != nil {
		if err == redis.Nil {
			c.logger.Info("No processed customers found in Redis, starting fresh",
				zap.String("db", c.dbName),
			)
			return nil
		}
		return fmt.Errorf("failed to load processed customers from Redis: %w", err)
	}

	count := 0
	for _, idStr := range processedIDs {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			c.logger.Warn("Invalid customer ID in Redis processed set",
				zap.String("id", idStr),
				zap.String("db", c.dbName),
			)
			continue
		}
		c.processedIDs[id] = true
		count++
	}

	c.logger.Info("Loaded processed customers from Redis",
		zap.Int("count", count),
		zap.String("db", c.dbName),
	)

	return nil
}

// MarkProcessed marks a customer as processed in Redis
func (c *RedisCustomerIDCache) MarkProcessed(ctx context.Context, customerID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.processedIDs[customerID] {
		return nil // Already marked
	}

	c.processedIDs[customerID] = true

	// Add to Redis Set
	processedKey := c.getKey("processed_customers")
	err := c.redisClient.SAdd(ctx, processedKey, customerID).Err()
	if err != nil {
		return fmt.Errorf("failed to mark customer as processed in Redis: %w", err)
	}

	return nil
}

// MarkBatchProcessed marks multiple customers as processed in Redis
func (c *RedisCustomerIDCache) MarkBatchProcessed(ctx context.Context, customerIDs []int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	processedKey := c.getKey("processed_customers")

	// Prepare batch of IDs to add
	toAdd := make([]interface{}, 0, len(customerIDs))
	for _, id := range customerIDs {
		if !c.processedIDs[id] {
			c.processedIDs[id] = true
			toAdd = append(toAdd, id)
		}
	}

	if len(toAdd) == 0 {
		return nil
	}

	// Batch add to Redis Set
	err := c.redisClient.SAdd(ctx, processedKey, toAdd...).Err()
	if err != nil {
		return fmt.Errorf("failed to mark batch as processed in Redis: %w", err)
	}

	return nil
}

// IsProcessed checks if a customer has been processed
func (c *RedisCustomerIDCache) IsProcessed(customerID int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.processedIDs[customerID]
}

// GetUnprocessedCustomers returns only customers that haven't been processed
func (c *RedisCustomerIDCache) GetUnprocessedCustomers(allCustomers []int) []int {
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
func (c *RedisCustomerIDCache) GetProcessedCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.processedIDs)
}

// loadFromRedis loads customer IDs from Redis cache
func (c *RedisCustomerIDCache) loadFromRedis(ctx context.Context) ([]int, error) {
	customerIDsKey := c.getKey("customer_ids")

	// Get all customer IDs from Redis List
	customerIDStrs, err := c.redisClient.LRange(ctx, customerIDsKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load customer IDs from Redis: %w", err)
	}

	customerIDs := make([]int, 0, len(customerIDStrs))
	for _, idStr := range customerIDStrs {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid customer ID in Redis cache: %s", idStr)
		}
		customerIDs = append(customerIDs, id)
	}

	return customerIDs, nil
}

// loadFromDatabase loads customer IDs from database
func (c *RedisCustomerIDCache) loadFromDatabase(ctx context.Context) ([]int, error) {
	c.logger.Info("Loading customer IDs from database", zap.String("db", c.dbName))

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
		zap.String("db", c.dbName),
	)

	return customerIDs, nil
}

// saveToRedis saves customer IDs to Redis cache
func (c *RedisCustomerIDCache) saveToRedis(ctx context.Context, customerIDs []int) error {
	c.logger.Info("Saving customer IDs to Redis cache",
		zap.String("db", c.dbName),
		zap.Int("count", len(customerIDs)),
	)

	customerIDsKey := c.getKey("customer_ids")

	// Use pipeline for efficiency
	pipe := c.redisClient.Pipeline()

	// Delete existing list
	pipe.Del(ctx, customerIDsKey)

	// Add all customer IDs to Redis List in batches
	batchSize := 1000
	for i := 0; i < len(customerIDs); i += batchSize {
		end := i + batchSize
		if end > len(customerIDs) {
			end = len(customerIDs)
		}

		batch := customerIDs[i:end]
		values := make([]interface{}, len(batch))
		for j, id := range batch {
			values[j] = id
		}
		pipe.RPush(ctx, customerIDsKey, values...)
	}

	// Save metadata
	metadata := CustomerIDCacheMetadata{
		TotalCount:     c.totalCount,
		CustomerCount:  len(customerIDs),
		ProcessedCount: len(c.processedIDs),
		LastUpdated:    time.Now().Unix(),
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metadataKey := c.getKey("customer_ids_metadata")
	pipe.Set(ctx, metadataKey, metadataJSON, 0) // 0 = no expiration

	// Execute pipeline
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to save customer IDs to Redis: %w", err)
	}

	c.logger.Info("Customer IDs saved to Redis successfully", zap.String("db", c.dbName))
	return nil
}

// ClearProcessedCustomers clears all processed customers from Redis (useful for fresh runs)
func (c *RedisCustomerIDCache) ClearProcessedCustomers(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	processedKey := c.getKey("processed_customers")
	err := c.redisClient.Del(ctx, processedKey).Err()
	if err != nil {
		return fmt.Errorf("failed to clear processed customers from Redis: %w", err)
	}

	c.processedIDs = make(map[int]bool)
	c.logger.Info("Cleared processed customers from Redis", zap.String("db", c.dbName))
	return nil
}

// RedisCacheClient provides a centralized Redis client for all cache operations
type RedisCacheClient struct {
	client *redis.Client
	dbName string
	logger *zap.Logger
}

// NewRedisCacheClient creates a new Redis cache client
func NewRedisCacheClient(host string, port int, password string, db int, poolSize int, dbName string, logger *zap.Logger) (*RedisCacheClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: password,
		DB:       db,
		PoolSize: poolSize,
		// DisableIdentity: true,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	logger.Info("Connected to Redis successfully",
		zap.String("host", host),
		zap.Int("port", port),
		zap.String("db_name", dbName),
	)

	return &RedisCacheClient{
		client: client,
		dbName: dbName,
		logger: logger,
	}, nil
}

// GetClient returns the underlying Redis client
func (r *RedisCacheClient) GetClient() *redis.Client {
	return r.client
}

// Close closes the Redis connection
func (r *RedisCacheClient) Close() error {
	return r.client.Close()
}

// getKey generates a Redis key with database name prefix
func (r *RedisCacheClient) getKey(suffix string) string {
	return fmt.Sprintf("%s_%s", r.dbName, suffix)
}

// SaveJSON saves a JSON-serializable object to Redis with the given key suffix
func (r *RedisCacheClient) SaveJSON(ctx context.Context, keySuffix string, data interface{}) error {
	key := r.getKey(keySuffix)

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	err = r.client.Set(ctx, key, jsonData, 0).Err() // 0 = no expiration (persistent)
	if err != nil {
		return fmt.Errorf("failed to save JSON to Redis: %w", err)
	}

	return nil
}

// LoadJSON loads a JSON object from Redis with the given key suffix
func (r *RedisCacheClient) LoadJSON(ctx context.Context, keySuffix string, target interface{}) error {
	key := r.getKey(keySuffix)

	jsonData, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("key not found: %s", key)
		}
		return fmt.Errorf("failed to load JSON from Redis: %w", err)
	}

	err = json.Unmarshal([]byte(jsonData), target)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return nil
}

// Exists checks if a key exists in Redis
func (r *RedisCacheClient) Exists(ctx context.Context, keySuffix string) (bool, error) {
	key := r.getKey(keySuffix)

	count, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check key existence: %w", err)
	}

	return count > 0, nil
}

// Delete deletes a key from Redis
func (r *RedisCacheClient) Delete(ctx context.Context, keySuffix string) error {
	key := r.getKey(keySuffix)

	err := r.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to delete key from Redis: %w", err)
	}

	return nil
}

// SaveStringList saves a list of strings to Redis
func (r *RedisCacheClient) SaveStringList(ctx context.Context, keySuffix string, items []string) error {
	key := r.getKey(keySuffix)

	pipe := r.client.Pipeline()
	pipe.Del(ctx, key)

	if len(items) > 0 {
		values := make([]interface{}, len(items))
		for i, item := range items {
			values[i] = item
		}
		pipe.RPush(ctx, key, values...)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to save string list to Redis: %w", err)
	}

	return nil
}

// LoadStringList loads a list of strings from Redis
func (r *RedisCacheClient) LoadStringList(ctx context.Context, keySuffix string) ([]string, error) {
	key := r.getKey(keySuffix)

	items, err := r.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load string list from Redis: %w", err)
	}

	return items, nil
}

// GetDBName returns the database name used for key prefixing
func (r *RedisCacheClient) GetDBName() string {
	return r.dbName
}

// ClearAll removes all keys with this database prefix (USE WITH CAUTION)
func (r *RedisCacheClient) ClearAll(ctx context.Context) error {
	pattern := fmt.Sprintf("%s_*", r.dbName)

	iter := r.client.Scan(ctx, 0, pattern, 0).Iterator()
	keys := []string{}

	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan keys: %w", err)
	}

	if len(keys) > 0 {
		err := r.client.Del(ctx, keys...).Err()
		if err != nil {
			return fmt.Errorf("failed to delete keys: %w", err)
		}
		r.logger.Info("Cleared all Redis cache keys",
			zap.String("db_name", r.dbName),
			zap.Int("count", len(keys)),
		)
	}

	return nil
}

// GetAllKeys returns all keys with this database prefix (for debugging)
func (r *RedisCacheClient) GetAllKeys(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf("%s_*", r.dbName)

	iter := r.client.Scan(ctx, 0, pattern, 0).Iterator()
	keys := []string{}

	for iter.Next(ctx) {
		// Strip the prefix for cleaner output
		key := iter.Val()
		suffix := strings.TrimPrefix(key, r.dbName+"_")
		keys = append(keys, suffix)
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan keys: %w", err)
	}

	return keys, nil
}
