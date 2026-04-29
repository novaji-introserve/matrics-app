package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// ConnectionConfig holds database connection configuration
type ConnectionConfig struct {
	Host            string
	Port            int
	Database        string
	User            string
	Password        string
	SSLMode         string
	PoolMinSize     int
	PoolMaxSize     int
	MaxIdleTime     int
	MaxLifetime     int
	ConnectTimeout  int
	QueryTimeout    int
}

// Connection manages database connections
type Connection struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
	config ConnectionConfig
}

// NewConnection creates a new database connection manager
func NewConnection(cfg ConnectionConfig, logger *zap.Logger) *Connection {
	return &Connection{
		config: cfg,
		logger: logger,
	}
}

// Connect establishes a connection pool to the database
func (c *Connection) Connect(ctx context.Context) error {
	// Construct connection string
	connString := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s pool_max_conns=%d pool_min_conns=%d pool_max_conn_idle_time=%ds pool_max_conn_lifetime=%ds connect_timeout=%d",
		c.config.Host,
		c.config.Port,
		c.config.Database,
		c.config.User,
		c.config.Password,
		c.config.SSLMode,
		c.config.PoolMaxSize,
		c.config.PoolMinSize,
		c.config.MaxIdleTime,
		c.config.MaxLifetime,
		c.config.ConnectTimeout,
	)

	// Parse the connection string into a config
	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Create a connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Ping the database to verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	c.pool = pool
	c.logger.Info("Database connection established",
		zap.String("host", c.config.Host),
		zap.Int("port", c.config.Port),
		zap.String("database", c.config.Database),
		zap.Int("pool_size", c.config.PoolMaxSize),
	)

	return nil
}

// GetPool returns the connection pool
func (c *Connection) GetPool() *pgxpool.Pool {
	return c.pool
}

// Close closes the database connection pool
func (c *Connection) Close() {
	if c.pool != nil {
		c.pool.Close()
		c.logger.Info("Database connection pool closed")
	}
}

// WithTimeout returns a context with timeout for database operations
func (c *Connection) WithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := time.Duration(c.config.QueryTimeout) * time.Second
	return context.WithTimeout(ctx, timeout)
}

// GetStats returns connection pool statistics
func (c *Connection) GetStats() pgxpool.Stat {
	if c.pool != nil {
		stat := c.pool.Stat()
		return *stat // Dereference the pointer to get the actual Stat value
	}
	return pgxpool.Stat{}
}
