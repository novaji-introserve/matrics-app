package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/ini.v1"
)

// Config holds application configuration for operational settings
// Note: Business rules (thresholds, computation methods) are NOT stored here
// as they are read directly from the database at runtime
type Config struct {
	// Database settings
	DBHost     string
	DBPort     int
	DBName     string
	DBUser     string
	DBPassword string
	DBSSLMode  string

	// Connection pool settings
	DBPoolMin         int
	DBPoolMax         int
	DBPoolMaxIdleTime int
	DBPoolMaxLifetime int
	DBConnectTimeout  int
	DBQueryTimeout    int

	// Processing settings
	BatchSize                  int
	WorkerCount                int
	WorkersPerBatch            int
	ChunkSize                  int
	EnableBulkOperations       bool
	BulkInsertBatchSize        int
	ProgressCheckpointInterval int

	// Cache settings
	CacheDirectory           string
	CustomerIDCacheFile      string
	ProcessedCustomersFile   string
	RiskFunctionsCacheFile   string
	RiskMetadataCacheFile    string

	// Redis cache settings
	RedisEnabled  bool
	RedisHost     string
	RedisPort     int
	RedisPassword string
	RedisDB       int
	RedisPoolSize int

	// Logging settings (for CLI/processor)
	LogLevel      string
	LogFormat     string
	LogOutput     string
	LogFile       string
	LogMaxSize    int
	LogMaxBackups int
	LogMaxAge     int

	// API Logging settings (separate from CLI)
	APILogLevel      string
	APILogFormat     string
	APILogOutput     string
	APILogFile       string
	APILogMaxSize    int
	APILogMaxBackups int
	APILogMaxAge     int

	// Retry settings
	MaxRetries          int
	RetryInitialInterval int
	RetryMaxInterval    int
	RetryMultiplier     int

	// Execution control
	DryRun              bool
	ResumeFromCheckpoint bool
	CheckpointFile       string
	CustomerIDs          []int // Optional specific customer IDs to process

	// API settings
	APIPort string
	APIHost string
}

// LoadConfig loads configuration from config.conf file
func LoadConfig() (*Config, error) {
	// Try to load settings.conf file
	CONFIGPATH := "/data/odoo/ETL_script/update_script/settings.conf" //--sterling-bank-config-path
	// CONFIGPATH := "/data/Altbank/ETL_script/update_script/settings.conf" //--altbank-config-path
	if _, err := os.Stat(CONFIGPATH); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", CONFIGPATH)
	}

	cfg, err := ini.Load(CONFIGPATH)
	if err != nil {
		return nil, fmt.Errorf("failed to load config file: %w", err)
	}

	// Get database section
	dbSection := cfg.Section("database")

	// Get risk_analysis section
	riskSection := cfg.Section("risk_analysis")

	// Get api section
	apiSection := cfg.Section("api")

	// Get redis section
	redisSection := cfg.Section("redis")

	// Get api.logging section (optional, falls back to risk_analysis logging)
	apiLoggingSection := cfg.Section("api.logging")

	config := &Config{
		// Database settings from [database] section
		DBHost:     dbSection.Key("host").MustString("localhost"),
		DBPort:     dbSection.Key("port").MustInt(5432),
		DBName:     dbSection.Key("dbname").String(),
		DBUser:     dbSection.Key("user").String(),
		DBPassword: dbSection.Key("password").String(),
		DBSSLMode:  dbSection.Key("ssl_mode").MustString("require"),

		// Connection pool settings from [database] section
		DBPoolMin:         dbSection.Key("pool_min").MustInt(10),
		DBPoolMax:         dbSection.Key("pool_max").MustInt(50),
		DBPoolMaxIdleTime: dbSection.Key("pool_max_idle_time").MustInt(300),
		DBPoolMaxLifetime: dbSection.Key("pool_max_lifetime").MustInt(3600),
		DBConnectTimeout:  dbSection.Key("connect_timeout").MustInt(10),
		DBQueryTimeout:    dbSection.Key("query_timeout").MustInt(30),

		// Processing settings from [risk_analysis] section
		BatchSize:                  riskSection.Key("batch_size").MustInt(1000),
		WorkerCount:                riskSection.Key("worker_count").MustInt(20),
		WorkersPerBatch:            riskSection.Key("workers_per_batch").MustInt(2),
		ChunkSize:                  riskSection.Key("chunk_size").MustInt(10000),
		EnableBulkOperations:       riskSection.Key("enable_bulk_operations").MustBool(true),
		BulkInsertBatchSize:        riskSection.Key("bulk_insert_batch_size").MustInt(500),
		ProgressCheckpointInterval: riskSection.Key("progress_checkpoint_interval").MustInt(10000),

		// Cache settings from [risk_analysis] section
		CacheDirectory:         riskSection.Key("cache_directory").MustString("/tmp"),
		CustomerIDCacheFile:    riskSection.Key("customer_id_cache_file").MustString("/tmp/customer_ids.cache"),
		ProcessedCustomersFile: riskSection.Key("processed_customers_file").MustString("/tmp/processed_customers.txt"),
		RiskFunctionsCacheFile: riskSection.Key("risk_functions_cache_file").MustString("/tmp/risk_functions.json"),
		RiskMetadataCacheFile:  riskSection.Key("risk_metadata_cache_file").MustString("/tmp/risk_calculator_metadata.json"),

		// Logging settings from [risk_analysis] section
		LogLevel:      riskSection.Key("log_level").MustString("INFO"),
		LogFormat:     riskSection.Key("log_format").MustString("json"),
		LogOutput:     riskSection.Key("log_output").MustString("stdout"),
		LogFile:       riskSection.Key("log_file").MustString("/var/log/risk-processor.log"),
		LogMaxSize:    riskSection.Key("log_max_size").MustInt(100),
		LogMaxBackups: riskSection.Key("log_max_backups").MustInt(5),
		LogMaxAge:     riskSection.Key("log_max_age").MustInt(30),

		// Retry settings from [risk_analysis] section
		MaxRetries:           riskSection.Key("max_retries").MustInt(3),
		RetryInitialInterval: riskSection.Key("retry_initial_interval").MustInt(1),
		RetryMaxInterval:     riskSection.Key("retry_max_interval").MustInt(10),
		RetryMultiplier:      riskSection.Key("retry_multiplier").MustInt(2),

		// Execution control from [risk_analysis] section
		DryRun:               riskSection.Key("dry_run").MustBool(false),
		ResumeFromCheckpoint: riskSection.Key("resume_from_checkpoint").MustBool(false),
		CheckpointFile:       riskSection.Key("checkpoint_file").MustString("/tmp/risk-processor-checkpoint.json"),

		// API settings
		APIPort: apiSection.Key("port").MustString("8080"),
		APIHost: apiSection.Key("host").MustString("0.0.0.0"),

		// Redis cache settings from [redis] section
		RedisEnabled:  redisSection.Key("enabled").MustBool(false),
		RedisHost:     redisSection.Key("host").MustString("localhost"),
		RedisPort:     redisSection.Key("port").MustInt(6379),
		RedisPassword: redisSection.Key("password").MustString(""),
		RedisDB:       redisSection.Key("db").MustInt(0),
		RedisPoolSize: redisSection.Key("pool_size").MustInt(10),

		// API Logging settings from [api.logging] section (falls back to [risk_analysis])
		APILogLevel:      apiLoggingSection.Key("log_level").MustString(riskSection.Key("log_level").MustString("INFO")),
		APILogFormat:     apiLoggingSection.Key("log_format").MustString(riskSection.Key("log_format").MustString("json")),
		APILogOutput:     apiLoggingSection.Key("log_output").MustString(riskSection.Key("log_output").MustString("stdout")),
		APILogFile:       apiLoggingSection.Key("log_file").MustString("/var/log/risk-api-server.log"),
		APILogMaxSize:    apiLoggingSection.Key("log_max_size").MustInt(riskSection.Key("log_max_size").MustInt(100)),
		APILogMaxBackups: apiLoggingSection.Key("log_max_backups").MustInt(riskSection.Key("log_max_backups").MustInt(5)),
		APILogMaxAge:     apiLoggingSection.Key("log_max_age").MustInt(riskSection.Key("log_max_age").MustInt(30)),
	}

	// Validate required fields
	if config.DBName == "" {
		return nil, fmt.Errorf("database.dbname is required")
	}
	if config.DBUser == "" {
		return nil, fmt.Errorf("database.user is required")
	}
	if config.DBPassword == "" {
		return nil, fmt.Errorf("database.password is required")
	}

	// Parse customer IDs if provided
	customerIDsStr := riskSection.Key("customer_ids").String()
	if customerIDsStr != "" {
		ids, err := parseCustomerIDs(customerIDsStr)
		if err != nil {
			return nil, fmt.Errorf("invalid customer_ids: %w", err)
		}
		config.CustomerIDs = ids
	}

	return config, nil
}

// Helper function to parse comma-separated customer IDs
func parseCustomerIDs(idStr string) ([]int, error) {
	if idStr == "" {
		return nil, nil
	}

	var result []int
	// Split by comma
	parts := strings.Split(idStr, ",")
	for _, part := range parts {
		// Trim whitespace
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		
		// Parse integer
		id, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid customer ID '%s': %w", part, err)
		}
		
		result = append(result, id)
	}
	
	return result, nil
}

