package test

import (
	"context"
	"fmt"
	"os"
	// "path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"gopkg.in/ini.v1"

	"risk_analysis/config"
	"risk_analysis/domain/services"
)

// loadConfigFromParent loads settings.conf from the parent directory
func loadConfigFromParent(t *testing.T) (*config.Config, error) {
	// I commented the following block of code out due to the fact that teh settings.conf on the server does not reside in the risk_analysis parent directory

	// Change to parent directory temporarily
	// wd, err := os.Getwd()
	// if err != nil {
	// 	return nil, err
	// }
	// parent := filepath.Dir(wd)

	// Try to load from parent directory
	// configPath := filepath.Join(parent, "settings.conf")
	CONFIGPATH := "/data/odoo/ETL_script/update_script/settings.conf"
	if _, err := os.Stat(CONFIGPATH); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", CONFIGPATH)
	}

	cfg, err := ini.Load(CONFIGPATH)
	if err != nil {
		return nil, fmt.Errorf("failed to load config file: %w", err)
	}

	dbSection := cfg.Section("database")
	riskSection := cfg.Section("risk_analysis")

	config := &config.Config{
		DBHost:                dbSection.Key("host").MustString("localhost"),
		DBPort:                dbSection.Key("port").MustInt(5432),
		DBName:                dbSection.Key("dbname").String(),
		DBUser:                dbSection.Key("user").String(),
		DBPassword:            dbSection.Key("password").String(),
		DBSSLMode:             dbSection.Key("ssl_mode").MustString("require"),
		DBPoolMin:             dbSection.Key("pool_min").MustInt(10),
		DBPoolMax:             dbSection.Key("pool_max").MustInt(50),
		DBPoolMaxIdleTime:     dbSection.Key("pool_max_idle_time").MustInt(300),
		DBPoolMaxLifetime:     dbSection.Key("pool_max_lifetime").MustInt(3600),
		DBConnectTimeout:      dbSection.Key("connect_timeout").MustInt(10),
		DBQueryTimeout:        dbSection.Key("query_timeout").MustInt(30),
		CacheDirectory:        riskSection.Key("cache_directory").MustString("/tmp"),
		RiskFunctionsCacheFile: riskSection.Key("risk_functions_cache_file").MustString("/tmp/risk_functions.json"),
		RiskMetadataCacheFile:  riskSection.Key("risk_metadata_cache_file").MustString("/tmp/risk_calculator_metadata.json"),
	}

	return config, nil
}

func setupTestDB(t *testing.T) (*pgxpool.Pool, *zap.Logger, *config.Config) {
	cfg, err := loadConfigFromParent(t)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	connString := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBUser, cfg.DBPassword, cfg.DBSSLMode)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Fatalf("Failed to parse connection string: %v", err)
	}

	db, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	return db, logger, cfg
}

func TestBatchedRiskCalculator_IntegrationTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	db, logger, cfg := setupTestDB(t)
	defer db.Close()

	calculator := services.NewBatchedFunctionRiskCalculator(
		db,
		logger,
		cfg.RiskFunctionsCacheFile,
		cfg.RiskMetadataCacheFile,
	)

	ctx := context.Background()
	err := calculator.InitializeCache(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize cache: %v", err)
	}

	t.Logf("Cache initialized successfully")

	customerIDs := []int{1, 2, 3}
	results := calculator.ProcessCustomerBatch(ctx, customerIDs, false, 2)

	successCount := 0
	for _, result := range results {
		if result.Error == nil {
			successCount++
		} else {
			t.Logf("Customer %d failed: %v", result.CustomerID, result.Error)
		}
	}

	t.Logf("Successfully processed batch: %d/%d customers succeeded", successCount, len(customerIDs))
}

func TestBatchedRiskCalculator_CacheInitialization(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	db, logger, cfg := setupTestDB(t)
	defer db.Close()

	calculator := services.NewBatchedFunctionRiskCalculator(
		db,
		logger,
		cfg.RiskFunctionsCacheFile,
		cfg.RiskMetadataCacheFile,
	)

	ctx := context.Background()
	err := calculator.InitializeCache(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize cache: %v", err)
	}

	t.Logf("Cache initialized successfully")
	t.Logf("  - Settings cached")
	t.Logf("  - Functions cached")
	t.Logf("  - Composite plans cached")
}

func TestConfigPaths(t *testing.T) {
	cfg, err := loadConfigFromParent(t)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.CacheDirectory == "" {
		t.Error("CacheDirectory should not be empty")
	}
	if cfg.RiskFunctionsCacheFile == "" {
		t.Error("RiskFunctionsCacheFile should not be empty")
	}
	if cfg.RiskMetadataCacheFile == "" {
		t.Error("RiskMetadataCacheFile should not be empty")
	}

	t.Logf("Cache paths configured:")
	t.Logf("  - Cache Directory: %s", cfg.CacheDirectory)
	t.Logf("  - Functions Cache: %s", cfg.RiskFunctionsCacheFile)
	t.Logf("  - Metadata Cache: %s", cfg.RiskMetadataCacheFile)
}
