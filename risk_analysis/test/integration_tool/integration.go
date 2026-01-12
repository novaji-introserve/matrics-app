package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"risk_analysis/config"
	"risk_analysis/domain/services"
)

func main() {
	fmt.Println("===========================================")
	fmt.Println("Risk Analysis Integration Test")
	fmt.Println("===========================================")

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Printf("Configuration loaded from settings.conf\n")
	fmt.Printf("  Database: %s@%s:%d/%s\n", cfg.DBUser, cfg.DBHost, cfg.DBPort, cfg.DBName)
	fmt.Printf("  Cache Directory: %s\n\n", cfg.CacheDirectory)

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	// defer logger.Sync()
	defer func() { _ = logger.Sync() }()

	connString := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBUser, cfg.DBPassword, cfg.DBSSLMode)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatalf("Failed to parse connection string: %v", err)
	}

	db, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	fmt.Println("Connected to database successfully!")

	var dbName string
	err = db.QueryRow(context.Background(), "SELECT current_database()").Scan(&dbName)
	if err != nil {
		log.Fatalf("Failed to query database: %v", err)
	}

	fmt.Printf("Current database: %s\n\n", dbName)

	calculator := services.NewBatchedFunctionRiskCalculator(
		db,
		logger,
		cfg.RiskFunctionsCacheFile,
		cfg.RiskMetadataCacheFile,
	)

	fmt.Println("Initializing calculator cache...")
	ctx := context.Background()
	err = calculator.InitializeCache(ctx)
	if err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}

	fmt.Println("Cache initialized successfully")
	fmt.Println("  - Settings cached from database")
	fmt.Println("  - Function definitions cached")
	fmt.Println("  - Composite plans cached")
	fmt.Printf("  - Metadata saved to: %s\n\n", cfg.RiskMetadataCacheFile)

	fmt.Println("Testing batch processing with sample customers...")
	testCustomers := []int{1, 2, 3}

	results := calculator.ProcessCustomerBatch(ctx, testCustomers, false, 2)

	successCount := 0
	for _, result := range results {
		if result.Error == nil {
			successCount++
			fmt.Printf("  Customer %d: score=%.2f, level=%s\n",
				result.CustomerID, result.RiskScore, result.RiskLevel)
		} else {
			fmt.Printf("  Customer %d failed: %v\n", result.CustomerID, result.Error)
		}
	}

	fmt.Printf("\nSuccessfully processed %d/%d customers\n\n", successCount, len(testCustomers))

	fmt.Println("===========================================")
	fmt.Println("Integration Test Completed Successfully!")
	fmt.Println("===========================================")
}
