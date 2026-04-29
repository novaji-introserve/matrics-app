// infrastructure/repository/customer_repo.go
package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"risk_analysis/domain/models"
)

// CustomerRepository handles database operations for customers
type CustomerRepository struct {
	db     *pgxpool.Pool
	logger *zap.Logger
}

// NewCustomerRepository creates a new customer repository
func NewCustomerRepository(db *pgxpool.Pool, logger *zap.Logger) *CustomerRepository {
	return &CustomerRepository{
		db:     db,
		logger: logger,
	}
}

// GetCustomerByID retrieves a customer by ID
func (r *CustomerRepository) GetCustomerByID(ctx context.Context, customerID int) (*models.Customer, error) {
	customer := &models.Customer{}
	
	err := r.db.QueryRow(ctx, `
		SELECT id, risk_score, risk_level, composite_risk_score, branch_id
		FROM res_partner
		WHERE id = $1
	`, customerID).Scan(
		&customer.ID,
		&customer.RiskScore,
		&customer.RiskLevel,
		&customer.CompositeRiskScore,
		&customer.BranchID,
	)
	
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("customer with ID %d not found", customerID)
		}
		return nil, fmt.Errorf("error retrieving customer: %w", err)
	}
	
	return customer, nil
}

// GetAllCustomerIDs retrieves all customer IDs
func (r *CustomerRepository) GetAllCustomerIDs(ctx context.Context, batchSize int) ([]int, error) {
	var customerIDs []int
	
	// Count total customers
	var totalCustomers int
	err := r.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner").Scan(&totalCustomers)
	if err != nil {
		return nil, fmt.Errorf("failed to count customers: %w", err)
	}
	
	r.logger.Info("Total customers in database", zap.Int("count", totalCustomers))
	
	// Pre-allocate slice for all customer IDs
	customerIDs = make([]int, 0, totalCustomers)
	
	// Process in batches to avoid loading all IDs at once
	for offset := 0; offset < totalCustomers; offset += batchSize {
		// Use limit and offset to get a batch of customer IDs
		rows, err := r.db.Query(ctx, 
			"SELECT id FROM res_partner ORDER BY id LIMIT $1 OFFSET $2", 
			batchSize, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to query customer IDs: %w", err)
		}
		
		// Scan customer IDs from rows
		batchIDs := make([]int, 0, batchSize)
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan customer ID: %w", err)
			}
			batchIDs = append(batchIDs, id)
		}
		rows.Close()
		
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating customer rows: %w", err)
		}
		
		// Append batch IDs to full list
		customerIDs = append(customerIDs, batchIDs...)
		
		r.logger.Info("Loaded customer ID batch",
			zap.Int("offset", offset),
			zap.Int("batch_size", len(batchIDs)),
			zap.Int("total_loaded", len(customerIDs)),
		)
	}
	
	return customerIDs, nil
}

// GetCustomerIDsAfter retrieves customer IDs after a specific ID
func (r *CustomerRepository) GetCustomerIDsAfter(ctx context.Context, afterID int, batchSize int) ([]int, error) {
	var customerIDs []int
	
	// Count remaining customers
	var remainingCustomers int
	err := r.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner WHERE id > $1", afterID).Scan(&remainingCustomers)
	if err != nil {
		return nil, fmt.Errorf("failed to count remaining customers: %w", err)
	}
	
	r.logger.Info("Remaining customers to process",
		zap.Int("after_id", afterID),
		zap.Int("count", remainingCustomers),
	)
	
	// Pre-allocate slice for customer IDs
	customerIDs = make([]int, 0, remainingCustomers)
	
	// Process in batches to avoid loading all IDs at once
	for offset := 0; offset < remainingCustomers; offset += batchSize {
		// Use limit and offset to get a batch of customer IDs
		rows, err := r.db.Query(ctx, 
			"SELECT id FROM res_partner WHERE id > $1 ORDER BY id LIMIT $2 OFFSET $3", 
			afterID, batchSize, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to query customer IDs: %w", err)
		}
		
		// Scan customer IDs from rows
		batchIDs := make([]int, 0, batchSize)
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan customer ID: %w", err)
			}
			batchIDs = append(batchIDs, id)
		}
		rows.Close()
		
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating customer rows: %w", err)
		}
		
		// Append batch IDs to full list
		customerIDs = append(customerIDs, batchIDs...)
		
		r.logger.Info("Loaded customer ID batch",
			zap.Int("offset", offset),
			zap.Int("batch_size", len(batchIDs)),
			zap.Int("total_loaded", len(customerIDs)),
		)
	}
	
	return customerIDs, nil
}

// UpdateCustomerRisk updates a customer's risk score and level
func (r *CustomerRepository) UpdateCustomerRisk(ctx context.Context, customerID int, riskScore float64, riskLevel string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE res_partner 
		SET risk_score = $1, risk_level = $2 
		WHERE id = $3
	`, riskScore, riskLevel, customerID)
	
	if err != nil {
		return fmt.Errorf("failed to update customer risk: %w", err)
	}
	
	return nil
}

// BatchUpdateCustomerRisk updates multiple customers' risk scores and levels in a single transaction
func (r *CustomerRepository) BatchUpdateCustomerRisk(ctx context.Context, updates map[int]struct {
	RiskScore float64
	RiskLevel string
}) error {
	if len(updates) == 0 {
		return nil
	}
	
	// Start transaction
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	// defer tx.Rollback(ctx)
	defer func() { _ = tx.Rollback(ctx) }()
	
	// Prepare batch
	batch := &pgx.Batch{}
	for customerID, update := range updates {
		batch.Queue(`
			UPDATE res_partner 
			SET risk_score = $1, risk_level = $2 
			WHERE id = $3
		`, update.RiskScore, update.RiskLevel, customerID)
	}
	
	// Execute batch
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	
	// Check for errors
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("failed to update customer (batch index %d): %w", i, err)
		}
	}
	
	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	
	return nil
}

// GetCustomerCount returns the total number of customers
func (r *CustomerRepository) GetCustomerCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRow(ctx, "SELECT COUNT(id) FROM res_partner").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count customers: %w", err)
	}
	return count, nil
}
