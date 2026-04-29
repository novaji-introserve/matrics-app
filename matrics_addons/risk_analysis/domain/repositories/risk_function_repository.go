package repositories

import (
	"context"

	"github.com/jackc/pgx/v5"
	"risk_analysis/domain/models"
)

// RiskFunctionRepository defines the contract for calling database check functions
// This interface follows the Dependency Inversion Principle (DIP) - high-level modules
// depend on this abstraction, not on concrete implementations
type RiskFunctionRepository interface {
	// CallCheckFunction calls a specific check_* function for a customer
	// Parameters:
	//   - ctx: context for cancellation and timeout
	//   - tx: database transaction
	//   - functionName: name of the check function (e.g., "check_cust_pep")
	//   - partnerID: customer ID to check
	// Returns:
	//   - *models.RiskFunctionResult: result with matches and scores
	//   - error: any error that occurred
	CallCheckFunction(ctx context.Context, tx pgx.Tx, functionName string, partnerID int) (*models.RiskFunctionResult, error)

	// CallAllCheckFunctions calls all check_* functions for a customer
	// This is more efficient than calling functions individually as it can batch operations
	// Parameters:
	//   - ctx: context for cancellation and timeout
	//   - tx: database transaction
	//   - partnerID: customer ID to check
	// Returns:
	//   - []*models.RiskFunctionResult: array of results from all functions
	//   - error: any error that occurred during discovery or execution
	CallAllCheckFunctions(ctx context.Context, tx pgx.Tx, partnerID int) ([]*models.RiskFunctionResult, error)

	// GetAvailableFunctions returns a list of all check_* functions in the database
	// This allows dynamic discovery of new check functions without code changes
	// Parameters:
	//   - ctx: context for cancellation and timeout
	// Returns:
	//   - []string: array of function names (e.g., ["check_cust_pep", "check_jurisdiction"])
	//   - error: any error that occurred during discovery
	GetAvailableFunctions(ctx context.Context) ([]string, error)
}
