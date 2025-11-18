package models

// RiskFunctionResult represents the result from a check_* database function
// Each function returns a JSON object with matched risk criteria and their scores
type RiskFunctionResult struct {
	// FunctionName is the name of the check function (e.g., "check_cust_pep")
	FunctionName string

	// Matches is a map of matched criteria to their risk scores
	// Example: {"cust_pep": 5.2, "high_risk_country": 10.0}
	Matches map[string]float64

	// HasMatch indicates if any criteria matched (Matches is not empty)
	HasMatch bool

	// Error holds any error that occurred during function execution
	Error error
}

// NewRiskFunctionResult creates a new RiskFunctionResult
func NewRiskFunctionResult(functionName string) *RiskFunctionResult {
	return &RiskFunctionResult{
		FunctionName: functionName,
		Matches:      make(map[string]float64),
		HasMatch:     false,
		Error:        nil,
	}
}

// AddMatch adds a matched criterion with its score
func (r *RiskFunctionResult) AddMatch(key string, score float64) {
	r.Matches[key] = score
	r.HasMatch = true
}

// GetScores returns all scores from matched criteria
func (r *RiskFunctionResult) GetScores() []float64 {
	scores := make([]float64, 0, len(r.Matches))
	for _, score := range r.Matches {
		if score > 0 {
			scores = append(scores, score)
		}
	}
	return scores
}

// GetTotalScore returns the sum of all scores
func (r *RiskFunctionResult) GetTotalScore() float64 {
	total := 0.0
	for _, score := range r.Matches {
		total += score
	}
	return total
}

// GetMaxScore returns the maximum score
func (r *RiskFunctionResult) GetMaxScore() float64 {
	maxScore := 0.0
	for _, score := range r.Matches {
		if score > maxScore {
			maxScore = score
		}
	}
	return maxScore
}

// GetAverageScore returns the average of all positive scores
func (r *RiskFunctionResult) GetAverageScore() float64 {
	scores := r.GetScores()
	if len(scores) == 0 {
		return 0.0
	}

	total := 0.0
	for _, score := range scores {
		total += score
	}
	return total / float64(len(scores))
}

// MatchCount returns the number of matched criteria
func (r *RiskFunctionResult) MatchCount() int {
	return len(r.Matches)
}
