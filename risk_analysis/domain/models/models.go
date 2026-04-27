// domain/models/models.go
package models

import "time"

// Customer represents a customer record in res_partner table
type Customer struct {
	ID                 int      `db:"id"`
	RiskScore          *float64 `db:"risk_score"` // Using pointer for nullable fields
	RiskLevel          *string  `db:"risk_level"`
	CompositeRiskScore *float64 `db:"composite_risk_score"`
	BranchID           *int     `db:"branch_id"`
}

// RiskPlan represents a risk assessment plan from res_compliance_risk_assessment_plan
type RiskPlan struct {
	ID                     int      `db:"id"`
	Name                   string   `db:"name"`
	State                  string   `db:"state"`
	Priority               int      `db:"priority"`
	ComputeScoreFrom       string   `db:"compute_score_from"`
	SQLQuery               string   `db:"sql_query"`
	RiskScore              float64  `db:"risk_score"`
	RiskAssessmentID       *int     `db:"risk_assessment_id"` // Using pointer for nullable fields
	UseCompositeCalculation bool     `db:"use_composite_calculation"`
	UniverseID             *int     `db:"universe_id"` // Using pointer for nullable fields
}

// RiskPlanLine represents a result of risk plan execution in res_partner_risk_plan_line
type RiskPlanLine struct {
	ID          int     `db:"id"`
	PartnerID   int     `db:"partner_id"`
	PlanLineID  int     `db:"plan_line_id"`
	RiskScore   float64 `db:"risk_score"`
}

// RiskAssessment represents a customer risk assessment in res_risk_assessment
type RiskAssessment struct {
	ID         int       `db:"id"`
	PartnerID  int       `db:"partner_id"`
	RiskRating *float64  `db:"risk_rating"` // Using pointer for nullable fields
	SubjectID  *int      `db:"subject_id"`  // Using pointer for nullable fields
	CreateDate time.Time `db:"create_date"`
}

// EnhancedDueDiligence represents a customer EDD record in res_partner_edd
type EnhancedDueDiligence struct {
	ID            int       `db:"id"`
	CustomerID    int       `db:"customer_id"`
	Status        string    `db:"status"`
	RiskScore     *float64  `db:"risk_score"`    // Using pointer for nullable fields
	DateApproved  *time.Time `db:"date_approved"` // Using pointer for nullable fields
}

// RiskUniverse represents a risk universe in res_risk_universe
type RiskUniverse struct {
	ID                    int     `db:"id"`
	Name                  string  `db:"name"`
	IsIncludedInComposite bool    `db:"is_included_in_composite"`
	WeightPercentage      float64 `db:"weight_percentage"`
}

// CompositePlanLine represents a result of composite plan execution in res_partner_composite_plan_line
type CompositePlanLine struct {
	ID           int     `db:"id"`
	PartnerID    int     `db:"partner_id"`
	PlanID       int     `db:"plan_id"`
	UniverseID   int     `db:"universe_id"`
	SubjectID    *int    `db:"subject_id"`    // Using pointer for nullable fields
	Matched      bool    `db:"matched"`
	RiskScore    float64 `db:"risk_score"`
	AssessmentID *int    `db:"assessment_id"` // Using pointer for nullable fields
}

// Setting represents a configuration setting in res_compliance_settings
type Setting struct {
	ID    int    `db:"id"`
	Code  string `db:"code"`
	Val   string `db:"val"`
}

// Helper types for business logic (not direct DB mappings)

// UniverseScoreData holds data for universe score calculation
type UniverseScoreData struct {
	Universe      RiskUniverse
	TotalScore    float64
	Weight        float64
	Name          string
	SubjectScores map[int]SubjectScoreData
}

// SubjectScoreData holds data for a subject within a universe
type SubjectScoreData struct {
	Subject      interface{}
	Score        float64
	MatchedPlans []int
	Assessment   *RiskAssessment
}
