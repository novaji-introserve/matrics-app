package responses

import (
	"encoding/json"
	"net/http"
	"time"
)

// APIResponse represents the standardized API response structure
type APIResponse struct {
	Status    bool                   `json:"status"`
	Message   string                 `json:"message"`
	Code      int                    `json:"code"` // 0 for success, 1 for failure
	Data      interface{}            `json:"data"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp string                 `json:"timestamp"`
	Path      string                 `json:"path"`
}

// Success sends a successful response
func Success(w http.ResponseWriter, status bool, message string, code int, data interface{}, metadata map[string]interface{}, path string) {
	response := APIResponse{
		Status:    status,
		Message:   message,
		Code:      code,
		Data:      data,
		Metadata:  metadata,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Path:      path,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// Error sends an error response
func Error(w http.ResponseWriter, statusCode int, message string, path string) {
	response := APIResponse{
		Status:    false,
		Message:   message,
		Code:      1,
		Data:      nil,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Path:      path,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}

// ValidationError sends a validation error response
func ValidationError(w http.ResponseWriter, message string, errors map[string]string, path string) {
	response := APIResponse{
		Status:  false,
		Message: message,
		Code:    1,
		Data:    errors,
		Metadata: map[string]interface{}{
			"error_type": "validation_error",
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Path:      path,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(response)
}
