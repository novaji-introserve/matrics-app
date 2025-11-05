.PHONY: all build run clean help

# Variables
BINARY_NAME=risk-processor
BUILD_DIR=./build

# Default target
all: build

# Build the application
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/risk-processor

# Run the application
run: build
	@echo "Running $(BINARY_NAME)..."
	@$(BUILD_DIR)/$(BINARY_NAME)

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)

# Run in dry-run mode
dry-run: build
	@echo "Running in dry-run mode..."
	@$(BUILD_DIR)/$(BINARY_NAME) --dry-run

# Process specific customers
process-customers: build
	@echo "Processing specific customers: $(CUSTOMER_IDS)"
	@$(BUILD_DIR)/$(BINARY_NAME) --customer-ids=$(CUSTOMER_IDS)

# Resume from checkpoint
resume: build
	@echo "Resuming from checkpoint..."
	@$(BUILD_DIR)/$(BINARY_NAME) --resume-from-checkpoint

# Help command
help:
	@echo "High-Performance Customer Risk Analysis System"
	@echo ""
	@echo "Usage:"
	@echo "  make [command]"
	@echo ""
	@echo "Available Commands:"
	@echo "  all               Build the application (default)"
	@echo "  build             Build the application"
	@echo "  clean             Remove build artifacts"
	@echo "  run               Build and run the application"
	@echo "  dry-run           Run in dry-run mode (no database updates)"
	@echo "  process-customers Process specific customers (CUSTOMER_IDS=1,2,3)"
	@echo "  resume            Resume from checkpoint"
	@echo "  help              Show this help message"
	@echo ""
	@echo "Example:"
	@echo "  make run"
	@echo "  make process-customers CUSTOMER_IDS=1000,1001,1002"