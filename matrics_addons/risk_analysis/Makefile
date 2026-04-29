.PHONY: build run test clean docker-build docker-run api-server cli-tool install deps

# Build the CLI tool (existing functionality)
build:
	@echo "Building risk processor CLI..."
	go build -o risk-processor cmd/risk-processor/main.go

# Build the API server (new)
build-api:
	@echo "Building risk processor API server..."
	go build -o risk-api-server cmd/api-server/main.go

# Build both CLI and API
build-all: build build-api

# Run the CLI tool
run:
	@echo "Running risk processor CLI..."
	./risk-processor

# Run the API server
run-api:
	@echo "Running risk processor API server..."
	./risk-api-server

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f risk-processor risk-api-server
	rm -f cache/*.cache cache/*.json cache/*.txt

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod verify

# Install the tools
install: build-all
	@echo "Installing..."
	cp risk-processor $(GOPATH)/bin/
	cp risk-api-server $(GOPATH)/bin/

# Docker build
docker-build:
	@echo "Building Docker image..."
	docker build -t risk-processor:latest .

# Docker run API
docker-run-api:
	@echo "Running API server in Docker..."
	docker run -p 8080:8080 risk-processor:latest

# Development mode - run with auto-reload (requires air)
dev-api:
	@echo "Starting API server in development mode..."
	air -c .air.toml

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Lint code
lint:
	@echo "Linting code..."
	golangci-lint run

# Run single customer analysis via API (example)
test-api:
	@echo "Testing API with customer ID 3182038..."
	curl -X POST http://localhost:8080/api/v1/risk-analysis \
		-H "Content-Type: application/json" \
		-d '{"customer_ids":[3182038]}'

# Run single customer analysis via CLI (existing)
test-cli:
	@echo "Testing CLI with customer ID 3182038..."
	./risk-processor --customer-ids=3182038


	

# .PHONY: all build run clean help

# # Variables
# BINARY_NAME=risk-processor
# BUILD_DIR=./build

# # Default target
# all: build

# # Build the application
# build:
# 	@echo "Building $(BINARY_NAME)..."
# 	@mkdir -p $(BUILD_DIR)
# 	@go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/risk-processor

# # Run the application
# run: build
# 	@echo "Running $(BINARY_NAME)..."
# 	@$(BUILD_DIR)/$(BINARY_NAME)

# # Clean build artifacts
# clean:
# 	@echo "Cleaning..."
# 	@rm -rf $(BUILD_DIR)

# # Run in dry-run mode
# dry-run: build
# 	@echo "Running in dry-run mode..."
# 	@$(BUILD_DIR)/$(BINARY_NAME) --dry-run

# # Process specific customers
# process-customers: build
# 	@echo "Processing specific customers: $(CUSTOMER_IDS)"
# 	@$(BUILD_DIR)/$(BINARY_NAME) --customer-ids=$(CUSTOMER_IDS)

# # Resume from checkpoint
# resume: build
# 	@echo "Resuming from checkpoint..."
# 	@$(BUILD_DIR)/$(BINARY_NAME) --resume-from-checkpoint

# # Help command
# help:
# 	@echo "High-Performance Customer Risk Analysis System"
# 	@echo ""
# 	@echo "Usage:"
# 	@echo "  make [command]"
# 	@echo ""
# 	@echo "Available Commands:"
# 	@echo "  all               Build the application (default)"
# 	@echo "  build             Build the application"
# 	@echo "  clean             Remove build artifacts"
# 	@echo "  run               Build and run the application"
# 	@echo "  dry-run           Run in dry-run mode (no database updates)"
# 	@echo "  process-customers Process specific customers (CUSTOMER_IDS=1,2,3)"
# 	@echo "  resume            Resume from checkpoint"
# 	@echo "  help              Show this help message"
# 	@echo ""
# 	@echo "Example:"
# 	@echo "  make run"
# 	@echo "  make process-customers CUSTOMER_IDS=1000,1001,1002"