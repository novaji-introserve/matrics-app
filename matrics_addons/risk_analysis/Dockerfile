# Multi-stage build for high-performance customer risk analysis system

# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata build-base

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w -X main.version=$(git describe --tags --always)" \
    -o risk-processor ./cmd/risk-processor

# Final stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata curl

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/risk-processor .

# Copy configs
COPY .env.example .env.example

# Create log directory
RUN mkdir -p /var/log/risk-processor

# Create data directory for checkpoints
RUN mkdir -p /data

# Set executable permissions
RUN chmod +x /app/risk-processor

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD [ "pgrep", "risk-processor" ] || exit 1

# Set environment variables
ENV LOG_OUTPUT=stdout \
    CHECKPOINT_FILE=/data/risk-processor-checkpoint.json

# Command to run
ENTRYPOINT ["/app/risk-processor"]
