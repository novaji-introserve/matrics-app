#!/bin/bash

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Risk Analysis System Deployment Script (with Rollback) ===${NC}"
echo ""

# Variables
GO_VERSION="1.22.2"
INSTALL_DIR="/opt/risk-analysis"
BINARY_DIR="/usr/local/bin"
SERVICE_USER="risk-processor"
CURRENT_DIR=$(pwd)
BACKUP_DIR="/tmp/risk-analysis-backup-$(date +%s)"

# Track what was installed for rollback
INSTALLED_GO=false
INSTALLED_REDIS=false
CREATED_USER=false
CREATED_INSTALL_DIR=false
INSTALLED_SERVICES=false
GO_BACKUP_PATH=""

# Function to print status
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Rollback function
rollback() {
    echo ""
    print_error "Deployment failed! Rolling back changes..."
    echo ""

    # Stop and remove services if installed
    if [ "$INSTALLED_SERVICES" = true ]; then
        print_status "Removing systemd services..."
        systemctl stop risk-processor.service 2>/dev/null || true
        systemctl stop risk-api-server.service 2>/dev/null || true
        systemctl disable risk-processor.service 2>/dev/null || true
        systemctl disable risk-api-server.service 2>/dev/null || true
        rm -f /etc/systemd/system/risk-processor.service
        rm -f /etc/systemd/system/risk-api-server.service
        systemctl daemon-reload
    fi

    # Remove installation directory if created
    if [ "$CREATED_INSTALL_DIR" = true ] && [ -d "$INSTALL_DIR" ]; then
        print_status "Removing installation directory..."
        rm -rf "$INSTALL_DIR"
    fi

    # Remove service user if created
    if [ "$CREATED_USER" = true ]; then
        print_status "Removing service user..."
        userdel -r "$SERVICE_USER" 2>/dev/null || true
    fi

    # Restore old Go if we backed it up
    if [ "$INSTALLED_GO" = true ] && [ -n "$GO_BACKUP_PATH" ] && [ -d "$GO_BACKUP_PATH" ]; then
        print_status "Restoring previous Go installation..."
        rm -rf /usr/local/go
        mv "$GO_BACKUP_PATH" /usr/local/go
    elif [ "$INSTALLED_GO" = true ]; then
        print_status "Removing newly installed Go..."
        rm -rf /usr/local/go
    fi

    # Note: We don't rollback Redis as it might be used by other services

    # Remove backup directory
    rm -rf "$BACKUP_DIR"

    echo ""
    print_error "Rollback completed. System restored to previous state."
    exit 1
}

# Set trap to call rollback on error
trap rollback ERR

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    print_error "Please run as root (use sudo)"
    exit 1
fi

print_status "Starting deployment process..."
echo ""
print_warning "Note: This script will rollback all changes if any step fails"
echo ""

# Create backup directory
mkdir -p "$BACKUP_DIR"
print_status "Backup directory created at: $BACKUP_DIR"

# Step 1: Install Go if not present
echo ""
echo -e "${YELLOW}Step 1: Installing Go ${GO_VERSION}${NC}"
if command -v go &> /dev/null; then
    CURRENT_GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
    print_warning "Go ${CURRENT_GO_VERSION} is already installed"

    # Check if version matches
    if [ "$CURRENT_GO_VERSION" = "$GO_VERSION" ]; then
        print_status "Go version matches. Skipping installation."
    else
        read -p "Do you want to reinstall Go ${GO_VERSION}? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # Backup old Go installation
            if [ -d "/usr/local/go" ]; then
                print_status "Backing up existing Go installation..."
                GO_BACKUP_PATH="${BACKUP_DIR}/go-backup"
                cp -r /usr/local/go "$GO_BACKUP_PATH"
            fi

            # Remove old Go installation
            print_status "Removing old Go installation..."
            rm -rf /usr/local/go

            # Download and install Go
            print_status "Downloading Go ${GO_VERSION}..."
            cd /tmp
            wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
            print_status "Extracting Go..."
            tar -C /usr/local -xzf "go${GO_VERSION}.linux-amd64.tar.gz"
            rm "go${GO_VERSION}.linux-amd64.tar.gz"

            # Add Go to PATH
            if ! grep -q "/usr/local/go/bin" /etc/profile; then
                echo "export PATH=\$PATH:/usr/local/go/bin" >> /etc/profile
            fi

            export PATH=$PATH:/usr/local/go/bin
            INSTALLED_GO=true
            print_status "Go ${GO_VERSION} installed successfully"
        else
            print_status "Keeping existing Go installation"
        fi
    fi
else
    print_status "Installing Go ${GO_VERSION}..."
    cd /tmp
    wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
    tar -C /usr/local -xzf "go${GO_VERSION}.linux-amd64.tar.gz"
    rm "go${GO_VERSION}.linux-amd64.tar.gz"

    # Add Go to PATH
    if ! grep -q "/usr/local/go/bin" /etc/profile; then
        echo "export PATH=\$PATH:/usr/local/go/bin" >> /etc/profile
    fi

    export PATH=$PATH:/usr/local/go/bin
    INSTALLED_GO=true
    print_status "Go ${GO_VERSION} installed successfully"
fi

# Verify Go installation
if ! command -v go &> /dev/null; then
    print_error "Go installation verification failed"
    exit 1
fi
print_status "Go version: $(go version)"

# Step 2: Install Redis
echo ""
echo -e "${YELLOW}Step 2: Installing Redis 8.2.3${NC}"
if command -v redis-server &> /dev/null; then
    print_warning "Redis is already installed: $(redis-server --version | head -n1)"
    read -p "Do you want to reinstall Redis? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [ -f "${CURRENT_DIR}/install_redis_8_2_3.sh" ]; then
            print_status "Running Redis installation script..."
            bash "${CURRENT_DIR}/install_redis_8_2_3.sh"
            INSTALLED_REDIS=true
            print_status "Redis installed successfully"
        else
            print_error "Redis installation script not found at ${CURRENT_DIR}/install_redis_8_2_3.sh"
            exit 1
        fi
    else
        print_status "Keeping existing Redis installation"
    fi
else
    if [ -f "${CURRENT_DIR}/install_redis_8_2_3.sh" ]; then
        print_status "Running Redis installation script..."
        bash "${CURRENT_DIR}/install_redis_8_2_3.sh"
        INSTALLED_REDIS=true
        print_status "Redis installed successfully"
    else
        print_warning "Redis installation script not found at ${CURRENT_DIR}/install_redis_8_2_3.sh"
        print_warning "Skipping Redis installation. Please install Redis manually."
    fi
fi

# Step 3: Create service user
echo ""
echo -e "${YELLOW}Step 3: Creating service user${NC}"
if id "$SERVICE_USER" &>/dev/null; then
    print_warning "User ${SERVICE_USER} already exists"
else
    print_status "Creating user ${SERVICE_USER}..."
    useradd -r -s /bin/bash -d /opt/risk-analysis -m "$SERVICE_USER"
    CREATED_USER=true
    print_status "User ${SERVICE_USER} created"
fi

# Step 4: Backup existing installation if present
echo ""
echo -e "${YELLOW}Step 4: Setting up project directory${NC}"
if [ -d "$INSTALL_DIR" ]; then
    print_warning "Installation directory already exists"
    read -p "Do you want to backup and replace it? (Y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        print_status "Backing up existing installation..."
        EXISTING_BACKUP="${BACKUP_DIR}/risk-analysis-existing"
        cp -r "$INSTALL_DIR" "$EXISTING_BACKUP"
        print_status "Backup saved to: $EXISTING_BACKUP"

        print_status "Removing old installation..."
        rm -rf "$INSTALL_DIR"
        CREATED_INSTALL_DIR=true
    else
        print_error "Cannot proceed without replacing installation directory"
        exit 1
    fi
else
    CREATED_INSTALL_DIR=true
fi

print_status "Creating directory ${INSTALL_DIR}..."
mkdir -p "$INSTALL_DIR"
mkdir -p "${INSTALL_DIR}/log"
mkdir -p "${INSTALL_DIR}/cache"
mkdir -p "${INSTALL_DIR}/tmp"

print_status "Copying project files..."
cp -r "${CURRENT_DIR}"/* "$INSTALL_DIR/"
chown -R "$SERVICE_USER":"$SERVICE_USER" "$INSTALL_DIR"

# Step 5: Install Go dependencies and build
echo ""
echo -e "${YELLOW}Step 5: Building binaries${NC}"
cd "$INSTALL_DIR"

print_status "Running go mod tidy..."
sudo -u "$SERVICE_USER" /usr/local/go/bin/go mod tidy

print_status "Building binaries..."
sudo -u "$SERVICE_USER" /usr/local/go/bin/go build -o risk-processor cmd/risk-processor/main.go
sudo -u "$SERVICE_USER" /usr/local/go/bin/go build -o risk-api-server cmd/api-server/main.go

# Verify binaries were built
if [ ! -f "${INSTALL_DIR}/risk-processor" ]; then
    print_error "Failed to build risk-processor binary"
    exit 1
fi

if [ ! -f "${INSTALL_DIR}/risk-api-server" ]; then
    print_error "Failed to build risk-api-server binary"
    exit 1
fi

print_status "Binaries built successfully"

# Copy binaries to system path
print_status "Installing binaries to ${BINARY_DIR}..."
cp "${INSTALL_DIR}/risk-processor" "${BINARY_DIR}/risk-processor"
cp "${INSTALL_DIR}/risk-api-server" "${BINARY_DIR}/risk-api-server"
chmod +x "${BINARY_DIR}/risk-processor"
chmod +x "${BINARY_DIR}/risk-api-server"

# Step 6: Create systemd service for risk-processor
echo ""
echo -e "${YELLOW}Step 6: Creating systemd services${NC}"

# Backup existing services if present
if [ -f "/etc/systemd/system/risk-processor.service" ]; then
    print_status "Backing up existing risk-processor.service..."
    cp /etc/systemd/system/risk-processor.service "${BACKUP_DIR}/risk-processor.service.backup"
fi

if [ -f "/etc/systemd/system/risk-api-server.service" ]; then
    print_status "Backing up existing risk-api-server.service..."
    cp /etc/systemd/system/risk-api-server.service "${BACKUP_DIR}/risk-api-server.service.backup"
fi

cat > /etc/systemd/system/risk-processor.service <<EOF
[Unit]
Description=Risk Analysis Processor
Documentation=https://github.com/your-org/risk-analysis
After=network.target postgresql.service redis.service
Requires=postgresql.service redis.service

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}
ExecStartPre=/bin/sleep 5
ExecStart=${INSTALL_DIR}/risk-processor
Restart=always
RestartSec=10
StandardOutput=append:${INSTALL_DIR}/log/risk-processor.log
StandardError=append:${INSTALL_DIR}/log/risk-processor-error.log

# Environment variables
Environment="PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin"

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${INSTALL_DIR}

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/risk-api-server.service <<EOF
[Unit]
Description=Risk Analysis API Server
Documentation=https://github.com/your-org/risk-analysis
After=network.target postgresql.service redis.service
Requires=postgresql.service redis.service

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}
ExecStartPre=/bin/sleep 5
ExecStart=${INSTALL_DIR}/risk-api-server
Restart=always
RestartSec=10
StandardOutput=append:${INSTALL_DIR}/log/risk-api-server.log
StandardError=append:${INSTALL_DIR}/log/risk-api-server-error.log

# Environment variables
Environment="PATH=/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin"

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${INSTALL_DIR}

[Install]
WantedBy=multi-user.target
EOF

INSTALLED_SERVICES=true
print_status "Systemd services created"

# Step 7: Enable and start services
echo ""
echo -e "${YELLOW}Step 7: Enabling and starting services${NC}"

print_status "Reloading systemd daemon..."
systemctl daemon-reload

print_status "Enabling risk-processor service..."
systemctl enable risk-processor.service

print_status "Enabling risk-api-server service..."
systemctl enable risk-api-server.service

# Ask user if they want to start services now
echo ""
read -p "Do you want to start the services now? (Y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    print_status "Starting risk-processor service..."
    systemctl start risk-processor.service

    print_status "Starting risk-api-server service..."
    systemctl start risk-api-server.service

    sleep 3

    # Check service status
    echo ""
    echo -e "${YELLOW}Service Status:${NC}"
    echo ""

    if systemctl is-active --quiet risk-processor.service; then
        print_status "risk-processor is running"
    else
        print_error "risk-processor failed to start"
        echo "Check logs: journalctl -u risk-processor.service -n 50"
        print_warning "You may need to configure config.conf before the services can start properly"
    fi

    if systemctl is-active --quiet risk-api-server.service; then
        print_status "risk-api-server is running"
    else
        print_error "risk-api-server failed to start"
        echo "Check logs: journalctl -u risk-api-server.service -n 50"
        print_warning "You may need to configure config.conf before the services can start properly"
    fi
else
    print_warning "Services not started. Start them manually with:"
    echo "  sudo systemctl start risk-processor.service"
    echo "  sudo systemctl start risk-api-server.service"
fi

# Disable trap now that deployment succeeded
trap - ERR

# Step 8: Cleanup and summary
echo ""
echo -e "${GREEN}=== Deployment Completed Successfully! ===${NC}"
echo ""
print_status "Backup directory preserved at: $BACKUP_DIR"
print_warning "You can delete the backup once you verify everything works:"
echo "  sudo rm -rf $BACKUP_DIR"
echo ""
echo "Installation Directory: ${INSTALL_DIR}"
echo "Service User: ${SERVICE_USER}"
echo "Binaries: ${BINARY_DIR}/risk-processor, ${BINARY_DIR}/risk-api-server"
echo ""
echo "Systemd Services:"
echo "  - risk-processor.service (always running)"
echo "  - risk-api-server.service (API on port 4567)"
echo ""
echo "Useful Commands:"
echo "  sudo systemctl status risk-processor"
echo "  sudo systemctl status risk-api-server"
echo "  sudo systemctl restart risk-processor"
echo "  sudo systemctl restart risk-api-server"
echo "  sudo journalctl -u risk-processor -f"
echo "  sudo journalctl -u risk-api-server -f"
echo "  tail -f ${INSTALL_DIR}/log/risk-processor.log"
echo "  tail -f ${INSTALL_DIR}/log/risk-api-server.log"
echo ""
echo "API Documentation:"
echo "  http://localhost:4567 (redirects to Swagger UI)"
echo "  http://localhost:4567/docs"
echo ""
echo -e "${GREEN}Deployment completed successfully!${NC}"
