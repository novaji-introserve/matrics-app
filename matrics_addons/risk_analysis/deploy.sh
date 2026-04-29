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
LOG_FILE="${BACKUP_DIR}/deployment.log"

# Track what was installed for rollback
INSTALLED_GO=false
INSTALLED_REDIS=false
CREATED_USER=false
CREATED_INSTALL_DIR=false
INSTALLED_SERVICES=false
GO_BACKUP_PATH=""
LAST_COMMAND=""
CURRENT_COMMAND=""

# Function to print status
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
    echo "[SUCCESS] $(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
    echo "[WARNING] $(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

# Rollback function
rollback() {
    echo "" | tee -a "$LOG_FILE"
    print_error "Deployment failed! Rolling back changes..."
    print_error "Last command: ${LAST_COMMAND}"
    print_error "Failed command: ${CURRENT_COMMAND}"
    echo "" | tee -a "$LOG_FILE"

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

    echo ""
    print_error "Rollback completed. System restored to previous state."
    echo "[COMPLETED] $(date '+%Y-%m-%d %H:%M:%S') - Rollback finished" >> "$LOG_FILE"
    exit 1
}

# Set trap to call rollback on error with improved error capture
trap 'LAST_COMMAND=$CURRENT_COMMAND; CURRENT_COMMAND=$BASH_COMMAND; echo "Error on line ${LINENO}: command ${CURRENT_COMMAND} failed with exit code $?"; rollback' ERR

# Function to check for required tools
check_requirements() {
    local missing_tools=()
    for cmd in wget tar systemctl grep sed; do
        if ! command -v $cmd &>/dev/null; then
            missing_tools+=($cmd)
        fi
    done
    
    if [ ${#missing_tools[@]} -gt 0 ]; then
        print_error "Missing required tools: ${missing_tools[*]}"
        print_warning "Please install them using: apt-get install ${missing_tools[*]}"
        exit 1
    fi
}

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    print_error "Please run as root (use sudo)"
    exit 1
fi

# Create backup directory and log file
mkdir -p "$BACKUP_DIR"
touch "$LOG_FILE"
echo "[STARTED] $(date '+%Y-%m-%d %H:%M:%S') - Deployment script started" >> "$LOG_FILE"

print_status "Starting deployment process..."
print_status "Backup directory created at: $BACKUP_DIR"
print_status "Log file: $LOG_FILE"
echo ""
print_warning "Note: This script will rollback all changes if any step fails"
echo ""

# Check for required tools
check_requirements

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

            # Add Go to PATH in /etc/profile
            if ! grep -q "/usr/local/go/bin" /etc/profile; then
                echo "export PATH=\$PATH:/usr/local/go/bin" >> /etc/profile
            fi

            # Add Go to PATH in /etc/bash.bashrc for all users
            if ! grep -q "/usr/local/go/bin" /etc/bash.bashrc; then
                echo "export PATH=\$PATH:/usr/local/go/bin" >> /etc/bash.bashrc
            fi

            # Update PATH for current session
            export PATH=/usr/local/go/bin:$PATH
            hash -r
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

    # Update PATH for current session
    export PATH=/usr/local/go/bin:$PATH
    hash -r
    INSTALLED_GO=true
    print_status "Go ${GO_VERSION} installed successfully"
fi

# Verify Go installation
if ! command -v go &> /dev/null; then
    print_error "Go installation verification failed"
    exit 1
fi

# Verify Go version
go_version=$(go version | awk '{print $3}' | sed 's/go//')
if [ "$go_version" != "$GO_VERSION" ]; then
    print_warning "Go version mismatch. Expected ${GO_VERSION}, got ${go_version}"
    print_warning "This might be due to PATH not being updated in the current shell"
    print_warning "Using full path to Go binary for builds"
else
    print_status "Go version verified: $(go version)"
fi

# Step 2: Install Redis
echo ""
echo -e "${YELLOW}Step 2: Installing Redis 8.2.3${NC}"

# Fix TimescaleDB repository issue before installing Redis
print_status "Checking for problematic package repositories..."
if [ -f /etc/apt/sources.list.d/timescaledb.list ]; then
    print_warning "Found TimescaleDB repository config, checking for issues..."
    if grep -q "timescale/timescaledb/debian" /etc/apt/sources.list.d/timescaledb.list; then
        print_status "Fixing TimescaleDB repository configuration..."
        cp /etc/apt/sources.list.d/timescaledb.list "${BACKUP_DIR}/timescaledb.list.backup"
        sed -i '/timescale\/timescaledb\/debian/d' /etc/apt/sources.list.d/timescaledb.list
        print_status "TimescaleDB repository fixed"
    fi
fi

if command -v redis-server &> /dev/null; then
    print_warning "Redis is already installed: $(redis-server --version | head -n1)"
    read -p "Do you want to reinstall Redis? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [ -f "${CURRENT_DIR}/install_redis_8_2_3.sh" ]; then
            print_status "Running Redis installation script..."
            if ! bash "${CURRENT_DIR}/install_redis_8_2_3.sh"; then
                print_warning "Redis installation script failed. Attempting direct installation..."
                apt-get update -y
                apt-get install -y redis-server
                systemctl enable redis
                systemctl start redis
            fi
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
        if ! bash "${CURRENT_DIR}/install_redis_8_2_3.sh"; then
            print_warning "Redis installation script failed. Attempting direct installation..."
            apt-get update -y
            apt-get install -y redis-server
            systemctl enable redis
            systemctl start redis
        fi
        INSTALLED_REDIS=true
        print_status "Redis installed successfully"
    else
        print_warning "Redis installation script not found at ${CURRENT_DIR}/install_redis_8_2_3.sh"
        print_warning "Attempting direct Redis installation..."
        apt-get update -y
        apt-get install -y redis-server
        systemctl enable redis
        systemctl start redis
        INSTALLED_REDIS=true
        print_status "Redis installed via package manager"
    fi
fi

# Verify Redis is actually running
if ! systemctl is-active --quiet redis; then
    print_warning "Redis service is not running. Attempting to start..."
    systemctl start redis
    sleep 2
    if ! systemctl is-active --quiet redis; then
        print_error "Failed to start Redis service"
        exit 1
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

# Step 4: Prepare project directory (work in place - no copying)
echo ""
echo -e "${YELLOW}Step 4: Preparing project directory${NC}"

# Work in the current directory where code already exists
INSTALL_DIR="${CURRENT_DIR}"
print_status "Working in current directory: ${INSTALL_DIR}"

# Ask for config file path
echo ""
echo -e "${YELLOW}Configuration File Location${NC}"
echo "Please provide the full path to your config.conf file"
echo "Press ENTER to use default: ${INSTALL_DIR}/config.conf"
read -p "Config file path: " CONFIG_FILE_PATH
echo

if [ -z "$CONFIG_FILE_PATH" ]; then
    CONFIG_FILE_PATH="${INSTALL_DIR}/config.conf"
fi

# Verify config file exists
if [ ! -f "$CONFIG_FILE_PATH" ]; then
    print_error "Config file not found at: $CONFIG_FILE_PATH"
    exit 1
fi

print_status "Using config file: ${CONFIG_FILE_PATH}"

# Create necessary subdirectories if they don't exist
print_status "Creating required subdirectories..."
mkdir -p "${INSTALL_DIR}/log"
mkdir -p "${INSTALL_DIR}/cache"
mkdir -p "${INSTALL_DIR}/tmp"

# Set ownership to service user
print_status "Setting ownership to ${SERVICE_USER}..."
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

# Step 6: Install systemd services
echo ""
echo -e "${YELLOW}Step 6: Installing systemd services${NC}"

# Backup existing services if present
if [ -f "/etc/systemd/system/risk-processor.service" ]; then
    print_status "Backing up existing risk-processor.service..."
    cp /etc/systemd/system/risk-processor.service "${BACKUP_DIR}/risk-processor.service.backup"
fi

if [ -f "/etc/systemd/system/risk-api-server.service" ]; then
    print_status "Backing up existing risk-api-server.service..."
    cp /etc/systemd/system/risk-api-server.service "${BACKUP_DIR}/risk-api-server.service.backup"
fi

# Install service files with path replacement
if [ -f "${INSTALL_DIR}/systemd/risk-processor.service" ]; then
    print_status "Installing risk-processor.service with correct paths..."
    # Replace /opt/risk-analysis with actual INSTALL_DIR and config path in the service file
    sed -e "s|/opt/risk-analysis|${INSTALL_DIR}|g" \
        -e "s|CONFIG_FILE=/opt/risk-analysis/config.conf|CONFIG_FILE=${CONFIG_FILE_PATH}|g" \
        "${INSTALL_DIR}/systemd/risk-processor.service" > /etc/systemd/system/risk-processor.service
    print_status "risk-processor.service installed (uses CONFIG_FILE=${CONFIG_FILE_PATH})"
else
    print_error "Service file not found: ${INSTALL_DIR}/systemd/risk-processor.service"
    exit 1
fi

if [ -f "${INSTALL_DIR}/systemd/risk-api-server.service" ]; then
    print_status "Installing risk-api-server.service with correct paths..."
    # Replace /opt/risk-analysis with actual INSTALL_DIR and config path in the service file
    sed -e "s|/opt/risk-analysis|${INSTALL_DIR}|g" \
        -e "s|CONFIG_FILE=/opt/risk-analysis/config.conf|CONFIG_FILE=${CONFIG_FILE_PATH}|g" \
        "${INSTALL_DIR}/systemd/risk-api-server.service" > /etc/systemd/system/risk-api-server.service
    print_status "risk-api-server.service installed (uses CONFIG_FILE=${CONFIG_FILE_PATH})"
else
    print_error "Service file not found: ${INSTALL_DIR}/systemd/risk-api-server.service"
    exit 1
fi

INSTALLED_SERVICES=true
print_status "Systemd services installed successfully"

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

# Step 8: Configure Apache (optional)
echo ""
echo -e "${YELLOW}Step 8: Apache Configuration (Optional)${NC}"
if command -v apache2 &> /dev/null || command -v httpd &> /dev/null; then
    read -p "Do you want to install Apache reverse proxy config for the API? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [ -f "${INSTALL_DIR}/apache/risk-api.conf" ]; then
            print_status "Installing Apache config..."

            # Determine Apache config directory
            if [ -d "/etc/apache2/sites-available" ]; then
                APACHE_CONF_DIR="/etc/apache2/sites-available"
                APACHE_CMD="apache2"
            elif [ -d "/etc/httpd/conf.d" ]; then
                APACHE_CONF_DIR="/etc/httpd/conf.d"
                APACHE_CMD="httpd"
            else
                print_error "Could not find Apache config directory"
                exit 1
            fi

            # Copy config
            cp "${INSTALL_DIR}/apache/risk-api.conf" "${APACHE_CONF_DIR}/"
            print_status "Apache config copied to ${APACHE_CONF_DIR}/"

            # Enable required modules
            if command -v a2enmod &> /dev/null; then
                print_status "Enabling Apache modules..."
                a2enmod proxy proxy_http rewrite headers ssl 2>/dev/null || true
            fi

            # Enable site if using Debian/Ubuntu
            if command -v a2ensite &> /dev/null; then
                print_status "Enabling risk-api site..."
                a2ensite risk-api.conf 2>/dev/null || true
            fi

            # Reload Apache
            print_status "Reloading Apache..."
            if command -v systemctl &> /dev/null; then
                systemctl reload ${APACHE_CMD} 2>/dev/null || systemctl restart ${APACHE_CMD}
            else
                service ${APACHE_CMD} reload 2>/dev/null || service ${APACHE_CMD} restart
            fi

            print_status "Apache reverse proxy configured"
            print_warning "Remember to update ServerName in ${APACHE_CONF_DIR}/risk-api.conf"
        else
            print_warning "Apache config not found at ${INSTALL_DIR}/apache/risk-api.conf"
        fi
    else
        print_status "Skipping Apache configuration"
    fi
else
    print_warning "Apache not installed, skipping reverse proxy setup"
fi

# Step 9: Cleanup and summary
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
echo "[COMPLETED] $(date '+%Y-%m-%d %H:%M:%S') - Deployment finished successfully" >> "$LOG_FILE"
