#!/bin/bash
set -e

echo "=== Installing Redis 8.2.3 from source ==="

# 1. Remove old Redis installations
echo "Removing old Redis installations..."
systemctl stop redis-server 2>/dev/null || true
systemctl disable redis-server 2>/dev/null || true
apt remove --purge -y redis redis-server redis-tools 2>/dev/null || true
rm -rf /etc/redis /var/lib/redis /run/redis

# 2. Install dependencies
echo "Installing dependencies..."
apt update
apt install -y build-essential tcl wget libssl-dev

# 3. Download Redis 8.2.3 source
echo "Downloading Redis 8.2.3 source..."
cd /usr/src
wget -q https://github.com/redis/redis/archive/refs/tags/8.2.3.tar.gz -O redis-8.2.3.tar.gz
tar xzf redis-8.2.3.tar.gz
cd redis-8.2.3

# 4. Build Redis
echo "Building Redis..."
make -j"$(nproc)" all
make install

# 5. Create Redis config and data directories
echo "Setting up configuration and directories..."
mkdir -p /etc/redis /var/lib/redis /var/log/redis /run/redis
cp redis.conf /etc/redis/redis.conf

# Configure Redis
sed -i 's/^supervised no/supervised systemd/' /etc/redis/redis.conf
sed -i 's#^dir ./#dir /var/lib/redis#' /etc/redis/redis.conf
sed -i 's#^pidfile .*#pidfile /run/redis/redis-server.pid#' /etc/redis/redis.conf
sed -i 's#^logfile .*#logfile /var/log/redis/redis-server.log#' /etc/redis/redis.conf

# 6. Create systemd service
echo "Creating systemd service..."
cat <<EOF > /etc/systemd/system/redis.service
[Unit]
Description=Redis In-Memory Data Store
After=network.target

[Service]
User=redis
Group=redis
ExecStart=/usr/local/bin/redis-server /etc/redis/redis.conf
ExecStop=/usr/local/bin/redis-cli shutdown
Restart=always
RuntimeDirectory=redis
RuntimeDirectoryMode=0755

[Install]
WantedBy=multi-user.target
EOF

# 7. Add redis user and permissions
echo "Adding redis user and setting permissions..."
id -u redis &>/dev/null || adduser --system --group --no-create-home redis
chown -R redis:redis /var/lib/redis /var/log/redis /run/redis
chmod -R 770 /var/lib/redis
chmod 755 /run/redis

# 8. Enable memory overcommit (to avoid warnings)
echo "Enabling memory overcommit..."
sysctl -w vm.overcommit_memory=1
if ! grep -q "vm.overcommit_memory" /etc/sysctl.conf; then
  echo "vm.overcommit_memory = 1" >> /etc/sysctl.conf
fi

# 9. Enable and start Redis
echo "Starting Redis service..."
systemctl daemon-reload
systemctl enable redis
systemctl start redis

# 10. Verify
echo "Verifying installation..."
redis-server --version
sleep 2
redis-cli ping || (echo "Redis did not start correctly." && exit 1)

# Ensure Redis binaries are available globally
ln -sf /usr/local/bin/redis-server /usr/bin/redis-server
ln -sf /usr/local/bin/redis-cli /usr/bin/redis-cli
hash -r

echo "=== Redis 8.2.3 installation completed successfully ==="
