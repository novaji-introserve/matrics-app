# CSV Import System Setup Guide

## Overview

This document provides comprehensive instructions for setting up the CSV Import System, which enables high-performance batch processing of CSV and Excel files in Odoo. The system supports:

- Chunked file uploads for handling large files (up to 2GB)
- Batch processing using job queues for reliable background processing
- WebSocket communication for real-time progress updates
- Automatic handling of required fields and data validation
- Resume capability for interrupted imports

## Prerequisites

All required Python dependencies are already listed in the `compliance_management/requirements.txt` file. These will be installed automatically when you install the module.

**Note**: The WebSocket service is automatically initialized in the top-level `__init__.py` file via the post-init hook, so no manual WebSocket setup is required on the Python side.

## Installation Steps

### 1. Clone the Queue Job Repository

```bash
cd /path/to/odoo
git clone https://github.com/OCA/queue.git
```

## Odoo Configuration

Update your Odoo configuration file (`odoo.conf`) with the following settings:

```ini
[options]
admin_passwd = YOUR_ADMIN_PASSWORD
db_host = localhost
db_port = 5432
db_user = YOUR_DB_USER
db_password = YOUR_DB_PASSWORD
addons_path = addons,custom_addons,odoo/addons/queue
logfile = logfile.log
default_productivity_apps = True

# WebSocket configuration
proxy_mode = True
longpolling_port = 8072
enable_websockets = True

# Increase timeout limits to prevent worker restarts
limit_time_real = 600
limit_time_cpu = 300

# Queue job specific settings
[queue_job]
channels = root:1

# Performance settings
worker_timeout = 240
db_maxconn = 64
cursor_timeout = 300
max_cron_threads = 2  
osv_memory_age_limit = 1.0
osv_memory_count_limit = false
workers = 4
limit_memory_hard = 2684354560
limit_memory_soft = 2147483648
```

## Starting Odoo with Queue Job Workers

You need to run Odoo with Queue Job workers for background processing. Use the following command:

```bash
# Start Odoo with job queue workers
cd /path/to/odoo
python3 odoo-bin -c odoo.conf --workers=4 --load=web,queue_job
```

### For Linux: Create a systemd Service

```bash
sudo nano /etc/systemd/system/odoo.service
```

Add the following content:

```ini
[Unit]
Description=Odoo with Queue Job
After=network.target postgresql.service

[Service]
Type=simple
User=odoo
Group=odoo
ExecStart=/usr/bin/python3 /path/to/odoo/odoo-bin -c /path/to/odoo.conf --workers=4 --load=web,queue_job
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable odoo
sudo systemctl start odoo
```

## Nginx Configuration for WebSockets

### 1. Install Nginx

```bash
# MacOS
brew install nginx

# Ubuntu/Debian
sudo apt-get install nginx

# CentOS/RHEL
sudo yum install nginx
```

### 2. Create Nginx Configuration File

#### For MacOS

```bash
sudo mkdir -p /usr/local/etc/nginx/sites-available
sudo mkdir -p /usr/local/etc/nginx/sites-enabled
sudo nano /usr/local/etc/nginx/sites-available/compliance_websocket.conf
```

#### For Linux

```bash
sudo mkdir -p /etc/nginx/sites-available
sudo mkdir -p /etc/nginx/sites-enabled
sudo nano /etc/nginx/sites-available/compliance_websocket.conf
```

Add the following content:

```nginx
# Nginx configuration for Odoo with WebSocket support

server {
    listen 8080;
    server_name localhost;
    
    # Proxy headers
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    
    # Handle WebSocket connections for CSV Import
    location /csv_import/ws {
        proxy_pass http://127.0.0.1:8072; 
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 86400;
        proxy_send_timeout 86400;
        proxy_buffering off;
        
        # For MacOS:
        error_log /usr/local/var/log/nginx/websocket_error.log debug;
        access_log /usr/local/var/log/nginx/websocket_access.log;
        
        # For Linux (uncomment these lines and comment out the MacOS ones):
        # error_log /var/log/nginx/websocket_error.log debug;
        # access_log /var/log/nginx/websocket_access.log;
    }    

    location /websocket {
        proxy_pass http://127.0.0.1:8072;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
        proxy_buffering off;
    }

    location / {
        proxy_pass http://127.0.0.1:8069;
        proxy_redirect off;
        
        proxy_connect_timeout 600;
        proxy_send_timeout 600;
        proxy_read_timeout 600;
        send_timeout 600;
        
        client_max_body_size 2G;
    }
    
    # Gzip compression
    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml application/xml application/xml+rss text/javascript;
    gzip_proxied any;
    gzip_min_length 1000;
    gzip_comp_level 6;
    gzip_vary on;
}
```

### 3. Update Main Nginx Configuration

**For MacOS:**

```bash
sudo nano /usr/local/etc/nginx/nginx.conf
```

**For Linux:**

```bash
sudo nano /etc/nginx/nginx.conf
```

Add the following content:

```nginx
worker_processes auto;

events {
    worker_connections 1024;
}

http {
    upstream odoo {
        server 127.0.0.1:8069;
    }
    upstream odoo-im {
        server 127.0.0.1:8072;
    }

    include mime.types;
    default_type application/octet-stream;

    sendfile on;
    keepalive_timeout 65;
    types_hash_max_size 2048;

    client_max_body_size 2G;

    server_names_hash_bucket_size 128;

    ssi on;

    gzip on;
    gzip_comp_level 5;
    gzip_min_length 256;
    gzip_proxied any;
    gzip_vary on;
    gzip_types
        application/atom+xml
        application/javascript
        application/json
        application/rss+xml
        application/vnd.ms-fontobject
        application/x-font-ttf
        application/x-web-app-manifest+json
        application/xhtml+xml
        application/xml
        font/opentype
        image/svg+xml
        image/x-icon
        text/css
        text/plain
        text/x-component;

    include /etc/nginx/sites-enabled/*.conf;  # Debian/Ubuntu
    include /etc/nginx/conf.d/*.conf;         # CentOS/RHEL
}
```

### 4. Create Symbolic Link and Test Nginx Configuration

**For MacOS:**

```bash
# Create symbolic link
sudo ln -s /usr/local/etc/nginx/sites-available/compliance_websocket.conf /usr/local/etc/nginx/sites-enabled/

# Test Nginx configuration
sudo nginx -t



# Reload Nginx
sudo nginx -s reload
```

**For Linux:**

```bash
# Create symbolic link
sudo ln -s /etc/nginx/sites-available/compliance_websocket.conf /etc/nginx/sites-enabled/

# Test Nginx configuration
sudo nginx -t

sudo mkdir -p /usr/local/var/log/nginx/

sudo touch /usr/local/var/log/nginx/websocket_error.log

sudo chown www-data:www-data /usr/local/var/log/nginx/websocket_error.log

sudo systemctl start nginx

# Reload Nginx
sudo systemctl reload nginx
```

## Testing the Setup

### 1. WebSocket Debugging Interfaces

Odoo provides built-in WebSocket debugging interfaces to help troubleshoot connection issues:

```bash

#### WebSocket Debug Interface

<http://localhost:8069/ws/debug>

#### WebSocket Test Interface

<http://localhost:8069/ws/test>

```

Replace `8069` with your Odoo port if different. These interfaces allow you to:

- View active WebSocket connections
- Check connection status and metrics
- Run test connections to verify your setup
- Debug any WebSocket-related issues with your Nginx configuration

### 2. Test WebSocket Connection using Command Line

Install wscat for testing WebSockets:

```bash
npm install -g wscat
```

Test the WebSocket connection:

```bash
wscat -c ws://localhost:8072/csv_import/ws
```

If the connection is successful, you'll see a connected message.

## 3. Check Queue Job Workers

Verify that job queue workers are running:

```bash
# For both MacOS and Linux
ps aux | grep odoo | grep queue_job
```

You should see several Odoo processes running, including some with the `--load=web,queue_job` parameter.

### 4. Test Import Functionality

1. Go to your Odoo instance at <http://localhost:8069>
2. Navigate to the CSV Import module
3. Upload a test CSV or Excel file
4. Verify that the import starts and shows progress updates

## Troubleshooting

### Connectivity Issues

If you're experiencing connectivity issues with WebSockets:

1. Check Nginx logs

   ```bash
   # MacOS
   tail -f /usr/local/var/log/nginx/websocket_error.log
   
   # Linux
   tail -f /var/log/nginx/websocket_error.log
   ```

2. Verify Odoo is running with WebSockets enabled:

   ```bash
   grep "enable_websockets" logfile.log
   ```

3. Ensure the WebSocket server started properly:

   ```bash
   grep "WebSocket server started" logfile.log
   ```

4. Use the WebSocket debug interface to check connection status

   ```bash
   <http://localhost:8069/ws/debug>
   ```

### Queue Job Issues

If jobs are not being processed:

1. Verify queue_job module is installed and enabled in Odoo
2. Check Odoo logs for job queue errors:

   ```bash
   grep "queue_job" logfile.log
   ```

3. Restart Odoo with correct parameters:

   ```bash
   # For direct execution
   python3 odoo-bin -c odoo.conf --workers=4 --load=web,queue_job
   
   # For systemd service (Linux)
   sudo systemctl restart odoo
   ```

### Import Process Issues

If imports are not processing correctly:

1. Check if the required fields are properly handled:
   - Verify the CSV processor is updated with the latest code
   - Ensure the import_log model has the process_file_batch method
   - Look for "NULL value in column" errors in the logs

2. Database connection issues:
   - Check PostgreSQL logs for connection errors or serialization failures
   - Increase connection limits if needed
   - Try reducing the batch size to prevent long transactions

## Security Considerations

- Ensure proper file permissions on uploaded files
- Implement access controls for import functionality
- Regularly monitor logs for unusual activity
- Consider setting up rate limiting for file uploads

By following these steps, you should have a fully functional CSV Import System with batch processing capabilities using Odoo's queue job framework and real-time progress updates via WebSockets.
