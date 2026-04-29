# Alert Scheduler

Standalone scheduler for processing alert rules. Replaces Odoo cron job for better control and true frequency support.

## Features

- ✅ Polls every 30 seconds (configurable)
- ✅ Respects individual alert rule frequencies (5 min, 10 min, etc.)
- ✅ Works in both dev and production
- ✅ Independent of Odoo cron system
- ✅ Proper logging to `logs/alert_scheduler.log`

## How It Works

```
Every 30 seconds:
1. Connect to Odoo via XML-RPC
2. Call alert.rules.process_alert_rules()
3. Odoo checks each rule's frequency
4. Processes only rules that are due
5. Sleep 30 seconds and repeat
```

## Dev Usage

### Manual Run:
```bash
cd /data/odoo2/custom_addons/icomply_odoo/alert_management/scripts

# Run with defaults (30s interval)
python3 alert_scheduler.py

# Run with custom interval
python3 alert_scheduler.py --poll-interval 60

# Run with custom Odoo settings
python3 alert_scheduler.py \
  --odoo-url http://localhost:8069 \
  --odoo-db icomply \
  --poll-interval 30
```

### Environment Variables:
```bash
export ODOO_URL="http://localhost:7070"
export DB_NAME="Aa"  # Your database name (matches your .env file: DB_NAME=Aa)
export ADMIN_USER="admin"
export ADMIN_PASSWORD="your_password"

python3 alert_scheduler.py
```

## Production Usage (Docker)

See docker-compose.yml service configuration below.

## Logs

Check logs at:
- Dev: `/data/odoo2/custom_addons/icomply_odoo/alert_management/logs/alert_scheduler.log`
- Prod: `/mnt/custom-addons/alert_management/logs/alert_scheduler.log`

## Comparison: Old vs New

### Old System (Odoo Cron):
```
Cron runs: Every 1 HOUR
Alert with 5-min frequency: Actually runs every 1 HOUR ❌
Alert with 10-min frequency: Actually runs every 1 HOUR ❌
Result: Alerts are delayed by up to 1 hour!
```

### New System (Standalone Scheduler):
```
Scheduler polls: Every 30 SECONDS
Alert with 5-min frequency: Runs every 5 MINUTES ✅
Alert with 10-min frequency: Runs every 10 MINUTES ✅
Result: Alerts run at their configured frequencies!
```

## Troubleshooting

### Connection Failed
```
❌ Failed to connect to Odoo: [Errno 111] Connection refused
```
**Solution:** Make sure Odoo is running and accessible

### Authentication Failed
```
❌ Authentication failed - check credentials
```
**Solution:** Check ODOO_ADMIN_USER and ODOO_ADMIN_PASSWORD environment variables

### Check if scheduler is running:
```bash
# Docker
docker ps | grep alert_scheduler

# View logs
docker logs alert_scheduler

# Dev
ps aux | grep alert_scheduler
```
