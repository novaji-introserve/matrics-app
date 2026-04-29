#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alert Scheduler - Standalone scheduler for alert processing
Runs continuously, checking alert frequencies and processing due alerts
"""
import os
import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Setup logging
LOG_DIR = Path(__file__).parent.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_DIR / 'alert_scheduler.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Clear any existing handlers and add our file handler
logger.handlers = []
logger.addHandler(file_handler)

_logger = logger


class AlertScheduler:
    """Standalone scheduler for alert processing"""
    
    def __init__(self, poll_interval=30, odoo_url=None, odoo_db=None):
        """
        Initialize alert scheduler
        
        Args:
            poll_interval: Seconds between polling cycles (default: 30)
            odoo_url: Odoo XML-RPC URL (default: http://localhost:7070)
            odoo_db: Odoo database name (from env or config)
        """
        self.poll_interval = poll_interval
        self.running = False
        
        # Odoo connection settings
        self.odoo_url = odoo_url or os.getenv('ODOO_URL', 'http://localhost:7070')
        # Use DB_NAME to match your existing .env file convention (DB_NAME=Aa)
        self.odoo_db = odoo_db or os.getenv('DB_NAME') or os.getenv('PGDATABASE', 'enterprise')
        self.odoo_user = os.getenv('ADMIN_USER', 'admin')
        self.odoo_password = os.getenv('ADMIN_PASSWORD', 'admin123_')
        
        _logger.info(f"Alert Scheduler initialized")
        _logger.info(f"  Odoo URL: {self.odoo_url}")
        _logger.info(f"  Database: {self.odoo_db}")
        _logger.info(f"  Poll interval: {self.poll_interval}s")
    
    def _get_odoo_connection(self):
        """Get Odoo XML-RPC connection"""
        try:
            import xmlrpc.client
            
            # Connect to Odoo - allow None values in responses
            common = xmlrpc.client.ServerProxy(
                f'{self.odoo_url}/xmlrpc/2/common',
                allow_none=True
            )
            
            # Authenticate
            uid = common.authenticate(self.odoo_db, self.odoo_user, self.odoo_password, {})
            
            if not uid:
                raise Exception("Authentication failed - check credentials")
            
            # Get models proxy - allow None values in responses
            models = xmlrpc.client.ServerProxy(
                f'{self.odoo_url}/xmlrpc/2/object',
                allow_none=True
            )
            
            _logger.info(f"Connected to Odoo as user ID {uid}")
            return models, uid
            
        except Exception as e:
            _logger.error(f"Failed to connect to Odoo: {str(e)}")
            raise
    
    def process_alerts(self):
        """Process all alert rules via Odoo XML-RPC"""
        try:
            start_time = time.time()
            _logger.info("Starting alert processing cycle")
            
            # Get Odoo connection
            models, uid = self._get_odoo_connection()
            
            # Call the process_alert_rules method
            # This calls the same method that the Odoo cron used to call
            result = models.execute_kw(
                self.odoo_db, uid, self.odoo_password,
                'alert.rules', 'process_alert_rules',
                []
            )
            
            duration = time.time() - start_time
            _logger.info(f"Alert processing cycle completed in {duration:.1f}s")
            
            return True
            
        except Exception as e:
            _logger.error(f"Error processing alerts: {str(e)}")
            return False
    
    def run(self):
        """Main scheduler loop"""
        self.running = True
        _logger.info("Alert Scheduler started")
        
        try:
            # Test connection on startup
            try:
                self._get_odoo_connection()
                _logger.info("✅ Initial Odoo connection successful")
            except Exception as e:
                _logger.error(f"❌ Failed to connect to Odoo on startup: {str(e)}")
                _logger.error("Check that Odoo is running and credentials are correct")
                raise
            
            cycle_count = 0
            
            while self.running:
                try:
                    cycle_count += 1
                    _logger.info(f"--- Cycle {cycle_count} ---")
                    
                    # Process alerts
                    self.process_alerts()
                    
                except Exception as e:
                    _logger.error(f"Error in scheduler loop: {str(e)}")
                
                # Sleep before next poll
                _logger.info(f"Sleeping for {self.poll_interval}s...")
                time.sleep(self.poll_interval)
                
        except KeyboardInterrupt:
            _logger.info("Scheduler interrupted by user")
        except Exception as e:
            _logger.error(f"Scheduler crashed: {str(e)}")
            raise
        finally:
            self.running = False
            _logger.info("Alert Scheduler stopped")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Alert Scheduler - Standalone poller')
    parser.add_argument('--poll-interval', type=int, default=30, 
                       help='Poll interval in seconds (default: 30)')
    parser.add_argument('--odoo-url', 
                       help='Odoo URL (default: http://localhost:8069)')
    parser.add_argument('--odoo-db', 
                       help='Odoo database name (default: from PGDATABASE env)')
    
    args = parser.parse_args()
    
    scheduler = AlertScheduler(
        poll_interval=args.poll_interval,
        odoo_url=args.odoo_url,
        odoo_db=args.odoo_db
    )
    
    scheduler.run()


if __name__ == '__main__':
    main()
