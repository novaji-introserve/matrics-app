#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Scheduler - Long-running poller for manual triggers and scheduled syncs
Runs continuously, checking for trigger files and scheduled tasks
"""
import os
import sys
import json
import time
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from threading import Lock

# Add scripts directory to path
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

# Setup logging - log ONLY to file (no terminal output)
LOG_DIR = SCRIPT_DIR.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_DIR / 'etl_scheduler.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Clear any existing handlers (to avoid duplicates if reloaded) and add our file handler
logger.handlers = []
logger.addHandler(file_handler)

_logger = logger


class ETLScheduler:
    """Long-running scheduler for ETL syncs"""
    
    def __init__(self, config_dir=None, poll_interval=30):
        """
        Initialize scheduler
        
        Args:
            config_dir: Directory containing config files (default: module/configs)
            poll_interval: Seconds between polling cycles (default: 30)
        """
        self.config_dir = Path(config_dir) if config_dir else SCRIPT_DIR.parent / 'configs'
        self.config_dir.mkdir(exist_ok=True)
        
        self.trigger_dir = self.config_dir / 'triggers'
        self.trigger_dir.mkdir(exist_ok=True)
        
        self.poll_interval = poll_interval
        self.running = False
        self.lock = Lock()
        
        # Track running processes
        self.running_processes = {}
        
        # Load schedule
        self.schedule_file = self.config_dir / 'sync_schedule.json'
        self.schedule = self._load_schedule()
        
        _logger.info(f"ETL Scheduler initialized - Config dir: {self.config_dir}, Poll interval: {self.poll_interval}s")
    
    def _load_schedule(self):
        """Load sync schedule from JSON file"""
        if not self.schedule_file.exists():
            _logger.info("No schedule file found, starting with empty schedule")
            return {}
        
        try:
            with open(self.schedule_file) as f:
                schedule = json.load(f)
            _logger.info(f"Loaded schedule: {len(schedule)} tables configured")
            return schedule
        except Exception as e:
            _logger.error(f"Failed to load schedule: {str(e)}")
            return {}
    
    def _save_schedule(self):
        """Save sync schedule to JSON file"""
        try:
            with open(self.schedule_file, 'w') as f:
                json.dump(self.schedule, f, indent=2)
        except Exception as e:
            _logger.error(f"Failed to save schedule: {str(e)}")
    
    def add_to_schedule(self, table_name, frequency_hours, sync_type='incremental'):
        """
        Add table to schedule
        
        Args:
            table_name: Name of the table config (without .json extension)
            frequency_hours: Hours between syncs
            sync_type: 'full' or 'incremental'
        """
        self.schedule[table_name] = {
            'frequency_hours': frequency_hours,
            'sync_type': sync_type,
            'last_run': None,
            'next_run': None
        }
        self._save_schedule()
        _logger.info(f"Added {table_name} to schedule: {sync_type} every {frequency_hours} hours")
    
    def remove_from_schedule(self, table_name):
        """Remove table from schedule"""
        if table_name in self.schedule:
            del self.schedule[table_name]
            self._save_schedule()
            _logger.info(f"Removed {table_name} from schedule")
    
    def update_schedule(self, table_name, frequency_hours=None, sync_type=None):
        """Update schedule entry"""
        if table_name not in self.schedule:
            self.add_to_schedule(table_name, frequency_hours or 24, sync_type or 'incremental')
            return
        
        if frequency_hours is not None:
            self.schedule[table_name]['frequency_hours'] = frequency_hours
        if sync_type is not None:
            self.schedule[table_name]['sync_type'] = sync_type
        
        self._save_schedule()
        _logger.info(f"Updated schedule for {table_name}")
    
    def check_manual_triggers(self):
        """Check for manual trigger files and process them (with dependency checking)"""
        trigger_files = list(self.trigger_dir.glob('*.trigger'))
        
        for trigger_file in trigger_files:
            try:
                # Check if file is empty
                if trigger_file.stat().st_size == 0:
                    _logger.warning(f"Trigger file {trigger_file.name} is empty, deleting")
                    trigger_file.unlink()
                    continue
                
                # Read trigger file
                with open(trigger_file) as f:
                    content = f.read().strip()
                    if not content:
                        _logger.warning(f"Trigger file {trigger_file.name} is empty, deleting")
                        trigger_file.unlink()
                        continue
                    trigger_data = json.loads(content)
                
                table_name = trigger_data.get('table_name')
                sync_type = trigger_data.get('sync_type', 'full')
                
                if not table_name:
                    _logger.warning(f"Invalid trigger file {trigger_file.name}: missing table_name")
                    trigger_file.unlink()
                    continue
                
                # Check if already running
                if table_name in self.running_processes:
                    proc = self.running_processes[table_name]
                    if proc.poll() is None:
                        _logger.info(f"Sync for {table_name} already running, skipping trigger")
                        continue
                    else:
                        # Process finished, remove from tracking
                        del self.running_processes[table_name]
                
                # Delete trigger file
                trigger_file.unlink()
                
                # Launch sync
                _logger.info(f"Processing manual trigger: {table_name} ({sync_type})")
                self._launch_sync(table_name, sync_type)
                
            except Exception as e:
                _logger.error(f"Error processing trigger {trigger_file.name}: {str(e)}")
                # Don't delete trigger file on error, allow retry
                continue
    
    def check_scheduled_syncs(self):
        """Check scheduled syncs and launch if due"""
        current_time = datetime.now()
        
        for table_name, schedule_info in self.schedule.items():
            try:
                frequency_hours = schedule_info.get('frequency_hours', 24)
                sync_type = schedule_info.get('sync_type', 'incremental')
                last_run = schedule_info.get('last_run')
                next_run = schedule_info.get('next_run')
                
                # Check if already running
                if table_name in self.running_processes:
                    proc = self.running_processes[table_name]
                    if proc.poll() is None:
                        continue
                    else:
                        # Process finished, update last_run
                        schedule_info['last_run'] = current_time.isoformat()
                        schedule_info['next_run'] = (current_time + timedelta(hours=frequency_hours)).isoformat()
                        self._save_schedule()
                        del self.running_processes[table_name]
                        continue
                
                # Check if due
                if next_run:
                    try:
                        next_run_time = datetime.fromisoformat(next_run)
                        if current_time < next_run_time:
                            continue
                    except (ValueError, TypeError):
                        # Invalid date, reset
                        next_run_time = None
                else:
                    next_run_time = None
                
                # If next_run is None (first time), set it without launching
                if next_run_time is None:
                    _logger.info(f"Setting initial next_run for {table_name}: {sync_type} every {frequency_hours} hours")
                    schedule_info['next_run'] = (current_time + timedelta(hours=frequency_hours)).isoformat()
                    self._save_schedule()
                    continue
                
                # If next_run is due, launch sync
                if current_time >= next_run_time:
                    _logger.info(f"Scheduled sync due: {table_name} ({sync_type})")
                    self._launch_sync(table_name, sync_type)
                    
                    # Update schedule
                    schedule_info['last_run'] = current_time.isoformat()
                    schedule_info['next_run'] = (current_time + timedelta(hours=frequency_hours)).isoformat()
                    self._save_schedule()
                    
            except Exception as e:
                _logger.error(f"Error checking schedule for {table_name}: {str(e)}")
                continue
    
    def _check_dependencies(self, table_name):
        """
        Check if all dependencies for a table are satisfied (not running)
        
        Returns:
            tuple: (all_satisfied: bool, blocking_tables: list)
        """
        table_config_file = self.config_dir / f'{table_name}_config.json'
        if not table_config_file.exists():
            return True, []  # No config = no dependencies
        
        try:
            with open(table_config_file) as f:
                config = json.load(f)
            
            dependencies = config.get('dependencies', [])
            if not dependencies:
                return True, []
            
            blocking = []
            for dep_table in dependencies:
                # Check if dependency is currently running
                if dep_table in self.running_processes:
                    proc = self.running_processes[dep_table]
                    if proc.poll() is None:  # Still running
                        blocking.append(dep_table)
            
            return len(blocking) == 0, blocking
            
        except Exception as e:
            _logger.warning(f"Error checking dependencies for {table_name}: {e}")
            return True, []  # On error, allow sync to proceed
    
    def _launch_sync(self, table_name, sync_type):
        """Launch a sync process"""
        table_config_file = self.config_dir / f'{table_name}_config.json'
        db_config_file = self.config_dir / 'db_config.json'
        
        if not table_config_file.exists():
            _logger.error(f"Table config not found: {table_config_file}")
            return
        
        if not db_config_file.exists():
            _logger.error(f"DB config not found: {db_config_file}")
            return
        
        # Check dependencies before launching
        deps_satisfied, blocking = self._check_dependencies(table_name)
        if not deps_satisfied:
            _logger.info(f"Skipping {table_name} sync - waiting for dependencies: {', '.join(blocking)}")
            return
        
        # Build command
        # Use the same Python interpreter that's running this scheduler
        # This ensures compatibility with both venv (dev) and Docker (prod) environments
        python_exe = sys.executable
        _logger.info(f"Scheduler running with Python: {sys.executable}")
        
        # Optional: Try to find .venv as fallback (for dev environments where scheduler
        # might be run with system Python but packages are in venv)
        # This is a convenience feature and won't break in Docker if .venv doesn't exist
        in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
        _logger.info(f"Detected in venv: {in_venv}")
        
        if not in_venv:
            # Not detected as being in venv, try to find .venv relative to script location
            # Calculate workspace root dynamically (works in both dev and Docker)
            try:
                # Go up from scripts/ -> icomply_etl_manager/ -> icomply_odoo/ -> custom_addons/ -> workspace root
                workspace_root = SCRIPT_DIR.parent.parent.parent.parent
                venv_python = workspace_root / '.venv' / 'bin' / 'python3'
                _logger.info(f"Checking for venv at: {venv_python}")
                
                if venv_python.exists() and venv_python.is_file():
                    python_exe = str(venv_python)
                    _logger.info(f"Found venv Python, using: {python_exe}")
                else:
                    _logger.info(f"Venv Python not found at {venv_python}, using: {python_exe}")
            except Exception as e:
                # If path calculation fails, just use sys.executable (fine for Docker)
                _logger.warning(f"Error checking for venv: {e}, using: {python_exe}")
        
        _logger.info(f"Will launch ETL engine with Python: {python_exe}")
        
        engine_script = SCRIPT_DIR / 'base_etl_engine.py'
        cmd = [
            python_exe,
            str(engine_script),
            '--db-config', str(db_config_file),
            '--table-config', str(table_config_file),
            '--sync-type', sync_type
        ]
        
        # Launch process
        try:
            log_file = LOG_DIR / f'{table_name}_{sync_type}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
            
            with open(log_file, 'w') as log_f:
                proc = subprocess.Popen(
                    cmd,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    cwd=str(SCRIPT_DIR)
                )
            
            self.running_processes[table_name] = proc
            _logger.info(f"Launched sync process for {table_name} (PID: {proc.pid})")
            
        except Exception as e:
            _logger.error(f"Failed to launch sync for {table_name}: {str(e)}")
    
    def cleanup_finished_processes(self):
        """Clean up finished processes from tracking and update schedule if needed"""
        current_time = datetime.now()
        finished = []
        
        for table_name, proc in self.running_processes.items():
            if proc.poll() is not None:
                finished.append(table_name)
                _logger.info(f"Sync process for {table_name} finished (exit code: {proc.returncode})")
                
                # If this table is in the schedule, update next_run to prevent immediate re-runs
                # This handles both manual triggers and scheduled syncs
                if table_name in self.schedule:
                    schedule_info = self.schedule[table_name]
                    frequency_hours = schedule_info.get('frequency_hours', 24)
                    
                    # Update next_run to prevent immediate scheduled re-run
                    # Only update if next_run is None or in the past (meaning it was due)
                    next_run = schedule_info.get('next_run')
                    if not next_run:
                        # Set next_run if it wasn't set
                        schedule_info['next_run'] = (current_time + timedelta(hours=frequency_hours)).isoformat()
                        self._save_schedule()
                        _logger.info(f"Updated next_run for {table_name} after sync completion")
                    else:
                        try:
                            next_run_time = datetime.fromisoformat(next_run)
                            # If next_run was in the past (sync was due), update it
                            if current_time >= next_run_time:
                                schedule_info['next_run'] = (current_time + timedelta(hours=frequency_hours)).isoformat()
                                self._save_schedule()
                                _logger.info(f"Updated next_run for {table_name} after sync completion")
                        except (ValueError, TypeError):
                            # Invalid date, set it
                            schedule_info['next_run'] = (current_time + timedelta(hours=frequency_hours)).isoformat()
                            self._save_schedule()
        
        for table_name in finished:
            del self.running_processes[table_name]
    
    def run(self):
        """Main scheduler loop"""
        self.running = True
        _logger.info("ETL Scheduler started")
        
        try:
            while self.running:
                try:
                    # Check manual triggers
                    self.check_manual_triggers()
                    
                    # Check scheduled syncs
                    self.check_scheduled_syncs()
                    
                    # Cleanup finished processes
                    self.cleanup_finished_processes()
                    
                except Exception as e:
                    _logger.error(f"Error in scheduler loop: {str(e)}")
                
                # Sleep before next poll
                time.sleep(self.poll_interval)
                
        except KeyboardInterrupt:
            _logger.info("Scheduler interrupted by user")
        except Exception as e:
            _logger.error(f"Scheduler crashed: {str(e)}")
            raise
        finally:
            self.running = False
            _logger.info("ETL Scheduler stopped")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Scheduler - Long-running poller')
    parser.add_argument('--config-dir', help='Config directory (default: module/configs)')
    parser.add_argument('--poll-interval', type=int, default=30, help='Poll interval in seconds (default: 30)')
    
    args = parser.parse_args()
    
    scheduler = ETLScheduler(
        config_dir=args.config_dir,
        poll_interval=args.poll_interval
    )
    
    scheduler.run()


if __name__ == '__main__':
    main()

