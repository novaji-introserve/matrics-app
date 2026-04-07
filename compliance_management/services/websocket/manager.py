# -*- coding: utf-8 -*-
"""
WebSocket Manager
================
Handles the lifecycle of the WebSocket server process.
This component is responsible for starting, monitoring, and stopping
the standalone WebSocket server process.
"""

import os
import sys
import logging
import threading
import time
import subprocess
import atexit
from odoo.tools import config

_logger = logging.getLogger(__name__)

# Global reference to the WebSocket process
_websocket_process = None
_process_lock = threading.Lock()

# Constants
WS_PID_FILE = '/tmp/odoo_websocket.pid'
WS_LOG_FILE = '/tmp/odoo_websocket.log'
STARTUP_DELAY = 5  # seconds to wait before starting the WebSocket server

def start_websocket_server():
    """
    Start the WebSocket server as a separate process if not already running.
    
    Returns:
        bool: True if the server was successfully started or is already running
    """
    global _websocket_process
    
    with _process_lock:
        # Check if process is already running
        if _websocket_process and _websocket_process.poll() is None:
            _logger.info("WebSocket server is already running")
            return True
            
        # Check if another process might be running it (via PID file)
        if os.path.exists(WS_PID_FILE):
            try:
                with open(WS_PID_FILE, 'r') as f:
                    pid = int(f.read().strip())
                    
                # Check if process with this PID exists
                try:
                    # Sending signal 0 checks if the process exists without affecting it
                    os.kill(pid, 0)
                    _logger.info(f"WebSocket server is already running (PID: {pid})")
                    return True
                except OSError:
                    # Process doesn't exist, clean up PID file
                    _logger.info(f"Stale PID file found ({pid}), cleaning up")
                    os.remove(WS_PID_FILE)
            except (ValueError, FileNotFoundError):
                # Invalid PID or file disappeared, continue
                pass
        
        # Path to the WebSocket server script
        script_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        script_path = os.path.join(script_dir, 'services', 'websocket', 'server.py')
        
        # Check if the script exists
        if not os.path.exists(script_path):
            _logger.error(f"WebSocket server script not found at {script_path}")
            return False
        
        # Get WebSocket configuration
        port = config.get('websocket_port', '8073')
        
        # Start the process
        try:
            _logger.info(f"Starting WebSocket server process on port {port}...")
            
            # Use the same Python interpreter that's running Odoo
            python_exe = sys.executable
            
            # Start the process with redirected output
            _websocket_process = subprocess.Popen(
                [python_exe, script_path, port],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Give it a moment to start
            time.sleep(1)
            
            # Check if it's running
            if _websocket_process.poll() is None:
                _logger.info("✅ WebSocket server process started successfully")
                return True
            else:
                stderr = _websocket_process.stderr.read()
                _logger.error(f"❌ WebSocket server process failed to start: {stderr}")
                return False
                
        except Exception as e:
            _logger.exception(f"Error starting WebSocket process: {e}")
            return False

def stop_websocket_server():
    """
    Stop the WebSocket server process gracefully.
    
    Returns:
        bool: True if the server was successfully stopped or wasn't running
    """
    global _websocket_process
    
    with _process_lock:
        if not _websocket_process:
            _logger.info("No WebSocket process to stop")
            return True
            
        try:
            # Check if process is still running
            if _websocket_process.poll() is None:
                _logger.info("Stopping WebSocket server process...")
                
                # Try to terminate gracefully first
                _websocket_process.terminate()
                
                # Wait for it to exit
                try:
                    _websocket_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't exit
                    _logger.warning("WebSocket process did not terminate, forcing kill")
                    _websocket_process.kill()
                
                _logger.info("WebSocket server process stopped")
            
            # Clean up PID file if it exists
            if os.path.exists(WS_PID_FILE):
                try:
                    os.remove(WS_PID_FILE)
                    _logger.info(f"Removed PID file: {WS_PID_FILE}")
                except OSError as e:
                    _logger.warning(f"Failed to remove PID file: {e}")
                    
            # Reset process reference
            _websocket_process = None
            return True
                
        except Exception as e:
            _logger.exception(f"Error stopping WebSocket process: {e}")
            _websocket_process = None
            return False

def get_server_status():
    """
    Get the current status of the WebSocket server.
    
    Returns:
        dict: Status information about the WebSocket server
    """
    status = {
        'is_running': False,
        'pid': None,
        'port': config.get('websocket_port', '8073'),
        'log_file': WS_LOG_FILE,
        'uptime': None,
        'connection_count': 0
    }
    
    # Check process reference
    if _websocket_process and _websocket_process.poll() is None:
        status['is_running'] = True
        status['pid'] = _websocket_process.pid
    
    # Check PID file as backup
    elif os.path.exists(WS_PID_FILE):
        try:
            with open(WS_PID_FILE, 'r') as f:
                pid = int(f.read().strip())
                
            # Check if process exists
            try:
                os.kill(pid, 0)  # Signal 0 checks process existence
                status['is_running'] = True
                status['pid'] = pid
            except OSError:
                # Process doesn't exist
                pass
        except (ValueError, FileNotFoundError):
            # Invalid PID or file disappeared
            pass
    
    # Get connection count from the last line of the log if available
    if os.path.exists(WS_LOG_FILE):
        try:
            with open(WS_LOG_FILE, 'r') as f:
                # Get file creation time for uptime calculation
                stats = os.stat(WS_LOG_FILE)
                creation_time = stats.st_ctime
                status['uptime'] = int(time.time() - creation_time)
                
                # Look for connection count in the log (basic approach)
                for line in f:
                    if "Active connections:" in line:
                        try:
                            count = line.split("Active connections:")[1].strip()
                            status['connection_count'] = int(count)
                        except (ValueError, IndexError):
                            pass
        except Exception as e:
            _logger.warning(f"Error reading WebSocket log file: {e}")
    
    return status

def restart_websocket_server():
    """
    Restart the WebSocket server.
    
    Returns:
        bool: True if the server was successfully restarted
    """
    stop_websocket_server()
    time.sleep(1)  # Brief pause between stop and start
    return start_websocket_server()

# Register stop function to ensure proper cleanup on Odoo shutdown
atexit.register(stop_websocket_server)

# Delayed auto-start when this module is imported
def _delayed_start():
    """Start the WebSocket server after a delay"""
    _logger.info(f"Waiting {STARTUP_DELAY} seconds before starting WebSocket server...")
    time.sleep(STARTUP_DELAY)
    start_websocket_server()

# Start the WebSocket server in a background thread when module is loaded
# This ensures we don't block Odoo's initialization
threading.Thread(target=_delayed_start, daemon=True).start()