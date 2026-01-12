import os
import logging
import subprocess
import re
from datetime import datetime
from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
from odoo.addons.bus.models.bus import channel_with_db
import platform
import getpass

_logger = logging.getLogger(__name__)


class IcomplyLogProfile(models.Model):
    _name = 'icomply.log.profile'
    _description = 'Log Profile Configuration'
    _order = 'sequence, name'

    name = fields.Char(string='Profile Name', required=True)
    log_file_path = fields.Char(string='Log File Path', required=True)
    description = fields.Text(string='Description')
    active = fields.Boolean(string='Active', default=True)
    sequence = fields.Integer(string='Sequence', default=10)
    color = fields.Integer(string='Color Index', default=0)
    
    # File tracking for monitoring
    last_file_position = fields.Integer(string='Last File Position', default=0)
    last_monitored = fields.Datetime(string='Last Monitored', readonly=True)
    
    # Statistics
    last_accessed = fields.Datetime(string='Last Accessed', readonly=True)
    file_size = fields.Char(string='File Size', compute='_compute_file_info', store=False)
    file_exists = fields.Boolean(string='File Exists', compute='_compute_file_info', store=False)
    line_count = fields.Integer(string='Approx. Lines', compute='_compute_file_info', store=False)
    
    # Display settings
    auto_scroll = fields.Boolean(string='Auto Scroll', default=True)
    show_timestamp = fields.Boolean(string='Show Timestamp', default=True)
    show_level = fields.Boolean(string='Show Level', default=True)
    max_lines = fields.Integer(string='Max Lines to Display', default=1000)
    
    # Monitoring settings
    enable_realtime = fields.Boolean(string='Enable Real-time Monitoring', default=True,
                                     help='Automatically monitor and push log updates via WebSocket')
    monitor_interval = fields.Integer(string='Monitor Interval (seconds)', default=2,
                                      help='How often to check for new logs')
    
    @api.depends('log_file_path')
    def _compute_file_info(self):
        for record in self:
            if record.log_file_path and os.path.exists(record.log_file_path):
                try:
                    record.file_exists = True
                    file_stat = os.stat(record.log_file_path)
                    
                    # Format file size
                    size_bytes = file_stat.st_size
                    if size_bytes < 1024:
                        record.file_size = f"{size_bytes} B"
                    elif size_bytes < 1024 * 1024:
                        record.file_size = f"{size_bytes / 1024:.2f} KB"
                    elif size_bytes < 1024 * 1024 * 1024:
                        record.file_size = f"{size_bytes / (1024 * 1024):.2f} MB"
                    else:
                        record.file_size = f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
                    
                    # Estimate line count (rough approximation)
                    with open(record.log_file_path, 'rb') as f:
                        record.line_count = sum(1 for _ in f)
                except Exception as e:
                    _logger.error(f"Error reading file info for {record.log_file_path}: {e}")
                    record.file_exists = False
                    record.file_size = "Error"
                    record.line_count = 0
            else:
                record.file_exists = False
                record.file_size = "N/A"
                record.line_count = 0

    @api.constrains('log_file_path')
    def _check_log_file_path(self):
        for record in self:
            if not record.log_file_path:
                raise ValidationError("Log file path is required.")
            
            # Check if path is absolute
            if not os.path.isabs(record.log_file_path):
                raise ValidationError("Log file path must be an absolute path.")

    def action_open_terminal(self):
        """Open terminal for this log profile"""
        self.ensure_one()
        self.last_accessed = fields.Datetime.now()
        
        return {
            'type': 'ir.actions.client',
            'tag': 'icomply_terminal',
            'name': f'Terminal - {self.name}',
            'target': 'current',
            'params': {
                'profile_id': self.id,
            }
        }

    def action_test_log_file(self):
        """Test if log file is accessible and attempt auto-fix"""
        self.ensure_one()
        file_path = self.log_file_path

        if not file_path or not os.path.exists(file_path):
            raise UserError(f"Log file does not exist: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline()

            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Success',
                    'message': f'Log file is accessible. First line: {first_line[:100]}...',
                    'type': 'success',
                    'sticky': False,
                }
            }

        except PermissionError:
            # Try to auto-fix permissions
            fix_result = self._attempt_permission_fix(file_path)
            
            if fix_result['success']:
                # Try reading again after successful fix
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        first_line = f.readline()
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'title': 'Fixed!',
                            'message': 'Permissions were automatically corrected.',
                            'type': 'success',
                            'sticky': False,
                        }
                    }
                except Exception as retry_error:
                    _logger.error(f"Still cannot read after permission fix: {retry_error}")
            
            # Return the manual fix instructions
            return fix_result['message']

        except Exception as e:
            raise UserError(f"Cannot read log file: {str(e)}")

    def _attempt_permission_fix(self, file_path):
        """Attempt to fix file permissions automatically"""
        current_user = getpass.getuser()
        os_name = platform.system().lower()
        
        # Prepare the fix command
        if 'windows' in os_name:
            fix_cmd = f'icacls "{file_path}" /grant "{current_user}":(R)'
        else:  # Linux / Unix
            fix_cmd = f'sudo chown {current_user}:{current_user} "{file_path}" && sudo chmod 640 "{file_path}"'
        
        try:
            if 'windows' in os_name:
                # Try to grant read permissions
                cmd = ['icacls', file_path, '/grant', f'{current_user}:(R)']
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                
                if result.returncode == 0:
                    _logger.info(f"Successfully granted permissions to {file_path}")
                    return {
                        'success': True,
                        'message': None
                    }
                else:
                    _logger.warning(f"Failed to grant permissions: {result.stderr}")
            else:  # Linux / Unix
                # Try to change ownership and permissions
                cmd1 = ['sudo', 'chown', f'{current_user}:{current_user}', file_path]
                cmd2 = ['sudo', 'chmod', '640', file_path]
                
                result1 = subprocess.run(cmd1, capture_output=True, text=True, timeout=10)
                result2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=10)
                
                if result1.returncode == 0 and result2.returncode == 0:
                    _logger.info(f"Successfully changed ownership and permissions for {file_path}")
                    return {
                        'success': True,
                        'message': None
                    }
                else:
                    _logger.warning(f"Failed to change permissions: {result1.stderr} {result2.stderr}")
            
        except subprocess.TimeoutExpired:
            _logger.warning(f"Permission fix timeout for {file_path}")
        except FileNotFoundError:
            _logger.error(f"Command not found when attempting permission fix")
        except Exception as e:
            _logger.error(f"Error attempting permission fix: {e}")
        
        # Return manual fix instructions
        return {
            'success': False,
            'message': {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Permission Denied',
                    'message': (
                        f'Odoo cannot read the file: {file_path}\n\n'
                        f'Automatic fix failed. Run this command as administrator:\n\n{fix_cmd}'
                    ),
                    'type': 'warning',
                    'sticky': True,
                }
            }
        }

    def action_refresh_info(self):
        """Refresh file information"""
        self._compute_file_info()
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Refreshed',
                'message': 'File information updated',
                'type': 'success',
            }
        }

    @api.model
    def action_open_profile_list(self):
        """Open log profiles list"""
        return {
            'name': 'Log Profiles',
            'type': 'ir.actions.act_window',
            'res_model': 'icomply.log.profile',
            'view_mode': 'kanban,tree,form',
            'target': 'current',
        }
    
    # ==================== WEBSOCKET MONITORING SYSTEM ====================
    
    @api.model
    def _monitor_log_files_cron(self):
        """
        Cron job to monitor active log files and send updates via WebSocket.
        This should be called every 1-5 seconds by an Odoo scheduled action.
        """
        profiles = self.search([
            ('active', '=', True),
            ('enable_realtime', '=', True)
        ])
        
        _logger.debug(f"Monitoring {len(profiles)} active log profiles")
        
        for profile in profiles:
            try:
                new_logs = profile._read_new_logs()
                if new_logs:
                    _logger.info(f"Found {len(new_logs)} new logs for profile '{profile.name}'")
                    profile.send_log_to_clients(profile.id, new_logs)
                    profile.last_monitored = fields.Datetime.now()
            except Exception as e:
                _logger.error(f"Error monitoring profile '{profile.name}': {e}", exc_info=True)

    def _read_new_logs(self):
        """Read new logs from file since last check"""
        self.ensure_one()
        
        if not self.log_file_path or not os.path.exists(self.log_file_path):
            _logger.warning(f"Log file does not exist: {self.log_file_path}")
            return []
        
        try:
            file_size = os.path.getsize(self.log_file_path)
            
            # Check if file was truncated (file size is less than last position)
            if file_size < self.last_file_position:
                _logger.info(f"Log file '{self.name}' was truncated, resetting position")
                self.last_file_position = 0
            
            # No new data
            if file_size == self.last_file_position:
                return []
            
            new_logs = []
            
            with open(self.log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Seek to last known position
                f.seek(self.last_file_position)
                
                # Read new lines
                lines = f.readlines()
                
                # Update file position
                self.last_file_position = f.tell()
                
                # Parse each line
                for line in lines:
                    line = line.strip()
                    if line:  # Skip empty lines
                        parsed_log = self._parse_log_line(line)
                        new_logs.append(parsed_log)
            
            return new_logs
            
        except Exception as e:
            _logger.error(f"Error reading new logs from {self.log_file_path}: {e}", exc_info=True)
            return []

    def _parse_log_line(self, line):
        """
        Parse a log line and extract timestamp, level, and message.
        Customize this based on your log format.
        """
        # Default log entry
        log_entry = {
            'message': line,
            'type': 'info',
            'level': 'INFO',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Try to parse standard Python/Odoo log format
        # Example: "2024-01-15 10:30:45,123 12345 INFO dbname module.name: Message here"
        
        # Pattern 1: Standard Python logging format
        pattern1 = r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})[,\s]+.*?\s+(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+.*?:\s*(.+)$'
        match = re.match(pattern1, line, re.IGNORECASE)
        
        if match:
            timestamp_str, level, message = match.groups()
            log_entry['timestamp'] = timestamp_str
            log_entry['level'] = level.upper()
            log_entry['message'] = message.strip()
            log_entry['type'] = self._level_to_type(level)
            return log_entry
        
        # Pattern 2: Simple timestamp + level format
        # Example: "[2024-01-15 10:30:45] ERROR: Something went wrong"
        pattern2 = r'^\[?(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]?\s+(DEBUG|INFO|WARNING|ERROR|CRITICAL)[:\s]+(.+)$'
        match = re.match(pattern2, line, re.IGNORECASE)
        
        if match:
            timestamp_str, level, message = match.groups()
            log_entry['timestamp'] = timestamp_str
            log_entry['level'] = level.upper()
            log_entry['message'] = message.strip()
            log_entry['type'] = self._level_to_type(level)
            return log_entry
        
        # Pattern 3: Just level at start
        # Example: "ERROR: Connection failed"
        pattern3 = r'^(DEBUG|INFO|WARNING|ERROR|CRITICAL)[:\s]+(.+)$'
        match = re.match(pattern3, line, re.IGNORECASE)
        
        if match:
            level, message = match.groups()
            log_entry['level'] = level.upper()
            log_entry['message'] = message.strip()
            log_entry['type'] = self._level_to_type(level)
            return log_entry
        
        # If no pattern matches, try to detect level from keywords
        line_upper = line.upper()
        if 'ERROR' in line_upper or 'FAILED' in line_upper or 'EXCEPTION' in line_upper:
            log_entry['type'] = 'error'
            log_entry['level'] = 'ERROR'
        elif 'WARNING' in line_upper or 'WARN' in line_upper:
            log_entry['type'] = 'warning'
            log_entry['level'] = 'WARNING'
        elif 'SUCCESS' in line_upper or 'COMPLETED' in line_upper or 'DONE' in line_upper:
            log_entry['type'] = 'success'
            log_entry['level'] = 'INFO'
        
        return log_entry

    def _level_to_type(self, level):
        """Convert log level to frontend type"""
        level_upper = level.upper()
        mapping = {
            'DEBUG': 'info',
            'INFO': 'info',
            'WARNING': 'warning',
            'ERROR': 'error',
            'CRITICAL': 'error',
        }
        return mapping.get(level_upper, 'info')

    def send_log_to_clients(self, profile_id, logs):
        """Send logs via WebSocket to all connected clients"""
        if not logs:
            return
        
        channel = f'icomply_logs_realtime_{profile_id}'
        
        try:
            self.env['bus.bus']._sendone(
                channel_with_db(self.env.cr.dbname, channel),
                'new_logs',
                {
                    'profile_id': profile_id,
                    'logs': logs,
                }
            )
            _logger.debug(f"Sent {len(logs)} logs to channel '{channel}'")
        except Exception as e:
            _logger.error(f"Error sending logs to WebSocket channel: {e}", exc_info=True)

    def action_reset_file_position(self):
        """Reset file position to start reading from beginning"""
        self.ensure_one()
        self.last_file_position = 0
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Reset',
                'message': 'File position reset to beginning',
                'type': 'success',
            }
        }

    def action_toggle_realtime(self):
        """Toggle real-time monitoring"""
        self.ensure_one()
        self.enable_realtime = not self.enable_realtime
        status = "enabled" if self.enable_realtime else "disabled"
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Real-time Monitoring',
                'message': f'Real-time monitoring {status}',
                'type': 'success',
            }
        }