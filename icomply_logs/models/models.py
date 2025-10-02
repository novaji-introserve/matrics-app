import os
import logging
import re
from datetime import datetime
from odoo import models, fields, api, http
from odoo.exceptions import AccessError, UserError
from odoo.tools.config import config  

_logger = logging.getLogger(__name__)


class IcomplyLogs(models.TransientModel):  
    _name = 'icomply.logs'
    _description = 'Real-time Log Viewer'

    def _get_log_file_path(self, profile_id=None, log_file_path=None):
        """Resolve log file path: profile → param → Odoo config → None"""
        if profile_id:
            profile = self.env['icomply.log.profile'].browse(profile_id)
            if profile.exists():
                return profile.log_file_path
        
        if log_file_path:
            return log_file_path
            
        return config.get("logfile")

    def _extract_timestamp(self, line):
        """Try to extract timestamp from common log formats"""
        timestamp_patterns = [
            # Odoo format: 2025-03-26 08:59:25
            (r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', '%Y-%m-%d %H:%M:%S'),
            # Nginx/Apache: [26/Mar/2025:08:59:25 +0000]
            (r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})', '%d/%b/%Y:%H:%M:%S'),
            # ISO format: 2025-03-26T08:59:25
            (r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', '%Y-%m-%dT%H:%M:%S'),
            # Syslog: Mar 26 08:59:25
            (r'(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})', '%b %d %H:%M:%S'),
        ]
        
        for pattern, date_format in timestamp_patterns:
            match = re.search(pattern, line)
            if match:
                try:
                    timestamp_str = match.group(1)
                    timestamp = datetime.strptime(timestamp_str, date_format)
                    # For syslog format without year, add current year
                    if date_format == '%b %d %H:%M:%S':
                        timestamp = timestamp.replace(year=datetime.now().year)
                    return timestamp
                except ValueError:
                    continue
        
        return None

    def _extract_level(self, line):
        """Try to extract log level from the line"""
        # Look for common log levels
        level_patterns = [
            (r'\b(CRITICAL|FATAL)\b', 'CRITICAL', 'error'),
            (r'\bERROR\b', 'ERROR', 'error'),
            (r'\b(WARN|WARNING)\b', 'WARNING', 'warning'),
            (r'\bINFO\b', 'INFO', 'info'),
            (r'\bDEBUG\b', 'DEBUG', 'info'),
        ]
        
        for pattern, level, log_type in level_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                return level, log_type
        
        # Check for HTTP status codes
        status_match = re.search(r'\s(\d{3})\s', line)
        if status_match:
            status = int(status_match.group(1))
            if status >= 500:
                return 'ERROR', 'error'
            elif status >= 400:
                return 'WARNING', 'warning'
            else:
                return 'INFO', 'info'
        
        # Default
        return 'INFO', 'info'

    def _parse_log_line(self, line, log_counter):
        """Parse any log line - universal parser"""
        try:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                return None
            
            # Extract timestamp (or use current time as fallback)
            timestamp = self._extract_timestamp(line)
            if not timestamp:
                timestamp = datetime.now()
            
            # Extract log level
            level, log_type = self._extract_level(line)
            
            # Generate unique ID
            log_id = f"{timestamp.timestamp()}-{log_counter}"
            
            # The entire line is the message
            message = line
            
            # Try to extract module/source (anything before a colon or in brackets)
            module = ''
            module_match = re.search(r'^\[?([^\]:\s]+)[\]:]', line)
            if module_match:
                module = module_match.group(1)
            
            return {
                'id': log_id,
                'message': message,
                'type': log_type,
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'level': level,
                'module': module,
            }
            
        except Exception as e:
            _logger.debug(f"Error parsing log line: {e}")
            # Even if parsing fails, return a basic log entry
            return {
                'id': f"parse-error-{log_counter}",
                'message': line.strip() if line else "Empty line",
                'type': 'info',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'level': 'INFO',
                'module': 'unknown',
            }

    @api.model
    def get_logs_from_file(self, profile_id=None, log_file_path=None, limit=1000, last_position=0):
        """Get new logs from file since last position"""
        log_file_path = self._get_log_file_path(profile_id, log_file_path)
        if not log_file_path or not os.path.exists(log_file_path):
            _logger.warning(f"No logfile configured or found: {log_file_path}")
            return {'logs': [], 'position': last_position}

        logs = []
        current_position = last_position

        try:
            with open(log_file_path, 'r', encoding="utf-8") as file:
                file.seek(last_position)
                new_lines = file.readlines()
                current_position = file.tell()

                if not new_lines:
                    return {'logs': [], 'position': current_position}

                log_counter = 0
                for line in new_lines:
                    parsed_log = self._parse_log_line(line, log_counter)
                    if parsed_log:
                        logs.append(parsed_log)
                        log_counter += 1
                        
                        if limit and len(logs) >= limit:
                            break
        except Exception as e:
            _logger.error(f"Error accessing log file: {e}")

        return {'logs': logs, 'position': current_position}

    @api.model
    def get_all_logs(self, profile_id=None, log_file_path=None, limit=None):
        """Get ALL logs from the log file from the beginning"""
        log_file_path = self._get_log_file_path(profile_id, log_file_path)
        if not log_file_path or not os.path.exists(log_file_path):
            _logger.warning(f"No logfile configured or found: {log_file_path}")
            return []

        logs = []
        log_counter = 0
        
        try:
            _logger.info(f"Reading all logs from: {log_file_path}")
            
            with open(log_file_path, 'r', encoding="utf-8", errors='replace') as file:
                for line in file:
                    parsed_log = self._parse_log_line(line, log_counter)
                    if parsed_log:
                        logs.append(parsed_log)
                        log_counter += 1
                        
                        if limit and len(logs) >= limit:
                            break
            
            _logger.info(f"Successfully read {len(logs)} logs from file")
                        
        except Exception as e:
            _logger.error(f"Error reading log file: {e}")
            return []

        return logs

    @api.model
    def get_recent_logs(self, profile_id=None, log_file_path=None, limit=100, get_all=False):
        """Get logs from the log file"""
        if get_all:
            return self.get_all_logs(profile_id=profile_id, log_file_path=log_file_path, limit=limit)
        
        log_file_path = self._get_log_file_path(profile_id, log_file_path)
        if not log_file_path or not os.path.exists(log_file_path):
            _logger.warning(f"No logfile configured or found: {log_file_path}")
            return []

        logs = []
        log_counter = 0
        
        try:
            with open(log_file_path, 'r', encoding="utf-8", errors='replace') as file:
                lines = file.readlines()
                
                for line in lines:
                    parsed_log = self._parse_log_line(line, log_counter)
                    if parsed_log:
                        logs.append(parsed_log)
                        log_counter += 1
                
                if limit and len(logs) > limit:
                    return logs[-limit:]
                    
        except Exception as e:
            _logger.error(f"Error reading log file: {e}")

        return logs

    @api.model
    def get_log_stats(self, profile_id=None, log_file_path=None):
        """Get statistics about today's logs"""
        today = datetime.now().date()
        stats = {'error': 0, 'warning': 0, 'info': 0, 'debug': 0}

        log_file_path = self._get_log_file_path(profile_id, log_file_path)
        if not log_file_path or not os.path.exists(log_file_path):
            _logger.warning(f"No logfile configured or found: {log_file_path}")
            return stats

        try:
            with open(log_file_path, 'r', encoding="utf-8", errors='replace') as file:
                log_counter = 0
                for line in file:
                    parsed_log = self._parse_log_line(line, log_counter)
                    if parsed_log:
                        try:
                            log_date = datetime.strptime(parsed_log['timestamp'], '%Y-%m-%d %H:%M:%S').date()
                            if log_date == today:
                                log_type = parsed_log['type']
                                if log_type in stats:
                                    stats[log_type] += 1
                        except Exception:
                            continue
                    log_counter += 1
        except Exception as e:
            _logger.error(f"Error reading log file: {e}")

        return stats

    @api.model
    def broadcast_new_logs(self, profile_id=None, log_file_path=None, last_position=0):
        """Broadcast new logs via bus service"""
        result = self.get_logs_from_file(profile_id=profile_id, log_file_path=log_file_path, last_position=last_position)
        new_logs = result['logs']
        current_position = result['position']

        if new_logs:
            channel = f'icomply_logs_realtime_{profile_id or "default"}'
            self.env['bus.bus']._sendone(channel, 'new_logs', {
                'logs': new_logs,
                'position': current_position,
                'profile_id': profile_id
            })
        return current_position