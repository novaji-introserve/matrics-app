import os
import logging
from datetime import datetime
from odoo import models, fields, api, http
from odoo.exceptions import AccessError, UserError
from odoo.http import request
from odoo.tools.config import config  

_logger = logging.getLogger(__name__)


class IcomplyLogs(models.TransientModel):  
    _name = 'icomply.logs'
    _description = 'Real-time Log Viewer'

    def _get_log_file_path(self, log_file_path=None):
        """Resolve log file path: param → Odoo config → None"""
        if log_file_path:
            return log_file_path
        return config.get("logfile")  # None if logs go to stdout

    def _is_valid_log_line(self, line):
        """Check if a line follows the expected Odoo log format"""
        if not line or len(line) < 20:
            return False
        try:
            date_part = line[:10]
            datetime.strptime(date_part, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def _tail(self, file, n):
        """Read last n lines from file efficiently"""
        file.seek(0, 2)  # Go to end of file
        file_size = file.tell()
        if file_size == 0:
            return []
        avg_line_length = 100
        read_size = min(file_size, n * avg_line_length)
        file.seek(max(0, file_size - read_size))
        lines = file.readlines()
        while len(lines) < n and file.tell() > read_size:
            read_size *= 2
            file.seek(max(0, file_size - read_size))
            lines = file.readlines()
        return lines[-n:]

 
    @api.model
    def get_logs_from_file(self, log_file_path=None, limit=1000, last_position=0):
        log_file_path = self._get_log_file_path(log_file_path)
        if not log_file_path or not os.path.exists(log_file_path):
            _logger.warning("No logfile configured or found.")
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
                    try:
                        line = line.strip()
                        if not line or not self._is_valid_log_line(line):
                            continue
                        parts = line.split(' ', 5)
                        if len(parts) >= 5:
                            timestamp_str = f"{parts[0]} {parts[1]}".split(',')[0]
                            try:
                                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                continue
                            log_id = f"{timestamp.timestamp()}-{log_counter}"
                            log_counter += 1
                            logs.append({
                                'id': log_id,
                                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                                'process_id': parts[2],
                                'level': parts[3],
                                'logger_name': parts[4],
                                'module': parts[5].split(':')[0] if ':' in parts[5] else '',
                                'message': parts[5].split(':', 1)[1].strip() if ':' in parts[5] else parts[5],
                            })
                    except Exception:
                        continue
        except Exception as e:
            _logger.error(f"Error accessing log file: {e}")

        return {'logs': logs, 'position': current_position}

    @api.model
    def get_recent_logs(self, limit=100):
        log_file_path = self._get_log_file_path()
        if not log_file_path or not os.path.exists(log_file_path):
            _logger.warning("No logfile configured or found.")
            return []

        logs = []
        try:
            with open(log_file_path, 'r', encoding="utf-8") as file:
                lines = self._tail(file, limit * 2)
                log_counter = 0
                for line in lines:
                    try:
                        line = line.strip()
                        if not line or not self._is_valid_log_line(line):
                            continue
                        parts = line.split(' ', 5)
                        if len(parts) >= 5:
                            timestamp_str = f"{parts[0]} {parts[1]}".split(',')[0]
                            try:
                                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                continue
                            level_mapping = {
                                'DEBUG': 'info',
                                'INFO': 'info',
                                'WARNING': 'warning',
                                'ERROR': 'error',
                                'CRITICAL': 'error',
                            }
                            log_id = f"{timestamp.timestamp()}-{log_counter}"
                            log_counter += 1
                            logs.append({
                                'id': log_id,
                                'message': parts[5].split(':', 1)[1].strip() if ':' in parts[5] else parts[5],
                                'type': level_mapping.get(parts[3], 'info'),
                                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                                'level': parts[3],
                                'module': parts[5].split(':')[0] if ':' in parts[5] else '',
                            })
                            if len(logs) >= limit:
                                break
                    except Exception:
                        continue
        except Exception as e:
            _logger.error(f"Error reading log file: {e}")

        return logs[-limit:] if len(logs) > limit else logs

    @api.model
    def get_log_stats(self):
        today = datetime.now().date()
        stats = {'error': 0, 'warning': 0, 'info': 0, 'debug': 0}

        log_file_path = self._get_log_file_path()
        if not log_file_path or not os.path.exists(log_file_path):
            _logger.warning("No logfile configured or found.")
            return stats

        try:
            with open(log_file_path, 'r', encoding="utf-8") as file:
                for line in file:
                    try:
                        line = line.strip()
                        if not line or not self._is_valid_log_line(line):
                            continue
                        parts = line.split(' ', 5)
                        if len(parts) >= 4:
                            timestamp_str = f"{parts[0]} {parts[1]}".split(',')[0]
                            try:
                                log_date = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S').date()
                            except ValueError:
                                continue
                            if log_date == today:
                                level = parts[3].lower()
                                if level in stats:
                                    stats[level] += 1
                    except Exception:
                        continue
        except Exception as e:
            _logger.error(f"Error reading log file: {e}")

        return stats


    @api.model
    def broadcast_new_logs(self, last_position=0):
        result = self.get_logs_from_file(last_position=last_position)
        new_logs = result['logs']
        current_position = result['position']

        if new_logs:
            level_mapping = {
                'DEBUG': 'info',
                'INFO': 'info',
                'WARNING': 'warning',
                'ERROR': 'error',
                'CRITICAL': 'error',
            }
            terminal_logs = []
            for log in new_logs:
                terminal_logs.append({
                    'id': log['id'],
                    'message': log['message'],
                    'type': level_mapping.get(log['level'], 'info'),
                    'timestamp': log['timestamp'],
                    'level': log['level'],
                    'module': log['module'],
                })
            channel = 'icomply_logs_realtime'
            self.env['bus.bus']._sendone(channel, 'new_logs', {
                'logs': terminal_logs,
                'position': current_position
            })
        return current_position

   
    @api.model
    def send_test_log(self, message="Test log message from terminal"):
        print(f"[TEST] {message}")
        return True

    @api.model
    def open_logs_terminal(self):
        return {
            'type': 'ir.actions.client',
            'tag': 'icomply_logs_terminal',
            'name': 'System Logs Terminal',
            'target': 'current',
        }



class IcomplyLogsController(http.Controller):

    @http.route('/icomply/logs/recent', type='json', auth='user')
    def get_recent_logs(self, limit=100):
        logs_model = request.env['icomply.logs']
        return logs_model.get_recent_logs(limit)

    @http.route('/icomply/logs/poll', type='json', auth='user')
    def poll_new_logs(self, last_position=0):
        logs_model = request.env['icomply.logs']
        return logs_model.get_logs_from_file(last_position=last_position)

    @http.route('/icomply/logs/stats', type='json', auth='user')
    def get_log_stats(self):
        logs_model = request.env['icomply.logs']
        return logs_model.get_log_stats()

    @http.route('/icomply/logs/broadcast', type='json', auth='user')
    def broadcast_logs(self, last_position=0):
        logs_model = request.env['icomply.logs']
        return {'position': logs_model.broadcast_new_logs(last_position)}

    @http.route('/icomply/logs/test', type='json', auth='user')
    def send_test_log(self, message="Test log from API"):
        logs_model = request.env['icomply.logs']
        return logs_model.send_test_log(message)
