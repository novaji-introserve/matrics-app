import os
import logging
import re
from datetime import datetime
from odoo import http
from odoo.http import request
from odoo.exceptions import AccessError, UserError


_logger = logging.getLogger(__name__)

class IcomplyLogsController(http.Controller):
    """HTTP Controller for log viewing endpoints with profile support"""

    @http.route('/icomply/logs/recent', type='json', auth='user')
    def get_recent_logs(self, profile_id=None, limit=100, get_all=False):
        """Get logs from the log file"""
        logs_model = request.env['icomply.logs']
        return logs_model.get_recent_logs(profile_id=profile_id, limit=limit, get_all=get_all)

    @http.route('/icomply/logs/all', type='json', auth='user')
    def get_all_logs(self, profile_id=None, limit=None):
        """Get ALL logs from the log file"""
        logs_model = request.env['icomply.logs']
        result = logs_model.get_all_logs(profile_id=profile_id, limit=limit)
        _logger.info(f"Returning {len(result)} logs to client for profile {profile_id}")
        return result

    @http.route('/icomply/logs/poll', type='json', auth='user')
    def poll_new_logs(self, profile_id=None, last_position=0):
        """Poll for new logs since last position"""
        logs_model = request.env['icomply.logs']
        return logs_model.get_logs_from_file(profile_id=profile_id, last_position=last_position)

    @http.route('/icomply/logs/stats', type='json', auth='user')
    def get_log_stats(self, profile_id=None):
        """Get log statistics"""
        logs_model = request.env['icomply.logs']
        return logs_model.get_log_stats(profile_id=profile_id)

    @http.route('/icomply/logs/broadcast', type='json', auth='user')
    def broadcast_logs(self, profile_id=None, last_position=0):
        """Broadcast new logs via bus service"""
        logs_model = request.env['icomply.logs']
        return {'position': logs_model.broadcast_new_logs(profile_id=profile_id, last_position=last_position)}

    @http.route('/icomply/logs/profile/info', type='json', auth='user')
    def get_profile_info(self, profile_id):
        """Get profile information"""
        profile = request.env['icomply.log.profile'].browse(int(profile_id))
        if not profile.exists():
            return {'error': 'Profile not found'}
        
        return {
            'id': profile.id,
            'name': profile.name,
            'log_file_path': profile.log_file_path,
            'file_exists': profile.file_exists,
            'file_size': profile.file_size,
            'line_count': profile.line_count,
            'auto_scroll': profile.auto_scroll,
            'show_timestamp': profile.show_timestamp,
            'show_level': profile.show_level,
            'max_lines': profile.max_lines,
        }