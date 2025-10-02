import os
import logging
import subprocess
from datetime import datetime
from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
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