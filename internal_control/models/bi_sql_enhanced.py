# models/bi_sql_enhanced.py - SIMPLIFIED VERSION FOR JAVASCRIPT APPROACH
from odoo import models, fields, api
from odoo.exceptions import AccessError
import logging
import re

_logger = logging.getLogger(__name__)

class BiSQLViewEnhanced(models.Model):
    _inherit = 'bi.sql.view'
    
    # Add department mapping field
    department_id = fields.Many2one(
        'hr.department', 
        string='Department', 
        required=True,
        help='Select the department that this report belongs to. Only users from this department will see this report.'
    )
    
    # Override state field to change display text
    state = fields.Selection(
        selection_add=[
            ('ui_valid', 'Published'),  # Changed from "Views, Action and Menu Created"
        ],
        ondelete={'ui_valid': 'set default'}
    )
    
    # REMOVED: _get_view method - let JavaScript handle UI security instead
    # This prevents server-side conflicts and caching issues
    
    # SECURITY CONTROL: Control form access when clicking rows
    def get_formview_action(self, access_uid=None):
        """Override to redirect non-CCO users to report data instead of technical forms"""
        
        # Check if user is CCO
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        
        if not is_cco:
            # Non-CCO users get redirected to report data instead of technical form
            if self.state == 'ui_valid':
                return self.action_open_report_data()
            else:
                # If report not ready, show message
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': 'Report Not Ready',
                        'message': f'Report "{self.name}" is not yet ready. Please contact your administrator.',
                        'type': 'warning',
                    }
                }
        else:
            # CCO users get normal form access
            return super().get_formview_action(access_uid)
    
    # JAVASCRIPT INTERFACE: Method for JavaScript to check user access
    @api.model
    def check_user_is_cco(self):
        """Method for JavaScript to check if current user is CCO"""
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        
        _logger.info(f"JavaScript called check_user_is_cco: user={self.env.user.name}, is_cco={is_cco}")
        
        return {
            'is_cco': is_cco,
            'user_name': self.env.user.name,
            'user_id': self.env.user.id,
        }
    
    # KEEP ALL YOUR EXISTING METHODS
    @api.model
    def search(self, domain, offset=0, limit=None, order=None, count=False):
        """Override search to filter reports by department for non-CCO users"""
        
        # Check if user is Chief Compliance Officer
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        
        if not is_cco:
            # Get current user's department
            user_employee = self.env.user.employee_id
            if user_employee and user_employee.department_id:
                # Add department filter to domain
                department_domain = [('department_id', '=', user_employee.department_id.id)]
                domain = domain + department_domain
                _logger.info(f"Non-CCO user {self.env.user.name} filtered to department: {user_employee.department_id.name}")
            else:
                # If user has no department, show no reports
                domain = domain + [('id', '=', False)]
                _logger.warning(f"User {self.env.user.name} has no department assigned - no reports visible")
        else:
            _logger.info(f"CCO user {self.env.user.name} can see all reports")
            
        return super().search(domain, offset, limit, order, count)
    
    @api.model
    def read_group(self, domain, fields, groupby, offset=0, limit=None, orderby=False, lazy=True):
        """Override read_group to filter department groups for non-CCO users"""
        
        # Check if user is Chief Compliance Officer
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        
        if not is_cco and 'department_id' in groupby:
            # Get current user's department
            user_employee = self.env.user.employee_id
            if user_employee and user_employee.department_id:
                # Add department filter to domain for grouping
                department_domain = [('department_id', '=', user_employee.department_id.id)]
                domain = domain + department_domain
            else:
                # If user has no department, show no groups
                domain = domain + [('id', '=', False)]
                
        return super().read_group(domain, fields, groupby, offset, limit, orderby, lazy)
    
    @api.model
    def default_get(self, fields_list):
        """Set default department when creating new report"""
        defaults = super().default_get(fields_list)
        
        # If user has a department, set it as default
        if 'department_id' in fields_list:
            user_employee = self.env.user.employee_id
            if user_employee and user_employee.department_id:
                defaults['department_id'] = user_employee.department_id.id
                
        return defaults
    
    @api.onchange('name')
    def _onchange_name_auto_technical(self):
        """Auto-populate technical name from report name"""
        if self.name and not self.technical_name:
            # Convert name to valid technical name
            technical_name = self._generate_technical_name(self.name)
            self.technical_name = technical_name
    
    def _generate_technical_name(self, report_name):
        """Generate a valid technical name from report name"""
        if not report_name:
            return ''
            
        # Convert to lowercase and replace spaces/special chars
        technical = report_name.lower()
        
        # Replace spaces and special characters with underscores
        technical = re.sub(r'[^a-z0-9_]', '_', technical)
        
        # Remove multiple consecutive underscores
        technical = re.sub(r'_+', '_', technical)
        
        # Remove leading/trailing underscores
        technical = technical.strip('_')
        
        # Limit length (PostgreSQL identifier limit is 63 chars, but keep shorter)
        if len(technical) > 30:
            technical = technical[:30].rstrip('_')
        
        # Ensure it doesn't start with a number
        if technical and technical[0].isdigit():
            technical = 'rpt_' + technical
            
        # Only check uniqueness if record exists (avoid NewId error)
        if self._origin and self._origin.id:
            # Make sure it's unique
            counter = 1
            base_technical = technical
            while self.search([('technical_name', '=', technical), ('id', '!=', self._origin.id)]):
                technical = f"{base_technical}_{counter}"
                counter += 1
            
        return technical or 'new_report'
    
    def action_open_report_data(self):
        """Direct navigation to report data instead of configuration form"""
        self.ensure_one()
        
        if self.state != 'ui_valid':
            # If report not ready, show message
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Report Not Ready',
                    'message': f'Report "{self.name}" is not yet ready. Please complete the setup first.',
                    'type': 'warning',
                }
            }
        
        # Open the report data directly using the existing action
        return self.button_open_view()