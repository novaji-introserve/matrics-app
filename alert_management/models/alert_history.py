from odoo import models, fields, api
from datetime import datetime
import time
import uuid

class alert_history(models.Model):
    _name = 'alert.history'
    _description = "alert history"
    _rec_name = "alert_id"
    _order = 'id desc'
    
    alert_id = fields.Char(string="alert_id", required=True, index=True, default=lambda self: self._generate_alert_id())
    attachment_data = fields.Char()
    attachment_link = fields.Char()
    html_body = fields.Html(string="html body")
    ref_id = fields.Reference(selection=[
        ('alert.rules', 'Alert Rules'),
        ('adverse.media', 'Adverse Media'),
        ('res.partner.edd', 'EDD'),
        ('case.management', 'Case'),
        ('case.manager', 'Case'),
        ('res.partner.screening.result', 'Sanction Screening'),
        ],
        string='Alert Source'
    )

    last_checked = fields.Char()
    risk_rating = fields.Char()
    process_id = fields.Char()
    case_ref = fields.Char()
    case_ref_display = fields.Html('Case Reference', compute='_compute_case_ref_display', sanitize=False)
    case_id= fields.Char()
    name = fields.Char()
    date_created = fields.Char()
    narration = fields.Char()
    email = fields.Char()
    email_cc = fields.Char()
    time = fields.Char(compute='get_time', store=False)
    source = fields.Char(required=True)
    
    user_in_emails = fields.Boolean(compute='_compute_user_in_emails', search='_search_user_in_emails')
    
    # TEMPLATE CONFIGURATION INTEGRATION
    template_config_id = fields.Many2one(
        'email.template.config', 
        string='Email Template Config',
        help="Template configuration used for this email"
    )
    
    # DYNAMIC COMPUTED FIELDS FOR EMAIL TEMPLATE
    dynamic_logo_url = fields.Char(compute='_compute_template_values')
    dynamic_company_name = fields.Char(compute='_compute_template_values')
    dynamic_phone = fields.Char(compute='_compute_template_values')
    dynamic_email = fields.Char(compute='_compute_template_values') 
    dynamic_website = fields.Char(compute='_compute_template_values')
    dynamic_primary_color = fields.Char(compute='_compute_template_values')
    dynamic_font_family = fields.Char(compute='_compute_template_values')
    
    @api.depends('case_ref')
    def _compute_case_ref_display(self):
        for record in self:
            if record.case_ref:
                record.case_ref_display = f'<a href="{record.case_ref}" target="_self" style="color: #875A7B; font-weight: bold; text-decoration: none; font-size: 18px;">View Case</a>'
            else:
                record.case_ref_display = ''

    def open_case_ref(self):
        """Handle navigation within Odoo"""
        return {
            'type': 'ir.actions.act_url',
            'url': self.case_ref,
            'target': 'self',
        }

    @api.model
    def _generate_alert_id(self):
        """Generates a unique Alert ID."""
        return f"ALERT{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    
    @api.depends('email', 'email_cc')
    def _compute_user_in_emails(self):
        for rec in self:
            current_user_email = self.env.user.email
            rec.user_in_emails = current_user_email and (current_user_email in (rec.email or "").split(',')) or (current_user_email and current_user_email in (rec.email_cc or "").split(',')) or False

    @api.model
    def _search_user_in_emails(self, operator, value):
        if self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer'):
            return [('id', '!=', False)]

        if operator == '=':
            if value:
                domain = ['|', ('email', 'ilike', self.env.user.email), ('email_cc', 'ilike', self.env.user.email)]
            else:
                domain = ['&', ('email', 'not ilike', self.env.user.email), ('email_cc', 'not ilike', self.env.user.email)]
            return domain
        elif operator == '!=':
            if value:
                domain = ['&', ('email', 'not ilike', self.env.user.email), ('email_cc', 'not ilike', self.env.user.email)]
            else:
                domain = ['|', ('email', 'ilike', self.env.user.email), ('email_cc', 'ilike', self.env.user.email)]
            return domain

        return []
    
    @api.onchange("last_checked")
    def get_time(self):
        for record in self:
            if record.last_checked:
                record.time = self._convert_to_time(record['last_checked'])
            else:
                record.time = None
   
    def read(self, fields=None, load='_classic_read'):
        """Override read to convert datetime formats"""
        records = super(alert_history, self).read(fields, load)

        for record in records:
            if 'date_created' in record:
                record['date_created'] = self._convert_to_date(record['date_created'])
            if 'last_checked' in record:
                record['last_checked'] = self._convert_to_date(record['last_checked'])

        return records
    
    def _convert_to_time(self, datetime_value):
        """Convert a datetime string to a time string in HH:MM:SS format."""
        if datetime_value:
            datetime_obj = datetime.strptime(datetime_value, '%Y-%m-%d %H:%M:%S')
            return datetime_obj.strftime('%H:%M:%S') if datetime_obj else None
        return None
    
    def _convert_to_date(self, datetime_value):
        """Convert a datetime string to a date string in YYYY-MM-DD format."""
        if datetime_value:
            datetime_obj = datetime.fromisoformat(datetime_value)
            return datetime_obj.date().isoformat()
        return None

    def generate_csv(self):
        """Generate CSV download action"""
        url = self.attachment_link
        if url:
            return {
                'type': 'ir.actions.act_url',
                'url': url,
                'target': 'self',
            }
                
    @api.depends('template_config_id')
    def _compute_template_values(self):
        """Compute template values for email - NO HARDCODED CLIENT DATA"""
        for record in self:
            if record.template_config_id:
                config = record.template_config_id
                record.dynamic_logo_url = self._get_logo_url(config)
                record.dynamic_company_name = config.effective_company_name
                record.dynamic_phone = config.custom_phone if not config.use_company_details else (config.company_id.phone or "")
                record.dynamic_email = config.custom_email if not config.use_company_details else (config.company_id.email or "")
                record.dynamic_website = config.custom_website if not config.use_company_details else (config.company_id.website or "")
                record.dynamic_primary_color = config.primary_brand_color or '#007046'
                record.dynamic_font_family = dict(config._fields['font_family'].selection)[config.font_family] if config.font_family else 'Verdana, Arial, sans-serif'
            else:
                # SAFE FALLBACKS - NO CLIENT-SPECIFIC DATA
                record.dynamic_logo_url = ""  # No logo if no config
                record.dynamic_company_name = "Company Name"  # Generic
                record.dynamic_phone = ""  # Empty
                record.dynamic_email = ""  # Empty
                record.dynamic_website = ""  # Empty
                record.dynamic_primary_color = "#007046"  # Generic color
                record.dynamic_font_family = "Verdana, Arial, sans-serif"
        
    def _get_logo_url(self, config):
        """Get logo URL for email template - FIXED VERSION"""
        try:
            if config.use_company_logo and config.company_id and config.company_id.logo:
                # Company logo - ensure it's properly formatted as string
                logo_data = config.company_id.logo
                # Convert bytes to string if needed
                if isinstance(logo_data, bytes):
                    logo_data = logo_data.decode('utf-8')
                elif not isinstance(logo_data, str):
                    logo_data = str(logo_data)
                
                return f"data:image/png;base64,{logo_data}"
                
            elif config.custom_logo:
                # Custom logo - ensure it's properly formatted as string  
                logo_data = config.custom_logo
                # Convert bytes to string if needed
                if isinstance(logo_data, bytes):
                    logo_data = logo_data.decode('utf-8')
                elif not isinstance(logo_data, str):
                    logo_data = str(logo_data)
                    
                return f"data:image/png;base64,{logo_data}"
            
            # No logo available
            return ""
            
        except Exception as e:
            # Log the error for debugging
            import logging
            _logger = logging.getLogger(__name__)
            _logger.warning(f"Error getting logo URL: {str(e)}")
            return ""