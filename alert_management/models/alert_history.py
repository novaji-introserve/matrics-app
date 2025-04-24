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
        ('res.partner.edd', 'EDD')
        ],
        string='Alert Source'
    )
    last_checked = fields.Char()
    risk_rating = fields.Char()
    process_id = fields.Char()
    name = fields.Char()
    date_created = fields.Char()
    narration = fields.Char()
    email = fields.Char()
    email_cc = fields.Char()
    time = fields.Char(compute='get_time', store=False)
    source = fields.Char(required=True)
    
    user_in_emails = fields.Boolean(compute='_compute_user_in_emails', search='_search_user_in_emails')

    @api.model
    def _generate_alert_id(self):
        """Generates a unique Alert ID."""
        return f"Alert{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

    @api.depends('email', 'email_cc')
    def _compute_user_in_emails(self):
        for rec in self:
            if self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer'):  # Replace with your group's XML ID
                rec.user_in_emails = True  # Chief Compliance Officer sees all
            else:
                current_user_email = self.env.user.email
                rec.user_in_emails = current_user_email and (current_user_email in (rec.email or "").split(',')) or (current_user_email and current_user_email in (rec.email_cc or "").split(',')) or False


    @api.model
    def _search_user_in_emails(self, operator, value):
        if self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer'):
            return [('id', '!=', False)] # Return all records for CCO.

        if operator == '=':
            if value: # True
                domain = ['|', ('email', 'ilike', self.env.user.email), ('email_cc', 'ilike', self.env.user.email)]
            else: # False
                domain = ['&', ('email', 'not ilike', self.env.user.email), ('email_cc', 'not ilike', self.env.user.email)]
            return domain
        elif operator == '!=':
            if value: # True
                domain = ['&', ('email', 'not ilike', self.env.user.email), ('email_cc', 'not ilike', self.env.user.email)]
            else: # False
                domain = ['|', ('email', 'ilike', self.env.user.email), ('email_cc', 'ilike', self.env.user.email)]
            return domain

        return [] # If operator is not = or != return empty domain.
    @api.onchange("last_checked")
    def get_time(self):
        for record in self:
            if record.last_checked:
                record.time = self._convert_to_time(record['last_checked'])
            else:
                record.time = None
        
   
    def read(self, fields=None, load='_classic_read'):
        # Call the super method to get the default behavior
        records = super(alert_history, self).read(fields, load)

        # Process the records to convert datetime to date format
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
            
        return None  # Return None if no value
    
    def _convert_to_date(self, datetime_value):
        """Convert a datetime string to a date string in YYYY-MM-DD format."""
        if datetime_value:
            datetime_obj = datetime.fromisoformat(datetime_value)
            return datetime_obj.date().isoformat()  # Return as YYYY-MM-DD
        return None  #
    
   
   
    def generate_csv(self):
        
            url = self.attachment_link
            if url:
                # Perform the download action
    
                return {
                    'type': 'ir.actions.act_url',
                    'url': url,
                    'target': 'self',
                }
    
    
