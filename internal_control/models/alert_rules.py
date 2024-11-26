from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError
import pytz

lagos_timezone = pytz.timezone("Africa/Lagos")

class alert_rules(models.Model):
    _name = 'alert.rules'
    _description = "alert rules for exception management"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char(string="Name", required=True, Tracking=True)
    narration = fields.Html(string="narration", required=True)
    sql_text = fields.Text(string="SQL Query", required=True, Tracking=True)
    frequency_id = fields.Many2one('exception.frequency', string="Frequency", required=True)
    # process_category = fields.Selection(selection=lambda self: self._get_all_process_category(), string="Process Category")
    process_id = fields.Many2one('process', string="Process")
    process_category_id = fields.Many2one('process.category', string="Process Category", required=True)
    email_to = fields.Char(string="Email To", Tracking=True)
    status_id = fields.Many2one('alert.rules.status', string="Alert Status") 
    alert_group_id = fields.Many2one('alert.group', string="Alert Group")
    process_id = fields.Many2one('process', string="Process", domain="[('process_category_id', '=', process_category_id)]")
    risk_rating = fields.Many2one("case.rating", string="Risk Rating")
    date_created = fields.Datetime(string="created_at", default=datetime.now(lagos_timezone).replace(tzinfo=None))
    last_checked = fields.Datetime(string="last_checked", default=datetime.now(lagos_timezone).replace(tzinfo=None))

            
    
    @api.model
    def process_alert_rules(self):
        alert_rules = self.search([("status_id.code", "=", True)])
    
        if len(alert_rules) > 0:
            for rule in alert_rules:
                self.process(rule)
        else:
            pass
            
    
    def process(self, rule):
        last_checked = rule.last_checked
        unit = rule.frequency_id.name
        period = rule.frequency_id.period
        next_check = ''
        
       
        
        if unit == 'minutes':
            next_check = last_checked + timedelta(minutes=period)
            
            
        elif unit == 'hourly':
            next_check = last_checked + timedelta(hours=period)

            
        elif unit == 'daily':
            next_check = last_checked + timedelta(days=period)
            
        elif unit == 'weekly':
            next_check = last_checked + timedelta(weeks=period)
            
        elif unit == 'monthly':
            next_check = last_checked + relativedelta(months=period)
            
        elif unit == 'yearly':
            next_check = last_checked + relativedelta(years=period)
        else:
            raise ValidationError(f"Unsupported Unit")
        
        current_time_lagos = datetime.now(lagos_timezone)
        print(next_check)
        
        if current_time_lagos.year == next_check.year and current_time_lagos.month == next_check.month and  current_time_lagos.day == next_check.day and current_time_lagos.hour == next_check.hour and current_time_lagos.minute == next_check.minute:
            print("sent ") 
            try:
              self.env.cr.execute(rule.sql_text)
              rows = self.env.cr.fetchall()
              
              template = self.env.ref('internal_control.alert_rules_mail_template')
              
              if template:
                    
                    template.with_context(
                    data={
                        "email": rule.alert_group.email,
                        "branch_name": rule.process_id.branch_id.branch_name,
                    }  # Pass any data you need here
                    ).send_mail(rule.id, force_send=True)
                  
                    rule.write({'last_checked': datetime.now(pytz.utc).replace(tzinfo=None)})
   
     
              else:
                  raise ValidationError("Mail Template Not Found")
                  
        
            except BaseException as e:
                raise ValueError(str(e))