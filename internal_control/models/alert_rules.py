from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError
import pytz
from pytz import timezone

lagos_timezone = pytz.timezone('Africa/Lagos')

# Get the current time in Lagos timezone
current_time = datetime.now(lagos_timezone)

utc_time = current_time.astimezone(timezone('UTC'))

class alert_rules(models.Model):
    _name = 'alert.rules'
    _description = "alert rules for exception management"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char(string="Name", required=True, Tracking=True)
    narration = fields.Html(string="narration", required=True)
    sql_text = fields.Many2one("process.sql",string="SQL Query", required=True)
    frequency_id = fields.Many2one('exception.frequency', string="Frequency", required=True)
    process_id = fields.Many2one('process', string="Process")
    process_category_id = fields.Many2one('process.category', string="Process Category", required=True)
    email_to = fields.Char(string="Email To", Tracking=True)
    status = fields.Selection(
    [("1", "Active"), ("0", "Inactive")],
    default="1",  # The default value is an integer (1)
    string="Alert Status"
    )
    alert_group_id = fields.Many2one('alert.group', string="Alert Group")
    process_id = fields.Many2one('process', string="Process", domain="[('process_category_id', '=', process_category_id)]")
    risk_rating = fields.Many2one("case.rating", string="Risk Rating")
    date_created = fields.Datetime(
    string="created_at", 
    default=lambda self:  utc_time.replace(tzinfo=None) 
    )
    last_checked = fields.Datetime(string="last_checked", default=lambda self:  utc_time.replace(tzinfo=None))
    branch_id = fields.Many2one('tbl.branch', string="Branch", required=True)


    @api.onchange('sql_text')
    def onchange_sql_text(self):
        if self.sql_text:
            row = self.search([("sql_text.id", "=", self.sql_text.id)])
            
            if len(row) > 0:
                raise ValidationError(f"alert rules for {self.sql_text.name} already exist")
            
    
    @api.model
    def process_alert_rules(self):
        pass
        alert_rules = self.search([("status", "=", "1")])
    
        if len(alert_rules) > 0:
            for rule in alert_rules:
                self.process(rule)
        else:
            pass
            
    
    def process(self, rule):
        last_checked = rule.last_checked.astimezone(lagos_timezone)
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
        
        print("next time in lagos")
        print(next_check)
        print("************************************************")
        
        if current_time_lagos.year == next_check.year and current_time_lagos.month == next_check.month and  current_time_lagos.day == next_check.day and current_time_lagos.hour == next_check.hour and current_time_lagos.minute == next_check.minute:
            print("sent ") 
            try:
                self.env.cr.execute(rule.sql_text.query)
                rows = self.env.cr.fetchall()
                
                column_headers = [description[0] for description in self.env.cr.description]
                 
                # Create the HTML table for the email
                table_html = "<table class='table table-bordered table-responsive'>"
                table_html += "<thead><tr>"
                
                # Add headers to the table
                for th_data in column_headers:
                    table_html += f"<th>{th_data}</th>"
                    table_html += "</tr></thead><tbody>"
                
                # Add the records to the table
                for row in rows:
                        table_html += "<tr>"
                        for td_data in row:
                            table_html += f"<td>{td_data}</td>"
                        table_html += "</tr>"
                
                table_html += "</tbody></table>"
                
                template = self.env.ref('internal_control.alert_rules_mail_template')
                
                if template:
                    
                                      
                        template.with_context(
                            table_html = table_html,
                            branch_name = rule.branch_id.branch_name
                            ).send_mail(rule.id, force_send=True)
                        
                    
                        rule.last_checked = utc_time.replace(tzinfo=None)
    
        
                else:
                  raise ValidationError("Mail Template Not Found")
                  
        
            except BaseException as e:
                raise ValueError(str(e))