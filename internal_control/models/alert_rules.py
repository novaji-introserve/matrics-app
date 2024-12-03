from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError
import pytz
from pytz import timezone
import csv
import io
import base64, time, uuid
import smtplib
from time import sleep
import logging

_logger = logging.getLogger(__name__)


class alert_rules(models.Model):
    _name = 'alert.rules'
    _description = "alert rules for exception management"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'
    
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
    default=fields.Datetime.now()
    )
    last_checked = fields.Datetime(string="last_checked", default=fields.Datetime.now())
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
        
         
        
    
        
        current_time_lagos = fields.Datetime.now()
        if current_time_lagos.year == next_check.year and current_time_lagos.month == next_check.month and  current_time_lagos.day == next_check.day and current_time_lagos.hour == next_check.hour and current_time_lagos.minute == next_check.minute:

            
            rule.write({'last_checked': fields.Datetime.now()})
            print("sent ") 
            self.send_alert(rule)
    
    
    def send_alert(self, rule):
        try:
                # first query to create csv
                self.env.cr.execute(f"{rule.sql_text.query}")
                rows = self.env.cr.fetchall()
                
                # Get column names dynamically
                columns = [desc[0] for desc in self.env.cr.description]

                # Find the index of subbranchcode dynamically
                subbranchcode_index = None
                for i, column in enumerate(columns):
                    if 'subbranchcode' in column.lower():  # Case insensitive search
                        subbranchcode_index = i
                        break

                if subbranchcode_index is None:
                    print("subbranchcode column not found")
                else:
                    branches = []

                    # Loop through the rows and get the value of subbranchcode dynamically
                    for row in rows:
                        subbranchcode = row[subbranchcode_index]  # Accessing subbranchcode dynamically by index
                        
                        if subbranchcode not in branches:
                            branches.append(subbranchcode)

                    # Initialize a dictionary to store the emails by branch
                    branch_emails = {}

                    for branch in branches:
                        
                        # Search for records in the email.branch model that match the current branch
                        email_branch = self.env['email.branch'].search([("branch_id.id", '=', int(branch))])
                        
                        if email_branch:
                                email_list = email_branch.email_list
                                branch_emails[email_branch.branch_id.id] = email_list
                                                    

                    for key, bEmails in branch_emails.items():
                        self.env.cr.execute(f"{rule.sql_text.query} WHERE subbranchcode = '{key}';")
                        rowsForEachBranch = self.env.cr.fetchall()
                
                        # Get column names dynamically
                        columnsForEachBranch = [desc[0] for desc in self.env.cr.description]
                        
                                 # Create a CSV in memory
                        csv_buffer = io.StringIO()
                        csv_writer = csv.writer(csv_buffer)

                        # Write headers to the CSV
                        csv_writer.writerow(columnsForEachBranch)

                        # Write data rows to the CSV
                        for row in rowsForEachBranch:
                            csv_writer.writerow(row)
                        
                        csv_content = csv_buffer.getvalue()
                        csv_buffer.close()
                        
                        
                        # Step 3: Base64 encode the CSV content
                        encoded_content = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')

                        
                        

                        # second query to create table
                     
                 
                        # Create the HTML table for the email
                    
                        table_html = """
                        <table class="table table-bordered table-hover" style="width: 100%; max-width: 100vw; border-collapse: collapse; font-family: Arial, sans-serif; border: 1px solid #ddd; overflow:auto;">
                            <thead style="background-color: #007046; color: #fff; padding: 12px;">
                                <tr>
                                    <!-- Table headers -->
                                    {header_columns}
                                </tr>
                            </thead>
                            <tbody>
                                {table_rows}
                            </tbody>
                        </table>
                        """
                                
                        # Generate the table header
                        header_html = "".join([f"<th style='padding: 8px;'>{header}</th>" for header in columnsForEachBranch])

                        # Generate the table rows
                        rows_html = ""
                        for row in rowsForEachBranch[:10]:
                            rows_html += "<tr>"
                            for cell in row:
                                rows_html += f"<td style='padding: 8px; border: 1px solid #ddd;'>{cell if cell is not None else ''}</td>"
                            rows_html += "</tr>"

                        # Insert the generated HTML into the main table structure
                        table_html = table_html.format(header_columns=header_html, table_rows=rows_html)

                        template = self.env.ref('internal_control.alert_rules_mail_template')
                        
                        if template:
                            

                                
                                # generate random string attached for each alert to be send
                                alert_id = f"Alert{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
                                
                               
                                
                                attachment = {
                                    'name': f'{key}-report.csv',
                                    'mimetype': 'text/csv',  # The MIME type for CSV files
                                    'type': 'binary',
                                    'datas': encoded_content,
                                    
                                }
                                
                                attachment_id = self.env['ir.attachment'].create(attachment)
                                
                                # record the history
                                new_alert_history = self.env['alert.history'].create({
                                    "alert_id": alert_id,
                                    "attachment": attachment_id.id,
                                    "html_body": table_html,
                                    "alert_rule_id": rule.id,
                                    "process_id": rule.process_id.name,
                                    "process_category": rule.process_category_id.name,
                                    "process_category": rule.process_category_id.name,
                                    "risk_rating": rule.risk_rating.name,
                                    "date_created": rule.date_created,
                                    "last_checked": rule.last_checked,
                                    "email": bEmails,
                                    "narration": rule.narration
                                
                                })
                                
                               
                                
                        

                                template.attachment_ids = [(4, attachment_id.id)]  # Attach the attachment to the template
                                
                                self.send_email_with_retries(template, new_alert_history)
                        
                
                        else:
                          raise ValidationError("Mail Template Not Found")
                  
        
        except BaseException as e:
            raise ValueError(str(e))
        
        

    def send_email_with_retries(self, template, new_alert_history):
        
        retries = 3  # Number of retries before failing
        delay = 5  # Delay in seconds between retries

        for attempt in range(retries):
            try:
                template.send_mail(new_alert_history.id, force_send=True)
                _logger.info(f"Email sent successfully on attempt {attempt + 1}")
                return  # Exit if the email is sent successfully
            except smtplib.SMTPServerDisconnected as e:
                _logger.error(f"SMTP server disconnected on attempt {attempt + 1}. Retrying...")
                time.sleep(delay)  # Wait before retrying
            except smtplib.SMTPException as e:
                _logger.error(f"SMTP error occurred: {e}")
                break  # Break the loop if a non-recoverable error occurs
            except Exception as e:
                _logger.error(f"Unexpected error occurred while sending email: {e}")
                break  # Break the loop on any other error

        # _logger.error("Failed to send email after several attempts.")