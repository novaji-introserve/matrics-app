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
import smtplib
from time import sleep
import logging
from collections import defaultdict

_logger = logging.getLogger(__name__)


class alert_rules(models.Model):
    _name = 'alert.rules'
    _description = "alert rules for exception management"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'
    
    name = fields.Char(string="Name", required=True, Tracking=True)
    narration = fields.Html(string="narration", required=True, tracking=True)
    sql_text = fields.Many2one("process.sql",string="SQL Query", required=True, tracking=True)
    frequency_id = fields.Many2one('exception.frequency', string="Frequency", required=True, tracking=True)
    # process_category_id = fields.Many2one('process.category', string="Process Category", required=True)
    status = fields.Selection(
    [("1", "Active"), ("0", "Inactive")],
    default="1",  # The default value is an integer (1)
    string="Alert Status",
    tracking=True
    )
    specific_email_recipients = fields.Many2many('res.users', "alert_rules_email_rel", "alert_rules_id", "user_id", string="Specific Recipients", required=True, tracking=True)
    alert_id = fields.Many2one("alert.group", string="Alert Group")
    first_owner = fields.Many2one("res.users",string="First Line Owner") 
    second_owner = fields.Many2one("res.users", string="Second Line Owner") 
    process_id = fields.Char(string="Process", tracking=True)
    risk_rating = fields.Selection(
        selection=[("low", "Low"),("medium", "Medium"), ("high", "High")],
        default= "low",  # The default value is the first risk rating
        string="Risk Rating"
    )
    date_created = fields.Datetime(
        string="created_at",
        default=fields.Datetime.now()
    )
    last_checked = fields.Datetime(string="last_checked", default=fields.Datetime.now())





    @api.onchange('sql_text')
    def onchange_sql_text(self):
        if self.sql_text:
            row = self.search([("sql_text.id", "=", self.sql_text.id)])
            
            if len(row) > 0:
                raise ValidationError(f"alert rules for {self.sql_text.name} already exist")
            
    
    @api.model
    def process_alert_rules(self):
    
        alert_rules = self.search([("status", "=", "1")])
    
        if len(alert_rules) > 0:
            for rule in alert_rules:
                self.process(rule)
        else:
            pass
            
    
    def process(self, rule):
        last_checked = rule.last_checked
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
    
            self.send_alert(rule)
    
    
    def send_alert(self, rule):
        try:
                query = ""
                
                if "*" in rule.sql_text.query:
                    
                    query = rule.sql_text.query
                    
                elif "subbranchcode" not in rule.sql_text.query:
                    
                    columns = rule.sql_text.query.split(",")
                    
                    # join the columns into string
                    query_string = ', '.join(columns)
                    
                    # Insert 'subbranchcode' before 'from'
                    query_string = query_string.replace(' from ', ', subbranchcode from ')
                    
                    query = query_string
                    
            
                # first query to create csv
                self.env.cr.execute(f"{query}")
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
                    raise ValidationError("subbranchcode column must be in the sql statement")
                else:
                    branches = []
                   
                    
                    # Loop through the rows and get the value of subbranchcode dynamically
                    for row in rows:
                        subbranchcode = row[subbranchcode_index]  # Accessing subbranchcode dynamically by index
                        
                        if subbranchcode == "" or subbranchcode == None:
                            raise ValidationError("fix empty subbranchcode in the table and try again")
                        elif subbranchcode not in branches:
                            branches.append(subbranchcode)

                    # Initialize a dictionary to store the emails 
                    mailto = set()
                    mailcc = set()
                    
                    
                    if rule.alert_id.tag != "internal":
    
                
                        # distinct branch
                        for branch in branches:
                            
                            
                            
                            # Search for records in the control officer model that match the current branch
        
                            branch_officer = self.env['control.officer'].sudo().search([("branch_id", '=', int(branch))])
                            
                            if branch_officer and rule.alert_id.id == branch_officer.alert_id.id:
                                
                                alert_group = branch_officer.alert_id.email
                                alert_group_cc = branch_officer.alert_id.email_cc
                                # specific mail recepients
                                for user in rule.specific_email_recipients:
                                    mailcc.add(user.email)
                                
                                
                                for user in alert_group:
                                        
                                    mailcc.add(user.email) 
                                     
                                for user in alert_group_cc:
                                        
                                    mailcc.add(user.email) 
                                
                                            # Send the email
                                self.env.cr.execute(f"{query} WHERE subbranchcode = '{branch_officer.branch_id.id}';")
                                rows = self.env.cr.fetchall()
                            
                                    # Get column names dynamically
                                columns = [" ".join(desc[0].split("_")).title() for desc in self.env.cr.description]
                                
                                        # Create a CSV in memory
                                csv_buffer = io.StringIO()
                                csv_writer = csv.writer(csv_buffer)
                                
                    #                # Write headers to the CSV
                                csv_writer.writerow(columns)
                    #                # Write data rows to the CSV
                                for row in rows:
                                    csv_writer.writerow(row)
                                
                                csv_content = csv_buffer.getvalue()
                                csv_buffer.close()
                                
                                
                                   # Step 3: Base64 encode the CSV content
                                encoded_content = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
                                
                                
                        
                                # Create the HTML table for the email
                                
                                table_html = """
                                    <table class="table table-bordered table-hover container-fluid" style="min-width: 100vw; max-width: 100vw; border-collapse: collapse; font-family: Arial, sans-serif; border: none; overflow:auto;">
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
                                header_html = "".join([f"<th style='padding: 8px;'>{header}</th>" for header in columns])
                                    # Create the HTML table for the email
                                
                                
                                    # Generate the table rows
                                rows_html = ""
                                for row in rows[:10]:
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
                                                'name': f'report.csv',
                                                'mimetype': 'text/csv',  # The MIME type for CSV files
                                                'type': 'binary',
                                                'datas': encoded_content,
                                                
                                    }
                                            
                                    attachment_id = self.env['ir.attachment'].create(attachment)
                                            
                                        # get the emails 
                                        
                                            
                                        # record the history
                                    new_alert_history = self.env['alert.history'].create({
                                                "alert_id": alert_id,
                                                "attachment_data": attachment_id.id,
                                                "attachment_link": f"/web/content/{attachment_id.id}?download=true",
                                                "html_body": table_html,
                                                "alert_rule_id": rule.id,
                                                "process_id": rule.process_id,
                                                "risk_rating": rule.risk_rating,
                                                "date_created": rule.date_created,
                                                "last_checked": rule.last_checked,
                                                "email": branch_officer.officer.email,
                                                "email_cc": "",
                                                "narration": rule.narration,
                                                "name": rule.name
                                            
                                    })
                                            
                                            
                                    template.send_mail(new_alert_history.id, force_send=True)
                                    
                            
                                else:
                                    raise ValidationError("Mail Template Not Found")
                                            
                                        
                                
                                  
                            else:
                                
                                alert_group = rule.alert_id.email
                                alert_group_cc = rule.alert_id.email_cc
                                # specific mail recepients
                                for user in rule.specific_email_recipients:
                                    mailcc.add(user.email)
                                
                                
                                for user in alert_group:
                                        
                                    mailto.add(user.email) 
                                     
                                for user in alert_group_cc:
                                        
                                    mailcc.add(user.email) 
                        
                        
                            
                        # check if mailto and mailcc not empty 
                        if len(mailto) > 0 or len(mailcc) > 0: 
                                        # Send the email
                                self.env.cr.execute(f"{query}")
                                rows = self.env.cr.fetchall()
                            
                                    # Get column names dynamically
                                columns = [" ".join(desc[0].split("_")).title() for desc in self.env.cr.description]
                                
                                        # Create a CSV in memory
                                csv_buffer = io.StringIO()
                                csv_writer = csv.writer(csv_buffer)
                                
                    #                # Write headers to the CSV
                                csv_writer.writerow(columns)
                    #                # Write data rows to the CSV
                                for row in rows:
                                    csv_writer.writerow(row)
                                
                                csv_content = csv_buffer.getvalue()
                                csv_buffer.close()
                                
                                
                    #                # Step 3: Base64 encode the CSV content
                                encoded_content = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
                                
                                
                        
                                # Create the HTML table for the email
                                
                                table_html = """
                                    <table class="table table-bordered table-hover container-fluid" style="min-width: 100vw; max-width: 100vw; border-collapse: collapse; font-family: Arial, sans-serif; border: none; overflow:auto;">
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
                                header_html = "".join([f"<th style='padding: 8px;'>{header}</th>" for header in columns])
                                    # Create the HTML table for the email
                                
                                
                                    # Generate the table rows
                                rows_html = ""
                                for row in rows[:10]:
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
                                                'name': f'report.csv',
                                                'mimetype': 'text/csv',  # The MIME type for CSV files
                                                'type': 'binary',
                                                'datas': encoded_content,
                                                
                                    }
                                            
                                    attachment_id = self.env['ir.attachment'].create(attachment)
                                            
                                        # get the emails 
                                        
                                            
                                        # record the history
                                    new_alert_history = self.env['alert.history'].create({
                                                "alert_id": alert_id,
                                                "attachment_data": attachment_id.id,
                                                "attachment_link": f"/web/content/{attachment_id.id}?download=true",
                                                "html_body": table_html,
                                                "alert_rule_id": rule.id,
                                                "process_id": rule.process_id,
                                                "risk_rating": rule.risk_rating,
                                                "date_created": rule.date_created,
                                                "last_checked": rule.last_checked,
                                                "email": ",".join(list(mailto)) if len(list(mailto)) > 0 else "techsupport@novajii.com",
                                                "email_cc": ",".join(list(mailcc)),
                                                "narration": rule.narration,
                                                "name": rule.name
                                            
                                    })
                                            
                                            
                                    template.send_mail(new_alert_history.id, force_send=True)
                                    
                            
                                else:
                                    raise ValidationError("Mail Template Not Found")
                                       
                                
                    else:
                        # internal user
                        mailto.add(rule.first_owner)
                         # specific mail recepients
                        for user in rule.specific_email_recipients:
                            mailto.add(user.email)
                        
                        mailcc.add(rule.second_owner)
                        for user in rule.alert_id.alert_group_cc:
                                            
                            mailcc.add(user.email) 
                        
                        
                    
                

                     # Send the email
                    self.env.cr.execute(f"{query}")
                    rows = self.env.cr.fetchall()
                
                        # Get column names dynamically
                    columns = [" ".join(desc[0].split("_")).title() for desc in self.env.cr.description]
                    
                               # Create a CSV in memory
                    csv_buffer = io.StringIO()
                    csv_writer = csv.writer(csv_buffer)
                    
        #                # Write headers to the CSV
                    csv_writer.writerow(columns)
        #                # Write data rows to the CSV
                    for row in rows:
                        csv_writer.writerow(row)
                    
                    csv_content = csv_buffer.getvalue()
                    csv_buffer.close()
                    
                    
        #                # Step 3: Base64 encode the CSV content
                    encoded_content = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
                    
                    
            
                    # Create the HTML table for the email
                    
                    table_html = """
                        <table class="table table-bordered table-hover container-fluid" style="min-width: 100vw; max-width: 100vw; border-collapse: collapse; font-family: Arial, sans-serif; border: none; overflow:auto;">
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
                    header_html = "".join([f"<th style='padding: 8px;'>{header}</th>" for header in columns])
                        # Create the HTML table for the email
                    
                    
                        # Generate the table rows
                    rows_html = ""
                    for row in rows[:10]:
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
                                    'name': f'report.csv',
                                    'mimetype': 'text/csv',  # The MIME type for CSV files
                                    'type': 'binary',
                                    'datas': encoded_content,
                                    
                        }
                                
                        attachment_id = self.env['ir.attachment'].create(attachment)
                                
                            # get the emails 
                            
                                
                            # record the history
                        new_alert_history = self.env['alert.history'].create({
                                    "alert_id": alert_id,
                                    "attachment_data": attachment_id.id,
                                    "attachment_link": f"/web/content/{attachment_id.id}?download=true",
                                    "html_body": table_html,
                                    "alert_rule_id": rule.id,
                                    "process_id": rule.process_id,
                                    "risk_rating": rule.risk_rating,
                                    "date_created": rule.date_created,
                                    "last_checked": rule.last_checked,
                                    "email": ",".join(list(mailto)) if len(list(mailto)) else "techsupport@novajii.com",
                                    "email_cc": ",".join(list(mailcc)),
                                    "narration": rule.narration,
                                    "name": rule.name
                                
                        })
                                
                                
                        template.send_mail(new_alert_history.id, force_send=True)
                        
                
                    else:
                        raise ValidationError("Mail Template Not Found")
                                
                              
                
                      
                       
                            
        except BaseException as e:
            raise ValueError(str(e))
        
        
