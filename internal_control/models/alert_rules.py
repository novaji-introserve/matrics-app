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
import re

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
        string="Created_Date",
        read_only=True)
    last_checked = fields.Datetime(string="Last_Checked", read_only=True)

            
        

    @api.model
    def create(self,vals_list):
       
        vals_list['last_checked'] = fields.Datetime.now()
        
        if 'date_created' not in vals_list:
           vals_list['date_created'] = fields.Datetime.now()
        

            
        res = super(alert_rules, self).create(vals_list)
        return res
    
   
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
        unit = rule.frequency_id.name
        period = rule.frequency_id.period
        next_check = self.calculate_next_check(last_checked, unit, period)

       
        current_time_lagos = fields.Datetime.now()
        if current_time_lagos.year == next_check.year and current_time_lagos.month == next_check.month and  current_time_lagos.day == next_check.day and current_time_lagos.hour == next_check.hour and current_time_lagos.minute == next_check.minute:

            rule.write({'last_checked': fields.Datetime.now()})
    
            self.send_alert(rule)
    
    def calculate_next_check(self, last_checked, unit, period):
        if unit == 'minutes':
            return last_checked + timedelta(minutes=period)
        elif unit == 'hourly':
            return last_checked + timedelta(hours=period)
        elif unit == 'daily':
            return last_checked + timedelta(days=period)
        elif unit == 'weekly':
            return last_checked + timedelta(weeks=period)
        elif unit == 'monthly':
            return last_checked + relativedelta(months=period)
        elif unit == 'yearly':
            return last_checked + relativedelta(years=period)
        else:
            raise ValidationError("Unsupported Unit")


    def format_query(self,rule):
        
        query = ""

        if "*" in rule.sql_text.query:
                    
            query = rule.sql_text.query
                
        elif re.search(r"\w+\.\w+\s+AS\s+\w+", rule.sql_text.query, re.IGNORECASE): 
            query_lower = rule.sql_text.query.lower()
            select_clause, from_clause = rule.sql_text.query.split("FROM", 1)
            select_clause = select_clause.strip()

            # Check if 'alias.branch_id' 
            check_pattern = re.search(r"(\w+)\.branch_id\b", rule.sql_text.query)

            if not check_pattern:
                # Find the alias for res_branch
                match = re.search(r"res_branch\s+(\w+)", from_clause.lower())
                if match:
                    branch_alias = match.group(1)
                    modified_select_clause = f"{select_clause}, {branch_alias}.id AS branch_id"
                    query = f"{modified_select_clause} FROM {from_clause}"
                    
            else:
                match = check_pattern
                branch_alias = match.group(1)
                modified_select_clause = f"{select_clause}, {branch_alias}.branch_id AS branch_id"
                query = f"{modified_select_clause} FROM {from_clause}"

        else:
                    
            if "branch_id" not in rule.sql_text.query:
                
                columns = rule.sql_text.query.split(",")
                
                # join the columns into string
                query_string = ', '.join(columns)
                
                # Insert 'subbranchcode' before 'from'
                query_string = query_string.replace(' from ', ', branch_id from ')
                
                query = query_string
            else:
                
                query = rule.sql_text.query
        

        return query
                        
 
    
    def create_csv(self, columns, rows):
                         
        # check if branch_id is in cloumn and store the index
        pattern = re.compile(r'\bbranch\s*_?\s*id\b', re.IGNORECASE)
        branch_id_indices = [i for i, col in enumerate(columns) if pattern.fullmatch(col)]
        
        # currency pattern
        currency_pattern = r"^-?\d+\.\d{2}$"
    
        # Create a CSV in memory
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        # Filter columns
        filtered_columns = [" ".join(th.split("_")).title() for th in columns if th.lower() != 'branch_id']
                            
        # Write headers to the CSV
        csv_writer.writerow(filtered_columns)
         # Write data rows to the CSV
        for row in rows:
            filtered_row = [cell for index, cell in enumerate(row) if index not in branch_id_indices]
            # Format currency values in the filtered row
            formatted_row = [f"{float(re.sub(r'[^\d\.]', '', str(cell))):,.2f}" if cell is not None and re.match(r'[\$\d,.]+', str(cell)) else cell for cell in filtered_row]
            csv_writer.writerow(formatted_row)
            
        
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()
        
        
           # Step 3: Base64 encode the CSV content
        encoded_content = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')

        return encoded_content
    
    
    def generate_table(self, columns, rows):

                            
        # check if branch_id is in cloumn and store the index
        pattern = re.compile(r'\bbranch\s*_?\s*id\b', re.IGNORECASE)
        branch_id_indices = [i for i, col in enumerate(columns) if pattern.fullmatch(col)]
        
        # check if currency_id is in cloumn and store the index
        currency_pattern = r"^-?\d+\.\d{2}$"
        
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
        header_html = "".join([f"<th style='padding: 8px;'>{" ".join(header.split("_")).title()}</th>" for header in columns if header != 'branch_id'])
        # Create the HTML table for the email
        
        
        # Generate the table rows
        rows_html = ""
        for row in rows[:10]:
            rows_html += "<tr>"
            for index, cell in enumerate(row):
                 # Check currency
                if bool(re.search(currency_pattern, str(cell))) and cell is not None:
                # Format the currency value
                    cell = f"{float(re.sub(r'[^\d\.]', '', str(cell))):,.2f}"  # Format with commas and two decimal places
               
                if index not in branch_id_indices:
                    
                    rows_html += f"<td style='padding: 8px; border: 1px solid #ddd;'>{cell if cell is not None else ''}</td>"
            rows_html += "</tr>"
        
        # Insert the generated HTML into the main table structure
        table_html = table_html.format(header_columns=header_html, table_rows=rows_html)
                    
            
        return table_html

    
    def prepare_email(self, rule, table_html, encoded_content, email, emailcc):

        template = self.env.ref('internal_control.alert_rules_mail_template')
        
        if template:
                
                    
            # generate random string attached for each alert to be send
            alert_id = f"Alert{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
                    
                
                    
            attachment = {
                        'name': 'report.csv',
                        'mimetype': 'text/csv',  # The MIME type for CSV files
                        'type': 'binary',
                        'datas': encoded_content,
                        
            }
                    
            attachment_id = self.env['ir.attachment'].create(attachment)
                    
                        
            # record the history
            new_alert_history = self.env['alert.history'].create({
                        "alert_id": alert_id,
                        "attachment_data": attachment_id.id,
                        "attachment_link": f"/web/content/{attachment_id.id}?download=true",
                        "html_body": table_html,
                        "alert_rule_id": rule.id,
                        "process_id": rule.process_id,
                        "risk_rating": rule.risk_rating,
                        "last_checked": rule.last_checked,
                        "email": ",".join(list(email)) if len(list(email)) > 0 else "techsupport@novajii.com",
                        "email_cc": ",".join(list(emailcc)),
                        "narration": rule.narration,
                        "name": rule.name
                    
            })
                    
                    
            try:
                mail_id =  template.send_mail(new_alert_history.id, force_send=True)
                if not mail_id:
                    history = self.env['alert.history'].browse(new_alert_history.id)
                    
                    if history.exists():
                        history.unlink()
                    
            except Exception as e:
                history = self.env['alert.history'].browse(new_alert_history.id)
                    
                if history.exists():
                    history.unlink()

                
            
    
        else:
            raise ValidationError("Mail Template Not Found")
                                        
                                    
                            
                            

    
    def send_alert(self, rule):
        try:

            query = self.format_query(rule)
    
            # first query to create csv
            self.env.cr.execute(f'{query}')

            rows = self.env.cr.fetchall()
            columns = [desc[0] for desc in self.env.cr.description]
            # Find the index of branch_id dynamically
            branchcode_index = None
    
            for i, column in enumerate(columns):
                
                if 'branch_id' in column.lower():  # Case insensitive search
                    branchcode_index = i
                    break
            if branchcode_index is None:
                raise ValidationError("branchcode column must be in the sql statement")
            else:
                branches = []
                
                # Loop through the rows and get distinct branch_id neglecting null field 
                for row in rows:
                    branchcode = row[branchcode_index]  # Accessing branchcode dynamically by index
                    
            
                    if branchcode == "" or branchcode == None:
                        pass
                    elif branchcode not in branches:
                        branches.append(branchcode) 
            
                
                if rule.alert_id.tag != "internal":

            
                    # distinct branch
                    for branch in branches:

                       
                        branch_officer = self.env['control.officer'].sudo().search([("branch_id", '=', int(branch))])
                        
                        if branch_officer and rule.alert_id.id == branch_officer.alert_id.id:

                            # retrieve data for branch officer by branch 
                            self.env.cr.execute(f"{query} WHERE branch_id = '{branch_officer.branch_id.id}';")

                            rows = self.env.cr.fetchall()
                            columns = [desc[0] for desc in self.env.cr.description]
                            
                            alert_group = branch_officer.alert_id.email
                            alert_group_cc = branch_officer.alert_id.email_cc
                            # specific mail recepients
                            for user in rule.specific_email_recipients:
                                mailcc.add(user.login)
                            
                            
                            for user in alert_group:
                                    
                                mailcc.add(user.login) 
                                
                            for user in alert_group_cc:
                                    
                                mailcc.add(user.login) 
                            
                            encoded_content = self.create_csv(columns, rows)
                            
                            table_html = self.generate_table(columns, rows)
 
                            self.prepare_email(rule, table_html, encoded_content, [branch_officer.officer.email], "")
                            
                        else:

                            mailto = set()
                            mailcc = set()

                             # retrieve all data no partition for alert group
                            self.env.cr.execute(f"{query}")

                            rows = self.env.cr.fetchall()
                            columns = [desc[0] for desc in self.env.cr.description]
                            
                            alert_group = rule.alert_id.email
                            alert_group_cc = rule.alert_id.email_cc
                            # specific mail recepients
                            for user in rule.specific_email_recipients:
                                mailcc.add(user.email)
                            
                            
                            for user in alert_group:
                                    
                                mailto.add(user.email) 
                                
                            for user in alert_group_cc:
                                    
                                mailcc.add(user.email) 
                            
                            encoded_content = self.create_csv(columns, rows)
                           
                            
                            table_html = self.generate_table(columns, rows)
                          
                    
                            # check if mailto and mailcc not empty 
                            if len(mailto) > 0 or len(mailcc) > 0: 
                                # Send the email
                                self.prepare_email(rule, table_html, encoded_content, mailto, mailcc)

                            
                else:
                    # Initialize a dictionary to store the emails 
                    mailto = set()
                    mailcc = set()
                    # internal user
                    mailto.add(rule.first_owner.login)
                    #  # specific mail recepients
                    for user in rule.specific_email_recipients:
                        mailto.add(user.login)
                    
                    
                    mailcc.add(rule.second_owner.login)
                    for user in rule.alert_id.email_cc:
                                        
                        mailcc.add(user.login) 

                    
                      #  Send the email
                    self.env.cr.execute(f"{query}")

                    rows = self.env.cr.fetchall()
                    columns = [desc[0] for desc in self.env.cr.description]
                    
                    encoded_content = self.create_csv(columns, rows)
                                
                    table_html = self.generate_table(columns, rows)
                                
                    self.prepare_email(rule, table_html, encoded_content, mailto, mailcc)
                        
                        
                                
        except BaseException as e:
            raise ValueError(str(e))
        
        
