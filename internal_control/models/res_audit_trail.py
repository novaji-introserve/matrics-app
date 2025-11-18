# from odoo import models, fields

# class AuditTrail(models.Model):
#     _name = 'res.audit.trail'
#     _description = 'Audit Trail'

#     audit_trail_id = fields.Char(string="ID")
#     titlecode = fields.Integer(string='Title Code')
#     table_affected = fields.Char(string='Table Affected', size=100)
#     action_performed = fields.Char(string='Action Performed', size=100)
#     userid = fields.Char(string='User ID', size=50)
#     eventlog_refid = fields.Integer(string='Event Log Ref ID')
#     old_value_script = fields.Text(string='Old Value Script')
#     new_value_script = fields.Text(string='New Value Script')
#     oldvalue = fields.Text(string='Old Value')
#     newvalue = fields.Text(string='New Value')
#     system_audit_date = fields.Date(string='System Audit Date')
#     current_audit_date = fields.Date(string='Current Audit Date')
#     auditstatement = fields.Text(string='Audit Statement')
#     customerid_affected = fields.Char(string='Customer ID Affected', size=50)
#     accountaffected = fields.Char(string='Account Affected', size=50)
#     authid = fields.Char(string='Authorization ID', size=50)

#     _sql_constraints = [
#         ('audit_rail_audit_trail_id_unique', 'unique(audit_trail_id)', 'Audit Trail_ID must be unique!')
#     ]

#     # ID is automatically created by Odoo as a primary key

from odoo import models, fields


class AuditTrail(models.Model):
    _name = 'res.audit.trail'
    _description = 'Audit Trail'

    _sql_constraints = [
        ('unique_source_id', 'UNIQUE(source_id)', 'source id must be unique!'),
    ]

    # Updated and new core fields
    source_id = fields.Char(string='Source ID', index=True)
    audit_id = fields.Char(string='Audit ID', index=True)
    action = fields.Char(string='Action', index=True)
    action_status = fields.Char(string='Action Status', index=True)
    affected_columns = fields.Text(string='Affected Columns')
    audit_log_date = fields.Date(string='Audit Log Date', index=True)
    audit_module_accessed = fields.Char(string='Audit Module Accessed', index=True)
    authorization_details = fields.Text(string='Authorization Details')
    
    # Branch and location info
    branch_code = fields.Char(string='Branch Code', index=True)
    branch_name = fields.Char(string='Branch Name', index=True)
    source_ip = fields.Char(string='Source IP', index=True)
    
    # Client and user info
    client_info = fields.Text(string='Client Info')
    user_id = fields.Char(string='User ID', index=True)
    user_name = fields.Char(string='User Name', index=True)
    full_name = fields.Char(string='Full Name', index=True)
    role = fields.Char(string='Role', index=True)
    role_id = fields.Char(string='Role ID', index=True)
    session_id = fields.Char(string='Session ID', index=True)
    user_activity_type = fields.Char(string='User Activity Type', index=True)
    access_type = fields.Char(string='Access Type')
    
    # Customer info
    customer_id = fields.Char(string='Customer ID', index=True)
    customer_name = fields.Char(string='Customer Name', index=True)
    customer_type = fields.Char(string='Customer Type', index=True)
    customer_mandate_view = fields.Text(string='Customer Mandate View')
    
    # Account info
    account_name = fields.Char(string='Account Name', index=True)
    account_number = fields.Char(string='Account Number', index=True)
    account_restriction_id = fields.Char(string='Account Restriction ID')
    credit_account_ledger = fields.Char(string='Credit Account Ledger')
    credit_accounts = fields.Text(string='Credit Accounts')
    debit_account_ledger = fields.Char(string='Debit Account Ledger')
    debit_accounts = fields.Text(string='Debit Accounts')
    currency_code = fields.Char(string='Currency Code', index=True)
    
    # Transaction info
    transaction_id = fields.Char(string='Transaction ID', index=True)
    transaction_value = fields.Integer(string='Transaction Value')
    transaction_type = fields.Char(string='Transaction Type', index=True)
    transaction_entry_information = fields.Text(string='Transaction Entry Information')
    
    # Approval workflow
    approver = fields.Char(string='Approver', index=True)
    approver_branch_code = fields.Char(string='Approver Branch Code')
    initiator = fields.Char(string='Initiator', index=True)
    initiator_branch_code = fields.Char(string='Initiator Branch Code')
    authorizer = fields.Char(string='Authorizer', index=True)
    inputter = fields.Char(string='Inputter', index=True)
    reason_for_restriction = fields.Text(string='Reason for Restriction')
    request_type = fields.Char(string='Request Type', index=True)
    
    # Technical fields
    module = fields.Char(string='Module', index=True)
    module_id = fields.Char(string='Module ID')
    endpoint_name = fields.Char(string='Endpoint Name', index=True)
    record_id = fields.Char(string='Record ID', index=True)
    record_name = fields.Char(string='Record Name', index=True)
    modified_parameter = fields.Text(string='Modified Parameter')
    old_values_json = fields.Text(string='Old Values JSON')
    new_values_json = fields.Text(string='New Values JSON')
    other_information = fields.Text(string='Other Information')
    reason_for_failure = fields.Text(string='Reason for Failure')
    
    # Date fields
    created_date = fields.Date(string='Created Date', index=True)
    updated_at = fields.Date(string='Updated At', index=True)
    start_date = fields.Date(string='Start Date')
    end_date = fields.Date(string='End Date')
    
    # Original existing fields (keeping for backward compatibility)
    titlecode = fields.Integer(string='Title Code')
    table_affected = fields.Char(string='Table Affected')
    action_performed = fields.Char(string='Action Performed')
    eventlog_refid = fields.Integer(string='Event Log Ref ID')
    old_value_script = fields.Text(string='Old Value Script')
    new_value_script = fields.Text(string='New Value Script')
    oldvalue = fields.Text(string='Old Value')
    newvalue = fields.Text(string='New Value')
    system_audit_date = fields.Date(string='System Audit Date')
    current_audit_date = fields.Date(string='Current Audit Date')
    auditstatement = fields.Text(string='Audit Statement')
    customerid_affected = fields.Char(string='Customer ID Affected')
    accountaffected = fields.Char(string='Account Affected')
    authid = fields.Char(string='Authorization ID')