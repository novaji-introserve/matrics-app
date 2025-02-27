from odoo import models, fields

class AuditTrail(models.Model):
    _name = 'res.audit.trail'
    _description = 'Audit Trail'

    audit_trail_id = fields.Char(string="ID")
    titlecode = fields.Integer(string='Title Code')
    table_affected = fields.Char(string='Table Affected', size=100)
    action_performed = fields.Char(string='Action Performed', size=100)
    userid = fields.Char(string='User ID', size=50)
    eventlog_refid = fields.Integer(string='Event Log Ref ID')
    old_value_script = fields.Text(string='Old Value Script')
    new_value_script = fields.Text(string='New Value Script')
    oldvalue = fields.Text(string='Old Value')
    newvalue = fields.Text(string='New Value')
    system_audit_date = fields.Datetime(string='System Audit Date')
    current_audit_date = fields.Datetime(string='Current Audit Date')
    auditstatement = fields.Text(string='Audit Statement')
    customerid_affected = fields.Char(string='Customer ID Affected', size=50)
    accountaffected = fields.Char(string='Account Affected', size=50)
    authid = fields.Char(string='Authorization ID', size=50)

    _sql_constraints = [
        ('audit_rail_audit_trail_id_unique', 'unique(audit_trail_id)', 'Audit Trail_ID must be unique!')
    ]

    # ID is automatically created by Odoo as a primary key