from odoo import models, fields

class Alert(models.Model):
    _name = 'alert'
    _description = 'Alert Record'
    _rec_name = 'alert_name'

    alert_name = fields.Char(string='Alert Name', required=False)
    alert_description = fields.Char(string='Alert Description', required=False)
    alert_action = fields.Char(string='Alert Action', required=False)
    alert_subject = fields.Char(string='Alert Subject', required=False)
    
    created_at = fields.Datetime(string='Created At', required=False)
    updated_at = fields.Datetime(string='Updated At', required=False)
    alert_date = fields.Datetime(string='Alert Date', required=False)
    
    status_id = fields.Integer(string='Status ID', required=False)
    
    user_id = fields.Many2one('res.users', string='User', required=False)
    rule_id = fields.Many2one('rule.list', string='Rule', required=False)
    alert_frequency_id = fields.Many2one('alert.frequency', string='Alert Frequency', required=False)
    team_id = fields.Many2one('team', string='Team', required=False)
    exception_process_id = fields.Many2one('exception.process.type', string='Exception Process', required=False)
    
    long_description = fields.Text(string='Long Description', required=False)
    mail_to = fields.Text(string='Mail To', required=False)
    mail_cc = fields.Text(string='Mail CC', required=False)