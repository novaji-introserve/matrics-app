from odoo import models, fields, api

class alert_rules(models.Model):
    _name = 'alert.rules'
    _description = "alert rules for exception management"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char(string="Name", required=True, Tracking=True)
    narration = fields.Html(string="narration", required=True)
    sql_text = fields.Text(string="SQL Query", required=True, Tracking=True)
    frequency_id = fields.Many2one('exception.frequency', string="Frequency", required=True)
    process_id = fields.Many2one('process', string="Process")
    process_category_id = fields.Many2one('process.category', string="Process Category", required=True)
    email_to = fields.Char(string="Email To", Tracking=True)
    status_id = fields.Many2one('alert.rules.status', string="Alert Status") 
    alert_group_id = fields.Many2one('alert.group', string="Alert Group")
    branch_code = fields.Many2one('tbl.branch', string="Branch Code")
    process_id = fields.Many2one('process')
    risk_rating = fields.Many2one("case.rating", string="Risk Rating")
    date_created = fields.Datetime(string="created_at", default=fields.Datetime.now())


    def submit_data(self):
        pass
