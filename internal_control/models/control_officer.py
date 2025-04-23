from odoo import models, fields, api

class Control_officer(models.Model):
    _name = "control.officer"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _description = 'control officer'
    _rec_name = "officer"
    
    branch_id = fields.Many2one("res.branch", string="Branch", required=True)
    alert_id = fields.Many2one("alert.group", string="Alert Group", required=True)
    officer = fields.Many2one("res.users", string="Control Officer", required=True)
    