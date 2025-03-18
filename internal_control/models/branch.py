from odoo import models, fields, api

class Branch(models.Model):
    _inherit = "res.branch"
    
    address = fields.Char(string='Address', index=True)
    branch_type = fields.Many2one("res.branchtype")