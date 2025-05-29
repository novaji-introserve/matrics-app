from odoo import models, fields

class Branch(models.Model):
    _name = 'branch'
    _description = 'Branch'

    branch_code = fields.Char(string='Branch Code', size=5, required=False)
    branch_name = fields.Char(string='Branch Name', size=30, required=False)
