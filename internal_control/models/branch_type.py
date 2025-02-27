from odoo import api, fields, models

class BranchType(models.Model):
    _name = 'res.branchtype'
    _description = 'branch type'
    _sql_constraints = [
        ('uniq_code', 'unique(code)',
         "Product already exists. Value must be unique!"),
    ]

    
    
    status = fields.Char(string="Status")
    desc = fields.Char(string="Description")
    userid = fields.Char(string="User ID")
    authid = fields.Char(string="Auth ID")
    code   = fields.Char(string="Branch Type ID")