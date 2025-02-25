from odoo import api, fields, models

class BranchType(models.Model):
    _name = 'res.branchtype'
    _description = 'branch type'
    
    
    status = fields.Char(string="Status")
    desc = fields.Char(string="Description")
    userid = fields.Char(string="User ID")
    authid = fields.Char(string="Auth ID")