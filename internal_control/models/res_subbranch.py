from odoo import api, fields, models

class Res_subbranch(models.Model):
    _name = 'res.subbranch'
    _description = 'sub branch'
    
    subbranchCode = fields.Char()
    subBranchName = fields.Char()
    Address = fields.Char()
    email = fields.Char()
    City = fields.Many2one("res.partner.town")
    State = fields.Many2one("res.country.state")
    Country = fields.Many2one("res.country")
    BranchType = fields.Many2one("res.branchtype")
    MBranchCode = fields.Many2one("res.branch")
    Userid = fields.Char(string="User ID")
    authid = fields.Char(string="Auth ID")