from odoo import api, fields, models

class Res_subbranch(models.Model):
    _name = 'res.subbranch'
    _description = 'sub branch'
    _sql_constraints = [
        ('uniq_subbranchCode', 'unique(subbranchcode)',
         "Product already exists. Value must be unique!"),
    ]

    
    subbranchcode = fields.Char()
    subbranchname = fields.Char()
    address = fields.Char()
    email = fields.Char()
    city = fields.Many2one("res.partner.town")
    state = fields.Many2one("res.country.state")
    country = fields.Many2one("res.country")
    branchtype = fields.Many2one("res.branchtype")
    mbranchcode = fields.Many2one("res.branch")
    userid = fields.Char(string="User ID")
    authid = fields.Char(string="Auth ID")