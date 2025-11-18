from odoo import models, fields, api


class ResImalBranches(models.Model):
    _name = 'res.imal.branches'
    _description = 'IMAL Branch Table'
    _rec_name = 'name'

    comp_code_id = fields.Many2one('res.imal.companies', string='Company Code')
    code = fields.Char(string='Code')
    name = fields.Char(string='Name')
    address1 = fields.Char(string='Address 1')
    address2 = fields.Char(string='Address 2')

    _sql_constraints = [
        ('unique_code', 'unique(code)', 'Code must be unique!'),
    ]