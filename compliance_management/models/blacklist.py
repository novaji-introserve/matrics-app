from odoo import _, api, fields, models


class Blacklist(models.Model):
    _name = 'res.partner.blacklist'
    _description = 'Blacklist'
    
    _sql_constraints = [
        ('uniq_blacklist_customer_id', 'unique(customer_id)',
         "Customer ID already exists. Value must be unique!")
    ]

    name = fields.Char(string='')
    customer_id = fields.Many2one(comodel_name='res.partner', string='Customer',required=True,index=True)
