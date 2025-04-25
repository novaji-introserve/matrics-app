from odoo import _, api, fields, models


class Blacklist(models.Model):
    _name = 'res.partner.blacklist'
    _description = 'Blacklist'
    
    _sql_constraints = [
        ('uniq_blacklist_customer_id', 'unique(customer_id)',
         "Customer ID already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name")
    surname = fields.Char(string="Surname",tracking=True,required=True,index=True)
    first_name = fields.Char(string="First Name",tracking=True,required=True,index=True)
    middle_name = fields.Char(string="Middle Name")
    customer_id = fields.Many2one(comodel_name='res.partner', string='Customer',required=False,index=True)
    bvn = fields.Char(string='BVN', tracking=True, index = True)