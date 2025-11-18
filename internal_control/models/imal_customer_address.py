from odoo import models, fields, api


class ResImalCustomerAddress(models.Model):
    _name = 'res.imal.customer.address'
    _description = 'IMAL Customer Address'
    _rec_name = 'address1'

    _sql_constraints = [
        ('unique_cif_no', 'unique(cif_no)', 'cif no must be unique!'),
    ]

    comp_code_id = fields.Many2one('res.imal.companies', string='Company Code', index=True)
    branch_id = fields.Many2one('res.imal.branches', string='Branch', index=True)
    expiry_date = fields.Date(string='Expiry Date')
    cif_no = fields.Char(string='CIF No', index=True)
    line_no = fields.Char(string='Line No')
    address1 = fields.Char(string='Address 1')
    address2 = fields.Char(string='Address 2')
    phone = fields.Char(string='Phone')
    phone2 = fields.Char(string='Phone2')
    email = fields.Char(string='Email')
    city = fields.Char(string='City')
    region = fields.Char(string='Region')
    country_code = fields.Char(string='Country Code')
    additional_reference = fields.Char(string='Additional Reference')
    default_address = fields.Integer(string='Default Address')