from odoo import models, fields, api


class ResImalCustomer(models.Model):
    _name = 'res.imal.customer'
    _description = 'IMAL Customer'
    _rec_name = 'fullname'

    _sql_constraints = [
        ('unique_cif_no', 'UNIQUE(cif_no)', 'cif no must be unique!'),
    ]

    comp_code_id = fields.Many2one('res.imal.companies', string='Company Code')
    branch_id = fields.Many2one('res.imal.branches', string='Branch')
    fullname = fields.Char(string='Full Name')
    dept = fields.Char(string='Department')
    cif_no = fields.Char(string='CIF No')
    bvn= fields.Char(string='BVN')
    tin = fields.Char(string='TIN')
    modified_by = fields.Char(string='Modified By')
    date_modified = fields.Date(string='Date Modified')
    customer_type_id = fields.Many2one('res.customer.type', string='Customer Type')
    address1 = fields.Char(string='Address 1')
    address2 = fields.Char(string='Address 2')
    id_no = fields.Char(string='ID No')
    date_created = fields.Date(string='Date Created')
    status = fields.Char(string='Status')
    sex_id = fields.Many2one('res.partner.gender', string='Gender')
    marital_status_id = fields.Many2one('res.marital.status', string='Marital Status')
    date_of_birth = fields.Date(string='Date of Birth')
    description = fields.Text(string='Description')
    additional_reference = fields.Char(string='Additional Reference')