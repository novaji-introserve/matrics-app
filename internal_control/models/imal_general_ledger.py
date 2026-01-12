from odoo import models, fields, api


class ResImalGeneralLedger(models.Model):
    _name = 'res.imal.general.ledger'
    _description = 'IMAL General Ledger'
    _rec_name = 'name'

    _sql_constraints = [
        ('unique_gl_code', 'unique(gl_code)', 'gl code must be unique!'),
    ]

    comp_code_id = fields.Many2one('res.imal.companies', string='Company Code', index=True)
    gl_code = fields.Char(string='GL Code', index=True)
    name = fields.Char(string='Name', index=True)
    gl_category = fields.Char(string='GL Category', index=True)
    gl_type = fields.Char(string='GL Type', index=True)
    acc_sign = fields.Char(string='ACC Sign')
    debit_account = fields.Char(string='Debit Account')
    credit_account = fields.Char(string='Credit Account')
    additional_reference = fields.Char(string='Additional Reference')
    gl_term = fields.Char(string='GL Term')
    parent_gl = fields.Char(string='Parent GL', index=True)
    date_created = fields.Date(string='Date Created')
    add_number1 = fields.Integer(string='Add Number 1')