from odoo import models, fields, api


class ResImalAccount(models.Model):
    _name = 'res.imal.account'
    _description = 'IMAL Account'
    _rec_name = 'fullname'

    _sql_constraints = [
        ('unique_additional_reference', 'unique(additional_reference)', 'Account Number must be unique!'),
    ]

    comp_code_id = fields.Many2one('res.imal.companies', string='Company Code')
    branch_id = fields.Many2one('res.imal.branches', string='Branch')
    credit_trans = fields.Float(string='Credit Trans')
    debit_trans = fields.Float(string='Debit Trans')
    date_closed = fields.Date(string='Date Closed')
    date_opened = fields.Date(string='Date Opened')
    fullname = fields.Char(string='Full Name')
    dept = fields.Char(string='Department')
    cif_sub_no = fields.Char(string='CIF Sub No')
    gl_code = fields.Char(string='GL Code')
    currency_code = fields.Char(string='Currency Code')
    # currency_id = fields.Many2one(comodel_name='res.currency', string='Currency')
    description = fields.Text(string='Description')
    additional_reference = fields.Char(string='Account Number')
    add_string5 = fields.Char(string='Add String 5')
    first_trans_date = fields.Date(string='First Trans Date')
    account_number = fields.Char(string='Account Number')
    status = fields.Many2one(comodel_name='customer.status',string='Status')
    old_status = fields.Many2one(comodel_name='customer.status',string='Old Status')
    remarks = fields.Text(string='Remarks')
    available_balance = fields.Float(string='Available Balance')
    last_trans_date = fields.Date(string='Last Trans Date')
    fc_available_balance = fields.Float(string='Foreign Currency Available Balance')
    suspended_by = fields.Char(string='Suspended By')
    date_suspended = fields.Date(string='Date Suspended')
    modified_by= fields.Char(string='Modified By')
    date_modified= fields.Date(string='Date Modified')
    reactivated_by= fields.Char(string='Reactivated By')
    date_reactivated= fields.Date(string='Date Reactivated')
    maturity_date = fields.Date(string='Maturity Date')
    maturity_days = fields.Integer(string='Maturity Days')
    maturity_gl = fields.Integer(string='Maturity GL')
    entered_by= fields.Char(string='Entered By')
    auth_rej_usr = fields.Char(string='Auth User')
    