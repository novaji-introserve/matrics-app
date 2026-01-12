from odoo import models, fields, api


class ResImalCustomerTransaction(models.Model):
    _name = 'res.imal.dof.customer.transaction'
    _description = 'IMAL DOF Customer Transaction'
    _rec_name = 'trans_id'

    _sql_constraints = [
        ('unique_trans_id', 'unique(trans_id)', 'trans id must be unique!'),
    ]

    comp_code_id = fields.Many2one('res.imal.companies', string='Company Code', index=True)
    branch_id = fields.Many2one('res.imal.branches', string='Branch', index=True)
    account_id = fields.Many2one('res.imal.account', string='Account', index=True, help='Additional reference')
    gl_code_id = fields.Many2one('res.imal.general.ledger', string='GL Code', index=True)
    trans_id = fields.Char(string='Trans ID', index=True)
    trans_date = fields.Date(string='Trans Date', index=True)
    post_date = fields.Date(string='Post Date', index=True)
    cif_sub_no = fields.Char(string='CIF Sub No', index=True)
    sl_no = fields.Char(string='SL No', index=True)
    op_no = fields.Char(string='OP No', index=True)
    line_no = fields.Char(string='Line No')
    country_code = fields.Char(string='Country Code')
    jv_type = fields.Char(string='JV Type', index=True)
    jv_reference = fields.Char(string='JV Reference', index=True)
    fc_amount = fields.Float(string='FC Amount', help='Foreign Currency Amount')
    cv_amount = fields.Float(string='CV Amount', help='Local Currency Amount')
    description = fields.Text(string='Description')
    terminal_id = fields.Char(string='Terminal ID', index=True)
    op_status = fields.Char(string='OP Status', index=True)
    created_by = fields.Char(string='Created By', index=True)
    posted_by = fields.Char(string='Posted By', index=True)
    authorized_by = fields.Char(string='Authorized By', index=True)
    date_authorized = fields.Date(string='Date Authorized')
    reversal_date = fields.Date(string='Reversal Date')
    reversal_flag = fields.Char(string='Reversal Flag')
    value_date = fields.Date(string='Value Date')
    cts_trs_no = fields.Char(string='Transaction Reference')
    cts_trs_type = fields.Char(string='Transaction Type')
    add_date = fields.Date(string='Add Date')
    add_string5 = fields.Char(string='Add String 5')
    tr_code = fields.Char(string='TR Code', index=True)