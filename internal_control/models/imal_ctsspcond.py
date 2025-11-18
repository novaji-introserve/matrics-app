from odoo import models, fields, api


class ResImalCtsspcond(models.Model):
    _name = 'res.imal.ctsspcond'
    _description = 'IMAL CTSSPCOND'
    _rec_name = 'line_no'

    _sql_constraints = [
        ('unique_line_no', 'unique(line_no)', 'line no must be unique!'),
    ]

    comp_code_id = fields.Many2one('res.imal.companies', string='Company Code', index=True)
    branch_id = fields.Many2one('res.imal.branches', string='Branch', index=True)
    entity_type = fields.Char(string='Entity Type')
    cif_no_id = fields.Many2one('res.imal.customer', string='CIF No', index=True)
    line_no = fields.Char(string='Line No', index=True)
    acc_country_code = fields.Char(string='ACC Country Code')
    acc_gl_code = fields.Char(string='ACC GL Code', index=True)
    remark = fields.Text(string='Remark')
    expiring_date = fields.Date(string='Expiring Date')
    starting_date = fields.Date(string='Starting Date')
    status = fields.Char(string='Status', index=True)
    forbid_trx = fields.Integer(string='Forbid Transaction', index=True)
    forbid_product = fields.Char(string='Forbid Product', index=True)
    date_created = fields.Date(string='Date Created')
    reason= fields.Char(string='Reason')
    reason_code= fields.Char(string='Reason', index=True)
    date_deleted= fields.Date(string='Date Deleted')
    deleted_by=fields.Char(string='Deleted By')
    to_be_deleted_by =fields.Char(string='Deleted By')
    account_currency=fields.Char(string='Account Curreny')