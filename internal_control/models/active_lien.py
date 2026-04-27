from odoo import models, fields, api

class ResActiveLien(models.Model):
    _name = 'res.active.lien'
    _description = 'Active Lien'
    
    _sql_constraints = [
        ('unique_active_lien_id', 'unique(active_lien_id)', 'The Active Lien ID must be unique.')
    ]

    active_lien_id = fields.Char(string='Active Lien ID', required=True)
    account_id = fields.Many2one(comodel_name='res.partner.account', string='Account Number')
    lien_amount = fields.Float(string='Lien Amount')
    lien_purpose = fields.Char(string='Lien Purpose')
    created_by = fields.Char(string='Created By')
    date_created = fields.Date(string='Date Created')
    approved_by = fields.Char(string='Approved By')
    date_approved = fields.Date(string='Date Approved')
    deleted_by = fields.Char(string='Deleted By')
    branch_id = fields.Many2one(comodel_name='res.branch', string='Branch Code')
    date_deleted = fields.Date(string='Date Deleted')
    end_date = fields.Date(string='End Date')
    start_date = fields.Date(string='Start Date')
    reason_for_lifting = fields.Char(string='Reason for Lifting')
    requested_by = fields.Char(string='Requested By')
    transaction_date = fields.Date(string='Transaction Date')
    bank_name = fields.Char(string='Bank Name')
    reason = fields.Char(string='Reason')
