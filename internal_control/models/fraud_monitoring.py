from odoo import api, fields, models


class FraudMonitoring(models.Model):
    _name = 'res.internal.control.fraud.monitoring'
    _description = 'Fraud Monitoring'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Reference', required=True, copy=False, readonly=True, default='New')
    fraud_type = fields.Selection([
        ('ledger_entry', 'Ledger Entry'),
        ('posting', 'Posting'),
        # Add other fraud types
    ], string='Fraud Type', required=True)
    fraud_reference = fields.Char(string="Fraud Reference", required=True)
    fraud_type = fields.Char(string="Fraud Type", required=True)
    fraud_date = fields.Datetime(string="Fraud Date")
    description = fields.Text(string="Description")
    status = fields.Selection([('pending', 'Pending'), 
                               ('approved', 'Approved'),
                               ('reversed', 'Reversed'),
                               ('backdated', 'Backdated'),
                               ('premature', 'Premature'),
                               ('reactivated', 'Reactivated'),
                               ('cancelled', 'Cancelled')],
                              string="Status", default='pending')
    amount = fields.Float(string="Amount")
    user_id = fields.Many2one('res.users', string="User", default=lambda self: self.env.user)
    company_id = fields.Many2one('res.company', string="Company", default=lambda self: self.env.company)


    @api.model
    def create(self, vals):
        return super(FraudMonitoring, self).create(vals)
    