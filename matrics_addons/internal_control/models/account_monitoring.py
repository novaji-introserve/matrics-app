from odoo import models, fields, api

class AccountMonitoring(models.Model):
    _name = 'res.internal.control.account.monitoring'
    _description = 'Account Monitoring'
    _rec_name = 'account_reference'

    account_reference = fields.Char(string="Account Reference", required=True)
    account_type = fields.Char(string="Account Type", required=True)
    account_date = fields.Datetime(string="Account Date")
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
        return super(AccountMonitoring, self).create(vals)
