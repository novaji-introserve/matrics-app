from odoo import _, api, fields, models


class TransactionScreeningHistory(models.Model):
    _name = 'res.transaction.screening.history'
    _description = 'Transaction Screening History'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    transaction_id = fields.Many2one(
        'res.customer.transaction',
        string='Transaction',
        index=True,
        ondelete='cascade',
        required=True)
    rule_id = fields.Many2one(
        'res.transaction.screening.rule',
        string='Rule',
        index=True,
        ondelete='cascade',
        required=True)
    name = fields.Char(string='Name', required=True,
                       readonly=True, related='transaction_id.name')
    customer_id = fields.Many2one(
        string='Customer', related='transaction_id.customer_id',store=True)
    account_id = fields.Many2one(
        string='Account', related='transaction_id.account_id',store=True)
    branch_id = fields.Many2one(
        'res.branch', string='Branch',
        related='transaction_id.branch_id',store=True)
    risk_level = fields.Selection(
        [('low', 'Low'),
         ('medium', 'Medium'),
         ('high', 'High')],
        string='Risk Level',
        required=True,
        store=True,
        related='rule_id.risk_level')

    @api.model
    def open_transaction_screening_history(self):

        user = self.env.user
        compliance_groups = [
            'compliance_management.group_compliance_chief_compliance_officer',
            'compliance_management.group_compliance_compliance_officer',
            'compliance_management.group_compliance_transaction_monitoring_team'
        ]
        has_compliance_access = any(user.has_group(group)
                                    for group in compliance_groups)

        # Set domain based on user group
        if has_compliance_access:
            domain = []
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id])]

        return {
            'name': _('Transaction Screening History'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.transaction.screening.history',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_rule_id': 1}
        }
