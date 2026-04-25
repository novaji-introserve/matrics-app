from odoo import _, api, fields, models


class Transaction(models.Model):
    _inherit = 'res.customer.transaction'

    blocked = fields.Boolean(string='Blocked', default=False,
                             help="Indicates if the transaction is blocked by any screening rule.", tracking=True, index=True)

    @api.model
    def action_view_blocked_transactions(self):

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
            domain = [('blocked', '=', True)]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]), ('blocked', '=', True)]

        return {
            'name': _('Transactions On-Hold / Blocked'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    def action_unblock(self):
        """
        Unblock the transaction by setting the blocked field to False.
        Add implementation logic as needed to handle unblocking.
        For example we can send a signal to the transaction processing system.
        We should override this method to implement the unblocking logic.
        This method can be called from a button in the UI or programmatically.
        """
        self.ensure_one()
        for transaction in self:
            transaction.write({'blocked': False})

    def action_block(self):
        """
        Block the transaction by setting the blocked field to True.
        """
        self.ensure_one()
        for transaction in self:
            transaction.write({'blocked': True})

    def action_screen(self):
        result = super().action_screen()
        exceptions = self.env['res.transaction.screening.history'].search([
            ('transaction_id', '=', self.id)
        ])
        if exceptions:
            for exception in exceptions:
                rule = exception.rule_id
                if rule.blocked:
                    self.write({'blocked': True})
        return result
