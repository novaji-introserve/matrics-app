from odoo import models, fields, api, _

class CustomerAccount(models.Model):
    _inherit = "res.partner.account"
    _rec_name = 'accounttitle'
    
    officercode = fields.Many2one(
        comodel_name='res.account.officer', string='Account Officer', index=True)
    sectorcode = fields.Many2one(
        comodel_name='res.partner.sector', string='Sector', index=True)
    lnbalance = fields.Char(string="Ln Bal.")
    bkbalance = fields.Char(string="Bk Bal.")
    unclearedbal = fields.Char(string="Total Creadit")
    holdbal = fields.Char(string="Hold Bal.")
    totdebit = fields.Char(string="Total Debit")
    totcredit = fields.Char(string="Total Creadit")
    last_month_balance = fields.Char(string="Last Month Bal.")
    lien = fields.Char(string="Lien")
    account_tier = fields.Many2one(
        comodel_name='res.partner.tier', string='Account Tier', index=True)
    source_account_id = fields.Char(string="Source Account ID", index=True)
    Status = fields.Boolean(default=False)
    accounttitle = fields.Char(string="Account Title")
        

    @api.model
    def open_accounts_tier_1(self):
        return {
            'name': _('Tier 1 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('account_tier', '=', 1)
            ],
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_accounts_tier_2(self):
        return {
            'name': _('Tier 2 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('account_tier', '=', 2)
            ],
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_accounts_tier_3(self):
        return {
            'name': _('Tier 3 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('account_tier', '=', 3)
            ],
            'context': {'search_default_group_branch': 1}
        }