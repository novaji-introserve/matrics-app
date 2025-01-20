from odoo import models, fields, api

class CustomerAccount(models.Model):
    _inherit = "res.partner.account"
    
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
        