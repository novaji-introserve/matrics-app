from odoo import models, fields, api

class CustomerAccount(models.Model):
    _inherit = "res.partner.account"
    
    accountnumber = fields.Char()
    accounttitle = fields.Char()
    productcode = fields.Char()
    branchcode = fields.Many2one("res.branch")
    customerid = fields.Char()
    officercode = fields.Char()
    taxcode1 = fields.Char()
    taxcode2= fields.Char()
    cintrate = fields.Char()
    cintrate = fields.Char()
    currencycode = fields.Many2one("res.currency")
    sectorcode = fields.Char()
    dateopened = fields.Char()
    lnbalance = fields.Char()
    bkbalance = fields.Char()
    odlimit = fields.Char()
    tod_limit = fields.Char()
    unclearedbal = fields.Char()
    holdbal = fields.Char()
    sibal = fields.Char()
    totdebit = fields.Char()
    totcredit = fields.Char()
    disableview = fields.Char()
    oldacctno = fields.Char()
    last_month_balance = fields.Char()
    lien = fields.Char()
    lastdatepay = fields.Char()
    account_tier = fields.Char()
    riskrating = fields.Char()
    ID = fields.Char()
    Status = fields.Boolean(default=False)
    
    def get_bal(self):
        return  '{0:.2f}'.format(self.bkbalance)
    
        