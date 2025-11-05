import os
import logging
import re
from datetime import datetime
from odoo import models, fields, api, http
from odoo.exceptions import AccessError, UserError
from odoo.tools.config import config  

_logger = logging.getLogger(__name__)

class CustomerDigitalProductExtend(models.Model):
    _inherit  = 'customer.digital.product'
    _sql_constraints = [
        ('uniq_account_no', 'unique(account_no)',
         "account no already exists. account no must be unique!"),
    ]
        
    account_no = fields.Char(string='Account No', readonly=True)
    ifuel = fields.Char(string='Ifuel', readonly=True)
    altpro = fields.Char(string='AltPro', readonly=True)
    altmall = fields.Char(string='AltMall', readonly=True)
    altinvest = fields.Char(string='AltInvest', readonly=True)
    altpower = fields.Char(string='AltPower', readonly=True)
    altdrive = fields.Char(string='AltDrive', readonly=True)
    chequebook = fields.Char(string='ChequeBook', readonly=True)
    

class CustomerDigitalProductMaterializedExtension(models.Model):
    _inherit = 'customer.digital.product.mat'

    account_no = fields.Char(string='Account No', readonly=True)
    ifuel = fields.Char(string='Ifuel', readonly=True)
    altpro = fields.Char(string='AltPro', readonly=True)
    altmall = fields.Char(string='AltMall', readonly=True)
    altinvest = fields.Char(string='AltInvest', readonly=True)
    altpower = fields.Char(string='AltPower', readonly=True)
    altdrive = fields.Char(string='AltDrive', readonly=True)
    chequebook = fields.Char(string='ChequeBook', readonly=True)
    
    
class CustomerCurrency(models.Model):
    _inherit  = 'res.currency'
    
    def init(self):
        self.env.cr.execute("""
            UPDATE res_currency
            SET code = CASE
                WHEN name = 'USD' THEN '840'
                WHEN name = 'EUR' THEN '987'
                WHEN name = 'NGN' THEN '566'
            END
            WHERE name IN ('USD', 'EUR', 'NGN')
            AND (
                (name = 'USD' AND code <> '840') OR
                (name = 'EUR' AND code <> '987') OR
                (name = 'NGN' AND code <> '566')
            );
        """)
        
        
class Transaction(models.Model):
    _inherit = 'res.customer.transaction'
    
    transaction_type = fields.Selection(selection=[(
        'credit', 'Credit'), ('Debit', 'Debit')],  index=True, string='Transaction Type')
    
    created_date = fields.Datetime(string='Transaction Date',  help="Transaction Date", index=True)


        
class WatchList(models.Model):
    _inherit = 'res.partner.watchlist'
    
    bank = fields.Char(string='bank', readonly=True)



        
    
    
