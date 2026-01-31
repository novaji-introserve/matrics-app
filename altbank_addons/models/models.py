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


class ResPartner(models.Model):
    """
    Extended res.partner with customer enrichment data.

    This extension provides direct linkage to the customer.enrichment
    model, allowing bank-specific enrichment data to be displayed on the
    customer form without relying on name-matching algorithms.

    The enrichment_id is automatically set by the customer.enrichment model
    when enrichment records are created/updated via ETL.
    """

    _inherit = 'res.partner'

    # -------------------------------------------------------------------------
    # Customer Enrichment Fields
    # -------------------------------------------------------------------------

    # Direct link to enrichment record - set automatically by customer.enrichment
    enrichment_id = fields.Many2one(
        comodel_name='customer.enrichment',
        string='Customer Enrichment',
        index=True,
        ondelete='set null',
        help='Link to bank-specific enrichment data for this customer. '
             'Automatically set when enrichment data is spooled via ETL.'
    )

    # Related fields from enrichment model for display
    bank_pep_status = fields.Text(
        string='Bank PEP Status',
        related='enrichment_id.position',
        readonly=True,
        store=False,
        help='The political position or designation from customer enrichment.'
    )

    pep_relationship = fields.Char(
        string='PEP Relationship',
        related='enrichment_id.relationship',
        readonly=True,
        store=False,
        help='Customer relationship with PEP (e.g., Account Owner, Signatory).'
    )

    pep_source_of_fund = fields.Text(
        string='Source of Fund',
        related='enrichment_id.source_of_fund',
        readonly=True,
        store=False,
        help='Declared source of funds from customer enrichment.'
    )

    pep_source_of_wealth = fields.Text(
        string='Source of Wealth',
        related='enrichment_id.source_of_wealth',
        readonly=True,
        store=False,
        help='Declared source of wealth from customer enrichment.'
    )

