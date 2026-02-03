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
        ('uniq_customer_id', 'unique(customer_id)',
         "Customer already exists. Customer must be unique!"),
    ]
        
    onebank = fields.Char(string='Uses One Bank', index=True, readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)


class CustomerDigitalProductMaterializedExtension(models.Model):
    _inherit = 'customer.digital.product.mat'

    onebank = fields.Char(string='Uses One Bank', readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)


class ResPartner(models.Model):
    """
    Extended res.partner with customer enrichment data for Sterling Bank.

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

    # Account Category - used for customer type classification
    account_category = fields.Char(
        string='Account Category',
        index=True,
        readonly=True,
        help='Customer status/account category from source system. '
             'Used to determine if account is Individual or Corporate for enrichment display.'
    )
