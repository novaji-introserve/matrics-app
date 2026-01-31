# -*- coding: utf-8 -*-
"""
Customer Enrichment Model
=========================

This module provides a customer enrichment table for storing extended customer
data that is directly linked via customer number. The enrichment data includes
PEP (Politically Exposed Person) information, source of funds/wealth, and
other compliance-related attributes.

Design Rationale:
-----------------
Unlike the global `pep_list` model which stores regulatory PEP records matched
by name, this enrichment model stores bank-specific customer data with direct
linkage. This provides:

    1. Direct Linking: Uses customer_no for guaranteed accurate matching
       (no fuzzy name matching required)

    2. Bank-Specific Data: Each bank can maintain their own enrichment data
       without affecting shared regulatory lists

    3. Extended Attributes: Stores additional compliance data such as
       source of funds, source of wealth, and PEP relationships

    4. Multi-Bank Support: Designed to be reusable across different bank
       implementations (e.g., Altbank, Sterling)

Source Table (Altbank):
-----------------------
    imal_altbank_pep_list_new

Usage:
------
    The enrichment data is spooled from the source database during ETL
    and linked to customers via the customer_id field.

Author: Olumide Awodeji
Version: 1.0.0
"""

from odoo import api, fields, models
import logging

_logger = logging.getLogger(__name__)


class CustomerEnrichment(models.Model):
    """
    Customer Enrichment

    Stores bank-specific enrichment data linked directly to customers.
    This model provides extended customer information including PEP status,
    source of funds/wealth, and other compliance-related data.

    Attributes:
        customer_id (Char): The bank's customer number for direct linking
        partner_id (Many2one): Computed link to res.partner record
        pep_name (Char): Name of the PEP (may differ from customer name)
        position (Text): Status, position, or designation of the PEP
        relationship (Char): Customer's relationship with the PEP
        source_of_fund (Text): Declared source of funds
        source_of_wealth (Text): Declared source of wealth
        nature_of_business (Text): Nature of business or occupation
        branch_name (Char): Branch where account is held
        bvn (Char): Bank Verification Number
    """

    _name = 'customer.enrichment'
    _description = 'Customer Enrichment Data'
    _order = 'customer_id'

    # -------------------------------------------------------------------------
    # SQL Constraints
    # -------------------------------------------------------------------------
    _sql_constraints = [
        (
            'uniq_customer_enrichment',
            'unique(customer_id)',
            'Customer enrichment record already exists. '
            'Each customer can only have one enrichment record.'
        ),
    ]

    # -------------------------------------------------------------------------
    # Field Definitions
    # -------------------------------------------------------------------------

    # Primary Identifier - Links directly to res_partner.customer_id
    customer_id = fields.Char(
        string='Customer Number',
        required=True,
        index=True,
        help='The bank customer number used to link this enrichment '
             'record to the corresponding customer in res.partner.'
    )

    # Computed relationship to res.partner
    partner_id = fields.Many2one(
        comodel_name='res.partner',
        string='Customer',
        compute='_compute_partner_id',
        store=True,
        help='The linked customer record computed from customer_id.'
    )

    # PEP Identity Information
    pep_name = fields.Char(
        string='PEP Name',
        help='Name of the Politically Exposed Person. This may be the '
             'customer themselves or a related individual.'
    )

    position = fields.Text(
        string='Position/Designation',
        help='The political or official position/designation of the PEP. '
             'This is displayed as Bank PEP Status on the customer form.'
    )

    relationship = fields.Char(
        string='Relationship with PEP',
        help='The nature of the customer\'s relationship with the PEP. '
             'Common values: ACCOUNT OWNER, SIGNATORY, RELATED PARTY.'
    )

    # Compliance Information
    source_of_fund = fields.Text(
        string='Source of Fund',
        help='Declared source of funds for the account.'
    )

    source_of_wealth = fields.Text(
        string='Source of Wealth',
        help='Declared source of wealth for the customer.'
    )

    nature_of_business = fields.Text(
        string='Nature of Business',
        help='Nature of business or occupation of the customer.'
    )

    # Account Information
    branch_name = fields.Char(
        string='Branch',
        help='The branch where the customer\'s account is held.'
    )

    bvn = fields.Char(
        string='BVN',
        help='Bank Verification Number.'
    )

    # -------------------------------------------------------------------------
    # Computed Fields
    # -------------------------------------------------------------------------

    @api.depends('customer_id')
    def _compute_partner_id(self):
        """
        Compute the partner_id by matching customer_id with res.partner.

        This creates a direct link between the enrichment record and the
        customer without requiring a foreign key constraint, allowing the
        ETL process to spool enrichment data before customers exist.
        """
        Partner = self.env['res.partner']
        for record in self:
            if record.customer_id:
                partner = Partner.search(
                    [('customer_id', '=', record.customer_id)],
                    limit=1
                )
                record.partner_id = partner.id if partner else False
            else:
                record.partner_id = False

    # -------------------------------------------------------------------------
    # CRUD Overrides - Automatic Partner Linking
    # -------------------------------------------------------------------------

    @api.model_create_multi
    def create(self, vals_list):
        """
        Override create to automatically link enrichment to res.partner.

        When enrichment records are created (e.g., via ETL), this method
        automatically sets the enrichment_id on the corresponding partner
        record, ensuring the bidirectional link is established.
        """
        records = super().create(vals_list)
        records._link_to_partners()
        return records

    def write(self, vals):
        """
        Override write to update partner link when customer_id changes.

        If customer_id is modified, this ensures the enrichment_id is
        updated on both the old and new partner records.
        """
        # If customer_id is changing, unlink from old partners first
        if 'customer_id' in vals:
            self._unlink_from_partners()

        result = super().write(vals)

        # Re-link to partners if customer_id was updated
        if 'customer_id' in vals:
            self._link_to_partners()

        return result

    def unlink(self):
        """
        Override unlink to remove enrichment_id from partners before deletion.
        """
        self._unlink_from_partners()
        return super().unlink()

    def _link_to_partners(self):
        """
        Link enrichment records to their corresponding partners.

        Sets enrichment_id on res.partner records that match by customer_id.
        This is called after create and write operations.
        """
        if not self:
            return

        Partner = self.env['res.partner'].sudo()
        for record in self:
            if record.customer_id:
                partner = Partner.search(
                    [('customer_id', '=', record.customer_id)],
                    limit=1
                )
                if partner and partner.enrichment_id != record:
                    partner.write({'enrichment_id': record.id})
                    _logger.debug(
                        f'Linked enrichment {record.id} to partner {partner.id} '
                        f'(customer_id: {record.customer_id})'
                    )

    def _unlink_from_partners(self):
        """
        Remove enrichment_id from partners linked to these records.

        Called before deletion or when customer_id changes.
        """
        if not self:
            return

        Partner = self.env['res.partner'].sudo()
        partners = Partner.search([('enrichment_id', 'in', self.ids)])
        if partners:
            partners.write({'enrichment_id': False})
            _logger.debug(
                f'Unlinked {len(partners)} partners from enrichment records'
            )

    # -------------------------------------------------------------------------
    # Database Initialization
    # -------------------------------------------------------------------------

    def init(self):
        """
        Initialize database indexes for optimal query performance.

        Creates indexes on commonly queried fields to improve lookup
        performance during ETL operations and UI queries.
        """
        # Index on customer_id for fast lookups during ETL and UI
        self.env.cr.execute("""
            SELECT 1 FROM pg_indexes
            WHERE indexname = 'customer_enrichment_customer_id_idx'
        """)
        if not self.env.cr.fetchone():
            _logger.info(
                'Creating index customer_enrichment_customer_id_idx'
            )
            self.env.cr.execute("""
                CREATE INDEX customer_enrichment_customer_id_idx
                ON customer_enrichment (customer_id)
            """)

        # Index on position for searching by PEP status
        self.env.cr.execute("""
            SELECT 1 FROM pg_indexes
            WHERE indexname = 'customer_enrichment_position_idx'
        """)
        if not self.env.cr.fetchone():
            _logger.info(
                'Creating index customer_enrichment_position_idx'
            )
            self.env.cr.execute("""
                CREATE INDEX customer_enrichment_position_idx
                ON customer_enrichment (LOWER(position))
            """)

        # Link orphaned enrichment records to their partners
        # This runs on every module upgrade to ensure all records are linked
        _logger.info('Linking orphaned enrichment records to partners...')
        self.env.cr.execute("""
            UPDATE res_partner rp
            SET enrichment_id = ce.id
            FROM customer_enrichment ce
            WHERE ce.customer_id = rp.customer_id
              AND rp.customer_id IS NOT NULL
              AND (rp.enrichment_id IS NULL OR rp.enrichment_id != ce.id)
        """)
        linked_count = self.env.cr.rowcount
        if linked_count > 0:
            _logger.info(f'Linked {linked_count} partners to enrichment records')
