# -*- coding: utf-8 -*-
from odoo import api, fields, models
import logging

_logger = logging.getLogger(__name__)


class CustomerEnrichment(models.Model):
    """
    Customer Enrichment Data for Sterling Bank.

    Stores bank-specific enrichment data linked directly to customers via
    customer number. Provides extended compliance information including PEP
    status, source of funds/wealth, and related attributes.

    Data is spooled from Sterling Bank's ETL source table and linked to
    customers automatically via customer_id.
    """

    _name = 'customer.enrichment'
    _description = 'Customer Enrichment Data'
    _order = 'customer_id'

    _sql_constraints = [
        (
            'uniq_customer_enrichment',
            'unique(customer_id)',
            'Customer enrichment record already exists. '
            'Each customer can only have one enrichment record.'
        ),
    ]

    customer_id = fields.Char(
        string='Customer Number',
        required=True,
        index=True,
        help='The bank customer number used to link this enrichment record to res.partner.'
    )

    partner_id = fields.Many2one(
        comodel_name='res.partner',
        string='Customer',
        compute='_compute_partner_id',
        store=True,
        help='The linked customer record computed from customer_id.'
    )

    customer_type = fields.Selection(
        selection=[('individual', 'Individual'), ('corporate', 'Corporate')],
        string='Customer Type',
        compute='_compute_customer_type',
        store=True,
    )

    display_name_enriched = fields.Char(
        string='Display Name',
        compute='_compute_display_name_enriched',
        store=True,
    )

    pep_name = fields.Char(string='PEP Name')

    individual_name = fields.Char(string='Individual Name')

    corporate_name = fields.Char(string='Corporate Name')

    position = fields.Text(
        string='Position/Designation',
        help='The political or official position/designation of the PEP.'
    )

    relationship = fields.Char(
        string='Relationship with PEP',
        help='e.g., ACCOUNT OWNER, SIGNATORY, RELATED PARTY'
    )

    source_of_fund = fields.Text(string='Source of Fund')

    source_of_wealth = fields.Text(string='Source of Wealth')

    nature_of_business = fields.Text(string='Nature of Business')

    branch_name = fields.Char(string='Branch')

    bvn = fields.Char(string='BVN')

    @api.depends('customer_id')
    def _compute_partner_id(self):
        Partner = self.env['res.partner']
        for record in self:
            if record.customer_id:
                partner = Partner.search([('customer_id', '=', record.customer_id)], limit=1)
                record.partner_id = partner.id if partner else False
            else:
                record.partner_id = False

    @api.depends('partner_id.account_category')
    def _compute_customer_type(self):
        CustomerTypeConfig = self.env['customer.type.config']
        for record in self:
            if record.partner_id and record.partner_id.account_category:
                record.customer_type = CustomerTypeConfig.get_customer_type(
                    record.partner_id.account_category
                )
            else:
                record.customer_type = 'corporate'

    @api.depends('customer_type', 'individual_name', 'corporate_name')
    def _compute_display_name_enriched(self):
        for record in self:
            if record.customer_type == 'corporate':
                record.display_name_enriched = record.individual_name or record.corporate_name or ''
            else:
                record.display_name_enriched = record.individual_name or ''

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        records._link_to_partners()
        return records

    def write(self, vals):
        if 'customer_id' in vals:
            self._unlink_from_partners()
        result = super().write(vals)
        if 'customer_id' in vals:
            self._link_to_partners()
        return result

    def unlink(self):
        self._unlink_from_partners()
        return super().unlink()

    def _link_to_partners(self):
        Partner = self.env['res.partner'].sudo()
        for record in self:
            if record.customer_id:
                partner = Partner.search([('customer_id', '=', record.customer_id)], limit=1)
                if partner and partner.enrichment_id != record:
                    partner.write({'enrichment_id': record.id})

    def _unlink_from_partners(self):
        Partner = self.env['res.partner'].sudo()
        partners = Partner.search([('enrichment_id', 'in', self.ids)])
        if partners:
            partners.write({'enrichment_id': False})

    def init(self):
        self.env.cr.execute("""
            SELECT 1 FROM pg_indexes WHERE indexname = 'customer_enrichment_customer_id_idx'
        """)
        if not self.env.cr.fetchone():
            self.env.cr.execute("""
                CREATE INDEX customer_enrichment_customer_id_idx
                ON customer_enrichment (customer_id)
            """)

        self.env.cr.execute("""
            SELECT 1 FROM pg_indexes WHERE indexname = 'customer_enrichment_position_idx'
        """)
        if not self.env.cr.fetchone():
            self.env.cr.execute("""
                CREATE INDEX customer_enrichment_position_idx
                ON customer_enrichment (LOWER(position))
            """)

        self.env.cr.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'res_partner' AND column_name = 'enrichment_id'
        """)
        if self.env.cr.fetchone():
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
