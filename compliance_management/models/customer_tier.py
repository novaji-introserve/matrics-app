# -*- coding: utf-8 -*-

from odoo import models, fields, api, _, tools


class CustomerTier(models.Model):
    _name = 'res.partner.tier'
    _description = 'Customer Tier'
    _sql_constraints = [
        ('uniq_customer_tier_code', 'unique(code)',
         "Tier code already exists. Code must be unique!"),
    ]
    _order = "name"
    
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True, index=True)
    tier_level = fields.Selection([
        ('1', 'Tier 1'),
        ('2', 'Tier 2'),
        ('3', 'Tier 3')
    ], string="Tier Level", required=True, index=True)
    risk_assessment = fields.Many2one(
        comodel_name='res.risk.assessment', string='Risk Assessment', index=True)
    status = fields.Selection(string='Status', selection=[
        ('active', 'Active'),
        ('inactive', 'Inactive')
    ], default='active', index=True)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
    @api.model_create_multi
    def create(self, vals_list):
        records = super(CustomerTier, self).create(vals_list)
        # Refresh the materialized view when tiers change
        self.env['account.tier.materialized'].refresh_view()
        return records

    def write(self, vals):
        result = super(CustomerTier, self).write(vals)
        # Refresh the materialized view when tiers change
        self.env['account.tier.materialized'].refresh_view()
        return result
    
        
class AccountTierMaterialized(models.Model):
    """
    Materialized view for efficient tier lookups across millions of records
    """
    _name = 'account.tier.materialized'
    _description = 'Account Tier Materialized View'
    _auto = False  # This is not a regular table

    id = fields.Integer(readonly=True)
    account_id = fields.Many2one(
        'res.partner.account', string="Account", readonly=True)
    name = fields.Char(string="Account Number", readonly=True)
    customer_id = fields.Many2one(
        'res.partner', string='Customer', readonly=True)
    category = fields.Char(string="Category Code", readonly=True)
    tier_level = fields.Selection([
        ('1', 'Tier 1'),
        ('2', 'Tier 2'),
        ('3', 'Tier 3')
    ], string="Tier Level", readonly=True)
    tier_name = fields.Char(string="Account Tier", readonly=True)

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
    def init(self):
        """Initialize the materialized view with indexes"""

        # Drop and recreate the materialized view
        self._cr.execute(
            "DROP MATERIALIZED VIEW IF EXISTS account_tier_materialized")
        self._cr.execute("""
            CREATE MATERIALIZED VIEW account_tier_materialized AS (
                SELECT 
                    a.id,
                    a.id as account_id,
                    a.name,
                    a.customer_id,
                    a.category,
                    COALESCE(t.tier_level, '3') as tier_level,
                    COALESCE(t.name, 'Tier 3') as tier_name
                FROM res_partner_account a 
                LEFT JOIN res_partner_tier t ON 
                    a.category = t.code AND t.status = 'active'
            )
        """)

        # Create indexes on the materialized view for fast lookups
        self._cr.execute(
            "CREATE INDEX account_tier_mat_id_idx ON account_tier_materialized(id)")
        self._cr.execute(
            "CREATE INDEX account_tier_mat_level_idx ON account_tier_materialized(tier_level)")
        self._cr.execute(
            "CREATE INDEX account_tier_mat_customer_idx ON account_tier_materialized(customer_id)")
        self._cr.execute(
            "CREATE INDEX account_tier_mat_tier_name_idx ON account_tier_materialized(tier_name)")

    @api.model
    def refresh_view(self):
        """Refresh the materialized view"""
        self._cr.execute("REFRESH MATERIALIZED VIEW account_tier_materialized")
        return True
   
    @api.model
    def get_tier_ids(self, tier_level):
        """Get account IDs with the specified tier level"""
        if tier_level not in ('1', '2', '3'):
            return []
        
        self._cr.execute("""
            SELECT account_id FROM account_tier_materialized 
            WHERE tier_level = %s
        """, (tier_level,))
        result = self._cr.fetchall()
        return [r[0] for r in result] if result else []
    