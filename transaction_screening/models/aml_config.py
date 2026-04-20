# -*- coding: utf-8 -*-
from odoo import api, fields, models
from odoo.exceptions import ValidationError


class AMLConfig(models.Model):
    _name = 'res.aml.config'
    _description = 'AML Detection Configuration'
    _order = 'write_date desc'

    name = fields.Char(string='Config Name', required=True, default='AML Configuration')
    active = fields.Boolean(default=True)

    currency_id = fields.Many2one(
        'res.currency', string='Currency',
        default=lambda self: self.env.company.currency_id,
        required=True,
    )

    # CTR threshold — configurable per NFIU directive
    ctr_threshold = fields.Monetary(
        string='CTR Threshold',
        currency_field='currency_id',
        default=5000000.0,
        required=True,
        help="Cash Transaction Report threshold. Transactions at or above this trigger CTR filing.",
    )

    # Velocity detection
    velocity_window_hours = fields.Integer(
        string='Velocity Window (hours)', default=24, required=True,
        help="Rolling window in hours for counting transactions per customer.",
    )
    velocity_max_count = fields.Integer(
        string='Max Transactions in Window', default=10, required=True,
        help="Flag if customer transaction count exceeds this within the velocity window.",
    )
    velocity_max_amount = fields.Monetary(
        string='Max Total Amount in Window',
        currency_field='currency_id',
        default=10000000.0,
        required=True,
        help="Flag if total transaction volume exceeds this within the velocity window.",
    )

    # Structuring / smurfing detection
    structuring_window_hours = fields.Integer(
        string='Structuring Window (hours)', default=72, required=True,
        help="Rolling window in hours for detecting structuring patterns.",
    )
    structuring_min_count = fields.Integer(
        string='Min Sub-Threshold Transactions', default=3, required=True,
        help="Minimum number of sub-CTR-threshold transactions to trigger structuring check.",
    )
    structuring_approach_pct = fields.Float(
        string='Threshold Approach % (for total)', default=80.0, required=True,
        help="Flag structuring if the sum of sub-threshold transactions reaches this % of the CTR threshold.",
    )

    # Anomaly / Z-score detection
    anomaly_zscore_threshold = fields.Float(
        string='Anomaly Z-Score Threshold', default=3.0, required=True,
        help="Transactions with a Z-score above this relative to customer baseline are flagged.",
    )
    anomaly_min_history = fields.Integer(
        string='Min Transactions for Baseline', default=10, required=True,
        help="Minimum historical transactions required before anomaly detection activates.",
    )

    # Composite risk weights (must sum to 1.0)
    velocity_risk_weight = fields.Float(string='Velocity Risk Weight', default=0.30)
    structuring_risk_weight = fields.Float(string='Structuring Risk Weight', default=0.40)
    anomaly_risk_weight = fields.Float(string='Anomaly Risk Weight', default=0.30)

    @api.constrains('velocity_risk_weight', 'structuring_risk_weight', 'anomaly_risk_weight')
    def _check_weights(self):
        for rec in self:
            total = rec.velocity_risk_weight + rec.structuring_risk_weight + rec.anomaly_risk_weight
            if abs(total - 1.0) > 0.001:
                raise ValidationError(
                    "Risk weights must sum to 1.0 (currently %.3f). Adjust Velocity + Structuring + Anomaly weights." % total
                )

    @api.constrains('ctr_threshold')
    def _check_ctr_threshold(self):
        for rec in self:
            if rec.ctr_threshold <= 0:
                raise ValidationError("CTR threshold must be a positive amount.")

    @api.model
    def get_active_config(self):
        """Return active config, creating a default if none exists."""
        config = self.search([('active', '=', True)], limit=1, order='write_date desc')
        if not config:
            config = self.create({'name': 'Default AML Configuration'})
        return config
