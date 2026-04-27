# -*- coding: utf-8 -*-
import math
from odoo import api, fields, models


class AMLCustomerProfile(models.Model):
    _name = 'res.aml.customer.profile'
    _description = 'AML Customer Behavioral Profile'
    _rec_name = 'customer_id'

    customer_id = fields.Many2one(
        'res.partner', string='Customer', required=True, index=True, ondelete='cascade',
    )
    transaction_count = fields.Integer(string='Transaction Count', default=0)
    mean_amount = fields.Float(string='Mean Transaction Amount', default=0.0, digits=(20, 4))
    # Welford's M2 accumulator: sum of squared deviations from the running mean
    m2 = fields.Float(string='Variance Accumulator (M2)', default=0.0, digits=(20, 6))
    stddev_amount = fields.Float(
        string='Std Dev Amount', compute='_compute_stddev', store=True, digits=(20, 4),
    )
    last_updated = fields.Datetime(string='Last Updated', readonly=True)

    _sql_constraints = [
        ('unique_customer', 'UNIQUE(customer_id)', 'A behavioral profile already exists for this customer.'),
    ]

    @api.depends('m2', 'transaction_count')
    def _compute_stddev(self):
        for rec in self:
            if rec.transaction_count >= 2:
                rec.stddev_amount = math.sqrt(rec.m2 / (rec.transaction_count - 1))
            else:
                rec.stddev_amount = 0.0

    def update_with_transaction(self, amount):
        """Welford's online algorithm — incremental mean and variance update."""
        self.ensure_one()
        n = self.transaction_count + 1
        delta = amount - self.mean_amount
        new_mean = self.mean_amount + delta / n
        delta2 = amount - new_mean
        self.write({
            'transaction_count': n,
            'mean_amount': new_mean,
            'm2': self.m2 + delta * delta2,
            'last_updated': fields.Datetime.now(),
        })

    def compute_zscore(self, amount):
        """Z-score of amount vs this customer's baseline. Returns None if insufficient history."""
        self.ensure_one()
        if self.transaction_count < 2 or self.stddev_amount == 0:
            return None
        return (amount - self.mean_amount) / self.stddev_amount

    @api.model
    def get_or_create_profile(self, customer_id):
        profile = self.search([('customer_id', '=', customer_id)], limit=1)
        if not profile:
            profile = self.create({'customer_id': customer_id})
        return profile

    def _cron_rebuild_profiles(self):
        """Full rebuild of all customer profiles from transaction history (weekly maintenance)."""
        self.env.cr.execute("""
            SELECT
                t.customer_id,
                COUNT(*)                               AS cnt,
                AVG(t.amount)                          AS mean,
                COALESCE(VAR_POP(t.amount) * COUNT(*), 0) AS m2_val
            FROM res_customer_transaction t
            WHERE t.customer_id IS NOT NULL
              AND t.state NOT IN ('cancelled', 'rejected')
            GROUP BY t.customer_id
        """)
        rows = self.env.cr.fetchall()
        for customer_id, cnt, mean, m2_val in rows:
            vals = {
                'transaction_count': int(cnt),
                'mean_amount': float(mean or 0),
                'm2': float(m2_val or 0),
                'last_updated': fields.Datetime.now(),
            }
            profile = self.search([('customer_id', '=', customer_id)], limit=1)
            if profile:
                profile.write(vals)
            else:
                vals['customer_id'] = customer_id
                self.create(vals)
