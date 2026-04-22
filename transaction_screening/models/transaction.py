# -*- coding: utf-8 -*-
from datetime import timedelta
from odoo import _, api, fields, models


class Transaction(models.Model):
    _inherit = 'res.customer.transaction'

    blocked = fields.Boolean(
        string='Blocked', default=False, tracking=True, index=True,
        help="Indicates if the transaction is blocked by any screening rule.",
    )
    aml_risk_score = fields.Float(
        string='AML Risk Score', digits=(5, 2), default=0.0, readonly=True,
        help="Composite AML risk score (0–100) from velocity, structuring and anomaly checks.",
    )
    aml_flags = fields.Char(
        string='AML Flags', readonly=True,
        help="Comma-separated list of triggered AML checks: VELOCITY, STRUCTURING, ANOMALY.",
    )
    aml_velocity_alert_ids = fields.One2many(
        'res.aml.velocity.alert', 'transaction_id', string='Velocity Alerts', readonly=True,
    )
    aml_structuring_alert_ids = fields.One2many(
        'res.aml.structuring.alert', 'transaction_id', string='Structuring Alerts', readonly=True,
    )
    aml_anomaly_alert_ids = fields.One2many(
        'res.aml.anomaly.alert', 'transaction_id', string='Anomaly Alerts', readonly=True,
    )
    aml_dormant_alert_ids = fields.One2many(
        'res.aml.dormant.alert', 'transaction_id', string='Dormant Account Alerts', readonly=True,
    )

    @api.model
    def action_view_blocked_transactions(self):
        user = self.env.user
        compliance_groups = [
            'compliance_management.group_compliance_chief_compliance_officer',
            'compliance_management.group_compliance_compliance_officer',
            'compliance_management.group_compliance_transaction_monitoring_team',
        ]
        has_compliance_access = any(user.has_group(g) for g in compliance_groups)

        if has_compliance_access:
            domain = [('blocked', '=', True)]
        else:
            domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('blocked', '=', True),
            ]

        return {
            'name': _('Transactions On-Hold / Blocked'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1},
        }

    def action_unblock(self):
        self.ensure_one()
        for transaction in self:
            transaction.write({'blocked': False})

    def action_screen(self, rules=None):
        result = super().action_screen(rules=rules)
        exceptions = self.env['res.transaction.screening.history'].search([
            ('transaction_id', '=', self.id)
        ])
        if exceptions:
            for exception in exceptions:
                rule = exception.rule_id
                if rule.blocked:
                    self.write({'blocked': True})
        self._run_aml_detection()
        return result

    # ------------------------------------------------------------------ AML detection

    def _run_aml_detection(self):
        """Run velocity, structuring, anomaly and dormant checks; update composite aml_risk_score."""
        if not self.customer_id:
            return 0.0

        config = self.env['res.aml.config'].get_active_config(currency=self.currency_id)
        flags = []

        v_score = self._check_velocity(config)
        if v_score > 0:
            flags.append('VELOCITY')

        s_score = self._check_structuring(config)
        if s_score > 0:
            flags.append('STRUCTURING')

        a_score = self._check_anomaly(config)
        if a_score > 0:
            flags.append('ANOMALY')

        d_score = self._check_dormant(config)
        if d_score > 0:
            flags.append('DORMANT')

        composite = round(
            v_score * config.velocity_risk_weight +
            s_score * config.structuring_risk_weight +
            a_score * config.anomaly_risk_weight +
            d_score * config.dormant_risk_weight,
            2,
        )

        self.write({
            'aml_risk_score': composite,
            'aml_flags': ','.join(flags) if flags else False,
        })
        return composite

    def _check_velocity(self, config):
        """Flag if customer has too many or too large transactions within the velocity window."""
        window_start = fields.Datetime.now() - timedelta(hours=config.velocity_window_hours)
        domain = [
            ('customer_id', '=', self.customer_id.id),
            ('date_created', '>=', window_start),
            ('id', '!=', self.id),
            ('state', 'not in', ['cancelled', 'rejected']),
        ]
        recent = self.env['res.customer.transaction'].search(domain)
        count = len(recent) + 1
        total = sum(t.amount for t in recent) + (self.amount or 0.0)

        if count > config.velocity_max_count or total > config.velocity_max_amount:
            count_ratio = count / config.velocity_max_count if config.velocity_max_count else 1
            amount_ratio = total / config.velocity_max_amount if config.velocity_max_amount else 1
            risk_score = min(100.0, max(count_ratio, amount_ratio) * 100)
            self.env['res.aml.velocity.alert'].create({
                'transaction_id': self.id,
                'customer_id': self.customer_id.id,
                'window_hours': config.velocity_window_hours,
                'txn_count': count,
                'total_amount': total,
                'risk_score': risk_score,
            })
            return risk_score
        return 0.0

    def _check_structuring(self, config):
        """Detect structuring: multiple sub-CTR-threshold transactions whose total approaches the threshold."""
        # Pull NFIU-mandated threshold by currency + customer type (individual vs corporate)
        status_value = self.customer_id.customer_status.customer_status
        customer_type = self.env['customer.type.config'].get_customer_type(status_value)
        nfiu_threshold = self.env['nfiu.currency.threshold'].search([
            ('currency_id', '=', self.currency_id.id),
            ('customer_type', '=', customer_type),
        ], limit=1)
        if not nfiu_threshold:
            return 0.0
        threshold = nfiu_threshold.threshold
        amount = self.amount or 0.0

        # A transaction at or above the threshold is a direct CTR event, not structuring
        if amount >= threshold:
            return 0.0

        window_start = fields.Datetime.now() - timedelta(hours=config.structuring_window_hours)
        domain = [
            ('customer_id', '=', self.customer_id.id),
            ('date_created', '>=', window_start),
            ('amount', '<', threshold),
            ('id', '!=', self.id),
            ('state', 'not in', ['cancelled', 'rejected']),
        ]
        sub_threshold = self.env['res.customer.transaction'].search(domain)
        count = len(sub_threshold) + 1
        total = sum(t.amount for t in sub_threshold) + amount

        approach_limit = threshold * (config.structuring_approach_pct / 100.0)
        if count >= config.structuring_min_count and total >= approach_limit:
            risk_score = min(100.0, (total / threshold) * 70 + (count / config.structuring_min_count) * 30)
            self.env['res.aml.structuring.alert'].create({
                'transaction_id': self.id,
                'customer_id': self.customer_id.id,
                'window_hours': config.structuring_window_hours,
                'txn_count': count,
                'total_amount': total,
                'ctr_threshold': threshold,
                'risk_score': risk_score,
            })
            return risk_score
        return 0.0

    def _check_anomaly(self, config):
        """Z-score anomaly detection against the customer's behavioral baseline."""
        amount = self.amount or 0.0
        profile = self.env['res.aml.customer.profile'].get_or_create_profile(self.customer_id.id)

        if profile.transaction_count < config.anomaly_min_history:
            profile.update_with_transaction(amount)
            return 0.0

        zscore = profile.compute_zscore(amount)
        risk_score = 0.0

        if zscore is not None and abs(zscore) >= config.anomaly_zscore_threshold:
            risk_score = min(100.0, (abs(zscore) / config.anomaly_zscore_threshold) * 50)
            self.env['res.aml.anomaly.alert'].create({
                'transaction_id': self.id,
                'customer_id': self.customer_id.id,
                'transaction_amount': amount,
                'customer_mean': profile.mean_amount,
                'customer_stddev': profile.stddev_amount,
                'zscore': zscore,
                'risk_score': risk_score,
            })

        profile.update_with_transaction(amount)
        return risk_score

    def _check_dormant(self, config):
        """Flag if the customer's account was inactive for longer than dormant_min_days."""
        if not config.dormant_enabled:
            return 0.0

        current_date = self.date_created or fields.Datetime.now()
        last_txn = self.env['res.customer.transaction'].search([
            ('customer_id', '=', self.customer_id.id),
            ('date_created', '<', current_date),
            ('id', '!=', self.id),
            ('state', 'not in', ['cancelled', 'rejected']),
        ], order='date_created desc', limit=1)

        if not last_txn:
            return 0.0

        dormant_days = (current_date - last_txn.date_created).days
        if dormant_days < config.dormant_min_days:
            return 0.0

        risk_score = min(100.0, (dormant_days / config.dormant_min_days) * 60)
        self.env['res.aml.dormant.alert'].create({
            'transaction_id': self.id,
            'customer_id': self.customer_id.id,
            'last_transaction_date': last_txn.date_created,
            'dormant_days': dormant_days,
            'transaction_amount': self.amount or 0.0,
            'risk_score': risk_score,
        })
        return risk_score
