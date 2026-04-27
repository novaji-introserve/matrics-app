# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from odoo import models, api

_logger = logging.getLogger(__name__)

RISK_TO_CASE_RATING = {
    'low': 'low',
    'medium': 'medium',
    'high': 'high',
}


class AlertHistoryCaseAutoCreate(models.Model):
    """Extends alert.history to auto-create a case.manager record on alert creation."""

    _inherit = 'alert.history'

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        for alert in records:
            try:
                self._create_case_from_alert(alert)
            except Exception as e:
                # Log but never block the alert from being saved
                _logger.warning(
                    "Failed to auto-create case for alert %s: %s",
                    alert.alert_id, e
                )
        return records

    def _create_case_from_alert(self, alert):
        """Create a case.manager record linked to this alert."""
        CaseManager = self.env['case.manager']

        # ── Case Rating (required) ────────────────────────────────────────
        risk = (alert.risk_rating or '').lower()
        case_rating = RISK_TO_CASE_RATING.get(risk, 'low')

        # ── Responsible officer & supervisors (required) ──────────────────
        officer = self.env.user
        supervisors = officer

        # ── Event date ───────────────────────────────────────────────────
        event_date = datetime.now().replace(microsecond=0)
        if alert.date_created:
            try:
                event_date = datetime.fromisoformat(alert.date_created)
            except (ValueError, TypeError):
                pass

        # ── Process category & process (required) ─────────────────────────
        # Look for an "Alert" category first, fall back to first available
        process_category = self.env['exception.category.'].search(
            [('name', 'ilike', 'alert')], limit=1
        )
        if not process_category:
            process_category = self.env['exception.category.'].search([], limit=1)
        if not process_category:
            _logger.warning(
                "No exception.category. records found; skipping case creation for alert %s",
                alert.alert_id
            )
            return

        process = self.env['exception.process.'].search(
            [('type_id', '=', process_category.id)], limit=1
        )
        if not process:
            process = self.env['exception.process.'].search([], limit=1)
        if not process:
            _logger.warning(
                "No exception.process. records found; skipping case creation for alert %s",
                alert.alert_id
            )
            return

        # ── Narration / required action ───────────────────────────────────
        narration = alert.narration or f"Auto-generated from alert {alert.alert_id}"
        cases_action = (
            f"Review alert {alert.alert_id} raised from source: {alert.source}. "
            f"{alert.narration or ''}"
        ).strip()

        # ── Customer (optional) ───────────────────────────────────────────
        customer_id = False
        if alert.ref_id and hasattr(alert.ref_id, 'customer_id'):
            customer_id = alert.ref_id.customer_id.id if alert.ref_id.customer_id else False

        case_vals = {
            'case_rating': case_rating,
            'case_status': 'draft',
            'officer_responsible': officer.id,
            'supervisors': [(4, officer.id)],
            'event_date': event_date,
            'process_category': process_category.id,
            'process': process.id,
            'narration': narration,
            'cases_action': cases_action,
            'customer_id': customer_id,
        }

        case = CaseManager.create(case_vals)
        _logger.info(
            "Auto-created case %s for alert %s",
            case.case_ref, alert.alert_id
        )
