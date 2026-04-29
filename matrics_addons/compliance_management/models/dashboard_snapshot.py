import json
import logging
from datetime import date, datetime, timedelta

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class DashboardSnapshot(models.Model):
    _name = 'dashboard.snapshot'
    _description = 'Dashboard Chart Snapshot'

    chart_id = fields.Many2one(
        'res.dashboard.charts', required=True, ondelete='cascade', index=True)
    raw_data = fields.Text(string='Raw Data (JSON)')
    last_updated = fields.Datetime(default=fields.Datetime.now, index=True)
    status = fields.Selection([('ok', 'OK'), ('error', 'Error')], default='ok')
    error_msg = fields.Text()

    _sql_constraints = [
        ('unique_chart', 'UNIQUE(chart_id)', 'One snapshot per chart')
    ]

    @api.model
    def _cron_refresh_all(self):
        """Run every 5 minutes: execute each active chart SQL and store results."""
        charts = self.env['res.dashboard.charts'].search([
            ('state', '=', 'active'), ('active', '=', True)
        ])
        refreshed = 0
        for chart in charts:
            self._refresh_chart(chart)
            refreshed += 1

        _logger.info(f"Dashboard snapshot: refreshed {refreshed} charts")

        # Notify all connected dashboards to reload
        try:
            self.env['bus.bus']._sendmany([[
                'dashboard_refresh_channel', 'refresh', {
                    'type': 'refresh',
                    'channelName': 'dashboard_refresh_channel',
                }
            ]])
        except Exception as e:
            _logger.warning(f"Dashboard bus notification failed: {e}")

    def _refresh_chart(self, chart):
        """Execute chart SQL and upsert the snapshot row."""
        def _serialize(obj):
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()
            if obj is None:
                return None
            try:
                return float(obj)
            except (TypeError, ValueError):
                return str(obj)

        try:
            self.env.cr.execute(chart.query)
            cols = [d[0] for d in self.env.cr.description]
            rows = [dict(zip(cols, r)) for r in self.env.cr.fetchall()]
            raw_json = json.dumps(rows, default=_serialize)

            vals = {
                'raw_data': raw_json,
                'last_updated': fields.Datetime.now(),
                'status': 'ok',
                'error_msg': False,
            }
            existing = self.search([('chart_id', '=', chart.id)], limit=1)
            if existing:
                existing.write(vals)
            else:
                self.create({'chart_id': chart.id, **vals})
            self.env.cr.commit()
        except Exception as e:
            _logger.error(f"Snapshot refresh failed for chart {chart.id} ({chart.name}): {e}")
            existing = self.search([('chart_id', '=', chart.id)], limit=1)
            if existing:
                existing.write({'status': 'error', 'error_msg': str(e)})
            self.env.cr.commit()

    def get_chart_data(self, chart, cco, branches_id, datepicked=None):
        """
        Return pre-computed rows for a chart, applying branch/date filters in Python.
        Returns None if no valid snapshot exists.
        """
        snapshot = self.search(
            [('chart_id', '=', chart.id), ('status', '=', 'ok')], limit=1)
        if not snapshot or not snapshot.raw_data:
            return None

        # Reject stale snapshots older than 15 minutes
        if snapshot.last_updated:
            age = (datetime.now() - fields.Datetime.from_string(snapshot.last_updated))
            if age > timedelta(minutes=15):
                return None

        rows = json.loads(snapshot.raw_data)

        # Branch filter in Python (only when not CCO and branch field is set)
        if not cco and branches_id and chart.branch_field:
            rows = [r for r in rows if r.get(chart.branch_field) in branches_id]

        # Date filter in Python
        if datepicked and chart.date_field:
            try:
                cutoff = (datetime.now() - timedelta(days=int(datepicked))).isoformat()
                rows = [r for r in rows if str(r.get(chart.date_field, '')) >= cutoff]
            except (ValueError, TypeError):
                pass

        return rows
