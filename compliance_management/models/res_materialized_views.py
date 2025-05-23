from odoo import models, api
import logging

_logger = logging.getLogger(__name__)

class CronCaller(models.Model):
    _name = 'res.materialized.views'
    _description = 'Cron Caller for Materialized Views'

    @api.model
    def run_all_materialized_views_cron_tasks(self, low_priority=False):
        _logger.info("Starting materialized views cron task (low_priority=%s)", low_priority)

        self.env['dashboard.chart.view.refresher'].refresh_chart_views(low_priority=low_priority)
        self.env['dashboard.stats.view.refresher'].refresh_stat_views(low_priority=low_priority)

        _logger.info("Finished materialized views cron task")
