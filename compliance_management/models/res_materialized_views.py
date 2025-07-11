from odoo import models, api,fields
import logging

_logger = logging.getLogger(__name__)

class CronCaller(models.Model):
    """Cron Caller for managing materialized views updates."""
    
    
    _name = 'res.materialized.views'
    _description = 'Cron Caller for Materialized Views'
    # active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
    @api.model
    def run_all_materialized_views_cron_tasks(self, low_priority=False):
        """Execute cron tasks to refresh all materialized views.

        This method triggers the refresh of dashboard chart and statistics materialized views.
        
        Args:
            low_priority (bool, optional): If True, the refresh tasks will run with low priority.

        Returns:
            None
        """
        _logger.info("Starting materialized views cron task (low_priority=%s)", low_priority)

        self.env['dashboard.chart.view.refresher'].refresh_chart_views(low_priority=low_priority)
        self.env['dashboard.stats.view.refresher'].refresh_stat_views(low_priority=low_priority)

        _logger.info("Finished materialized views cron task")
        