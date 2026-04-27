# -*- coding: utf-8 -*-

from odoo import models, fields, api
import logging

from ..services.materialized_view import MaterializedViewService
from ..services.database_service import DatabaseService

_logger = logging.getLogger(__name__)

class StatisticViewRefresher(models.Model):
    """Model for refreshing dashboard statistic materialized views."""

    _name = "dashboard.stats.view.refresher"
    _description = "Statistics View Refresher"
    name = fields.Char(string="Refresher Name", default="Statistics View Refresher")
    last_run = fields.Datetime(string="Last Run", readonly=True)
    stat_id = fields.Many2one(
        "res.compliance.stat", string="Statistic", ondelete="cascade"
    )
    view_name = fields.Char(string="View Name", readonly=True)
    last_refresh = fields.Datetime(string="Last Refresh", readonly=True)
    refresh_interval = fields.Integer(string="Refresh Interval (minutes)", default=60)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    _sql_constraints = [
        (
            "unique_stat",
            "unique(stat_id)",
            "Only one materialized view per statistic is allowed.",
        )
    ]

    @api.model
    def refresh_stat_views(self, low_priority=False):
        """Refresh all statistic materialized views.

        Args:
            low_priority (bool, optional): Indicates if the refresh should be low priority.

        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        mv_service = MaterializedViewService(self.env)
        result = mv_service.refresh_all_stat_views(low_priority)
        
        refresher = self.search([], limit=1)
        if refresher:
            refresher.write({"last_run": fields.Datetime.now()})
            
        return result.get('refreshed', 0) > 0

    @api.model
    def refresh_stat_view(self, stat_id, low_priority=False):
        """Refresh a materialized view for a statistic.

        Args:
            stat_id (int): The ID of the statistic to refresh.
            low_priority (bool, optional): Indicates if the refresh should be low priority.

        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        mv_service = MaterializedViewService(self.env)
        success = mv_service.refresh_stat_view(stat_id, low_priority)
        
        if success:
            refresher = self.search([("stat_id", "=", stat_id)], limit=1)
            now = fields.Datetime.now()
            if refresher:
                refresher.write({"last_refresh": now})
                
            stat = self.env["res.compliance.stat"].browse(stat_id)
            if stat.exists():
                stat.write({"materialized_view_last_refresh": now})
                
        return success

    @api.model
    def create_materialized_view_for_stat(self, stat_id):
        """Create or update a materialized view for a statistic.

        Args:
            stat_id (int): The ID of the statistic for which to create the view.

        Returns:
            bool: True if the view was created successfully, False otherwise.
        """
        mv_service = MaterializedViewService(self.env)
        success = mv_service.create_stat_materialized_view(stat_id)

        if success:
            stat = self.env["res.compliance.stat"].browse(stat_id)
            sanitized_code = MaterializedViewService.sanitize_view_name(stat.code)
            view_name = f"stat_view_{sanitized_code}"
            
            refresher = self.search([("stat_id", "=", stat_id)], limit=1)
            now = fields.Datetime.now()
            
            if refresher:
                refresher.write({
                    "view_name": view_name,
                    "last_refresh": now,
                })
            else:
                self.create({
                    "name": f"Refresher for {stat.name}",
                    "stat_id": stat_id,
                    "view_name": view_name,
                    "last_refresh": now,
                })
                
        return success

    @api.model
    def ensure_all_stat_views_exist(self):
        """Ensure all statistic materialized views exist.

        This method checks for all active statistics and creates their corresponding materialized views.

        Returns:
            bool: True if all views exist, False if an error occurred.
        """
        _logger.info("Ensuring all statistic materialized views exist")
        try:
            stats = self.env["res.compliance.stat"].search([
                ("state", "=", "active"), 
                ("use_materialized_view", "=", True)
            ])
            
            if not stats:
                _logger.info("No statistics found with materialized views enabled")
                return True
                
            _logger.info(f"Found {len(stats)} statistics to create views for")
            created = 0
            errors = 0
            
            for stat in stats:
                sanitized_code = MaterializedViewService.sanitize_view_name(stat.code)
                view_name = f"stat_view_{sanitized_code}"

                db_service = DatabaseService(self.env)
                view_exists = db_service.check_view_exists(view_name)
                
                if not view_exists:
                    _logger.info(f"Creating materialized view for statistic {stat.id}")
                    if self.create_materialized_view_for_stat(stat.id):
                        created += 1
                    else:
                        errors += 1
                        
            _logger.info(f"Materialized view initialization complete: {created} created, {errors} errors")
            return errors == 0
        except Exception as e:
            _logger.error(f"Error ensuring all statistic views exist: {e}")
            return False

    @api.model
    def init(self):
        """Initialize views on server startup.

        This method ensures that a refresher record exists and all necessary statistic views are created.
        """
        super(StatisticViewRefresher, self).init()
        
        if not self.search([], limit=1):
            self.create({"name": "Statistics View Refresher"})
            
        self.env.cr.commit()
        
        try:
            self.ensure_all_stat_views_exist()
        except Exception as e:
            _logger.error(f"Error in init for StatisticViewRefresher: {e}")
            