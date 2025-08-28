# -*- coding: utf-8 -*-

from odoo import models, fields, api
import logging

from ..services.materialized_view import MaterializedViewService
from ..services.database_service import DatabaseService
from ..services.security_service import SecurityService

_logger = logging.getLogger(__name__)

class DashboardChartViewRefresher(models.Model):
    """Model for refreshing dashboard chart materialized views"""

    _name = "dashboard.chart.view.refresher"
    _description = "Dashboard Chart View Refresher"
    name = fields.Char(
        string="Refresher Name", default="Dashboard Chart View Refresher"
    )
    last_run = fields.Datetime(string="Last Run", readonly=True)
    chart_id = fields.Many2one(
        "res.dashboard.charts", string="Chart", ondelete="cascade"
    )
    view_name = fields.Char(string="View Name", readonly=True)
    last_refresh = fields.Datetime(string="Last Refresh", readonly=True)
    refresh_interval = fields.Integer(string="Refresh Interval (minutes)", default=60)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    _sql_constraints = [
        (
            "unique_chart",
            "unique(chart_id)",
            "Only one materialized view per chart is allowed.",
        )
    ]

    @api.model
    def refresh_chart_views(self, low_priority=False):
        """Refresh all chart materialized views with isolated transactions.
        
        Args:
            low_priority (bool, optional): Indicates if the refresh should be low priority.
            
        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        mv_service = MaterializedViewService(self.env)
        result = mv_service.refresh_all_chart_views(low_priority)
        
        refresher_record = self.search([], limit=1)
        if not refresher_record:
            try:
                refresher_record = self.create(
                    {"name": "Dashboard Chart View Refresher"}
                )
                _logger.info("Created new Dashboard Chart View Refresher record")
            except Exception as e:
                _logger.error(f"Could not create refresher record: {e}")
        
        if refresher_record:
            refresher_record.write({"last_run": fields.Datetime.now()})
            
        return True if result.get('refreshed', 0) > 0 else False

    @api.model
    def refresh_chart_view(self, chart_id, low_priority=False):
        """Refresh a materialized view for a chart with robust error handling and concurrency control.
        
        Args:
            chart_id (int): The ID of the chart to refresh.
            low_priority (bool, optional): Indicates if the refresh should be low priority.
            
        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        mv_service = MaterializedViewService(self.env)
        return mv_service.refresh_chart_view(chart_id, low_priority)

    def diagnose_materialized_view(self, chart_id):
        """Diagnose issues with a materialized view for a chart.
        
        Args:
            chart_id (int): The ID of the chart to diagnose.
            
        Returns:
            dict: A dictionary containing diagnosis results including view existence and column details.
        """
        db_service = DatabaseService(self.env)
        view_name = f"dashboard_chart_view_{chart_id}"
        
        view_exists = db_service.check_view_exists(view_name)
        if not view_exists:
            _logger.error(f"Materialized view {view_name} does not exist!")
            return {"view_exists": False, "has_columns": False}

        columns = db_service.get_table_columns(view_name)
        if not columns:
            return {"view_exists": True, "has_columns": False}
 
        try:
            with self.env.registry.cursor() as cr:
                cr.execute(f"SELECT COUNT(*) FROM {view_name}")
                row_count = cr.fetchone()[0]
        except Exception as e:
            _logger.error(f"Error counting rows in {view_name}: {e}")
            row_count = -1
        
        try:
            with self.env.registry.cursor() as cr:
                cr.execute(
                    """
                    SELECT query 
                    FROM res_dashboard_charts
                    WHERE id = %s
                """,
                    (chart_id,),
                )
                query_result = cr.fetchone()
                original_query = query_result[0] if query_result else None
                query_works = False
                
                if original_query:
                    try:
                        clean_query = original_query.strip()
                        if clean_query.endswith(";"):
                            clean_query = clean_query[:-1]
                        cr.execute("SET statement_timeout = 10000")
                        cr.execute(clean_query)
                        query_works = True
                    except Exception as query_error:
                        _logger.error(f"Original query error: {query_error}")
        except Exception as e:
            _logger.error(f"Error checking original query: {e}")
            query_works = False
            
        return {
            "view_exists": True,
            "has_columns": len(columns) > 0,
            "column_count": len(columns),
            "columns": [{"name": col, "type": "unknown"} for col in columns],
            "row_count": row_count,
            "original_query_works": query_works,
        }

    @api.model
    def create_materialized_view_for_chart(self, chart_id):
        """Create or update a materialized view for a chart with robust transaction handling.
        
        Args:
            chart_id (int): The ID of the chart for which to create the view.
            
        Returns:
            bool: True if the view was created successfully, False otherwise.
        """
        mv_service = MaterializedViewService(self.env)
        success = mv_service.create_chart_materialized_view(chart_id)
        
        if success:
            chart = self.env["res.dashboard.charts"].browse(chart_id)
            view_name = f"dashboard_chart_view_{chart_id}"
            
            refresher = self.search([("chart_id", "=", chart_id)], limit=1)
            now = fields.Datetime.now()
            
            if refresher:
                refresher.write({
                    "view_name": view_name,
                    "last_refresh": now,
                    "refresh_interval": chart.materialized_view_refresh_interval or 60,
                })
            else:
                self.create({
                    "name": f"Refresher for {chart.name}",
                    "chart_id": chart_id,
                    "view_name": view_name,
                    "last_refresh": now,
                    "refresh_interval": chart.materialized_view_refresh_interval or 60,
                })
                
        return success

    @api.model
    def drop_materialized_view_for_chart(self, chart_id):
        """Drop a materialized view for a chart.
        
        Args:
            chart_id (int): The ID of the chart for which to drop the view.
            
        Returns:
            bool: True if the view was dropped successfully, False otherwise.
        """
        view_name = f"dashboard_chart_view_{chart_id}"
        db_service = DatabaseService(self.env)
        success = db_service.drop_materialized_view(view_name)
        
        if success:
            refresher = self.search([("chart_id", "=", chart_id)], limit=1)
            if refresher:
                refresher.unlink()
                
            chart = self.env["res.dashboard.charts"].browse(chart_id)
            if chart.exists():
                chart.write({
                    "materialized_view_last_refresh": None,
                })
                
        return success

    def diagnose_chart_issues(self):
        """Diagnose and fix common issues with charts.
        
        Returns:
            dict: A dictionary containing the results of the diagnosis.
        """
        try:
            timeout_charts = self.env["res.dashboard.charts"].search(
                [
                    ("last_error_message", "ilike", "timeout"),
                    ("use_materialized_view", "=", False),
                ]
            )
            
            for chart in timeout_charts:
                chart.write({
                    "use_materialized_view": True,
                    "materialized_view_refresh_interval": 60,
                    "last_error_message": "Auto-enabled materialized view due to timeout history",
                })
                self.create_materialized_view_for_chart(chart.id)
                
            syntax_error_charts = self.env["res.dashboard.charts"].search(
                [
                    "|",
                    ("last_error_message", "ilike", "syntax error"),
                    ("last_error_message", "ilike", "missing FROM-clause"),
                ]
            )
            
            self.env.cr.execute(
                """
                UPDATE dashboard_chart_view_refresher r
                SET view_name = 'dashboard_chart_view_' || r.chart_id
                WHERE view_name IS NULL OR view_name = ''
            """
            )
            
            return {
                "timeout_charts_fixed": len(timeout_charts),
                "syntax_error_charts": len(syntax_error_charts),
            }
        except Exception as e:
            _logger.error(f"Error diagnosing chart issues: {e}")
            return {"error": str(e)}

    def create_performance_indexes(self):
        """Create database indexes to improve query performance.
        
        Returns:
            bool: True if the indexes were created successfully, False otherwise.
        """
        db_service = DatabaseService(self.env)
        try:
            self.env.cr.execute(
                """
                -- Index for res_partner.branch_id - improves join performance
                CREATE INDEX IF NOT EXISTS idx_res_partner_branch_id 
                ON res_partner (branch_id);
                -- Index for res_partner.risk_level - improves filtering
                CREATE INDEX IF NOT EXISTS idx_res_partner_risk_level
                ON res_partner (risk_level) 
                WHERE risk_level = 'high';
                -- Composite index for branch + risk filtering
                CREATE INDEX IF NOT EXISTS idx_res_partner_branch_risk
                ON res_partner (branch_id, risk_level) 
                WHERE risk_level = 'high';
                -- Index for origin filtering
                CREATE INDEX IF NOT EXISTS idx_res_partner_origin
                ON res_partner (origin) 
                WHERE origin IN ('demo', 'test', 'prod');
            """
            )
            _logger.info("Created performance indexes successfully")
            return True
        except Exception as e:
            _logger.error(f"Error creating performance indexes: {e}")
            return False

    def initialize_database_settings(self):
        """Initialize database settings for optimal performance.
        
        Returns:
            bool: True if the settings were initialized successfully, False otherwise.
        """
        db_service = DatabaseService(self.env)
        return db_service.initialize_db_settings()

    def setup_dashboard_tables(self):
        """Set up required tables for dashboard functionality.
        
        Returns:
            bool: True if the tables were created successfully, False otherwise.
        """
        try:
            self.env.cr.execute(
                """
                -- Create update log table for tracking materialized view updates
                CREATE TABLE IF NOT EXISTS res_dashboard_charts_update_log (
                    chart_id INTEGER PRIMARY KEY,
                    update_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    message TEXT
                );
                -- Create lock tracking table for managing concurrent operations
                CREATE TABLE IF NOT EXISTS res_dashboard_operation_locks (
                    lock_key VARCHAR(255) PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMP NOT NULL
                );
                -- Create index on expiry to help with cleanup
                CREATE INDEX IF NOT EXISTS idx_dashboard_locks_expiry 
                ON res_dashboard_operation_locks(expires_at);
            """
            )
            _logger.info("Dashboard tables created successfully")
            return True
        except Exception as e:
            _logger.error(f"Error setting up dashboard tables: {e}")
            return False

    @api.model
    def ensure_all_views_exist(self):
        """Ensure all materialized views exist and are properly created.
        
        Returns:
            bool: True if all views exist, False if an error occurred.
        """
        mv_service = MaterializedViewService(self.env)
        result = mv_service.ensure_all_views_exist()
        return result.get('chart_errors', 0) == 0

    @api.model
    def init(self):
        """Initialize views on server startup.
        
        This method ensures that a refresher record exists and all necessary 
        materialized views are created.
        """
        super(DashboardChartViewRefresher, self).init()
        self.env.cr.commit()
        
        try:
            self.ensure_all_views_exist()
        except Exception as e:
            _logger.error(f"Error in init for DashboardChartViewRefresher: {e}")
            
        try:
            if not self.setup_dashboard_tables():
                _logger.error("Failed to set up dashboard tables during init")
        except Exception as e:
            _logger.error(f"Error setting up dashboard tables in init: {e}")
            
        if not self.search([], limit=1):
            self.create({"name": "Dashboard Chart View Refresher"})
            
            charts = self.env["res.dashboard.charts"].search(
                [("state", "=", "active"), ("use_materialized_view", "=", True)]
            )
            for chart in charts:
                self.create_materialized_view_for_chart(chart.id)
                