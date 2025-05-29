# -*- coding: utf-8 -*-

import logging
import time
from odoo import api, fields
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class MaterializedViewService:
    """Service for managing materialized views for chart and statistic optimization."""

    def __init__(self, env=None):
        """Initialize the MaterializedViewService.

        Args:
            env (Environment, optional): The Odoo environment. Defaults to None.
        """
        self.env = env

    def create_chart_materialized_view(self, chart_id):
        """Create or update a materialized view for a chart.

        Args:
            chart_id (int): The ID of the chart for which to create the view.

        Returns:
            bool: True if the view was created successfully, False otherwise.
        """
        if not self.env:
            return False

        _logger.info(f"Creating materialized view for chart {chart_id}")
        view_name = f"dashboard_chart_view_{chart_id}"

        try:
            chart = self.env["res.dashboard.charts"].browse(chart_id)
            if not chart.exists():
                _logger.error(f"Chart {chart_id} not found")
                return False

            chart_query = chart.query
            chart_name = chart.name
            if not chart_query:
                _logger.error(f"Chart {chart_id} has no query defined")
                return False

            original_query = chart_query.strip()
            if original_query.endswith(";"):
                original_query = original_query[:-1]

            # Enhance query for branch coverage
            enhanced_query = self._create_enhanced_query_for_branch_coverage(original_query, chart)

            from ..services.database_service import DatabaseService
            db_service = DatabaseService(self.env)

            # Drop existing view if any
            db_service.drop_materialized_view(view_name)

            # Create the view
            if not db_service.create_materialized_view(view_name, enhanced_query):
                _logger.error(f"Failed to create materialized view {view_name}")
                return False

            # Get columns to create indexes
            columns = db_service.get_table_columns(view_name)
            if not columns:
                _logger.error(f"No columns found in materialized view {view_name}")
                return False

            # Create indexes for improved performance
            self._create_indexes_for_view(view_name, chart, columns)

            # Create or update refresher record
            refresher = self.env["dashboard.chart.view.refresher"].search([("chart_id", "=", chart_id)], limit=1)
            now = fields.Datetime.now()
            if refresher:
                refresher.write({
                    "view_name": view_name,
                    "last_refresh": now,
                    "refresh_interval": chart.materialized_view_refresh_interval or 60,
                })
            else:
                self.env["dashboard.chart.view.refresher"].create({
                    "name": f"Refresher for {chart_name}",
                    "chart_id": chart_id,
                    "view_name": view_name,
                    "last_refresh": now,
                    "refresh_interval": chart.materialized_view_refresh_interval or 60,
                })

            # Update chart record
            chart.write({
                "materialized_view_last_refresh": now,
                "last_execution_status": "success",
                "last_error_message": False,
            })

            _logger.info(f"Materialized view {view_name} created successfully")
            return True
        except Exception as e:
            _logger.error(f"Error creating materialized view for chart {chart_id}: {e}")
            try:
                chart = self.env["res.dashboard.charts"].browse(chart_id)
                if chart.exists():
                    chart.write({
                        "last_execution_status": "error",
                        "last_error_message": str(e),
                    })
            except Exception as write_error:
                _logger.error(f"Error updating chart error status: {write_error}")
            return False

    def _create_enhanced_query_for_branch_coverage(self, original_query, chart):
        """Create an enhanced query to ensure all branches are included by converting INNER JOINs to LEFT JOINs.

        Args:
            original_query (str): The original SQL query to enhance.
            chart (record): The chart record used for context.

        Returns:
            str: The enhanced SQL query.
        """
        from ..services.query_service import QueryService
        
        has_branch_field = bool(chart.branch_field)
        if not has_branch_field:
            _logger.info(f"Query doesn't need enhancement - no branch field")
            return original_query

        _logger.info(f"Enhancing query to include ALL branches using LEFT JOINs")
        try:
            enhanced_query = QueryService.convert_inner_joins_to_left_joins(original_query)
            if enhanced_query != original_query:
                _logger.info(f"Enhanced query created")
                return enhanced_query
            else:
                _logger.info(f"No JOIN conversion needed, using original query")
                return original_query
        except Exception as e:
            _logger.warning(f"Could not enhance query: {e}")
            return original_query

    def _create_indexes_for_view(self, view_name, chart, columns):
        """Create indexes on a materialized view for better performance.

        Args:
            view_name (str): The name of the materialized view.
            chart (record): The chart record containing field information.
            columns (list): The list of column names in the view.

        Returns:
            bool: True if successful, False otherwise.
        """
        from ..services.database_service import DatabaseService
        from ..services.query_service import QueryService
        
        db_service = DatabaseService(self.env)
        
        try:
            # Check for a potential unique column for concurrency
            primary_candidates = ["id", "record_id", "row_id", "row_number"]
            unique_col = None
            for col in primary_candidates:
                if col in columns:
                    unique_col = col
                    break

            if not unique_col and "id" in columns:
                try:
                    with self.env.registry.cursor() as cr:
                        cr.execute(f"SELECT COUNT(*), COUNT(DISTINCT id) FROM {view_name}")
                        total_count, distinct_count = cr.fetchone()
                        if total_count == distinct_count:
                            unique_col = "id"
                            _logger.info(f"'id' column is unique, using it for unique index")
                except Exception as e:
                    _logger.debug(f"Could not determine uniqueness: {e}")

            if unique_col:
                db_service.create_index_on_view(view_name, unique_col, unique=True, 
                                               index_name=f"{view_name}_unique_idx")

            # Create indexes for common fields
            if chart.x_axis_field:
                x_col = QueryService.find_column_in_view(chart.x_axis_field, columns)
                if x_col:
                    db_service.create_index_on_view(view_name, x_col, index_name=f"{view_name}_x_idx")

            if chart.y_axis_field:
                y_col = QueryService.find_column_in_view(chart.y_axis_field, columns)
                if y_col:
                    db_service.create_index_on_view(view_name, y_col, index_name=f"{view_name}_y_idx")

            if chart.date_field:
                date_col = QueryService.find_column_in_view(chart.date_field, columns)
                if date_col:
                    db_service.create_index_on_view(view_name, date_col, index_name=f"{view_name}_date_idx")

            if chart.branch_field:
                branch_col = QueryService.find_column_in_view(chart.branch_field, columns)
                if branch_col:
                    db_service.create_index_on_view(view_name, branch_col, index_name=f"{view_name}_branch_idx")

            return True
        except Exception as e:
            _logger.error(f"Error creating indexes for view {view_name}: {e}")
            return False

    def refresh_chart_view(self, chart_id, low_priority=False):
        """Refresh a materialized view for a chart with robust error handling and transaction isolation.

        Args:
            chart_id (int): The ID of the chart to refresh.
            low_priority (bool, optional): Indicates if the refresh should be low priority.

        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        if not self.env:
            return False
            
        try:
            refresher = self.env["dashboard.chart.view.refresher"].search([("chart_id", "=", chart_id)], limit=1)
            if not refresher:
                return self.create_chart_materialized_view(chart_id)
                
            chart = refresher.chart_id
            view_name = refresher.view_name
            
            # Check if refresh is needed based on interval
            if (
                not low_priority
                and refresher.last_refresh
                and refresher.refresh_interval
            ):
                now = fields.Datetime.now()
                if (
                    refresher.last_refresh
                    + timedelta(minutes=refresher.refresh_interval)
                    > now
                ):
                    _logger.debug(f"Skipping refresh for chart {chart_id}, not time yet")
                    return True
                    
            # Check if view exists
            from ..services.database_service import DatabaseService
            db_service = DatabaseService(self.env)
            view_exists = db_service.check_view_exists(view_name)
            
            if not view_exists:
                return self.create_chart_materialized_view(chart_id)
                
            # Try to refresh concurrently first
            registry = self.env.registry
            concurrent_success = False
            
            try:
                with registry.cursor() as cr:
                    if low_priority:
                        cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                    lock_acquired = cr.fetchone()[0]
                    if not lock_acquired:
                        _logger.info(f"Another process is refreshing view for chart {chart_id}, skipping")
                        return False
                        
                    cr.execute(
                        """
                        SELECT COUNT(*) FROM pg_indexes 
                        WHERE tablename = %s 
                        AND indexdef LIKE %s
                    """,
                        (view_name, "%UNIQUE%"),
                    )
                    has_unique_index = cr.fetchone()[0] > 0
                    
                    if has_unique_index:
                        _logger.info(f"Refreshing view {view_name} with CONCURRENTLY option")
                        cr.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
                        cr.commit()
                        concurrent_success = True
                    else:
                        _logger.info(f"View {view_name} has no unique index, skipping CONCURRENTLY refresh")
                        cr.rollback()
            except Exception as e:
                _logger.info(f"CONCURRENTLY refresh failed: {e}")
                
            # If concurrent refresh failed, try regular refresh
            if not concurrent_success:
                with registry.cursor() as cr:
                    try:
                        if low_priority:
                            cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                        lock_acquired = cr.fetchone()[0]
                        if not lock_acquired:
                            _logger.info(f"Another process is refreshing view for chart {chart_id}, skipping")
                            return False
                            
                        _logger.info(f"Refreshing view {view_name} with regular refresh")
                        cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                        cr.commit()
                    except Exception as e:
                        cr.rollback()
                        _logger.error(f"Regular refresh failed: {e}")
                        raise e
                        
            # Update timestamps
            now = fields.Datetime.now()
            
            with registry.cursor() as write_cr:
                try:
                    write_cr.execute(
                        """
                        UPDATE dashboard_chart_view_refresher
                        SET last_refresh = %s
                        WHERE id = %s
                    """,
                        (now, refresher.id),
                    )
                    write_cr.commit()
                except Exception as e:
                    write_cr.rollback()
                    _logger.error(f"Failed to update refresher timestamp: {e}")
                    
            with registry.cursor() as write_cr:
                try:
                    write_cr.execute(
                        """
                        UPDATE res_dashboard_charts
                        SET materialized_view_last_refresh = %s,
                            last_execution_status = %s,
                            last_error_message = NULL
                        WHERE id = %s
                    """,
                        (now, "success", chart_id),
                    )
                    write_cr.commit()
                except Exception as e:
                    write_cr.rollback()
                    _logger.error(f"Failed to update chart timestamp: {e}")
                    
            _logger.info(f"Successfully refreshed materialized view {view_name} for chart {chart_id}")
            return True
        except Exception as e:
            _logger.error(f"Error refreshing materialized view for chart {chart_id}: {e}")
            try:
                registry = self.env.registry
                with registry.cursor() as err_cr:
                    err_cr.execute(
                        """
                        UPDATE res_dashboard_charts
                        SET last_execution_status = %s,
                            last_error_message = %s
                        WHERE id = %s
                    """,
                        ("error", str(e), chart_id),
                    )
                    err_cr.commit()
            except Exception as write_err:
                _logger.error(f"Failed to update error status: {write_err}")
            return False

    def create_stat_materialized_view(self, stat_id):
        """Create or update a materialized view for a statistic.

        Args:
            stat_id (int): The ID of the statistic for which to create the view.

        Returns:
            bool: True if the view was created successfully, False otherwise.
        """
        if not self.env:
            return False
            
        try:
            stat = self.env["res.compliance.stat"].browse(stat_id)
            if not stat.exists():
                _logger.error(f"Statistic {stat_id} not found")
                return False
                
            view_name = f"stat_view_{stat_id}"
            
            from ..services.database_service import DatabaseService
            db_service = DatabaseService(self.env)
            
            # Check if view already exists
            if db_service.check_view_exists(view_name):
                _logger.info(f"Materialized view {view_name} already exists, skipping creation")
                return True
                
            # Prepare query
            original_query, query = stat._prepare_and_validate_query(stat.sql_query)
            if not original_query:
                _logger.error(f"Invalid query for statistic {stat_id}")
                return False
                
            # Ensure query doesn't end with semicolon
            if original_query.strip().endswith(";"):
                original_query = original_query.strip()[:-1]
                
            # Create the view
            if not db_service.create_materialized_view(view_name, original_query):
                _logger.error(f"Failed to create materialized view {view_name}")
                return False
                
            # Create or update refresher record
            refresher = self.env["dashboard.stats.view.refresher"].search([("stat_id", "=", stat_id)], limit=1)
            now = fields.Datetime.now()
            if refresher:
                refresher.write({
                    "view_name": view_name,
                    "last_refresh": now,
                })
            else:
                self.env["dashboard.stats.view.refresher"].create({
                    "name": f"Refresher for {stat.name}",
                    "stat_id": stat_id,
                    "view_name": view_name,
                    "last_refresh": now,
                })
                
            # Update statistic record
            stat.write({
                "materialized_view_last_refresh": now,
            })
            
            _logger.info(f"Successfully created materialized view {view_name} for statistic {stat_id}")
            return True
        except Exception as e:
            _logger.error(f"Error creating materialized view for statistic {stat_id}: {e}")
            return False

    def refresh_stat_view(self, stat_id, low_priority=False):
        """Refresh a materialized view for a statistic.

        Args:
            stat_id (int): The ID of the statistic to refresh.
            low_priority (bool, optional): Indicates if the refresh should be low priority.

        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        if not self.env:
            return False
            
        try:
            registry = self.env.registry
            with registry.cursor() as cr:
                if low_priority:
                    cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (stat_id,))
                lock_acquired = cr.fetchone()[0]
                if not lock_acquired:
                    _logger.info(f"Another process is refreshing view for statistic {stat_id}, skipping")
                    return False
                    
            view_name = f"stat_view_{stat_id}"
            
            from ..services.database_service import DatabaseService
            db_service = DatabaseService(self.env)
            
            # Check if view exists
            if not db_service.check_view_exists(view_name):
                return self.create_stat_materialized_view(stat_id)
                
            # Refresh the view
            if not db_service.refresh_materialized_view(view_name, concurrently=False):
                _logger.error(f"Failed to refresh materialized view {view_name}")
                return False
                
            # Update refresher record
            refresher = self.env["dashboard.stats.view.refresher"].search([("stat_id", "=", stat_id)], limit=1)
            now = fields.Datetime.now()
            if refresher:
                refresher.write({
                    "last_refresh": now,
                })
                
            # Update statistic record
            stat = self.env["res.compliance.stat"].browse(stat_id)
            if stat.exists():
                stat.write({
                    "materialized_view_last_refresh": now,
                })
                
            _logger.info(f"Refreshed materialized view {view_name} for statistic {stat_id}")
            return True
        except Exception as e:
            _logger.error(f"Error refreshing materialized view for statistic {stat_id}: {e}")
            return False

    def refresh_all_chart_views(self, low_priority=False):
        """Refresh all chart materialized views with isolated transactions.
        
        Args:
            low_priority (bool, optional): Indicates if the refresh should be low priority.
            
        Returns:
            dict: A dictionary containing success and error counts.
        """
        if not self.env:
            return {"refreshed": 0, "errors": 0}
            
        refreshed = 0
        errors = 0
        
        try:
            # Initialize refresher record if needed
            refresher_record = self.env["dashboard.chart.view.refresher"].search([], limit=1)
            if not refresher_record:
                try:
                    refresher_record = self.env["dashboard.chart.view.refresher"].create(
                        {"name": "Dashboard Chart View Refresher"}
                    )
                    _logger.info("Created new Dashboard Chart View Refresher record")
                except Exception as e:
                    _logger.error(f"Could not create refresher record: {e}")
                    
            # Get charts to refresh
            charts_to_refresh = self.env["res.dashboard.charts"].search(
                [("state", "=", "active"), ("use_materialized_view", "=", True)]
            )
            
            _logger.info(f"Found {len(charts_to_refresh)} charts with materialized views to refresh")
            
            # Initialize database settings for optimal performance
            from ..services.database_service import DatabaseService
            db_service = DatabaseService(self.env)
            db_service.initialize_db_settings()
            
            # Refresh each chart
            for chart in charts_to_refresh:
                try:
                    if self.refresh_chart_view(chart.id, low_priority):
                        refreshed += 1
                    else:
                        errors += 1
                except Exception as chart_error:
                    _logger.error(f"Error processing chart {chart.id}: {chart_error}")
                    errors += 1
                    
            # Update refresher record
            try:
                with self.env.registry.cursor() as update_cr:
                    env = api.Environment(update_cr, self.env.uid, self.env.context)
                    refresher_to_update = env["dashboard.chart.view.refresher"].browse(refresher_record.id)
                    if refresher_to_update.exists():
                        refresher_to_update.write({"last_run": fields.Datetime.now()})
                        update_cr.commit()
            except Exception as update_err:
                _logger.warning(f"Could not update last run time: {update_err}")
                
            _logger.info(f"Refreshed {refreshed} dashboard chart views, {errors} errors")
            return {"refreshed": refreshed, "errors": errors}
        except Exception as e:
            _logger.error(f"Error in refresh_all_chart_views: {e}")
            return {"refreshed": refreshed, "errors": errors}

    def refresh_all_stat_views(self, low_priority=False):
        """Refresh all statistic materialized views.
        
        Args:
            low_priority (bool, optional): Indicates if the refresh should be low priority.
            
        Returns:
            dict: A dictionary containing success and error counts.
        """
        if not self.env:
            return {"refreshed": 0, "errors": 0}
            
        refreshed = 0
        errors = 0
        
        try:
            # Get statistics to refresh
            stats = self.env["res.compliance.stat"].search([("state", "=", "active"), ("use_materialized_view", "=", True)])
            
            # Refresh each statistic
            for stat in stats:
                if self.refresh_stat_view(stat.id, low_priority):
                    refreshed += 1
                else:
                    errors += 1
                    
            # Update refresher record
            refresher = self.env["dashboard.stats.view.refresher"].search([], limit=1)
            if refresher:
                refresher.write({"last_run": fields.Datetime.now()})
                
            _logger.info(f"Refreshed {refreshed} statistic views, {errors} errors")
            return {"refreshed": refreshed, "errors": errors}
        except Exception as e:
            _logger.error(f"Error refreshing statistic views: {e}")
            return {"refreshed": refreshed, "errors": errors}

    def ensure_all_views_exist(self):
        """Ensure all materialized views exist and are properly created.
        
        Returns:
            dict: A dictionary containing the results of the operation.
        """
        if not self.env:
            return {"created": 0, "errors": 0}
            
        chart_created = 0
        chart_errors = 0
        stat_created = 0
        stat_errors = 0
        
        try:
            # Ensure chart views exist
            charts = self.env["res.dashboard.charts"].search(
                [("state", "=", "active"), ("use_materialized_view", "=", True)]
            )
            
            if charts:
                _logger.info(f"Found {len(charts)} charts with materialized views")
                for chart in charts:
                    view_name = f"dashboard_chart_view_{chart.id}"
                    
                    from ..services.database_service import DatabaseService
                    db_service = DatabaseService(self.env)
                    if not db_service.check_view_exists(view_name):
                        _logger.info(f"View for chart {chart.id} needs creation")
                        if self.create_chart_materialized_view(chart.id):
                            chart_created += 1
                        else:
                            chart_errors += 1
                            
            # Ensure statistic views exist
            stats = self.env["res.compliance.stat"].search([("state", "=", "active"), ("use_materialized_view", "=", True)])
            
            if stats:
                _logger.info(f"Found {len(stats)} statistics with materialized views")
                for stat in stats:
                    view_name = f"stat_view_{stat.id}"
                    
                    from ..services.database_service import DatabaseService
                    db_service = DatabaseService(self.env)
                    if not db_service.check_view_exists(view_name):
                        _logger.info(f"View for statistic {stat.id} needs creation")
                        if self.create_stat_materialized_view(stat.id):
                            stat_created += 1
                        else:
                            stat_errors += 1
                            
            _logger.info(f"View initialization complete: {chart_created} charts, {stat_created} stats created, {chart_errors + stat_errors} errors")
            return {
                "chart_created": chart_created,
                "chart_errors": chart_errors,
                "stat_created": stat_created,
                "stat_errors": stat_errors,
            }
        except Exception as e:
            _logger.error(f"Error ensuring all views exist: {e}")
            return {
                "chart_created": chart_created,
                "chart_errors": chart_errors,
                "stat_created": stat_created,
                "stat_errors": stat_errors,
                "error": str(e),
            }