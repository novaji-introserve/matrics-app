# -*- coding: utf-8 -*-

import time
from requests import request
from ..controllers.charts import DynamicChartController
from odoo import models, fields, api
import logging
import re
from datetime import timedelta
from ..services.chart_data_service import ChartDataService

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
        refreshed = 0
        errors = 0
        refresher_record = self.search([], limit=1)
        if not refresher_record:
            try:
                refresher_record = self.create(
                    {"name": "Dashboard Chart View Refresher"}
                )
                _logger.info("Created new Dashboard Chart View Refresher record")
            except Exception as e:
                _logger.error(f"Could not create refresher record: {e}")
        try:
            charts_to_refresh = self.env["res.dashboard.charts"].search(
                [("state", "=", "active"), ("use_materialized_view", "=", True)]
            )
            _logger.info(
                f"Found {len(charts_to_refresh)} charts with materialized views to refresh"
            )
            for chart in charts_to_refresh:
                try:
                    view_name = f"dashboard_chart_view_{chart.id}"
                    view_exists = False
                    with self.env.registry.cursor() as check_cr:
                        check_cr.execute(
                            """
                            SELECT EXISTS (
                                SELECT FROM pg_catalog.pg_class c
                                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                WHERE c.relname = %s AND c.relkind = 'm'
                            )
                        """,
                            (view_name,),
                        )
                        view_exists = check_cr.fetchone()[0]
                    if not view_exists:
                        _logger.info(
                            f"Materialized view for chart {chart.id} doesn't exist, creating it"
                        )
                        if self.create_materialized_view_for_chart(chart.id):
                            refreshed += 1
                        else:
                            errors += 1
                        continue
                    with self.env.registry.cursor() as settings_cr:
                        try:
                            env = api.Environment(
                                settings_cr, self.env.uid, self.env.context
                            )
                            refresher = env["dashboard.chart.view.refresher"]
                            refresher.initialize_database_settings()
                            settings_cr.commit()
                            _logger.info(
                                "Database settings initialized for optimal performance"
                            )
                        except Exception as settings_err:
                            _logger.warning(
                                f"Could not initialize settings, continuing anyway: {settings_err}"
                            )
                            settings_cr.rollback()
                    success = self.refresh_chart_view(chart.id, low_priority)
                    if success:
                        refreshed += 1
                    else:
                        errors += 1
                except Exception as chart_error:
                    _logger.error(f"Error processing chart {chart.id}: {chart_error}")
                    errors += 1
                    continue
            try:
                with self.env.registry.cursor() as update_cr:
                    env = api.Environment(update_cr, self.env.uid, self.env.context)
                    refresher_to_update = env["dashboard.chart.view.refresher"].browse(
                        refresher_record.id
                    )
                    if refresher_to_update.exists():
                        refresher_to_update.write({"last_run": fields.Datetime.now()})
                        update_cr.commit()
            except Exception as update_err:
                _logger.warning(f"Could not update last run time: {update_err}")
            _logger.info(
                f"Refreshed {refreshed} dashboard chart views, {errors} errors"
            )
            return True
        except Exception as e:
            _logger.error(f"Error in refresh_chart_views main process: {e}")
            return False

    @api.model
    def refresh_chart_view(self, chart_id, low_priority=False):
        """Refresh a materialized view for a chart with robust error handling and concurrency control.
        Args:
            chart_id (int): The ID of the chart to refresh.
            low_priority (bool, optional): Indicates if the refresh should be low priority.
        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        try:
            registry = self.env.registry
            with registry.cursor() as cr:
                try:
                    if low_priority:
                        cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                    lock_acquired = cr.fetchone()[0]
                    if not lock_acquired:
                        _logger.info(
                            f"Another process is refreshing view for chart {chart_id}, skipping"
                        )
                        return False
                    cr.execute(
                        """
                        SELECT r.id, r.view_name, r.last_refresh, r.refresh_interval, c.id as chart_id
                        FROM dashboard_chart_view_refresher r
                        JOIN res_dashboard_charts c ON r.chart_id = c.id
                        WHERE r.chart_id = %s
                        FOR UPDATE SKIP LOCKED
                    """,
                        (chart_id,),
                    )
                    refresher_data = cr.dictfetchone()
                    if not refresher_data:
                        cr.rollback()
                        return self.create_materialized_view_for_chart(chart_id)
                    view_name = refresher_data["view_name"]
                    last_refresh = refresher_data["last_refresh"]
                    refresh_interval = refresher_data["refresh_interval"]
                    if not low_priority and last_refresh and refresh_interval:
                        now = fields.Datetime.now()
                        if last_refresh + timedelta(minutes=refresh_interval) > now:
                            _logger.debug(
                                f"Skipping refresh for chart {chart_id}, not time yet"
                            )
                            cr.rollback()
                            return True
                    cr.execute("SET LOCAL statement_timeout = 120000;")
                    cr.execute(
                        f"""
                        SELECT EXISTS (
                            SELECT FROM pg_catalog.pg_class c
                            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                            WHERE c.relname = %s
                            AND c.relkind = 'm'
                        )
                    """,
                        (view_name,),
                    )
                    view_exists = cr.fetchone()[0]
                    if not view_exists:
                        cr.rollback()
                        return self.create_materialized_view_for_chart(chart_id)
                    cr.execute(
                        f"/* Refreshing materialized view {view_name} for chart {chart_id} */"
                    )
                    try:
                        cr.execute(
                            f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}"
                        )
                    except Exception as e:
                        _logger.info(
                            f"CONCURRENTLY refresh failed, using regular refresh: {e}"
                        )
                        cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                    now = fields.Datetime.now()
                    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                    cr.execute(
                        """
                        UPDATE dashboard_chart_view_refresher
                        SET last_refresh = %s,
                            write_uid = %s, write_date = %s
                        WHERE id = %s
                    """,
                        (now_str, self.env.uid, now_str, refresher_data["id"]),
                    )
                    cr.execute(
                        """
                        UPDATE res_dashboard_charts
                        SET materialized_view_last_refresh = %s,
                            last_execution_status = %s,
                            last_error_message = NULL,
                            write_uid = %s, write_date = %s
                        WHERE id = %s
                    """,
                        (now_str, "success", self.env.uid, now_str, chart_id),
                    )
                    cr.commit()
                    _logger.info(
                        f"Refreshed materialized view {view_name} for chart {chart_id}"
                    )
                    return True
                except Exception as e:
                    cr.rollback()
                    _logger.error(
                        f"Error refreshing materialized view for chart {chart_id}: {e}"
                    )
                    with registry.cursor() as err_cr:
                        try:
                            err_cr.execute(
                                """
                                UPDATE res_dashboard_charts
                                SET last_execution_status = %s,
                                    last_error_message = %s,
                                    write_uid = %s, write_date = %s
                                WHERE id = %s
                            """,
                                (
                                    "error",
                                    str(e),
                                    self.env.uid,
                                    fields.Datetime.now(),
                                    chart_id,
                                ),
                            )
                            err_cr.commit()
                        except Exception as err_write:
                            err_cr.rollback()
                            _logger.error(f"Failed to update error status: {err_write}")
                    return False
        except Exception as e:
            _logger.error(
                f"Error refreshing materialized view for chart {chart_id}: {e}"
            )
            return False

    def diagnose_materialized_view(self, chart_id):
        """Diagnose issues with a materialized view for a chart.
        Args:
            chart_id (int): The ID of the chart to diagnose.
        Returns:
            dict: A dictionary containing diagnosis results including view existence and column details.
        """
        try:
            view_name = f"dashboard_chart_view_{chart_id}"
            registry = self.env.registry
            with registry.cursor() as cr:
                cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cr.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %s
                        AND c.relkind = 'm'
                    )
                """,
                    (view_name,),
                )
                view_exists = cr.fetchone()[0]
                if not view_exists:
                    _logger.error(f"Materialized view {view_name} does not exist!")
                    return {"view_exists": False, "has_columns": False}
                columns = []
                try:
                    cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    columns = [desc[0] for desc in cr.description]
                except Exception as e:
                    _logger.debug(f"Error in direct column query: {e}")
                if not columns:
                    try:
                        cr.execute(
                            """
                            SELECT a.attname
                            FROM pg_catalog.pg_attribute a
                            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                            WHERE c.relname = %s
                            AND a.attnum > 0 AND NOT a.attisdropped
                            ORDER BY a.attnum
                        """,
                            (view_name,),
                        )
                        columns = [row[0] for row in cr.fetchall()]
                    except Exception as e:
                        _logger.debug(f"Error in system catalog query: {e}")
                if not columns:
                    try:
                        cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                        columns = [desc[0] for desc in cr.description]
                    except Exception as e:
                        _logger.debug(f"Error in real query: {e}")
                column_info = [{"name": col, "type": "unknown"} for col in columns]
                row_count = -1
                try:
                    cr.execute(f"SELECT COUNT(*) FROM {view_name}")
                    row_count = cr.fetchone()[0]
                except Exception as e:
                    _logger.error(f"Error counting rows in {view_name}: {e}")
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
                return {
                    "view_exists": True,
                    "has_columns": len(columns) > 0,
                    "column_count": len(column_info),
                    "columns": column_info,
                    "row_count": row_count,
                    "original_query_works": query_works,
                }
        except Exception as e:
            _logger.error(f"Diagnostic error for view {chart_id}: {e}")
            return {"error": str(e), "view_exists": False, "has_columns": False}

    def create_materialized_view_for_chart(self, chart_id):
        """Create or update a materialized view for a chart with robust transaction handling.
        Args:
            chart_id (int): The ID of the chart for which to create the view.
        Returns:
            bool: True if the view was created successfully, False otherwise.
        """
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
            enhanced_query = self._create_enhanced_query_for_branch_coverage(
                original_query, chart
            )
            registry = self.env.registry
            with registry.cursor() as cr:
                try:
                    _logger.info(f"Attempting to drop existing view {view_name}")
                    cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                    cr.execute("SET LOCAL statement_timeout = 120000;")
                    sanitized_query = enhanced_query.replace("\n", " ").replace(
                        "\r", ""
                    )
                    _logger.info(
                        f"Creating materialized view with enhanced query: {sanitized_query[:200]}..."
                    )
                    create_view_query = f"""
                        CREATE MATERIALIZED VIEW {view_name} AS
                        {enhanced_query}
                        WITH DATA
                    """
                    cr.execute(create_view_query)
                    cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    columns = [desc[0] for desc in cr.description]
                    if not columns:
                        _logger.error(
                            f"View created but has no columns: {view_name}. This indicates a query issue."
                        )
                        raise Exception("View created with no columns")
                    _logger.info(
                        f"Successfully created enhanced materialized view with {len(columns)} columns: {columns}"
                    )
                    cr.execute(f"SELECT COUNT(*) FROM {view_name}")
                    row_count = cr.fetchone()[0]
                    _logger.info(
                        f"Enhanced materialized view contains {row_count} rows"
                    )
                    primary_candidates = ["id", "record_id", "row_id", "row_number"]
                    unique_col = None
                    for col in primary_candidates:
                        if col in columns:
                            unique_col = col
                            break
                    if not unique_col:
                        try:
                            if "id" in columns:
                                cr.execute(
                                    f"SELECT COUNT(*), COUNT(DISTINCT id) FROM {view_name}"
                                )
                                total_count, distinct_count = cr.fetchone()
                                if total_count == distinct_count:
                                    unique_col = "id"
                                    _logger.info(
                                        f"'id' column is unique, using it for unique index"
                                    )
                        except Exception as e:
                            _logger.debug(f"Could not determine uniqueness: {e}")
                    if unique_col:
                        try:
                            _logger.info(
                                f"Creating unique index on column: {unique_col}"
                            )
                            cr.execute(
                                f"CREATE UNIQUE INDEX {view_name}_unique_idx ON {view_name} ({unique_col})"
                            )
                        except Exception as e:
                            _logger.warning(f"Could not create unique index: {e}")
                    if chart.x_axis_field:
                        x_col = self._find_column_in_view(chart.x_axis_field, columns)
                        if x_col:
                            _logger.info(f"Creating x-axis index on column: {x_col}")
                            idx_name = f"{view_name}_x_idx"
                            cr.execute(
                                f"CREATE INDEX {idx_name} ON {view_name} ({x_col})"
                            )
                    if chart.y_axis_field:
                        y_col = self._find_column_in_view(chart.y_axis_field, columns)
                        if y_col:
                            _logger.info(f"Creating y-axis index on column: {y_col}")
                            idx_name = f"{view_name}_y_idx"
                            cr.execute(
                                f"CREATE INDEX {idx_name} ON {view_name} ({y_col})"
                            )
                    if chart.date_field:
                        date_col = self._find_column_in_view(chart.date_field, columns)
                        if date_col:
                            _logger.info(f"Creating date index on column: {date_col}")
                            idx_name = f"{view_name}_date_idx"
                            cr.execute(
                                f"CREATE INDEX {idx_name} ON {view_name} ({date_col})"
                            )
                    if chart.branch_field:
                        branch_col = self._find_column_in_view(
                            chart.branch_field, columns
                        )
                        if branch_col:
                            _logger.info(
                                f"Creating branch index on column: {branch_col}"
                            )
                            idx_name = f"{view_name}_branch_idx"
                            cr.execute(
                                f"CREATE INDEX {idx_name} ON {view_name} ({branch_col})"
                            )
                    refresher = self.search([("chart_id", "=", chart_id)], limit=1)
                    now = fields.Datetime.now()
                    if refresher:
                        refresher.write(
                            {
                                "view_name": view_name,
                                "last_refresh": now,
                                "refresh_interval": chart.materialized_view_refresh_interval
                                or 60,
                            }
                        )
                    else:
                        self.create(
                            {
                                "name": f"Refresher for {chart_name}",
                                "chart_id": chart_id,
                                "view_name": view_name,
                                "last_refresh": now,
                                "refresh_interval": chart.materialized_view_refresh_interval
                                or 60,
                            }
                        )
                    chart.write(
                        {
                            "materialized_view_last_refresh": now,
                            "last_execution_status": "success",
                            "last_error_message": False,
                        }
                    )
                    cr.commit()
                    _logger.info(
                        f"Enhanced materialized view {view_name} created successfully with {row_count} rows"
                    )
                    return True
                except Exception as e:
                    cr.rollback()
                    _logger.error(
                        f"Failed to create enhanced materialized view for chart {chart_id}: {e}"
                    )
                    chart.write(
                        {"last_execution_status": "error", "last_error_message": str(e)}
                    )
                    return False
        except Exception as e:
            _logger.error(
                f"Fatal error creating enhanced materialized view for chart {chart_id}: {e}"
            )
            return False

    def _create_enhanced_query_for_branch_coverage(self, original_query, chart):
        """Create an enhanced query to ensure all branches are included by converting INNER JOINs to LEFT JOINs.
        Args:
            original_query (str): The original SQL query to enhance.
            chart (record): The chart record used for context.
        Returns:
            str: The enhanced SQL query.
        """
        has_branch_field = bool(chart.branch_field)
        if not has_branch_field:
            _logger.info(f"Query doesn't need enhancement - no branch field")
            return original_query
        _logger.info(f"Enhancing query to include ALL branches using LEFT JOINs")
        _logger.info(f"Original query: {original_query}")
        try:
            enhanced_query = self._convert_inner_joins_to_left_joins(original_query)
            if enhanced_query != original_query:
                _logger.info(f"Enhanced query: {enhanced_query}")
                return enhanced_query
            else:
                _logger.info(f"No JOIN conversion needed, using original query")
                return original_query
        except Exception as e:
            _logger.warning(f"Could not enhance query: {e}")
            return original_query

    def _convert_inner_joins_to_left_joins(self, query):
        """Convert INNER JOINs to LEFT JOINs to include all branches.
        Args:
            query (str): The original SQL query.
        Returns:
            str: The modified SQL query with LEFT JOINs.
        """
        enhanced_query = query
        enhanced_query = re.sub(
            r"\bJOIN\b", "LEFT JOIN", enhanced_query, flags=re.IGNORECASE
        )
        _logger.info(
            f"Converted INNER JOINs to LEFT JOINs for inclusive branch coverage"
        )
        return enhanced_query

    def _get_chart_data_from_materialized_view(self, chart, cco, branches_id):
        """Get chart data from the materialized view with improved column detection.
        Args:
            chart (record): The chart record to retrieve data from.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter data.
        Returns:
            dict: The chart data extracted from the materialized view.
        """
        try:
            view_name = f"dashboard_chart_view_{chart.id}"
            with request.env.registry.cursor() as cr:
                cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cr.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        WHERE c.relname = %s AND c.relkind = 'm'
                    )
                """,
                    (view_name,),
                )
                view_exists = cr.fetchone()[0]
                direct_query = DynamicChartController()
                if not view_exists:
                    _logger.warning(
                        f"Materialized view {view_name} does not exist - creating it"
                    )
                    success = (
                        request.env["dashboard.chart.view.refresher"]
                        .sudo()
                        .create_materialized_view_for_chart(chart.id)
                    )
                    if not success:
                        _logger.error(
                            f"Failed to create materialized view for chart {chart.id}"
                        )
                        return direct_query._get_chart_data_from_direct_query(
                            chart, cco, branches_id
                        )
                try:
                    cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    columns = [desc[0] for desc in cr.description]
                    if not columns:
                        cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                        columns = [desc[0] for desc in cr.description]
                    _logger.info(f"Detected columns for view {view_name}: {columns}")
                except Exception as e:
                    _logger.error(f"Error getting columns directly from view: {e}")
                    columns = []
                if not columns:
                    try:
                        cr.execute(
                            """
                            SELECT a.attname
                            FROM pg_attribute a
                            JOIN pg_class c ON c.oid = a.attrelid
                            WHERE c.relname = %s
                            AND a.attnum > 0 AND NOT a.attisdropped
                            ORDER BY a.attnum
                        """,
                            (view_name,),
                        )
                        columns = [row[0] for row in cr.fetchall()]
                        _logger.info(f"Retrieved columns via system catalog: {columns}")
                    except Exception as e:
                        _logger.error(f"Error querying system catalog: {e}")
                if not columns:
                    direct_query = DynamicChartController()
                    _logger.warning(
                        f"No columns found in materialized view {view_name} - using direct query"
                    )
                    return direct_query._get_chart_data_from_direct_query(
                        chart, cco, branches_id
                    )
                branch_col = None
                if chart.branch_field:
                    branch_field = (
                        chart.branch_field.split(".")[-1]
                        if "." in chart.branch_field
                        else chart.branch_field
                    )
                    if branch_field in columns:
                        branch_col = branch_field
                    else:
                        for col in columns:
                            if col == "id" or "branch" in col.lower():
                                branch_col = col
                                break
                query = f"SELECT * FROM {view_name}"
                if (
                    chart.branch_field
                    and not cco
                    and not self.security_service.is_cco_user()
                ):
                    user_branches = self.security_service.get_user_branch_ids()
                    effective_branches = []
                    if branches_id:
                        if user_branches:
                            effective_branches = [
                                b for b in branches_id if b in user_branches
                            ]
                        else:
                            effective_branches = branches_id
                    elif user_branches:
                        effective_branches = user_branches
                    if effective_branches and branch_col:
                        if len(effective_branches) == 1:
                            query += f" WHERE {branch_col} = {effective_branches[0]}"
                        else:
                            query += (
                                f" WHERE {branch_col} IN {tuple(effective_branches)}"
                            )
                    elif branch_col:
                        query += " WHERE 1=0"
                sort_col = None
                if chart.y_axis_field:
                    y_field = (
                        chart.y_axis_field.split(".")[-1]
                        if "." in chart.y_axis_field
                        else chart.y_axis_field
                    )
                    if y_field in columns:
                        sort_col = y_field
                    else:
                        for col in columns:
                            if any(
                                term in col.lower()
                                for term in ["count", "sum", "amount", "value", "total"]
                            ):
                                sort_col = col
                                break
                if sort_col:
                    query += f" ORDER BY {sort_col} DESC"
                query += " LIMIT 100"
                cr.execute("SET LOCAL statement_timeout = 30000")
                cr.execute(query)
                results = cr.dictfetchall()
                chart_data_service = ChartDataService()
                direct_query = DynamicChartController()
                return chart_data_service._extract_chart_data(chart, results, query)
        except Exception as e:
            _logger.error(f"Error getting chart from materialized view: {e}")
            return direct_query._get_chart_data_from_direct_query(
                chart, cco, branches_id
            )

    def _find_branch_column_in_view(self, chart, columns):
        """Find the appropriate branch column in the materialized view.
        Args:
            chart (record): The chart record used for context.
            columns (list): The list of column names in the materialized view.
        Returns:
            str: The name of the branch column if found, otherwise None.
        """
        if not chart.branch_field:
            return None
        branch_field = chart.branch_field
        if "." in branch_field:
            branch_field = branch_field.split(".")[-1]
        if branch_field in columns:
            return branch_field
        candidates = ["branch_id", "id", "branch"]
        for candidate in candidates:
            if candidate in columns:
                return candidate
        for column in columns:
            if "branch" in column.lower():
                return column
        return None

    def _find_order_column_in_view(self, chart, columns):
        """Find the appropriate ordering column in the materialized view.
        Args:
            chart (record): The chart record used for context.
            columns (list): The list of column names in the materialized view.
        Returns:
            str: The name of the ordering column if found, otherwise None.
        """
        if chart.y_axis_field:
            y_field = chart.y_axis_field
            if "." in y_field:
                y_field = y_field.split(".")[-1]
            if y_field in columns:
                return y_field
        candidates = [
            "customer_count",
            "high_risk_customers",
            "count",
            "value",
            "amount",
        ]
        for candidate in candidates:
            if candidate in columns:
                return candidate
        if columns:
            return columns[0]
        return None

    def _create_view_indexes(self, cr, view_name, chart_id, chart_data, column_names):
        """Create indexes on the materialized view to improve query performance.
        Args:
            cr: The database cursor for executing SQL commands.
            view_name (str): The name of the materialized view.
            chart_id (int): The ID of the chart associated with the view.
            chart_data (dict): The chart data containing field information.
            column_names (list): The list of column names in the materialized view.
        """
        timestamp = int(time.time())
        if chart_data["x_axis_field"]:
            x_col = self._find_column_in_view(chart_data["x_axis_field"], column_names)
            if x_col:
                index_name = f"{view_name}_x_{timestamp}_idx"
                _logger.info(f"Creating x-axis index on column: {x_col}")
                cr.execute(f"CREATE INDEX {index_name} ON {view_name} ({x_col})")
        if chart_data["y_axis_field"]:
            y_col = self._find_column_in_view(chart_data["y_axis_field"], column_names)
            if y_col:
                index_name = f"{view_name}_y_{timestamp}_idx"
                _logger.info(f"Creating y-axis index on column: {y_col}")
                cr.execute(f"CREATE INDEX {index_name} ON {view_name} ({y_col})")
        if chart_data["date_field"]:
            date_col = self._find_column_in_view(chart_data["date_field"], column_names)
            if date_col:
                index_name = f"{view_name}_date_{timestamp}_idx"
                _logger.info(f"Creating date index on column: {date_col}")
                cr.execute(f"CREATE INDEX {index_name} ON {view_name} ({date_col})")
        if chart_data["branch_field"]:
            branch_col = self._find_column_in_view(
                chart_data["branch_field"], column_names
            )
            if branch_col:
                index_name = f"{view_name}_branch_{timestamp}_idx"
                _logger.info(f"Creating branch index on column: {branch_col}")
                cr.execute(f"CREATE INDEX {index_name} ON {view_name} ({branch_col})")

    def create_performance_indexes(self):
        """Create database indexes to improve query performance.
        Returns:
            bool: True if the indexes were created successfully, False otherwise.
        """
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
        try:
            self.env.cr.execute("SET work_mem = '32MB'")
            self.env.cr.execute("SET statement_timeout = '30s'")
            self.env.cr.execute("SET max_parallel_workers_per_gather = 4")
            self.env.cr.execute("SET enable_parallel_append = on")
            self.env.cr.execute("SET enable_parallel_hash = on")
            self.env.cr.execute("SET enable_partition_pruning = on")
            _logger.info("Database settings initialized for optimal performance")
            return True
        except Exception as e:
            _logger.error(f"Error initializing database settings: {e}")
            return False

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
                chart.write(
                    {
                        "use_materialized_view": True,
                        "materialized_view_refresh_interval": 60,
                        "last_error_message": "Auto-enabled materialized view due to timeout history",
                    }
                )
                self.env[
                    "dashboard.chart.view.refresher"
                ].create_materialized_view_for_chart(chart.id)
            syntax_error_charts = self.env["res.dashboard.charts"].search(
                [
                    "|",
                    ("last_error_message", "ilike", "syntax error"),
                    ("last_error_message", "ilike", "missing FROM-clause"),
                ]
            )
            if syntax_error_charts:
                _logger.warning(
                    f"Found {len(syntax_error_charts)} charts with syntax errors: {syntax_error_charts.ids}"
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

    @api.model
    def _find_column_in_view(self, field_name, column_names):
        """Find the most appropriate column name in the materialized view.
        Args:
            field_name (str): The field name to find.
            column_names (list): The list of column names in the materialized view.
        Returns:
            str: The most appropriate column name if found, otherwise None.
        """
        original_field = field_name
        if "." in field_name:
            _, field_name = field_name.split(".", 1)
        if field_name in column_names:
            _logger.debug(
                f"Found exact column match: {field_name} for {original_field}"
            )
            return field_name
        field_lower = field_name.lower()
        for col in column_names:
            if col.lower() == field_lower:
                _logger.debug(
                    f"Found case-insensitive match: {col} for {original_field}"
                )
                return col
        for col in column_names:
            if field_lower in col.lower():
                _logger.debug(f"Found partial match: {col} for {original_field}")
                return col
        if field_lower.endswith("_id"):
            base_name = field_lower[:-3]
            for col in column_names:
                if col.lower() == base_name or col.lower().startswith(base_name):
                    _logger.debug(
                        f"Found match without '_id' suffix: {col} for {original_field}"
                    )
                    return col
        if field_lower == "id" and "branch_id" in column_names:
            _logger.debug(f"Found special case match 'branch_id' for 'id' field")
            return "branch_id"
        _logger.warning(
            f"Could not find column match for {original_field} in columns: {column_names}"
        )
        return None

    @api.model
    def drop_materialized_view_for_chart(self, chart_id):
        """Drop a materialized view for a chart.
        Args:
            chart_id (int): The ID of the chart for which to drop the view.
        Returns:
            bool: True if the view was dropped successfully, False otherwise.
        """
        try:
            view_name = f"dashboard_chart_view_{chart_id}"
            registry = self.env.registry
            with registry.cursor() as cr:
                try:
                    cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                    lock_acquired = cr.fetchone()[0]
                    if not lock_acquired:
                        _logger.info(
                            f"Another process is modifying chart {chart_id}, skipping view drop"
                        )
                        return False
                    cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                    cr.execute(
                        """
                        DELETE FROM dashboard_chart_view_refresher
                        WHERE chart_id = %s
                    """,
                        (chart_id,),
                    )
                    cr.execute(
                        """
                        UPDATE res_dashboard_charts
                        SET materialized_view_last_refresh = NULL,
                            write_uid = %s, write_date = %s
                        WHERE id = %s
                    """,
                        (self.env.uid, fields.Datetime.now(), chart_id),
                    )
                    cr.commit()
                    _logger.info(
                        f"Dropped materialized view {view_name} for chart {chart_id}"
                    )
                    return True
                except Exception as e:
                    cr.rollback()
                    raise e
        except Exception as e:
            _logger.error(f"Error dropping materialized view for chart {chart_id}: {e}")
            return False

    @api.model
    def refresh_chart_view(self, chart_id, low_priority=False):
        """Refresh a materialized view for a chart with robust error handling and transaction isolation.

        Args:
            chart_id (int): The ID of the chart to refresh.
            low_priority (bool, optional): Indicates if the refresh should be low priority.

        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        try:
            refresher = self.search([("chart_id", "=", chart_id)], limit=1)
            if not refresher:
                return self.create_materialized_view_for_chart(chart_id)
            chart = refresher.chart_id
            view_name = refresher.view_name
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
                    _logger.debug(
                        f"Skipping refresh for chart {chart_id}, not time yet"
                    )
                    return True
            registry = self.env.registry
            view_exists = False
            with registry.cursor() as cr:
                cr.execute(
                    f"""
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %s
                        AND c.relkind = 'm'
                    )
                """,
                    (view_name,),
                )
                view_exists = cr.fetchone()[0]
            if not view_exists:
                return self.create_materialized_view_for_chart(chart_id)
            concurrent_success = False
            try:
                with registry.cursor() as cr:
                    if low_priority:
                        cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    cr.execute("SET LOCAL statement_timeout = 120000;")
                    cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                    lock_acquired = cr.fetchone()[0]
                    if not lock_acquired:
                        _logger.info(
                            f"Another process is refreshing view for chart {chart_id}, skipping"
                        )
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
                        _logger.info(
                            f"Refreshing view {view_name} with CONCURRENTLY option"
                        )
                        cr.execute(
                            f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}"
                        )
                        cr.commit()
                        concurrent_success = True
                    else:
                        _logger.info(
                            f"View {view_name} has no unique index, skipping CONCURRENTLY refresh"
                        )
                        cr.rollback()
            except Exception as e:
                _logger.info(f"CONCURRENTLY refresh failed: {e}")
            if not concurrent_success:
                with registry.cursor() as cr:
                    try:
                        if low_priority:
                            cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        cr.execute("SET LOCAL statement_timeout = 120000;")
                        cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                        lock_acquired = cr.fetchone()[0]
                        if not lock_acquired:
                            _logger.info(
                                f"Another process is refreshing view for chart {chart_id}, skipping"
                            )
                            return False
                        _logger.info(
                            f"Refreshing view {view_name} with regular refresh"
                        )
                        cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                        cr.commit()
                    except Exception as e:
                        cr.rollback()
                        _logger.error(f"Regular refresh failed: {e}")
                        raise e
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
            _logger.info(
                f"Successfully refreshed materialized view {view_name} for chart {chart_id}"
            )
            return True
        except Exception as e:
            _logger.error(
                f"Error refreshing materialized view for chart {chart_id}: {e}"
            )
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

    @api.model
    def ensure_all_views_exist(self):
        """Ensure all materialized views exist and are properly created.
        Returns:
            bool: True if all views exist, False if an error occurred.
        """
        _logger.info("Ensuring all materialized views exist and are correctly created")
        try:
            charts = self.env["res.dashboard.charts"].search(
                [("state", "=", "active"), ("use_materialized_view", "=", True)]
            )
            if not charts:
                _logger.info("No charts with materialized views found")
                return True
            _logger.info(f"Found {len(charts)} charts with materialized views")
            created = 0
            errors = 0
            for chart in charts:
                view_name = f"dashboard_chart_view_{chart.id}"
                with self.env.registry.cursor() as cr:
                    cr.execute(
                        """
                        SELECT EXISTS (
                            SELECT FROM pg_catalog.pg_class c
                            WHERE c.relname = %s AND c.relkind = 'm'
                        )
                    """,
                        (view_name,),
                    )
                    view_exists = cr.fetchone()[0]
                if not view_exists:
                    _logger.info(f"View for chart {chart.id} needs creation")
                    success = self.create_materialized_view_for_chart(chart.id)
                    if success:
                        created += 1
                        _logger.info(
                            f"Successfully created materialized view for chart {chart.id}"
                        )
                    else:
                        errors += 1
                        _logger.error(
                            f"Failed to create materialized view for chart {chart.id}"
                        )
            _logger.info(
                f"Materialized view initialization complete: {created} created, {errors} errors"
            )
            return True
        except Exception as e:
            _logger.error(f"Error ensuring materialized views exist: {e}")
            return False

    @api.model
    def init(self):
        """Ensure a refresher record exists and initialize necessary settings.

        This method is called during the initialization of the model to ensure that all necessary
        materials views exist and that the database settings are configured properly.
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

    def setup_dashboard_tables(self):
        """Set up required tables for dashboard functionality.

        This method creates necessary tables for tracking updates and managing concurrent operations
        related to dashboard charts.

        Returns:
            bool: True if the tables were created successfully, False otherwise.
        """
        try:
            self.env.cr.execute(
                """
                -- Create update log table for tracking materialized view updates
                CREATE TABLE IF NOT EXISTS res_dashboard_charts_update_log (
                    chart_id INTEGER PRIMARY KEY,
                    update_time TIMESTAMP NOT NULL,
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
