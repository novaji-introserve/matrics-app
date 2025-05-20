import time

from requests import request
from odoo import models, fields, api
import logging
from datetime import timedelta

_logger = logging.getLogger(__name__)

class DashboardChartViewRefresher(models.Model):
    """Model for refreshing dashboard chart materialized views"""
    _name = 'dashboard.chart.view.refresher'
    _description = 'Dashboard Chart View Refresher'
    
    name = fields.Char(string='Refresher Name', default='Dashboard Chart View Refresher')
    last_run = fields.Datetime(string='Last Run', readonly=True)
    
    # Track materialized views
    chart_id = fields.Many2one('res.dashboard.charts', string='Chart', ondelete='cascade')
    view_name = fields.Char(string='View Name', readonly=True)
    last_refresh = fields.Datetime(string='Last Refresh', readonly=True)
    refresh_interval = fields.Integer(string='Refresh Interval (minutes)', default=60)
    
    _sql_constraints = [
        ('unique_chart', 'unique(chart_id)', 'Only one materialized view per chart is allowed.')
    ]
    
    # @api.model
    # def refresh_chart_views(self, low_priority=False):
    #     """Refresh all chart materialized views"""
    #     try:
    #         # Apply session-specific database settings
    #         self.initialize_database_settings()
        
    #         # When running in low_priority mode (from cron), use a lower transaction isolation level
    #         if low_priority:
    #             self.env.cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                
    #         # Get all charts with materialized views enabled
    #         charts = self.env['res.dashboard.charts'].search([
    #             ('state', '=', 'active'),
    #             ('use_materialized_view', '=', True)
    #         ])
            
    #         refreshed = 0
    #         for chart in charts:
    #             if self.refresh_chart_view(chart.id):
    #                 refreshed += 1
            
    #         # Update last run time
    #         refresher = self.search([], limit=1)
    #         if refresher:
    #             refresher.write({'last_run': fields.Datetime.now()})
            
    #         _logger.info(f"Refreshed {refreshed} dashboard chart views")
    #         return True
    #     except Exception as e:
    #         _logger.error(f"Error refreshing dashboard chart views: {e}")
    #         return False
    
    
    @api.model
    def refresh_chart_views(self, low_priority=False):
        """Refresh all chart materialized views with isolated transactions"""
        refreshed = 0
        errors = 0
        
        # First, ensure we have a refresher record
        refresher_record = self.search([], limit=1)
        if not refresher_record:
            try:
                refresher_record = self.create({'name': 'Dashboard Chart View Refresher'})
                _logger.info("Created new Dashboard Chart View Refresher record")
            except Exception as e:
                _logger.error(f"Could not create refresher record: {e}")
        
        try:
            # Get all charts with materialized views enabled - do this outside any transaction
            charts_to_refresh = self.env['res.dashboard.charts'].search([
                ('state', '=', 'active'),
                ('use_materialized_view', '=', True)
            ])
            
            _logger.info(f"Found {len(charts_to_refresh)} charts with materialized views to refresh")
            
            # Process each chart in its own transaction
            for chart in charts_to_refresh:
                try:
                    # First check if the view exists
                    view_name = f"dashboard_chart_view_{chart.id}"
                    view_exists = False
                    
                    with self.env.registry.cursor() as check_cr:
                        check_cr.execute("""
                            SELECT EXISTS (
                                SELECT FROM pg_catalog.pg_class c
                                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                WHERE c.relname = %s AND c.relkind = 'm'
                            )
                        """, (view_name,))
                        
                        view_exists = check_cr.fetchone()[0]
                    
                    if not view_exists:
                        _logger.info(f"Materialized view for chart {chart.id} doesn't exist, creating it")
                        if self.create_materialized_view_for_chart(chart.id):
                            refreshed += 1
                        else:
                            errors += 1
                        continue
                    
                    # Initialize database settings in a separate cursor
                    with self.env.registry.cursor() as settings_cr:
                        try:
                            env = api.Environment(settings_cr, self.env.uid, self.env.context)
                            refresher = env['dashboard.chart.view.refresher']
                            refresher.initialize_database_settings()
                            settings_cr.commit()
                            _logger.info("Database settings initialized for optimal performance")
                        except Exception as settings_err:
                            _logger.warning(f"Could not initialize settings, continuing anyway: {settings_err}")
                            settings_cr.rollback()
                    
                    # Now refresh the chart
                    success = self.refresh_chart_view(chart.id, low_priority)
                    if success:
                        refreshed += 1
                    else:
                        errors += 1
                        
                except Exception as chart_error:
                    _logger.error(f"Error processing chart {chart.id}: {chart_error}")
                    errors += 1
                    continue
            
            # Update last run time in a separate transaction
            try:
                with self.env.registry.cursor() as update_cr:
                    env = api.Environment(update_cr, self.env.uid, self.env.context)
                    refresher_to_update = env['dashboard.chart.view.refresher'].browse(refresher_record.id)
                    if refresher_to_update.exists():
                        refresher_to_update.write({'last_run': fields.Datetime.now()})
                        update_cr.commit()
            except Exception as update_err:
                _logger.warning(f"Could not update last run time: {update_err}")
            
            _logger.info(f"Refreshed {refreshed} dashboard chart views, {errors} errors")
            return True
        except Exception as e:
            _logger.error(f"Error in refresh_chart_views main process: {e}")
            return False
    
    # @api.model
    # def refresh_chart_views(self, low_priority=False):
    #     """Refresh all chart materialized views"""
    #     try:
    #         # Create a new cursor to ensure we're not in an aborted transaction
    #         with self.env.registry.cursor() as cr:
    #             env = api.Environment(cr, self.env.uid, self.env.context)
    #             refresher = env['dashboard.chart.view.refresher']
                
    #             # Apply session-specific database settings
    #             refresher.initialize_database_settings()
            
    #             # When running in low_priority mode (from cron), use a lower transaction isolation level
    #             if low_priority:
    #                 cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    
    #             # Get all charts with materialized views enabled
    #             charts = env['res.dashboard.charts'].search([
    #                 ('state', '=', 'active'),
    #                 ('use_materialized_view', '=', True)
    #             ])
                
    #             refreshed = 0
    #             for chart in charts:
    #                 if refresher.refresh_chart_view(chart.id):
    #                     refreshed += 1
                
    #             # Update last run time
    #             refresher_record = refresher.search([], limit=1)
    #             if refresher_record:
    #                 refresher_record.write({'last_run': fields.Datetime.now()})
                    
    #             cr.commit()
                
    #             _logger.info(f"Refreshed {refreshed} dashboard chart views")
    #             return True
    #     except Exception as e:
    #         _logger.error(f"Error refreshing dashboard chart views: {e}")
    #         return False
    
    @api.model
    def refresh_chart_view(self, chart_id, low_priority=False):
        """Refresh a materialized view for a chart with robust error handling and concurrency control"""
        try:
            # Use a single transaction for the entire operation to avoid serialization conflicts
            registry = self.env.registry
            with registry.cursor() as cr:
                try:
                    # Set transaction isolation level if low_priority is True
                    if low_priority:
                        cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    
                    # Try to acquire an advisory lock for this chart_id with small timeout
                    cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                    lock_acquired = cr.fetchone()[0]
                    
                    if not lock_acquired:
                        _logger.info(f"Another process is refreshing view for chart {chart_id}, skipping")
                        return False
                    
                    # Lock the refresher record for update
                    cr.execute("""
                        SELECT r.id, r.view_name, r.last_refresh, r.refresh_interval, c.id as chart_id
                        FROM dashboard_chart_view_refresher r
                        JOIN res_dashboard_charts c ON r.chart_id = c.id
                        WHERE r.chart_id = %s
                        FOR UPDATE SKIP LOCKED
                    """, (chart_id,))
                    
                    refresher_data = cr.dictfetchone()
                    
                    if not refresher_data:
                        # If no refresher record or it's locked, try to create the view first in a separate transaction
                        cr.rollback()  # Release the lock
                        return self.create_materialized_view_for_chart(chart_id)
                    
                    view_name = refresher_data['view_name']
                    last_refresh = refresher_data['last_refresh']
                    refresh_interval = refresher_data['refresh_interval']
                    
                    # Check if we need to refresh based on interval - only when not explicitly requested
                    if not low_priority and last_refresh and refresh_interval:
                        now = fields.Datetime.now()
                        if last_refresh + timedelta(minutes=refresh_interval) > now:
                            # Not time to refresh yet
                            _logger.debug(f"Skipping refresh for chart {chart_id}, not time yet")
                            cr.rollback()  # Release the lock
                            return True
                    
                    # Set a higher timeout (2 minutes) for the refresh operation
                    cr.execute("SET LOCAL statement_timeout = 120000;")
                    
                    # Check if the view exists
                    cr.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM pg_catalog.pg_class c
                            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                            WHERE c.relname = %s
                            AND c.relkind = 'm'
                        )
                    """, (view_name,))
                    
                    view_exists = cr.fetchone()[0]
                    
                    if not view_exists:
                        # View doesn't exist, create it
                        cr.rollback()  # Release the lock
                        return self.create_materialized_view_for_chart(chart_id)
                    
                    # Add query comment for easier debugging
                    cr.execute(f"/* Refreshing materialized view {view_name} for chart {chart_id} */")
                    
                    # Refresh the view with CONCURRENTLY option if possible (reduces locking)
                    try:
                        # Try with CONCURRENTLY for less locking, but requires unique index
                        cr.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
                    except Exception as e:
                        # Fall back to regular refresh if CONCURRENTLY fails
                        _logger.info(f"CONCURRENTLY refresh failed, using regular refresh: {e}")
                        cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                    
                    # Update the refresher and chart records directly with SQL
                    now = fields.Datetime.now()
                    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
                    
                    cr.execute("""
                        UPDATE dashboard_chart_view_refresher
                        SET last_refresh = %s,
                            write_uid = %s, write_date = %s
                        WHERE id = %s
                    """, (now_str, self.env.uid, now_str, refresher_data['id']))
                    
                    cr.execute("""
                        UPDATE res_dashboard_charts
                        SET materialized_view_last_refresh = %s,
                            last_execution_status = %s,
                            last_error_message = NULL,
                            write_uid = %s, write_date = %s
                        WHERE id = %s
                    """, (now_str, 'success', self.env.uid, now_str, chart_id))
                    
                    # Commit everything at once
                    cr.commit()
                    
                    _logger.info(f"Refreshed materialized view {view_name} for chart {chart_id}")
                    return True
                    
                except Exception as e:
                    # Rollback in case of error
                    cr.rollback()
                    _logger.error(f"Error refreshing materialized view for chart {chart_id}: {e}")
                    
                    # Update the chart with the error in a separate transaction
                    with registry.cursor() as err_cr:
                        try:
                            err_cr.execute("""
                                UPDATE res_dashboard_charts
                                SET last_execution_status = %s,
                                    last_error_message = %s,
                                    write_uid = %s, write_date = %s
                                WHERE id = %s
                            """, ('error', str(e), self.env.uid, fields.Datetime.now(), chart_id))
                            err_cr.commit()
                        except Exception as err_write:
                            err_cr.rollback()
                            _logger.error(f"Failed to update error status: {err_write}")
                    
                    return False
            
        except Exception as e:
            _logger.error(f"Error refreshing materialized view for chart {chart_id}: {e}")
            return False
    
    # def create_materialized_view_for_chart(self, chart_id):
    #     """Create or update a materialized view for a chart with more robust error handling"""
    #     try:
    #         # Use a simplified approach to check for existing locks
    #         registry = self.env.registry
    #         with registry.cursor() as check_cr:
    #             # Check if view exists first
    #             check_cr.execute("""
    #                 SELECT EXISTS (
    #                     SELECT FROM pg_catalog.pg_class c
    #                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    #                     WHERE c.relname = %s AND c.relkind = 'm'
    #                 )
    #             """, (f"dashboard_chart_view_{chart_id}",))
    #             view_exists = check_cr.fetchone()[0]
                
    #             # Skip if another process is already working on this chart
    #             check_cr.execute("""
    #                 SELECT EXISTS (
    #                     SELECT 1 FROM res_dashboard_charts_update_log
    #                     WHERE chart_id = %s 
    #                     AND update_time > (NOW() - INTERVAL '5 minutes')
    #                     AND status = 'in_progress'
    #                 )
    #             """, (chart_id,))
    #             in_progress = check_cr.fetchone()[0]
                
    #             if in_progress:
    #                 _logger.info(f"View creation already in progress for chart {chart_id}, skipping")
    #                 return False
                
    #             # Record that we're starting the process
    #             check_cr.execute("""
    #                 INSERT INTO res_dashboard_charts_update_log 
    #                 (chart_id, update_time, status, message) 
    #                 VALUES (%s, NOW(), 'in_progress', 'Starting view creation')
    #                 ON CONFLICT (chart_id) DO UPDATE
    #                 SET update_time = NOW(),
    #                     status = 'in_progress',
    #                     message = 'Starting view creation'
    #             """, (chart_id,))
    #             check_cr.commit()
            
    #         # Use a separate transaction for view creation
    #         with registry.cursor() as cr:
    #             try:
    #                 # Get the chart data
    #                 cr.execute("""
    #                     SELECT id, query, materialized_view_refresh_interval,
    #                         x_axis_field, y_axis_field, date_field, branch_field,
    #                         name
    #                     FROM res_dashboard_charts 
    #                     WHERE id = %s
    #                 """, (chart_id,))
    #                 chart_data = cr.dictfetchone()
                    
    #                 if not chart_data:
    #                     _logger.error(f"Chart {chart_id} not found")
    #                     return False
                    
    #                 chart_query = chart_data['query']
    #                 chart_name = chart_data['name']
    #                 view_name = f"dashboard_chart_view_{chart_id}"
                    
    #                 # Remove any trailing semicolons from the query
    #                 original_query = chart_query.strip()
    #                 if original_query.endswith(';'):
    #                     original_query = original_query[:-1]
                    
    #                 # Drop existing view if it exists
    #                 if view_exists:
    #                     cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                    
    #                 # Create the view with a proper timeout
    #                 cr.execute("SET LOCAL statement_timeout = 120000;")
    #                 create_view_query = f"""
    #                     CREATE MATERIALIZED VIEW {view_name} AS
    #                     {original_query}
    #                     WITH DATA
    #                 """
                    
    #                 _logger.info(f"Creating materialized view for chart {chart_id}: {view_name}")
    #                 cr.execute(create_view_query)
                    
    #                 # Get a sample row to identify the actual column names in the view
    #                 cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
    #                 result = cr.dictfetchone()
                    
    #                 if result:
    #                     # Create indexes using the actual column names from the view
    #                     column_names = list(result.keys())
                        
    #                     _logger.info(f"Found columns in materialized view: {column_names}")
                        
    #                     # X-axis index
    #                     if chart_data['x_axis_field']:
    #                         x_col = self._find_column_in_view(chart_data['x_axis_field'], column_names)
    #                         if x_col:
    #                             _logger.info(f"Creating x-axis index on column: {x_col}")
    #                             cr.execute(f"CREATE INDEX IF NOT EXISTS {view_name}_x_idx ON {view_name} ({x_col})")
                        
    #                     # Y-axis index
    #                     if chart_data['y_axis_field']:
    #                         y_col = self._find_column_in_view(chart_data['y_axis_field'], column_names)
    #                         if y_col:
    #                             _logger.info(f"Creating y-axis index on column: {y_col}")
    #                             cr.execute(f"CREATE INDEX IF NOT EXISTS {view_name}_y_idx ON {view_name} ({y_col})")
                        
    #                     # Date index
    #                     if chart_data['date_field']:
    #                         date_col = self._find_column_in_view(chart_data['date_field'], column_names)
    #                         if date_col:
    #                             _logger.info(f"Creating date index on column: {date_col}")
    #                             cr.execute(f"CREATE INDEX IF NOT EXISTS {view_name}_date_idx ON {view_name} ({date_col})")
                        
    #                     # Branch index
    #                     if chart_data['branch_field']:
    #                         branch_col = self._find_column_in_view(chart_data['branch_field'], column_names)
    #                         if branch_col:
    #                             _logger.info(f"Creating branch index on column: {branch_col}")
    #                             cr.execute(f"CREATE INDEX IF NOT EXISTS {view_name}_branch_idx ON {view_name} ({branch_col})")
                    
    #                 # First, ensure the update log table exists
    #                 cr.execute("""
    #                     CREATE TABLE IF NOT EXISTS res_dashboard_charts_update_log (
    #                         chart_id INTEGER PRIMARY KEY,
    #                         update_time TIMESTAMP NOT NULL,
    #                         status VARCHAR(20) NOT NULL,
    #                         message TEXT
    #                     )
    #                 """)
                    
    #                 # Record successful creation in the log
    #                 cr.execute("""
    #                     INSERT INTO res_dashboard_charts_update_log 
    #                     (chart_id, update_time, status, message) 
    #                     VALUES (%s, NOW(), 'success', 'View created successfully')
    #                     ON CONFLICT (chart_id) DO UPDATE
    #                     SET update_time = NOW(),
    #                         status = 'success',
    #                         message = 'View created successfully'
    #                 """, (chart_id,))
                    
    #                 # Record the view in refresher model
    #                 refresher = self.search([('chart_id', '=', chart_id)], limit=1)
    #                 if refresher:
    #                     refresher.write({
    #                         'view_name': view_name,
    #                         'last_refresh': fields.Datetime.now(),
    #                         'refresh_interval': chart_data['materialized_view_refresh_interval'] or 60
    #                     })
    #                 else:
    #                     self.create({
    #                         'name': f"Refresher for {chart_name}",
    #                         'chart_id': chart_id,
    #                         'view_name': view_name,
    #                         'last_refresh': fields.Datetime.now(),
    #                         'refresh_interval': chart_data['materialized_view_refresh_interval'] or 60
    #                     })
                    
    #                 # Update chart's last refresh time
    #                 now = fields.Datetime.now()
    #                 charts_model = self.env['res.dashboard.charts']
    #                 chart = charts_model.browse(chart_id)
    #                 if chart.exists():
    #                     # Try direct write with isolation
    #                     try:
    #                         chart.write({
    #                             'materialized_view_last_refresh': now,
    #                             'last_execution_status': 'success',
    #                             'last_error_message': False
    #                         })
    #                     except Exception as chart_write_error:
    #                         _logger.warning(f"Could not update chart record: {str(chart_write_error)}")
    #                         # Use direct SQL as fallback
    #                         cr.execute("""
    #                             UPDATE res_dashboard_charts
    #                             SET materialized_view_last_refresh = NOW(),
    #                                 last_execution_status = 'success',
    #                                 last_error_message = NULL
    #                             WHERE id = %s
    #                         """, (chart_id,))
                    
    #                 # Commit the transaction
    #                 cr.commit()
                    
    #                 _logger.info(f"Successfully created materialized view {view_name} for chart {chart_id}")
    #                 return True
                    
    #             except Exception as e:
    #                 # Rollback in case of error
    #                 cr.rollback()
    #                 _logger.error(f"Error creating materialized view for chart {chart_id}: {e}")
                    
    #                 # Record error in a separate transaction
    #                 with registry.cursor() as err_cr:
    #                     try:
    #                         err_cr.execute("""
    #                             INSERT INTO res_dashboard_charts_update_log 
    #                             (chart_id, update_time, status, message) 
    #                             VALUES (%s, NOW(), 'error', %s)
    #                             ON CONFLICT (chart_id) DO UPDATE
    #                             SET update_time = NOW(),
    #                                 status = 'error',
    #                                 message = %s
    #                         """, (chart_id, str(e), str(e)))
    #                         err_cr.commit()
    #                     except Exception as log_error:
    #                         _logger.error(f"Failed to log error: {str(log_error)}")
                    
    #                 return False
            
    #     except Exception as e:
    #         _logger.error(f"Fatal error creating materialized view for chart {chart_id}: {e}")
    #         return False
    
    # Enhanced Materialized View Creation and Column Detection


    def diagnose_materialized_view(self, chart_id):
        """Diagnose issues with a materialized view for a chart - improved column detection"""
        try:
            view_name = f"dashboard_chart_view_{chart_id}"
            registry = self.env.registry
            
            with registry.cursor() as cr:
                # Set transaction isolation level
                cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                
                # Check if view exists
                cr.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %s
                        AND c.relkind = 'm'
                    )
                """, (view_name,))
                
                view_exists = cr.fetchone()[0]
                
                if not view_exists:
                    _logger.error(f"Materialized view {view_name} does not exist!")
                    return {'view_exists': False, 'has_columns': False}
                
                # Check if view has columns - try multiple methods
                columns = []
                
                # Method 1: Try direct query
                try:
                    cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    columns = [desc[0] for desc in cr.description]
                except Exception as e:
                    _logger.debug(f"Error in direct column query: {e}")
                
                # Method 2: If that fails, try system catalogs
                if not columns:
                    try:
                        cr.execute("""
                            SELECT a.attname
                            FROM pg_catalog.pg_attribute a
                            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                            WHERE c.relname = %s
                            AND a.attnum > 0 AND NOT a.attisdropped
                            ORDER BY a.attnum
                        """, (view_name,))
                        
                        columns = [row[0] for row in cr.fetchall()]
                    except Exception as e:
                        _logger.debug(f"Error in system catalog query: {e}")
                
                # Method 3: Try with a real query
                if not columns:
                    try:
                        cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                        columns = [desc[0] for desc in cr.description]
                    except Exception as e:
                        _logger.debug(f"Error in real query: {e}")
                
                column_info = [{'name': col, 'type': 'unknown'} for col in columns]
                
                # Check if view has data
                row_count = -1
                try:
                    cr.execute(f"SELECT COUNT(*) FROM {view_name}")
                    row_count = cr.fetchone()[0]
                except Exception as e:
                    _logger.error(f"Error counting rows in {view_name}: {e}")
                
                # Check original query
                cr.execute("""
                    SELECT query 
                    FROM res_dashboard_charts
                    WHERE id = %s
                """, (chart_id,))
                
                query_result = cr.fetchone()
                original_query = query_result[0] if query_result else None
                
                # Try executing the original query
                query_works = False
                if original_query:
                    try:
                        clean_query = original_query.strip()
                        if clean_query.endswith(';'):
                            clean_query = clean_query[:-1]
                        
                        cr.execute("SET statement_timeout = 10000")  # 10 seconds
                        cr.execute(clean_query)
                        query_works = True
                    except Exception as query_error:
                        _logger.error(f"Original query error: {query_error}")
                
                return {
                    'view_exists': True,
                    'has_columns': len(columns) > 0,
                    'column_count': len(column_info),
                    'columns': column_info,
                    'row_count': row_count,
                    'original_query_works': query_works
                }
        except Exception as e:
            _logger.error(f"Diagnostic error for view {chart_id}: {e}")
            return {'error': str(e), 'view_exists': False, 'has_columns': False}
        
        
    # def diagnose_materialized_view(self, chart_id):
    #     """Diagnose issues with a materialized view for a chart"""
    #     try:
    #         view_name = f"dashboard_chart_view_{chart_id}"
    #         registry = self.env.registry
            
    #         with registry.cursor() as cr:
    #             # Check if view exists
    #             cr.execute("""
    #                 SELECT EXISTS (
    #                     SELECT FROM pg_catalog.pg_class c
    #                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    #                     WHERE c.relname = %s
    #                     AND c.relkind = 'm'
    #                 )
    #             """, (view_name,))
                
    #             view_exists = cr.fetchone()[0]
                
    #             if not view_exists:
    #                 _logger.error(f"Materialized view {view_name} does not exist!")
    #                 return {'view_exists': False}
                
    #             # Check if view has columns
    #             cr.execute(f"""
    #                 SELECT column_name, data_type 
    #                 FROM information_schema.columns 
    #                 WHERE table_name = %s
    #             """, (view_name,))
                
    #             columns = cr.fetchall()
    #             column_info = [{'name': col[0], 'type': col[1]} for col in columns]
                
    #             # Check if view has data
    #             try:
    #                 cr.execute(f"SELECT COUNT(*) FROM {view_name}")
    #                 row_count = cr.fetchone()[0]
    #             except Exception as e:
    #                 _logger.error(f"Error counting rows in {view_name}: {e}")
    #                 row_count = -1
                
    #             # Check original query
    #             cr.execute("""
    #                 SELECT query 
    #                 FROM res_dashboard_charts
    #                 WHERE id = %s
    #             """, (chart_id,))
                
    #             query_result = cr.fetchone()
    #             original_query = query_result[0] if query_result else None
                
    #             # Try executing the original query
    #             query_works = False
    #             if original_query:
    #                 try:
    #                     clean_query = original_query.strip()
    #                     if clean_query.endswith(';'):
    #                         clean_query = clean_query[:-1]
                        
    #                     cr.execute("SET statement_timeout = 10000")  # 10 seconds
    #                     cr.execute(clean_query)
    #                     query_works = True
    #                 except Exception as query_error:
    #                     _logger.error(f"Original query error: {query_error}")
                
    #             return {
    #                 'view_exists': True,
    #                 'column_count': len(column_info),
    #                 'columns': column_info,
    #                 'row_count': row_count,
    #                 'original_query_works': query_works
    #             }
                
    #     except Exception as e:
    #         _logger.error(f"Diagnostic error for view {chart_id}: {e}")
    #         return {'error': str(e)}
        
    
    def create_materialized_view_for_chart(self, chart_id):
        """Create or update a materialized view for a chart with robust transaction handling"""
        _logger.info(f"Creating materialized view for chart {chart_id}")
        
        # Define view name - consistent naming pattern
        view_name = f"dashboard_chart_view_{chart_id}"
        
        try:
            # Get chart data first - we need this outside the transaction
            chart = self.env['res.dashboard.charts'].browse(chart_id)
            if not chart.exists():
                _logger.error(f"Chart {chart_id} not found")
                return False
                
            chart_query = chart.query
            chart_name = chart.name
            
            if not chart_query:
                _logger.error(f"Chart {chart_id} has no query defined")
                return False
                
            # Clean the original query
            original_query = chart_query.strip()
            if original_query.endswith(';'):
                original_query = original_query[:-1]
                
            # Use a transaction for the create operation
            registry = self.env.registry
            with registry.cursor() as cr:
                try:
                    # First, attempt to drop the view if it exists - makes this operation idempotent
                    _logger.info(f"Attempting to drop existing view {view_name}")
                    cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                    
                    # Set a longer timeout for complex queries
                    cr.execute("SET LOCAL statement_timeout = 120000;")  # 2 minutes
                    
                    # Log the query for debugging
                    sanitized_query = original_query.replace('\n', ' ').replace('\r', '')
                    _logger.info(f"Creating materialized view with query: {sanitized_query[:200]}...")
                    
                    # Create the view - This is the critical part
                    create_view_query = f"""
                        CREATE MATERIALIZED VIEW {view_name} AS
                        {original_query}
                        WITH DATA
                    """
                    
                    # Execute creation query
                    cr.execute(create_view_query)
                    
                    # IMPORTANT FIX: Directly check for columns using a SELECT query
                    # instead of information_schema, which may not be immediately updated
                    cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    
                    # Get columns from cursor description
                    columns = [desc[0] for desc in cr.description]
                    
                    if not columns:
                        _logger.error(f"View created but has no columns: {view_name}. This indicates a query issue.")
                        raise Exception("View created with no columns")
                    
                    # Log success - we have columns!
                    _logger.info(f"Successfully created materialized view with {len(columns)} columns: {columns}")
                    
                    # Now create indexes for the view
                    # First, try to create a unique index if possible (for CONCURRENTLY refresh)
                    primary_candidates = ['id', 'record_id', 'row_id', 'row_number']
                    unique_col = None
                    
                    for col in primary_candidates:
                        if col in columns:
                            unique_col = col
                            break
                    
                    if unique_col:
                        _logger.info(f"Creating unique index on column: {unique_col}")
                        cr.execute(f"CREATE UNIQUE INDEX {view_name}_unique_idx ON {view_name} ({unique_col})")
                    
                    # Create indexes for common search/filter fields
                    # X-axis field
                    if chart.x_axis_field:
                        x_col = self._find_column_in_view(chart.x_axis_field, columns)
                        if x_col:
                            _logger.info(f"Creating x-axis index on column: {x_col}")
                            idx_name = f"{view_name}_x_idx"
                            cr.execute(f"CREATE INDEX {idx_name} ON {view_name} ({x_col})")
                    
                    # Y-axis field (often used for sorting)
                    if chart.y_axis_field:
                        y_col = self._find_column_in_view(chart.y_axis_field, columns)
                        if y_col:
                            _logger.info(f"Creating y-axis index on column: {y_col}")
                            idx_name = f"{view_name}_y_idx"
                            cr.execute(f"CREATE INDEX {idx_name} ON {view_name} ({y_col})")
                    
                    # Date field (frequently used in filters)
                    if chart.date_field:
                        date_col = self._find_column_in_view(chart.date_field, columns)
                        if date_col:
                            _logger.info(f"Creating date index on column: {date_col}")
                            idx_name = f"{view_name}_date_idx"
                            cr.execute(f"CREATE INDEX {idx_name} ON {view_name} ({date_col})")
                    
                    # Branch field (critical for security filtering)
                    if chart.branch_field:
                        branch_col = self._find_column_in_view(chart.branch_field, columns)
                        if branch_col:
                            _logger.info(f"Creating branch index on column: {branch_col}")
                            idx_name = f"{view_name}_branch_idx"
                            cr.execute(f"CREATE INDEX {idx_name} ON {view_name} ({branch_col})")
                    
                    # Record the creation in refresher model
                    refresher = self.search([('chart_id', '=', chart_id)], limit=1)
                    now = fields.Datetime.now()
                    
                    if refresher:
                        # Update existing record
                        refresher.write({
                            'view_name': view_name,
                            'last_refresh': now,
                            'refresh_interval': chart.materialized_view_refresh_interval or 60
                        })
                    else:
                        # Create new refresher record
                        self.create({
                            'name': f"Refresher for {chart_name}",
                            'chart_id': chart_id,
                            'view_name': view_name,
                            'last_refresh': now,
                            'refresh_interval': chart.materialized_view_refresh_interval or 60
                        })
                    
                    # Update the chart record with success info
                    chart.write({
                        'materialized_view_last_refresh': now,
                        'last_execution_status': 'success',
                        'last_error_message': False
                    })
                    
                    # Commit everything at once
                    cr.commit()
                    
                    _logger.info(f"Materialized view {view_name} created successfully")
                    return True
                    
                except Exception as e:
                    # Make sure to rollback on any error
                    cr.rollback()
                    _logger.error(f"Failed to create materialized view for chart {chart_id}: {e}")
                    
                    # Update the chart with error status
                    chart.write({
                        'last_execution_status': 'error',
                        'last_error_message': str(e)
                    })
                    return False
        
        except Exception as e:
            _logger.error(f"Fatal error creating materialized view for chart {chart_id}: {e}")
            return False
    
    def _get_chart_data_from_materialized_view(self, chart, cco, branches_id):
        """Get chart data from materialized view with improved column detection - no unnecessary recreations"""
        try:
            view_name = f"dashboard_chart_view_{chart.id}"
            
            # Create a dedicated cursor with appropriate transaction isolation
            with request.env.registry.cursor() as cr:
                # Set appropriate transaction isolation level
                cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                
                # Check if view exists first
                cr.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        WHERE c.relname = %s AND c.relkind = 'm'
                    )
                """, (view_name,))
                
                view_exists = cr.fetchone()[0]
                
                if not view_exists:
                    _logger.warning(f"Materialized view {view_name} does not exist - creating it")
                    # Create it if doesn't exist
                    success = request.env['dashboard.chart.view.refresher'].sudo().create_materialized_view_for_chart(chart.id)
                    if not success:
                        _logger.error(f"Failed to create materialized view for chart {chart.id}")
                        return self._get_chart_data_from_direct_query(chart, cco, branches_id)
                
                # DIRECT QUERY APPROACH: Get columns directly from the view
                # This bypasses information_schema completely which can be stale
                try:
                    # Execute a query directly on the view to get columns
                    cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    columns = [desc[0] for desc in cr.description]
                    
                    if not columns:
                        # Try with an actual row - sometimes that works when LIMIT 0 doesn't
                        cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                        columns = [desc[0] for desc in cr.description]
                    
                    _logger.info(f"Detected columns for view {view_name}: {columns}")
                except Exception as e:
                    _logger.error(f"Error getting columns directly from view: {e}")
                    columns = []
                    
                # If we still have no columns, try the system catalogs - but don't recreate
                if not columns:
                    try:
                        # Query PostgreSQL system catalogs directly
                        cr.execute("""
                            SELECT a.attname
                            FROM pg_attribute a
                            JOIN pg_class c ON c.oid = a.attrelid
                            WHERE c.relname = %s
                            AND a.attnum > 0 AND NOT a.attisdropped
                            ORDER BY a.attnum
                        """, (view_name,))
                        
                        columns = [row[0] for row in cr.fetchall()]
                        _logger.info(f"Retrieved columns via system catalog: {columns}")
                    except Exception as e:
                        _logger.error(f"Error querying system catalog: {e}")
                
                # If still no columns, fall back to direct query - we don't recreate if it exists
                if not columns:
                    _logger.warning(f"No columns found in materialized view {view_name} - using direct query")
                    return self._get_chart_data_from_direct_query(chart, cco, branches_id)
                
                # We have columns! Now build and execute the query
                
                # Find the proper column for branch filtering
                branch_col = None
                if chart.branch_field:
                    branch_field = chart.branch_field.split('.')[-1] if '.' in chart.branch_field else chart.branch_field
                    
                    # Try direct match
                    if branch_field in columns:
                        branch_col = branch_field
                    else:
                        # Try finding a suitable column
                        for col in columns:
                            if col == 'id' or 'branch' in col.lower():
                                branch_col = col
                                break
                
                # Build query against the materialized view
                query = f"SELECT * FROM {view_name}"
                
                # Apply security filters with proper column name
                if chart.branch_field and not cco and not self.security_service.is_cco_user():
                    user_branches = self.security_service.get_user_branch_ids()
                    effective_branches = []
                    
                    if branches_id:
                        # If branches specified in UI, intersect with user's branches
                        if user_branches:
                            effective_branches = [b for b in branches_id if b in user_branches]
                        else:
                            effective_branches = branches_id
                    elif user_branches:
                        effective_branches = user_branches
                    
                    # Build WHERE clause using the correct column name
                    if effective_branches and branch_col:
                        if len(effective_branches) == 1:
                            query += f" WHERE {branch_col} = {effective_branches[0]}"
                        else:
                            query += f" WHERE {branch_col} IN {tuple(effective_branches)}"
                    elif branch_col:
                        # No branches specified, return no results
                        query += " WHERE 1=0"
                
                # Find column for sorting
                sort_col = None
                if chart.y_axis_field:
                    y_field = chart.y_axis_field.split('.')[-1] if '.' in chart.y_axis_field else chart.y_axis_field
                    if y_field in columns:
                        sort_col = y_field
                    else:
                        # Try to find a suitable numeric column
                        for col in columns:
                            if any(term in col.lower() for term in ['count', 'sum', 'amount', 'value', 'total']):
                                sort_col = col
                                break
                
                # Add ORDER BY if found a suitable column
                if sort_col:
                    query += f" ORDER BY {sort_col} DESC"
                
                # Add LIMIT
                query += " LIMIT 100"  # Default reasonable limit
                
                # Execute query with timeout protection
                cr.execute("SET LOCAL statement_timeout = 30000")  # 30 seconds
                cr.execute(query)
                results = cr.dictfetchall()
                
                # Process and return results
                return self._extract_chart_data(chart, results, query)
                
        except Exception as e:
            _logger.error(f"Error getting chart from materialized view: {e}")
            # Fall back to direct query
            return self._get_chart_data_from_direct_query(chart, cco, branches_id)
    
    
    
    # def _get_chart_data_from_materialized_view(self, chart, cco, branches_id):
    #     """Get chart data from materialized view with enhanced error handling"""
    #     try:
    #         view_name = f"dashboard_chart_view_{chart.id}"
            
    #         # First, check if the view actually exists
    #         with request.env.registry.cursor() as check_cr:
    #             check_cr.execute("""
    #                 SELECT EXISTS (
    #                     SELECT FROM pg_catalog.pg_class c
    #                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    #                     WHERE c.relname = %s AND c.relkind = 'm'
    #                 )
    #             """, (view_name,))
                
    #             view_exists = check_cr.fetchone()[0]
                
    #             if not view_exists:
    #                 _logger.warning(f"Materialized view {view_name} does not exist!")
    #                 # Try to create it on-demand
    #                 request.env['dashboard.chart.view.refresher'].create_materialized_view_for_chart(chart.id)
                    
    #                 # Recheck if it exists now
    #                 check_cr.execute("""
    #                     SELECT EXISTS (
    #                         SELECT FROM pg_catalog.pg_class c
    #                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    #                         WHERE c.relname = %s AND c.relkind = 'm'
    #                     )
    #                 """, (view_name,))
                    
    #                 view_exists = check_cr.fetchone()[0]
                    
    #                 if not view_exists:
    #                     _logger.error(f"Failed to create materialized view {view_name}")
    #                     return self._get_chart_data_from_direct_query(chart, cco, branches_id)
            
    #         # Now check columns and execute query
    #         with request.env.registry.cursor() as cr:
    #             # Get all columns
    #             cr.execute(f"""
    #                 SELECT column_name 
    #                 FROM information_schema.columns 
    #                 WHERE table_name = %s
    #             """, (view_name,))
                
    #             columns = [row[0] for row in cr.fetchall()]
                
    #             if not columns:
    #                 _logger.warning(f"No columns found in materialized view {view_name}")
                    
    #                 # Try to diagnose the issue
    #                 refresher = request.env['dashboard.chart.view.refresher']
    #                 diagnosis = refresher.diagnose_materialized_view(chart.id)
    #                 _logger.warning(f"Materialized view diagnosis: {diagnosis}")
                    
    #                 # Attempt to recreate the view
    #                 _logger.info(f"Attempting to recreate materialized view for chart {chart.id}")
    #                 refresher.create_materialized_view_for_chart(chart.id)
                    
    #                 # Check again for columns
    #                 cr.execute(f"""
    #                     SELECT column_name 
    #                     FROM information_schema.columns 
    #                     WHERE table_name = %s
    #                 """, (view_name,))
                    
    #                 columns = [row[0] for row in cr.fetchall()]
                    
    #                 if not columns:
    #                     _logger.error(f"Still no columns after recreation - falling back to direct query")
    #                     return self._get_chart_data_from_direct_query(chart, cco, branches_id)
                
    #             # Log columns for debugging
    #             _logger.info(f"Columns in view {view_name}: {columns}")
                
    #             # Find the proper column for branch filtering
    #             branch_col = None
    #             if chart.branch_field:
    #                 branch_field = chart.branch_field.split('.')[-1] if '.' in chart.branch_field else chart.branch_field
                    
    #                 # First, try exact match
    #                 if branch_field in columns:
    #                     branch_col = branch_field
    #                     _logger.debug(f"Found exact branch column match: {branch_col}")
    #                 else:
    #                     # Try all column detection methods
    #                     branch_candidates = ['branch_id', 'id', 'branch', 'partner_branch_id']
    #                     for candidate in branch_candidates:
    #                         if candidate in columns:
    #                             branch_col = candidate
    #                             _logger.debug(f"Found branch column from candidates: {branch_col}")
    #                             break
                        
    #                     # If still not found, try columns containing 'branch'
    #                     if not branch_col:
    #                         for col in columns:
    #                             if 'branch' in col.lower():
    #                                 branch_col = col
    #                                 _logger.debug(f"Found branch column by partial match: {branch_col}")
    #                                 break
                
    #             # Build query against the materialized view
    #             query = f"SELECT * FROM {view_name}"
                
    #             # Apply security filters with proper column name
    #             where_clause_added = False
    #             if chart.branch_field and not cco and not self.security_service.is_cco_user():
    #                 user_branches = self.security_service.get_user_branch_ids()
    #                 effective_branches = []
                    
    #                 if branches_id:
    #                     # If branches specified in UI, intersect with user's branches
    #                     if user_branches:
    #                         effective_branches = [b for b in branches_id if b in user_branches]
    #                     else:
    #                         effective_branches = branches_id
    #                 elif user_branches:
    #                     effective_branches = user_branches
                    
    #                 # Build WHERE clause using the correct column name (not table alias)
    #                 if effective_branches and branch_col:
    #                     if len(effective_branches) == 1:
    #                         query += f" WHERE {branch_col} = {effective_branches[0]}"
    #                     else:
    #                         query += f" WHERE {branch_col} IN {tuple(effective_branches)}"
    #                     where_clause_added = True
    #                 elif branch_col:
    #                     # No branches specified, but we have a branch column - return no results
    #                     query += " WHERE 1=0"
    #                     where_clause_added = True
                
    #             # Add high-risk filter if needed
    #             high_risk_filter = request.httprequest.cookies.get('high_risk_filter')
    #             if high_risk_filter == 'on':
    #                 _logger.info("High risk filter is enabled")
    #                 # Look for risk_level column
    #                 risk_column = None
    #                 risk_candidates = ['risk_level', 'partner_risk_level', 'customer_risk_level']
                    
    #                 for candidate in risk_candidates:
    #                     if candidate in columns:
    #                         risk_column = candidate
    #                         break
                    
    #                 if risk_column:
    #                     if where_clause_added:
    #                         query += f" AND {risk_column} = 'high'"
    #                     else:
    #                         query += f" WHERE {risk_column} = 'high'"
    #                         where_clause_added = True
                
    #             # Find column for sorting
    #             sort_col = None
    #             if chart.y_axis_field:
    #                 y_field = chart.y_axis_field.split('.')[-1] if '.' in chart.y_axis_field else chart.y_axis_field
    #                 if y_field in columns:
    #                     sort_col = y_field
    #                 else:
    #                     # Look for numeric column names
    #                     candidates = ['count', 'customer_count', 'high_risk_customers', 'value', 'amount', 'total']
    #                     for candidate in candidates:
    #                         if candidate in columns:
    #                             sort_col = candidate
    #                             break
                
    #             # Add ORDER BY if we found a suitable column
    #             if sort_col:
    #                 query += f" ORDER BY {sort_col} DESC"
                
    #             # Add LIMIT
    #             query += " LIMIT 100"  # Reasonable default limit
                
    #             # Log the query for debugging
    #             _logger.info(f"Executing materialized view query: {query}")
                
    #             # Execute query with a timeout
    #             cr.execute("SET LOCAL statement_timeout = 30000;")  # 30 seconds
    #             try:
    #                 cr.execute(query)
    #                 results = cr.dictfetchall()
                    
    #                 # Log result count for debugging
    #                 _logger.info(f"Query returned {len(results)} rows")
                    
    #                 # Extract chart data
    #                 return self._extract_chart_data(chart, results, query)
    #             except Exception as query_err:
    #                 _logger.error(f"Error executing materialized view query: {query_err}")
    #                 # Fall back to direct query
    #                 return self._get_chart_data_from_direct_query(chart, cco, branches_id)
                
    #     except Exception as e:
    #         _logger.error(f"Error getting chart from materialized view: {e}")
    #         # Fall back to direct query
    #         return self._get_chart_data_from_direct_query(chart, cco, branches_id)

        
    def _find_branch_column_in_view(self, chart, columns):
        """Find the appropriate branch column in materialized view"""
        if not chart.branch_field:
            return None
            
        # 1. Try to find the exact branch field (without table alias)
        branch_field = chart.branch_field
        if '.' in branch_field:
            # Remove table alias if present (rb.id -> id)
            branch_field = branch_field.split('.')[-1]
        
        if branch_field in columns:
            return branch_field
        
        # 2. Try common branch field names
        candidates = ['branch_id', 'id', 'branch']
        for candidate in candidates:
            if candidate in columns:
                return candidate
        
        # 3. Look for columns with "branch" in the name
        for column in columns:
            if 'branch' in column.lower():
                return column
        
        # No suitable column found
        return None

    def _find_order_column_in_view(self, chart, columns):
        """Find appropriate ordering column in materialized view"""
        # 1. Try y_axis_field first
        if chart.y_axis_field:
            y_field = chart.y_axis_field
            # Remove table alias if present
            if '.' in y_field:
                y_field = y_field.split('.')[-1]
            
            if y_field in columns:
                return y_field
        
        # 2. Try common value column names
        candidates = ['customer_count', 'high_risk_customers', 'count', 'value', 'amount']
        for candidate in candidates:
            if candidate in columns:
                return candidate
        
        # 3. Try to find numeric columns
        # In this case we'd need to check the column types, 
        # but this would require an additional query to information_schema
        
        # 4. Fall back to first column if nothing else found
        if columns:
            return columns[0]
        
        return None

    def _create_view_indexes(self, cr, view_name, chart_id, chart_data, column_names):
        """Create indexes on materialized view with unique names"""
        timestamp = int(time.time())
        
        # X-axis index
        if chart_data['x_axis_field']:
            x_col = self._find_column_in_view(chart_data['x_axis_field'], column_names)
            if x_col:
                index_name = f"{view_name}_x_{timestamp}_idx"
                _logger.info(f"Creating x-axis index on column: {x_col}")
                cr.execute(f"CREATE INDEX {index_name} ON {view_name} ({x_col})")
        
        # Y-axis index
        if chart_data['y_axis_field']:
            y_col = self._find_column_in_view(chart_data['y_axis_field'], column_names)
            if y_col:
                index_name = f"{view_name}_y_{timestamp}_idx"
                _logger.info(f"Creating y-axis index on column: {y_col}")
                cr.execute(f"CREATE INDEX {index_name} ON {view_name} ({y_col})")
        
        # Date index
        if chart_data['date_field']:
            date_col = self._find_column_in_view(chart_data['date_field'], column_names)
            if date_col:
                index_name = f"{view_name}_date_{timestamp}_idx"
                _logger.info(f"Creating date index on column: {date_col}")
                cr.execute(f"CREATE INDEX {index_name} ON {view_name} ({date_col})")
        
        # Branch index
        if chart_data['branch_field']:
            branch_col = self._find_column_in_view(chart_data['branch_field'], column_names)
            if branch_col:
                index_name = f"{view_name}_branch_{timestamp}_idx"
                _logger.info(f"Creating branch index on column: {branch_col}")
                cr.execute(f"CREATE INDEX {index_name} ON {view_name} ({branch_col})")
                
    def create_performance_indexes(self):
        """Create database indexes to improve query performance"""
        try:
            # Use direct SQL for creating indexes
            self.env.cr.execute("""
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
            """)
            
            _logger.info("Created performance indexes successfully")
            return True
        except Exception as e:
            _logger.error(f"Error creating performance indexes: {e}")
            return False
        
    def initialize_database_settings(self):
        """Initialize database settings for optimal performance"""
        try:
            # Increase work_mem for better query performance
            self.env.cr.execute("SET work_mem = '32MB'")
            
            # Adjust statement timeout for the current session (not database-wide)
            self.env.cr.execute("SET statement_timeout = '30s'")
            
            # Adjust max_parallel_workers_per_gather for better parallelization
            self.env.cr.execute("SET max_parallel_workers_per_gather = 4")
            
            # Enable parallel scan
            self.env.cr.execute("SET enable_parallel_append = on")
            self.env.cr.execute("SET enable_parallel_hash = on")
            self.env.cr.execute("SET enable_partition_pruning = on")
            
            _logger.info("Database settings initialized for optimal performance")
            return True
        except Exception as e:
            _logger.error(f"Error initializing database settings: {e}")
            return False
        
    def diagnose_chart_issues(self):
        """Diagnose and fix common chart issues"""
        try:
            # 1. Find charts with timeouts
            timeout_charts = self.env['res.dashboard.charts'].search([
                ('last_error_message', 'ilike', 'timeout'),
                ('use_materialized_view', '=', False)
            ])
            
            # Enable materialized views for them
            for chart in timeout_charts:
                chart.write({
                    'use_materialized_view': True,
                    'materialized_view_refresh_interval': 60,
                    'last_error_message': 'Auto-enabled materialized view due to timeout history'
                })
                self.env['dashboard.chart.view.refresher'].create_materialized_view_for_chart(chart.id)
                
            # 2. Find charts with syntax errors
            syntax_error_charts = self.env['res.dashboard.charts'].search([
                '|',
                ('last_error_message', 'ilike', 'syntax error'),
                ('last_error_message', 'ilike', 'missing FROM-clause')
            ])
            
            # Log them for manual review
            if syntax_error_charts:
                _logger.warning(f"Found {len(syntax_error_charts)} charts with syntax errors: {syntax_error_charts.ids}")
            
            # 3. Fix materialized view refreshers
            self.env.cr.execute("""
                UPDATE dashboard_chart_view_refresher r
                SET view_name = 'dashboard_chart_view_' || r.chart_id
                WHERE view_name IS NULL OR view_name = ''
            """)
            
            return {
                'timeout_charts_fixed': len(timeout_charts),
                'syntax_error_charts': len(syntax_error_charts),
            }
        except Exception as e:
            _logger.error(f"Error diagnosing chart issues: {e}")
            return {'error': str(e)}

    # @api.model
    # def _find_column_in_view(self, field_name, column_names):
    #     """Find the most appropriate column name in the materialized view"""
    #     # Strip table aliases if present (e.g., 'rb.name' -> 'name')
    #     if '.' in field_name:
    #         _, field_name = field_name.split('.', 1)
        
    #     # First check for exact match
    #     if field_name in column_names:
    #         return field_name
        
    #     # Try lowercase match
    #     field_lower = field_name.lower()
    #     for col in column_names:
    #         if col.lower() == field_lower:
    #             return col
        
    #     # Try partial matches
    #     for col in column_names:
    #         if field_lower in col.lower():
    #             return col
        
    #     # Try matching field name without the 'id' suffix 
    #     # (e.g., 'branch_id' might be stored as 'branch')
    #     if field_lower.endswith('_id'):
    #         base_name = field_lower[:-3]
    #         for col in column_names:
    #             if col.lower() == base_name or col.lower().startswith(base_name):
    #                 return col
        
    #     return None
    
    @api.model
    def _find_column_in_view(self, field_name, column_names):
        """Find the most appropriate column name in the materialized view"""
        # Strip table aliases if present (e.g., 'rb.name' -> 'name')
        original_field = field_name  # keep original for logging
        if '.' in field_name:
            _, field_name = field_name.split('.', 1)
        
        # First check for exact match
        if field_name in column_names:
            _logger.debug(f"Found exact column match: {field_name} for {original_field}")
            return field_name
        
        # Try lowercase match
        field_lower = field_name.lower()
        for col in column_names:
            if col.lower() == field_lower:
                _logger.debug(f"Found case-insensitive match: {col} for {original_field}")
                return col
        
        # Try partial matches
        for col in column_names:
            if field_lower in col.lower():
                _logger.debug(f"Found partial match: {col} for {original_field}")
                return col
        
        # Try matching field name without the 'id' suffix 
        # (e.g., 'branch_id' might be stored as 'branch')
        if field_lower.endswith('_id'):
            base_name = field_lower[:-3]
            for col in column_names:
                if col.lower() == base_name or col.lower().startswith(base_name):
                    _logger.debug(f"Found match without '_id' suffix: {col} for {original_field}")
                    return col
        
        # Special case handling
        if field_lower == 'id' and 'branch_id' in column_names:
            _logger.debug(f"Found special case match 'branch_id' for 'id' field")
            return 'branch_id'
        
        # Log all columns to help with debugging
        _logger.warning(f"Could not find column match for {original_field} in columns: {column_names}")
        return None
        
    @api.model
    def drop_materialized_view_for_chart(self, chart_id):
        """Drop a materialized view for a chart"""
        try:
            # Use direct view name generation instead of ChartDataService dependency
            view_name = f"dashboard_chart_view_{chart_id}"
            
            # Use a separate cursor for this operation
            registry = self.env.registry
            with registry.cursor() as cr:
                try:
                    # Try to acquire an advisory lock to prevent concurrent operations
                    cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                    lock_acquired = cr.fetchone()[0]
                    
                    if not lock_acquired:
                        _logger.info(f"Another process is modifying chart {chart_id}, skipping view drop")
                        return False
                    
                    # Drop the view
                    cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                    
                    # Record the drop in our database
                    cr.execute("""
                        DELETE FROM dashboard_chart_view_refresher
                        WHERE chart_id = %s
                    """, (chart_id,))
                    
                    # Update the chart record
                    cr.execute("""
                        UPDATE res_dashboard_charts
                        SET materialized_view_last_refresh = NULL,
                            write_uid = %s, write_date = %s
                        WHERE id = %s
                    """, (self.env.uid, fields.Datetime.now(), chart_id))
                    
                    cr.commit()
                    
                    _logger.info(f"Dropped materialized view {view_name} for chart {chart_id}")
                    return True
                    
                except Exception as e:
                    cr.rollback()
                    raise e
            
        except Exception as e:
            _logger.error(f"Error dropping materialized view for chart {chart_id}: {e}")
            return False
    
    @api.model
    def refresh_chart_view(self, chart_id, low_priority=False):
        """Refresh a materialized view for a chart with robust error handling and transaction isolation"""
        try:
            # Get refresher record
            refresher = self.search([('chart_id', '=', chart_id)], limit=1)
            if not refresher:
                # If no refresher record, try to create the view first
                return self.create_materialized_view_for_chart(chart_id)
            
            # Get the chart
            chart = refresher.chart_id
            view_name = refresher.view_name
            
            # Check if we need to refresh based on interval - only when not explicitly requested
            if not low_priority and refresher.last_refresh and refresher.refresh_interval:
                now = fields.Datetime.now()
                if refresher.last_refresh + timedelta(minutes=refresher.refresh_interval) > now:
                    # Not time to refresh yet
                    _logger.debug(f"Skipping refresh for chart {chart_id}, not time yet")
                    return True
            
            # Check if the view exists in a separate transaction
            registry = self.env.registry
            view_exists = False
            with registry.cursor() as cr:
                cr.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %s
                        AND c.relkind = 'm'
                    )
                """, (view_name,))
                view_exists = cr.fetchone()[0]
            
            if not view_exists:
                # View doesn't exist, create it
                return self.create_materialized_view_for_chart(chart_id)
            
            # First try CONCURRENTLY refresh in a separate transaction
            concurrent_success = False
            try:
                with registry.cursor() as cr:
                    if low_priority:
                        cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    cr.execute("SET LOCAL statement_timeout = 120000;")  # 2 minutes
                    
                    # Try to acquire an advisory lock first
                    cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (chart_id,))
                    lock_acquired = cr.fetchone()[0]
                    
                    if not lock_acquired:
                        _logger.info(f"Another process is refreshing view for chart {chart_id}, skipping")
                        return False
                    
                    # Check if the view has a unique index (required for CONCURRENTLY)
                    cr.execute("""
                        SELECT COUNT(*) FROM pg_indexes 
                        WHERE tablename = %s 
                        AND indexdef LIKE %s
                    """, (view_name, '%UNIQUE%'))
                    
                    has_unique_index = cr.fetchone()[0] > 0
                    
                    if has_unique_index:
                        # Try with CONCURRENTLY for less locking if there's a unique index
                        _logger.info(f"Refreshing view {view_name} with CONCURRENTLY option")
                        cr.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
                        cr.commit()
                        concurrent_success = True
                    else:
                        _logger.info(f"View {view_name} has no unique index, skipping CONCURRENTLY refresh")
                        cr.rollback()  # We'll try regular refresh instead
            except Exception as e:
                _logger.info(f"CONCURRENTLY refresh failed: {e}")
                # Transaction should already be rolled back by the database
            
            # If CONCURRENTLY refresh didn't work, try regular refresh in a new transaction
            if not concurrent_success:
                with registry.cursor() as cr:
                    try:
                        if low_priority:
                            cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        cr.execute("SET LOCAL statement_timeout = 120000;")  # 2 minutes
                        
                        # Try to acquire an advisory lock
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
                        raise e  # Re-raise for error handling below
            
            # Update timestamps in separate transactions
            now = fields.Datetime.now()
            
            # Update refresher last_refresh time
            with registry.cursor() as write_cr:
                try:
                    write_cr.execute("""
                        UPDATE dashboard_chart_view_refresher
                        SET last_refresh = %s
                        WHERE id = %s
                    """, (now, refresher.id))
                    write_cr.commit()
                except Exception as e:
                    write_cr.rollback()
                    _logger.error(f"Failed to update refresher timestamp: {e}")
            
            # Update chart's last refresh time 
            with registry.cursor() as write_cr:
                try:
                    write_cr.execute("""
                        UPDATE res_dashboard_charts
                        SET materialized_view_last_refresh = %s,
                            last_execution_status = %s,
                            last_error_message = NULL
                        WHERE id = %s
                    """, (now, 'success', chart_id))
                    write_cr.commit()
                except Exception as e:
                    write_cr.rollback()
                    _logger.error(f"Failed to update chart timestamp: {e}")
            
            _logger.info(f"Successfully refreshed materialized view {view_name} for chart {chart_id}")
            return True
            
        except Exception as e:
            _logger.error(f"Error refreshing materialized view for chart {chart_id}: {e}")
            
            # Update the chart with the error in a separate transaction
            try:
                registry = self.env.registry
                with registry.cursor() as err_cr:
                    err_cr.execute("""
                        UPDATE res_dashboard_charts
                        SET last_execution_status = %s,
                            last_error_message = %s
                        WHERE id = %s
                    """, ('error', str(e), chart_id))
                    err_cr.commit()
            except Exception as write_err:
                _logger.error(f"Failed to update error status: {write_err}")
            
            return False
    
        
    # @api.model
    # def refresh_chart_view(self, chart_id, low_priority=False):
    #     """Refresh a materialized view for a chart with robust error handling"""
    #     try:
    #         refresher = self.search([('chart_id', '=', chart_id)], limit=1)
    #         if not refresher:
    #             # If no refresher record, try to create the view first
    #             return self.create_materialized_view_for_chart(chart_id)
            
    #         # Get the chart
    #         chart = refresher.chart_id
            
    #         # Check if we need to refresh based on interval - only when not explicitly requested
    #         if not low_priority and refresher.last_refresh and refresher.refresh_interval:
    #             now = fields.Datetime.now()
    #             if refresher.last_refresh + timedelta(minutes=refresher.refresh_interval) > now:
    #                 # Not time to refresh yet
    #                 _logger.debug(f"Skipping refresh for chart {chart_id}, not time yet")
    #                 return True
            
    #         # Use a new cursor to isolate this operation
    #         registry = self.env.registry
    #         with registry.cursor() as cr:
    #             try:
    #                 # Set transaction isolation level if low_priority is True
    #                 if low_priority:
    #                     cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    
    #                 # Set a higher timeout (2 minutes)
    #                 cr.execute("SET LOCAL statement_timeout = 120000;")
                    
    #                 # Check if the view exists
    #                 cr.execute(f"""
    #                     SELECT EXISTS (
    #                         SELECT FROM pg_catalog.pg_class c
    #                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    #                         WHERE c.relname = %s
    #                         AND c.relkind = 'm'
    #                     )
    #                 """, (refresher.view_name,))
                    
    #                 view_exists = cr.fetchone()[0]
                    
    #                 if not view_exists:
    #                     # View doesn't exist, create it
    #                     cr.rollback()  # Clean up this cursor
    #                     return self.create_materialized_view_for_chart(chart_id)
                    
    #                 # Refresh the view with a higher timeout
    #                 try:
    #                     # Try with CONCURRENTLY for less locking, but requires unique index
    #                     cr.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {refresher.view_name}")
    #                 except Exception as e:
    #                     # Fall back to regular refresh if CONCURRENTLY fails
    #                     _logger.info(f"CONCURRENTLY refresh failed, using regular refresh: {e}")
    #                     cr.execute(f"REFRESH MATERIALIZED VIEW {refresher.view_name}")
                    
    #                 # Commit the transaction explicitly
    #                 cr.commit()
                    
    #                 # Update timestamps in separate transactions to avoid conflicts
    #                 now = fields.Datetime.now()
                    
    #                 # Update refresher last_refresh time
    #                 with registry.cursor() as write_cr:
    #                     env = api.Environment(write_cr, self.env.uid, self.env.context)
    #                     refresher_to_update = env['dashboard.chart.view.refresher'].browse(refresher.id)
    #                     if refresher_to_update.exists():
    #                         refresher_to_update.write({
    #                             'last_refresh': now
    #                         })
    #                         write_cr.commit()
                    
    #                 # Update chart's last refresh time 
    #                 with registry.cursor() as write_cr:
    #                     env = api.Environment(write_cr, self.env.uid, self.env.context)
    #                     chart_to_update = env['res.dashboard.charts'].browse(chart.id)
    #                     if chart_to_update.exists():
    #                         chart_to_update.write({
    #                             'materialized_view_last_refresh': now,
    #                             'last_execution_status': 'success',
    #                             'last_error_message': False
    #                         })
    #                         write_cr.commit()
                    
    #                 _logger.info(f"Refreshed materialized view {refresher.view_name} for chart {chart_id}")
    #                 return True
                    
    #             except Exception as e:
    #                 # Rollback in case of error
    #                 cr.rollback()
    #                 _logger.error(f"Error refreshing materialized view for chart {chart_id}: {e}")
                    
    #                 # Update the chart with the error using a new cursor
    #                 try:
    #                     with registry.cursor() as write_cr:
    #                         env = api.Environment(write_cr, self.env.uid, self.env.context)
    #                         chart_to_update = env['res.dashboard.charts'].browse(chart.id)
    #                         if chart_to_update.exists():
    #                             chart_to_update.write({
    #                                 'last_execution_status': 'error',
    #                                 'last_error_message': str(e)
    #                             })
    #                             write_cr.commit()
    #                 except Exception as write_err:
    #                     _logger.error(f"Failed to update chart error status: {str(write_err)}")
                    
    #                 return False
            
    #     except Exception as e:
    #         _logger.error(f"Error refreshing materialized view for chart {chart_id}: {e}")
    #         return False
    
    @api.model
    def ensure_all_views_exist(self):
        """Ensure all materialized views exist and are properly created - called at startup"""
        _logger.info("Ensuring all materialized views exist and are correctly created")
        
        try:
            # Get all charts that should have materialized views
            charts = self.env['res.dashboard.charts'].search([
                ('state', '=', 'active'),
                ('use_materialized_view', '=', True)
            ])
            
            if not charts:
                _logger.info("No charts with materialized views found")
                return True
                
            _logger.info(f"Found {len(charts)} charts with materialized views")
            
            # Process each chart
            created = 0
            errors = 0
            
            for chart in charts:
                # Check if view exists and has columns
                view_name = f"dashboard_chart_view_{chart.id}"
                
                # First check if the view exists
                with self.env.registry.cursor() as cr:
                    cr.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_catalog.pg_class c
                            WHERE c.relname = %s AND c.relkind = 'm'
                        )
                    """, (view_name,))
                    
                    view_exists = cr.fetchone()[0]
                
                # Only create if it doesn't exist
                if not view_exists:
                    _logger.info(f"View for chart {chart.id} needs creation")
                    
                    # Create the view
                    success = self.create_materialized_view_for_chart(chart.id)
                    
                    if success:
                        created += 1
                        _logger.info(f"Successfully created materialized view for chart {chart.id}")
                    else:
                        errors += 1
                        _logger.error(f"Failed to create materialized view for chart {chart.id}")
            
            _logger.info(f"Materialized view initialization complete: {created} created, {errors} errors")
            return True
            
        except Exception as e:
            _logger.error(f"Error ensuring materialized views exist: {e}")
            return False
    
    # @api.model
    # def ensure_all_views_exist(self):
    #     """Ensure all materialized views exist and are properly created - called at startup"""
    #     _logger.info("Ensuring all materialized views exist and are correctly created")
        
    #     try:
    #         # Get all charts that should have materialized views
    #         charts = self.env['res.dashboard.charts'].search([
    #             ('state', '=', 'active'),
    #             ('use_materialized_view', '=', True)
    #         ])
            
    #         if not charts:
    #             _logger.info("No charts with materialized views found")
    #             return True
                
    #         _logger.info(f"Found {len(charts)} charts with materialized views")
            
    #         # Process each chart
    #         created = 0
    #         errors = 0
            
    #         for chart in charts:
    #             # Check if view exists and has columns
    #             diagnosis = self.diagnose_materialized_view(chart.id)
                
    #             if not diagnosis.get('exists') or not diagnosis.get('has_columns'):
    #                 _logger.info(f"View for chart {chart.id} needs creation or repair")
                    
    #                 # Force create the view
    #                 # success = self.force_create_materialized_view(chart.id)
    #                 success = self.create_materialized_view_for_chart(chart.id)
                    
    #                 if success:
    #                     created += 1
    #                     _logger.info(f"Successfully created materialized view for chart {chart.id}")
    #                 else:
    #                     errors += 1
    #                     _logger.error(f"Failed to create materialized view for chart {chart.id}")
            
    #         _logger.info(f"Materialized view initialization complete: {created} created, {errors} errors")
    #         return True
            
    #     except Exception as e:
    #         _logger.error(f"Error ensuring materialized views exist: {e}")
    #         return False
    
    @api.model
    def init(self):
        """Ensure a refresher record exists"""
        super(DashboardChartViewRefresher, self).init()
        
        # Delay the initialization to allow the server to fully start
        # This will be executed after all models are loaded
        self.env.cr.commit()  # Commit any pending changes first
        
        try:
            # Schedule the view initialization to run soon after startup
            self.ensure_all_views_exist()
        except Exception as e:
            _logger.error(f"Error in init for DashboardChartViewRefresher: {e}")
        # Set up dashboard tables
        try:
            if not self.setup_dashboard_tables():
                _logger.error("Failed to set up dashboard tables during init")
        except Exception as e:
            _logger.error(f"Error setting up dashboard tables in init: {e}")
    
    # Create refresher record if none exists
        if not self.search([], limit=1):
            self.create({'name': 'Dashboard Chart View Refresher'})
            
            # Create materialized views for all charts that have it enabled
            charts = self.env['res.dashboard.charts'].search([
                ('state', '=', 'active'),
                ('use_materialized_view', '=', True)
            ])
            
            for chart in charts:
                self.create_materialized_view_for_chart(chart.id)
                
    def setup_dashboard_tables(self):
        """Set up required tables for dashboard functionality"""
        try:
            self.env.cr.execute("""
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
            """)
            
            # Log successful setup
            _logger.info("Dashboard tables created successfully")
            return True
        except Exception as e:
            _logger.error(f"Error setting up dashboard tables: {e}")
            return False