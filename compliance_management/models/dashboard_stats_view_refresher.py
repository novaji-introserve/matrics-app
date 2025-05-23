# -*- coding: utf-8 -*-

from odoo import models, fields, api
import logging
import time
from datetime import timedelta

_logger = logging.getLogger(__name__)

class StatisticViewRefresher(models.Model):
    _name = 'dashboard.stats.view.refresher'
    _description = 'Statistics View Refresher'
    
    name = fields.Char(string='Refresher Name', default='Statistics View Refresher')
    last_run = fields.Datetime(string='Last Run', readonly=True)
    
    # Track materialized views
    stat_id = fields.Many2one('res.compliance.stat', string='Statistic', ondelete='cascade')
    view_name = fields.Char(string='View Name', readonly=True)
    last_refresh = fields.Datetime(string='Last Refresh', readonly=True)
    refresh_interval = fields.Integer(string='Refresh Interval (minutes)', default=60)
    
    _sql_constraints = [
        ('unique_stat', 'unique(stat_id)', 'Only one materialized view per statistic is allowed.')
    ]
    
    @api.model
    def refresh_stat_views(self, low_priority=False):
        """Refresh all statistic materialized views"""
        try:
            # Find all active statistics
            stats = self.env['res.compliance.stat'].search([('state', '=', 'active')])
            
            refreshed = 0
            errors = 0
            
            for stat in stats:
                if self.refresh_stat_view(stat.id, low_priority):
                    refreshed += 1
                else:
                    errors += 1
            
            # Update last run time
            refresher = self.search([], limit=1)
            if refresher:
                refresher.write({'last_run': fields.Datetime.now()})
            
            _logger.info(f"Refreshed {refreshed} statistic views, {errors} errors")
            return True
        except Exception as e:
            _logger.error(f"Error refreshing statistic views: {e}")
            return False
    
    @api.model
    def refresh_stat_view(self, stat_id, low_priority=False):
        """Refresh a materialized view for a statistic"""
        try:
            registry = self.env.registry
            with registry.cursor() as cr:
                # Set transaction isolation level if low_priority is True
                if low_priority:
                    cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                
                # Try to acquire an advisory lock for this chart_id with small timeout
                cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (stat_id,))
                view_name = f"stat_view_{stat_id}"
            
            # Check if view exists
            self.env.cr.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_catalog.pg_class c
                    WHERE c.relname = %s AND c.relkind = 'm'
                )
            """, (view_name,))
            
            view_exists = self.env.cr.fetchone()[0]
            
            if not view_exists:
                # If view doesn't exist, create it
                return self.create_materialized_view_for_stat(stat_id)
            
            # Refresh the view
            self.env.cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
            
            # Update the refresher record
            refresher = self.search([('stat_id', '=', stat_id)], limit=1)
            now = fields.Datetime.now()
            
            if refresher:
                refresher.write({
                    'last_refresh': now
                })
            
            # Update the statistic record
            stat = self.env['res.compliance.stat'].browse(stat_id)
            if stat.exists():
                stat.write({
                    'materialized_view_last_refresh': now
                })
            
            _logger.info(f"Refreshed materialized view {view_name} for statistic {stat_id}")
            return True
            
        except Exception as e:
            _logger.error(f"Error refreshing materialized view for statistic {stat_id}: {e}")
            return False
    
    @api.model
    def create_materialized_view_for_stat(self, stat_id):
        """Create or update a materialized view for a statistic"""
        try:
            # Get statistic
            stat = self.env['res.compliance.stat'].browse(stat_id)
            if not stat.exists():
                _logger.error(f"Statistic {stat_id} not found")
                return False
            
            # Define view name
            view_name = f"stat_view_{stat_id}"
            
            # First explicitly check if view exists
            self.env.cr.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_catalog.pg_class c
                    WHERE c.relname = %s AND c.relkind = 'm'
                )
            """, (view_name,))
            
            view_exists = self.env.cr.fetchone()[0]
            
            # If view already exists, no need to create it again
            if view_exists:
                _logger.info(f"Materialized view {view_name} already exists, skipping creation")
                return True
            
            # Get the query
            original_query, query = stat._prepare_and_validate_query(stat.sql_query)
            if not original_query:
                _logger.error(f"Invalid query for statistic {stat_id}")
                return False
            
            # Create a transaction to handle database operations
            with self.env.registry.cursor() as cr:
                try:
                    # Set a longer timeout for complex queries
                    cr.execute("SET LOCAL statement_timeout = 120000;")  # 2 minutes
                    
                    # Double-check view doesn't exist in this transaction
                    cr.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_catalog.pg_class c
                            WHERE c.relname = %s AND c.relkind = 'm'
                        )
                    """, (view_name,))
                    
                    if cr.fetchone()[0]:
                        _logger.info(f"Materialized view {view_name} already exists (double-check), skipping creation")
                        return True
                    
                    # Remove trailing semicolons from the query
                    if original_query.strip().endswith(';'):
                        original_query = original_query.strip()[:-1]
                    
                    # Create the view
                    create_view_query = f"""
                        CREATE MATERIALIZED VIEW {view_name} AS
                        {original_query}
                        WITH DATA
                    """
                    
                    _logger.info(f"Creating materialized view for statistic {stat_id}: {view_name}")
                    cr.execute(create_view_query)
                    
                    # Record the creation in refresher model
                    refresher = self.search([('stat_id', '=', stat_id)], limit=1)
                    now = fields.Datetime.now()
                    
                    if refresher:
                        # Update existing record
                        refresher.write({
                            'view_name': view_name,
                            'last_refresh': now
                        })
                    else:
                        # Create new refresher record
                        self.create({
                            'name': f"Refresher for {stat.name}",
                            'stat_id': stat_id,
                            'view_name': view_name,
                            'last_refresh': now
                        })
                    
                    # Update statistic record
                    stat.write({
                        'materialized_view_last_refresh': now
                    })
                    
                    # Commit the transaction
                    cr.commit()
                    
                    _logger.info(f"Successfully created materialized view {view_name} for statistic {stat_id}")
                    return True
                except Exception as e:
                    cr.rollback()
                    _logger.error(f"Error creating materialized view for statistic {stat_id}: {e}")
                    return False
                
        except Exception as e:
            _logger.error(f"Error creating materialized view for statistic {stat_id}: {e}")
            return False
        
    @api.model
    def ensure_all_stat_views_exist(self):
        """Ensure all statistic materialized views exist"""
        _logger.info("Ensuring all statistic materialized views exist")
        
        try:
            # Get all active statistics
            stats = self.env['res.compliance.stat'].search([('state', '=', 'active')])
            
            if not stats:
                _logger.info("No statistics found")
                return True
                
            _logger.info(f"Found {len(stats)} statistics to create views for")
            
            # Process each statistic
            created = 0
            errors = 0
            
            for stat in stats:
                # Check if view exists
                view_name = f"stat_view_{stat.id}"
                
                self.env.cr.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        WHERE c.relname = %s AND c.relkind = 'm'
                    )
                """, (view_name,))
                
                view_exists = self.env.cr.fetchone()[0]
                
                if not view_exists:
                    _logger.info(f"Creating materialized view for statistic {stat.id}")
                    if self.create_materialized_view_for_stat(stat.id):
                        created += 1
                    else:
                        errors += 1
            
            _logger.info(f"Materialized view initialization complete: {created} created, {errors} errors")
            return True
            
        except Exception as e:
            _logger.error(f"Error ensuring all statistic views exist: {e}")
            return False
    
    @api.model
    def init(self):
        """Initialize views on server startup"""
        super(StatisticViewRefresher, self).init()
        
        # Create a refresher record if none exists
        if not self.search([], limit=1):
            self.create({'name': 'Statistics View Refresher'})
        
        # Schedule the view initialization to run after startup
        self.env.cr.commit()  # Commit any pending changes first
        
        try:
            self.ensure_all_stat_views_exist()
        except Exception as e:
            _logger.error(f"Error in init for StatisticViewRefresher: {e}")
            
        # # Create refresher record if none exists
        # if not self.search([], limit=1):
        #     self.create({'name': 'Dashboard Stat View Refresher'})
            
        #     # Create materialized views for all charts that have it enabled
        #     stats = self.env['res.compliance.stat'].search([
        #         ('use_materialized_view', '=', True)
        #     ])
            
        #     for chart in stats:
        #         self.create_materialized_view_for_stat(stats.id)