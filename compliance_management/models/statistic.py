# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import re
from datetime import datetime, timedelta
import logging

_logger = logging.getLogger(__name__)

class Statistic(models.Model):
    _name = 'res.compliance.stat'
    _description = 'Compliance Statistics'
    _sql_constraints = [
        ('uniq_stats_code', 'unique(code)',
         "Stats code already exists. Value must be unique!"),
        ('uniq_stats_name', 'unique(name)',
         "Name already exists. Value must be unique!")
    ]
    _inherit = ['mail.thread','mail.activity.mixin']

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    sql_query = fields.Text(string='SQL Query', required=True)
    scope = fields.Selection(string='Scope', selection=[(
        'bank', 'Bank Wide'), ('branch', 'Branch'), ('compliance', 'Compliance'),
        ('regulatory', 'Regulatory'),('risk','Risk Assessment')], default='bank')
    state = fields.Selection(string='State', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active')
    val = fields.Char(string='Value', compute='_compute_val', store=True, readonly=True)
    narration = fields.Text(string='Narration')
    scope_color = fields.Char()
    
    use_materialized_view = fields.Boolean(
        string="Use Materialized View",
        default=False,
        help="If enabled, a materialized view will be created for this statistic to optimize performance",
        tracking=True
    )
    materialized_view_refresh_interval = fields.Integer(
        string="Refresh Interval (minutes)",
        default=60,
        help="How often the materialized view should be refreshed (in minutes)",
    )
    materialized_view_last_refresh = fields.Datetime(
        string="Last View Refresh",
        readonly=True
    )
    last_execution_time = fields.Float(
        string="Last Execution Time (ms)",
        readonly=True,
        help="Time taken to execute this query the last time it ran"
    )
    last_execution_status = fields.Selection(
        [('success', 'Success'), ('error', 'Error')],
        string="Last Execution Status",
        readonly=True
    )
    last_error_message = fields.Text(
        string="Last Error Message",
        readonly=True
    )

    def _prepare_and_validate_query(self, sql_query):
        """Helper method to prepare and validate SQL query with consistent filtering"""
        if not sql_query:
            return None

        pattern = r"\bres_partner\b"    
        try:
            # Keep original query for execution but use lowercase for checks
            original_query = sql_query.strip()
            query = original_query.lower()
            
            if not query.startswith('select'):
                raise ValidationError('Query not supported.\nHint: Start with SELECT')
            
            if re.search(pattern, query, re.IGNORECASE):
                # Remove trailing semicolon if present
                if query.endswith(";"):
                    query = query[:-1]
                    original_query = original_query[:-1]
                
                has_where = bool(re.search(r'\bwhere\b', query))
                
                # Determine the condition to add
                condition = " AND origin IN ('demo','test','prod')" if has_where else " WHERE origin IN ('demo','test','prod')"
                
                # Add the condition before any GROUP BY, ORDER BY, LIMIT, etc.
                for clause in ['group by', 'order by', 'limit', 'offset', 'having']:
                    clause_pos = query.find(' ' + clause + ' ')
                    if clause_pos > -1:
                        # Insert condition before this clause in the original query
                        original_query = original_query[:clause_pos] + condition + original_query[clause_pos:]
                        break
                else:
                    # No such clauses found, append at the end
                    original_query += condition
                    
            return original_query, query
            
        except Exception as e:
            self.env.cr.rollback()  # Important: rollback on error
            raise ValidationError(f'Invalid SQL query:\n{str(e)}')
    
    def _execute_query_and_get_value(self, original_query, query):
        """Execute the query and return the appropriate value"""
        self.env.cr.execute(original_query)
        
        aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
        pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\("
        match = re.search(pattern, query, re.IGNORECASE)
        
        if match:
            result = self.env.cr.fetchone()
            return str(result[0]) if result and result[0] is not None else '0'
        else:
            records = self.env.cr.fetchall()
            return str(len(records)) if records else '0'
   
    @api.model
    def create(self, vals):
        sql_query = vals.get('sql_query')
        
        if sql_query:
            try:
                original_query, query = self._prepare_and_validate_query(sql_query)
                # We don't set vals['val'] here since it's a computed field
                
                # Test if the query actually executes correctly
                self._execute_query_and_get_value(original_query, query)
            except Exception as e:
                # Only raise validation error for the SQL query here
                raise ValidationError(f'Invalid SQL query:\n{str(e)}')
        
        return super(Statistic, self).create(vals)
    
    def write(self, vals):
        # If updating SQL query, validate it before calling super
        if 'sql_query' in vals:
            sql_query = vals['sql_query']
            try:
                original_query, query = self._prepare_and_validate_query(sql_query)
                # Test if the query actually executes correctly
                self._execute_query_and_get_value(original_query, query)
            except Exception as e:
                # Only raise validation error for the SQL query here
                raise ValidationError(f'Invalid SQL query:\n{str(e)}')
                
        return super(Statistic, self).write(vals)

    # @api.depends('sql_query')
    # def _compute_val(self):
    #     for record in self:
    #         if not record.sql_query:
    #             record.val = '0'
    #             continue
                
    #         try:
    #             original_query, query = record._prepare_and_validate_query(record.sql_query)
    #             if original_query:
    #                 record.val = record._execute_query_and_get_value(original_query, query)
    #         except Exception as e:
    #             record.val = 'Error'
    #             # Don't rollback here as it can interfere with the form view
    #             _logger.error(f"Error computing value for stat {record.name}: {str(e)}")
    
    @api.depends('sql_query', 'use_materialized_view')
    def _compute_val(self):
        for record in self:
            if not record.sql_query:
                record.val = '0'
                continue
            
            # Try materialized view first if enabled
            if record.use_materialized_view:
                view_value = record.get_value_from_materialized_view()
                if view_value is not None:
                    record.val = view_value
                    continue
            
            # Fall back to direct query if view doesn't exist or isn't enabled
            try:
                original_query, query = record._prepare_and_validate_query(record.sql_query)
                if original_query:
                    record.val = record._execute_query_and_get_value(original_query, query)
            except Exception as e:
                record.val = 'Error'
                _logger.error(f"Error computing value for stat {record.name}: {str(e)}")
    
    @api.onchange('sql_query')
    def _onchange_sql_query(self):
        # This will update the field in the UI before saving
        self._compute_val()
        
    def get_value_from_materialized_view(self):
        """Get value from materialized view if exists and enabled"""
        self.ensure_one()
        
        if not self.use_materialized_view:
            return None
        
        view_name = f"stat_view_{self.id}"
        
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
            refresher = self.env['dashboard.stats.view.refresher'].sudo()
            if not refresher.create_materialized_view_for_stat(self.id):
                return None
        
        # Query the view
        try:
            # Use a dedicated cursor with transaction isolation
            with self.env.registry.cursor() as cr:
                cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                
                result = cr.fetchone()
                
                if result is None:
                    return '0'
                
                # Handle different result types
                if len(result) == 1:
                    # Single column result (common for aggregates)
                    value = result[0]
                else:
                    # Multiple columns, count rows instead
                    value = 1  # We fetched 1 row
                
                # Format the value
                if isinstance(value, (int, float)):
                    return "{:,}".format(value)
                else:
                    return str(value) if value is not None else '0'
        except Exception as e:
            _logger.error(f"Error querying materialized view for statistic {self.id}: {e}")
            return None
        
    def refresh_materialized_view(self):
        """Manually refresh the materialized view for this statistic"""
        self.ensure_one()
        
        if not self.use_materialized_view:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Error',
                    'message': 'Materialized view is not enabled for this statistic.',
                    'type': 'danger',
                    'sticky': False,
                }
            }
        
        refresher = self.env['dashboard.stats.view.refresher'].sudo()
        success = refresher.refresh_stat_view(self.id)
        
        if success:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Success',
                    'message': 'Materialized view refreshed successfully.',
                    'type': 'success',
                    'sticky': False,
                }
            }
        else:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Error',
                    'message': 'Failed to refresh materialized view.',
                    'type': 'danger',
                    'sticky': False,
                }
            }
    