# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError
from datetime import datetime, timedelta
from odoo import tools
import logging
import json
import pytz

_logger = logging.getLogger(__name__)

class ETLDashboard(models.Model):
    _name = 'etl.dashboard'
    _description = 'ETL Monitoring Dashboard'
    _auto = False  # This is a SQL view
    
    name = fields.Char('Name', readonly=True)
    category_id = fields.Many2one('etl.category', string='Category', readonly=True)
    frequency_id = fields.Many2one('etl.frequency', string='Frequency', readonly=True)
    last_sync_time = fields.Datetime('Last Sync Time', readonly=True)
    last_sync_status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('running', 'Running'),
    ], string='Last Sync Status', readonly=True)
    total_records_synced = fields.Integer('Total Records', readonly=True)
    new_records = fields.Integer('New Records', readonly=True)
    updated_records = fields.Integer('Updated Records', readonly=True)
    error_count = fields.Integer('Error Count', readonly=True)
    avg_sync_time = fields.Float('Avg Sync Time (min)', readonly=True)
    success_rate = fields.Float('Success Rate (%)', readonly=True)
    
    def init(self):
        """Initialize the SQL view for the dashboard"""
        tools.drop_view_if_exists(self.env.cr, 'etl_dashboard')
        self.env.cr.execute("""
        CREATE OR REPLACE VIEW etl_dashboard AS (
            WITH recent_logs AS (
                SELECT 
                    table_id,
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
                    AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60) AS avg_time_minutes,
                    SUM(new_records) AS new_records_sum,
                    SUM(updated_records) AS updated_records_sum
                FROM 
                    etl_sync_log
                WHERE 
                    start_time > (NOW() - INTERVAL '30 days')
                GROUP BY 
                    table_id
            )
            SELECT
                t.id,
                t.name,
                t.category_id,
                t.frequency_id,
                t.last_sync_time,
                t.last_sync_status,
                t.total_records_synced,
                COALESCE(l.new_records_sum, 0) AS new_records,
                COALESCE(l.updated_records_sum, 0) AS updated_records,
                COALESCE(l.failed_runs, 0) AS error_count,
                COALESCE(l.avg_time_minutes, 0) AS avg_sync_time,
                CASE 
                    WHEN COALESCE(l.total_runs, 0) > 0 
                    THEN (COALESCE(l.successful_runs, 0) * 100.0 / COALESCE(l.total_runs, 1))
                    ELSE 0
                END AS success_rate
            FROM
                etl_source_table t
            LEFT JOIN
                recent_logs l ON t.id = l.table_id
            WHERE
                t.active = true
        )
        """)
    
    def action_view_table(self):
        """Navigate to the ETL table form"""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _('ETL Table'),
            'res_model': 'etl.source.table',
            'view_mode': 'form',
            'res_id': self.id,
            'target': 'current'
        }
    
    def action_view_logs(self):
        """View logs for this table"""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _('Sync Logs'),
            'res_model': 'etl.sync.log',
            'view_mode': 'tree,form',
            'domain': [('table_id', '=', self.id)],
            'context': {'search_default_group_by_status': 1}
        }
    
    def action_run_sync(self):
        """Run sync for this table"""
        self.ensure_one()
        table = self.env['etl.source.table'].browse(self.id)
        return table.action_sync_table()


class ETLMonitoring(models.Model):
    _name = 'etl.monitoring'
    _description = 'ETL Process Monitoring'
    
    name = fields.Char('Name', compute='_compute_name')
    date = fields.Date('Date', required=True, default=fields.Date.context_today)
    total_tables = fields.Integer('Total Tables', readonly=True)
    synced_tables = fields.Integer('Synced Tables', readonly=True)
    failed_tables = fields.Integer('Failed Tables', readonly=True)
    pending_tables = fields.Integer('Pending Tables', readonly=True)
    total_records = fields.Integer('Total Records Synced', readonly=True)
    new_records = fields.Integer('New Records', readonly=True)
    updated_records = fields.Integer('Updated Records', readonly=True)
    total_time = fields.Float('Total Processing Time (min)', readonly=True)
    avg_time = fields.Float('Average Processing Time (min)', readonly=True)
    success_rate = fields.Float('Success Rate (%)', readonly=True)
    state = fields.Selection([
        ('collecting', 'Collecting Data'),
        ('done', 'Completed'),
    ], default='collecting', readonly=True)
    
    detail_ids = fields.One2many('etl.monitoring.detail', 'monitoring_id', string='Details')
    
    @api.depends('date')
    def _compute_name(self):
        for record in self:
            record.name = f"ETL Monitoring - {record.date}"
    
    @api.model
    def generate_daily_report(self):
        """Generate a daily ETL monitoring report"""
        today = fields.Date.context_today(self)
        
        # Check if a report already exists for today
        existing = self.search([('date', '=', today)], limit=1)
        if existing:
            _logger.info(f"Daily report for {today} already exists")
            return existing
        
        # Create a new report
        report = self.create({
            'date': today,
            'state': 'collecting'
        })
        
        # Collect data
        report.collect_data()
        
        return report
    
    def collect_data(self):
        """Collect monitoring data for all tables"""
        self.ensure_one()
        
        if self.state != 'collecting':
            return
        
        # Get yesterday's date for comparison
        yesterday = self.date - timedelta(days=1)
        today = self.date
        
        # Get all active tables
        tables = self.env['etl.source.table'].search([('active', '=', True)])
        
        # Initialize counters
        total_tables = len(tables)
        synced_tables = 0
        failed_tables = 0
        pending_tables = 0
        total_records = 0
        new_records = 0
        updated_records = 0
        total_time = 0
        
        # Process each table
        for table in tables:
            # Get logs for today
            logs = self.env['etl.sync.log'].search([
                ('table_id', '=', table.id),
                ('start_time', '>=', datetime.combine(today, datetime.min.time())),
                ('start_time', '<', datetime.combine(today + timedelta(days=1), datetime.min.time()))
            ], order='start_time desc')
            
            # Create detail record
            detail = self.env['etl.monitoring.detail'].create({
                'monitoring_id': self.id,
                'table_id': table.id,
                'category_id': table.category_id.id,
                'frequency_id': table.frequency_id.id,
                'last_sync_time': table.last_sync_time,
                'last_sync_status': table.last_sync_status,
                'total_records': table.total_records_synced
            })
            
            # Update counters based on logs
            if logs:
                latest_log = logs[0]
                
                if latest_log.status == 'success':
                    synced_tables += 1
                    detail.write({
                        'new_records': latest_log.new_records or 0,
                        'updated_records': latest_log.updated_records or 0,
                        'sync_time': (latest_log.end_time - latest_log.start_time).total_seconds() / 60
                                      if latest_log.end_time else 0
                    })
                    
                    # Update totals
                    new_records += latest_log.new_records or 0
                    updated_records += latest_log.updated_records or 0
                    total_records += latest_log.total_records or 0
                    
                    if latest_log.end_time and latest_log.start_time:
                        time_diff = (latest_log.end_time - latest_log.start_time).total_seconds() / 60
                        total_time += time_diff
                    
                elif latest_log.status == 'failed':
                    failed_tables += 1
                    detail.write({
                        'error_message': latest_log.error_message
                    })
                    
                else:  # running
                    pending_tables += 1
            else:
                # No logs for today
                pending_tables += 1
        
        # Calculate overall metrics
        avg_time = total_time / synced_tables if synced_tables > 0 else 0
        success_rate = (synced_tables * 100.0 / total_tables) if total_tables > 0 else 0
        
        # Update the report
        self.write({
            'total_tables': total_tables,
            'synced_tables': synced_tables,
            'failed_tables': failed_tables,
            'pending_tables': pending_tables,
            'total_records': total_records,
            'new_records': new_records,
            'updated_records': updated_records,
            'total_time': total_time,
            'avg_time': avg_time,
            'success_rate': success_rate,
            'state': 'done'
        })
        
        _logger.info(f"Completed daily ETL monitoring report for {self.date}")
        return True
    
    def action_refresh(self):
        """Refresh the monitoring data"""
        self.ensure_one()
        
        # Reset state to collecting
        self.write({'state': 'collecting'})
        
        # Clear existing details
        self.detail_ids.unlink()
        
        # Collect data again
        return self.collect_data()


class ETLMonitoringDetail(models.Model):
    _name = 'etl.monitoring.detail'
    _description = 'ETL Monitoring Detail'
    
    monitoring_id = fields.Many2one('etl.monitoring', string='Monitoring', required=True, ondelete='cascade')
    table_id = fields.Many2one('etl.source.table', string='Table', required=True)
    category_id = fields.Many2one('etl.category', string='Category', readonly=True)
    frequency_id = fields.Many2one('etl.frequency', string='Frequency', readonly=True)
    last_sync_time = fields.Datetime('Last Sync Time', readonly=True)
    last_sync_status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('running', 'Running'),
    ], string='Last Sync Status', readonly=True)
    total_records = fields.Integer('Total Records', readonly=True)
    new_records = fields.Integer('New Records', readonly=True)
    updated_records = fields.Integer('Updated Records', readonly=True)
    sync_time = fields.Float('Sync Time (min)', readonly=True)
    error_message = fields.Text('Error Message', readonly=True)
    
    def action_view_table(self):
        """Navigate to the ETL table form"""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _('ETL Table'),
            'res_model': 'etl.source.table',
            'view_mode': 'form',
            'res_id': self.table_id.id,
            'target': 'current'
        }
