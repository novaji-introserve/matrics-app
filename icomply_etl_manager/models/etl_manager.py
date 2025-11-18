# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError
import logging
from datetime import datetime, timedelta

# Simple queue job support
try:
    from odoo.addons.queue_job.job import Job
    QUEUE_JOB_AVAILABLE = True
except ImportError:
    QUEUE_JOB_AVAILABLE = False

_logger = logging.getLogger(__name__)

class ETLManager(models.AbstractModel):
    _name = 'etl.manager'
    _description = 'ETL Manager - Simplified'

    @api.model
    def run_scheduled_sync(self, frequency_code='daily'):
        """Run scheduled synchronization"""
        
        try:
            _logger.info(f"Starting scheduled sync for frequency: {frequency_code}")
            
            # Get tables that need sync for this frequency
            frequency = self.env['etl.frequency'].search([('code', '=', frequency_code)], limit=1)
            if not frequency:
                _logger.error(f"ETL Frequency '{frequency_code}' not found")
                return {'error': f"Frequency '{frequency_code}' not found"}
            
            # Find tables with this frequency and full sync enabled
            tables = self.env['etl.source.table'].search([
                ('full_sync_frequency_id', '=', frequency.id),
                ('full_sync_enabled', '=', True),
                ('active', '=', True)
            ])
            
            _logger.info(f"Found {len(tables)} tables for {frequency_code} sync")
            
            # Process each table
            queued_jobs = 0
            
            for table in tables:
                try:
                    # Get immediate sync threshold
                    threshold = int(self.env['ir.config_parameter'].sudo().get_param(
                        'etl.immediate_sync_threshold', '1000'))
                    
                    record_count = table.estimated_record_count or 0
                    
                    if record_count <= threshold and record_count > 0:
                        # Sync immediately
                        processor = self.env['etl.processor']
                        processor.process_table_full_sync(table)
                        _logger.info(f"Completed immediate sync for {table.name}")
                    else:
                        # Queue the job
                        if QUEUE_JOB_AVAILABLE:
                            job = table.with_delay(
                                description=f"ETL Full Sync: {table.name} (Scheduled)"
                            ).sync_table_job('full')
                            
                            table.write({
                                'job_uuid': job.uuid,
                                'job_status': 'pending',
                                'last_sync_message': 'Scheduled sync queued'
                            })
                            
                            queued_jobs += 1
                            _logger.info(f"Queued sync for {table.name}")
                        else:
                            # Direct processing if no queue_job
                            processor = self.env['etl.processor']
                            processor.process_table_full_sync(table)
                            _logger.info(f"Completed direct sync for {table.name}")
                    
                except Exception as e:
                    _logger.error(f"Failed to process {table.name}: {str(e)}")
                    table.write({
                        'job_status': 'failed',
                        'last_sync_status': 'failed',
                        'last_sync_message': f'Scheduled sync failed: {str(e)}'
                    })
            
            result = {
                'frequency': frequency_code,
                'tables_found': len(tables),
                'jobs_queued': queued_jobs,
                'queue_job_used': QUEUE_JOB_AVAILABLE
            }
            
            _logger.info(f"Scheduled sync completed: {result}")
            return result
            
        except Exception as e:
            _logger.error(f"Scheduled sync failed: {str(e)}")
            raise

    @api.model
    def run_incremental_sync(self):
        """Run incremental synchronization"""
        
        try:
            _logger.info("Starting incremental sync batch")
            
            # Find tables that need incremental sync
            current_time = fields.Datetime.now()
            tables_to_sync = []
            
            incremental_tables = self.env['etl.source.table'].search([
                ('incremental_sync_enabled', '=', True),
                ('active', '=', True)
            ])
            
            for table in incremental_tables:
                if self._should_run_incremental_sync(table, current_time):
                    tables_to_sync.append(table)
            
            _logger.info(f"Found {len(tables_to_sync)} tables needing incremental sync")
            
            # Process each table
            queued_count = 0
            
            for table in tables_to_sync:
                try:
                    # Get immediate sync threshold
                    threshold = int(self.env['ir.config_parameter'].sudo().get_param(
                        'etl.immediate_sync_threshold', '1000'))
                    
                    record_count = table.estimated_record_count or 0
                    
                    if record_count <= threshold:
                        # Sync immediately
                        processor = self.env['etl.processor']
                        processor.process_table_incremental_sync(table)
                        _logger.info(f"Completed immediate incremental sync for {table.name}")
                    else:
                        # Queue the job
                        if QUEUE_JOB_AVAILABLE:
                            job = table.with_delay(
                                description=f"ETL Incremental Sync: {table.name}"
                            ).sync_table_job('incremental')
                            
                            table.write({
                                'job_uuid': job.uuid,
                                'job_status': 'pending',
                                'last_sync_message': 'Incremental sync queued'
                            })
                            
                            queued_count += 1
                            _logger.info(f"Queued incremental sync for {table.name}")
                        else:
                            # Direct processing if no queue_job
                            processor = self.env['etl.processor']
                            processor.process_table_incremental_sync(table)
                            _logger.info(f"Completed direct incremental sync for {table.name}")
                    
                except Exception as e:
                    _logger.error(f"Failed incremental sync for {table.name}: {str(e)}")
            
            result = {
                'tables_checked': len(incremental_tables),
                'tables_needing_sync': len(tables_to_sync),
                'tables_queued': queued_count,
                'queue_job_used': QUEUE_JOB_AVAILABLE
            }
            
            _logger.info(f"Incremental sync batch completed: {result}")
            return result
            
        except Exception as e:
            _logger.error(f"Incremental sync batch failed: {str(e)}")
            raise

    def _should_run_incremental_sync(self, table, current_time):
        """Check if table should run incremental sync"""
        try:
            if not table.incremental_sync_enabled:
                return False
            
            if not table.incremental_frequency_minutes:
                return False
            
            if not table.last_incremental_sync:
                return True  # Never synced
            
            # Check if enough time has passed
            next_sync_time = table.last_incremental_sync + timedelta(minutes=table.incremental_frequency_minutes)
            return current_time >= next_sync_time
        except Exception as e:
            _logger.error(f"Error checking incremental sync for {table.name}: {str(e)}")
            return False

    @api.model
    def get_system_status(self):
        """Get basic ETL system status"""
        
        try:
            # Get table statistics
            tables = self.env['etl.source.table'].search([('active', '=', True)])
            table_stats = {
                'total_tables': len(tables),
                'full_sync_enabled': len(tables.filtered('full_sync_enabled')),
                'incremental_sync_enabled': len(tables.filtered('incremental_sync_enabled')),
                'success_rate': self._calculate_success_rate(tables),
                'failed_tables': len(tables.filtered(lambda t: t.last_sync_status == 'failed')),
                'running_tables': len(tables.filtered(lambda t: t.last_sync_status == 'running')),
            }
            
            # Get connection statistics
            connections = self.env['etl.database.connection'].search([('active', '=', True)])
            connection_stats = {
                'total_connections': len(connections),
                'healthy_connections': len(connections.filtered(lambda c: c.last_test_status == 'success')),
                'failed_connections': len(connections.filtered(lambda c: c.last_test_status == 'failed')),
                'never_tested': len(connections.filtered(lambda c: c.last_test_status == 'never')),
            }
            
            return {
                'table_stats': table_stats,
                'connection_stats': connection_stats,
                'queue_job_available': QUEUE_JOB_AVAILABLE,
                'last_updated': fields.Datetime.now(),
            }
            
        except Exception as e:
            _logger.error(f"Failed to get system status: {str(e)}")
            return {
                'error': str(e),
                'last_updated': fields.Datetime.now(),
            }

    def _calculate_success_rate(self, tables):
        """Calculate overall success rate for tables"""
        if not tables:
            return 0
        
        successful_tables = tables.filtered(lambda t: t.last_sync_status == 'success')
        return (len(successful_tables) / len(tables)) * 100

    @api.model
    def test_all_connections(self):
        """Test all database connections"""
        
        try:
            connections = self.env['etl.database.connection'].search([('active', '=', True)])
            
            results = {
                'total_tested': len(connections),
                'successful': 0,
                'failed': 0,
                'details': []
            }
            
            for connection in connections:
                try:
                    test_result = connection.action_test_connection()
                    success = test_result.get('params', {}).get('type') == 'success'
                    
                    if success:
                        results['successful'] += 1
                    else:
                        results['failed'] += 1
                    
                    results['details'].append({
                        'connection': connection.name,
                        'success': success,
                        'message': test_result.get('params', {}).get('message', 'Unknown result')
                    })
                    
                except Exception as e:
                    results['failed'] += 1
                    results['details'].append({
                        'connection': connection.name,
                        'success': False,
                        'message': str(e)
                    })
            
            _logger.info(f"Connection test completed: {results['successful']}/{results['total_tested']} successful")
            return results
            
        except Exception as e:
            _logger.error(f"Connection test failed: {str(e)}")
            raise

    @api.model
    def cleanup_old_logs(self, days=30):
        """Clean up old sync logs"""
        
        try:
            cutoff_date = fields.Datetime.now() - timedelta(days=days)
            
            old_logs = self.env['etl.sync.log'].search([
                ('create_date', '<', cutoff_date)
            ])
            
            count = len(old_logs)
            old_logs.unlink()
            
            _logger.info(f"Cleaned up {count} old sync logs (older than {days} days)")
            
            return {
                'logs_cleaned': count
            }
            
        except Exception as e:
            _logger.error(f"Failed to cleanup old logs: {str(e)}")
            raise