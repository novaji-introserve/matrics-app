# -*- coding: utf-8 -*-

from odoo import models, fields, api
import logging

_logger = logging.getLogger(__name__)


class FdwSyncWatermark(models.Model):
    """Stores last sync timestamp for each FDW-synced table."""
    _name = 'fdw.sync.watermark'
    _description = 'FDW Sync Watermark'
    _rec_name = 'table_name'

    table_name = fields.Char(
        string='Table Name',
        required=True,
        index=True,
        help='Name of the table being synced (e.g., ActiveTransaction)'
    )
    
    last_sync_time = fields.Datetime(
        string='Last Sync Time',
        required=True,
        help='Timestamp of the last successful sync'
    )
    
    last_sync_rows = fields.Integer(
        string='Last Sync Rows',
        help='Number of rows synced in the last run'
    )
    
    updated_at = fields.Datetime(
        string='Updated At',
        default=fields.Datetime.now,
        help='When this record was last updated'
    )

    _sql_constraints = [
        ('unique_table_name', 'unique(table_name)',
         'Each table can only have one watermark record!'),
    ]

    @api.model
    def get_watermark(self, table_name):
        """Get watermark for a table, return None if not found."""
        watermark = self.search([('table_name', '=', table_name)], limit=1)
        return watermark.last_sync_time if watermark else None

    @api.model
    def update_watermark(self, table_name, sync_time, rows_synced=None):
        """Update or create watermark for a table."""
        watermark = self.search([('table_name', '=', table_name)], limit=1)
        vals = {
            'last_sync_time': sync_time,
            'updated_at': fields.Datetime.now,
        }
        if rows_synced is not None:
            vals['last_sync_rows'] = rows_synced
            
        if watermark:
            watermark.write(vals)
        else:
            vals['table_name'] = table_name
            self.create(vals)
        
        return True

