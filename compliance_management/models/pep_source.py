# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
import logging
import threading
import uuid
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class PEPSource(models.Model):
    _name = 'pep.source'
    _description = 'PEP Source'
    
    name = fields.Char('Source Name', required=True)
    domain = fields.Char('Domain/URL', required=True)
    source_type = fields.Selection([
        ('government', 'Government List'),
        ('regulatory', 'Regulatory Body'),
        ('commercial', 'Commercial Database'),
        ('other', 'Other')
    ], string='Source Type', required=True)
    active = fields.Boolean('Active', default=True)
    description = fields.Text('Description')
    last_update = fields.Datetime('Last Updated')
    country_id = fields.Many2one('res.country', string='Country')
    api_key = fields.Char('API Key')
    is_free = fields.Boolean('Is Free', default=True)
    frequency = fields.Selection([
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('quarterly', 'Quarterly')
    ], string='Update Frequency', default='monthly')

    def action_create_import_job(self):
        """
        Create an import job for this source
        """
        self.ensure_one()
        
        try:
            # Determine appropriate job type based on source configuration
            if self.source_format == 'both' and self.use_api and self.api_key:
                job_type = 'both'
            elif self.source_format == 'api' and self.use_api and self.api_key:
                job_type = 'api'
            else:
                job_type = 'csv'
                
            # Create the job
            job_queue = self.env['opensanctions.job.queue']
            job = job_queue.create({
                'source_id': self.id,
                # Name and job_type will be auto-filled by onchange
                'state': 'pending',
                'priority': 15 if self.is_opensanctions else 10,
                'batch_size': 500,  # Default batch size
                'api_limit': 1000  # Default API limit
            })
            
            # Show the created job
            return {
                'name': _('Import Job'),
                'view_mode': 'form',
                'res_model': 'opensanctions.job.queue',
                'res_id': job.id,
                'type': 'ir.actions.act_window',
                'target': 'current',
            }
            
        except Exception as e:
            _logger.error(f"Error creating import job: {str(e)}")
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Job Creation Failed"),
                    'message': _(f"Error: {str(e)}"),
                    'sticky': False,
                    'type': 'error',
                }
            }
            
    def action_view_jobs(self):
        """
        View jobs for this source
        """
        self.ensure_one()
        
        action = self.env.ref('compliance_management.action_opensanctions_job_queue').read()[0]
        action.update({
            'domain': [('source_id', '=', self.id)],
            'context': {'default_source_id': self.id},
            'name': f'Jobs for {self.name}'
        })
        
        return action