from odoo import models, fields, api
from odoo.exceptions import AccessError
from dateutil.relativedelta import relativedelta
from odoo.exceptions import UserError, ValidationError
from datetime import timedelta, datetime, time
import re
from dotenv import load_dotenv
import os
import json
from concurrent.futures import ThreadPoolExecutor
import hashlib
import requests
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
from odoo.modules.module import get_module_resource
import base64
import logging


load_dotenv()
_logger = logging.getLogger(__name__)


class KeywordTracking(models.Model):
    _name = 'keyword.tracking'  # Model name, customize as needed
    _description = 'Keyword'

    name = fields.Char(string='Name', required=True)
    code = fields.Char(string='Code', required=True)
    risk_level = fields.Selection([
        ('low', 'Low'),
        ('medium', 'Medium'),
        ('high', 'High'),
    ], string='Risk Level', default='high', tracking=True)
    assigned_officers = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        'keyword_tracking_officers',
        string="Officer(s) To Alert",
        tracking=True,
    )


class KeywordAlertLog(models.Model):
    _name = 'keyword.alert.log'
    _description = 'Keyword Alert Logs'
    _order = 'create_date desc'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    # this should be related to the assigned_officers is keywords
    # user_id = fields.Many2one('res.users', string='Officers Alerted')
    name = fields.Char(string='Alert Reference', required=True, copy=False,
                       readonly=True, default=lambda self: 'New')
    status = fields.Selection([
        ('sent', 'Sent'),
        ('read', 'Acknowledge')
    ], string='Status', default='sent')
    # error_message = fields.Text(string='Error Message', readonly=True)
    create_date = fields.Datetime(string='Alert Date', readonly=True)

    officers_alerted = fields.Many2many('res.users', string='Officers Alerted',
                                        compute='_compute_officers_alerted', store=True, ondelete='cascade')

    keyword_id = fields.Many2many(
        'keyword.tracking', string='Keywords for Monitoring', required=True,
        tracking=True, index=True,  ondelete='cascade',      default=lambda self: self._default_keywords())
    
    document_id = fields.Many2one(
        'rulebook.title', string='Document', store=True, ondelete='cascade')  # this should hold the value of the document that was proccessed
    
    risk_level = fields.Char(
         string='Risk Level', store=True)
    
    active = fields.Boolean(default=True)
    
    match_text = fields.Text(string='Matched Context',
                             help="Text surrounding the matched keyword")

    
    @api.depends('keyword_id')
    def _compute_officers_alerted(self):
        for record in self:
            if record.keyword_id and record.keyword_id.assigned_officers:
                record.officers_alerted = [
                    (6, 0, record.keyword_id.assigned_officers.ids)]
            else:
                record.officers_alerted = [(5, 0, 0)]
                
    def _default_keywords(self):
        """Return all records from keyword tracking as default value."""
        return self.env['keyword.tracking'].search([]).ids
    
    def action_mark_as_read(self):
        self.ensure_one()
        self.write({'status': 'read'})    


    @api.model
    def create(self, vals):
        if vals.get('name', 'New') == 'New':
            vals['name'] = self.env['ir.sequence'].next_by_code(
                'keyword.alert.log')
            if not vals['name']:
                raise ValueError(
                    "Sequence 'keyword.alert.log' is not configured correctly.")
        return super(KeywordAlertLog, self).create(vals)
    
    

