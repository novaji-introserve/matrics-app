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
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Name', required=True,tracking=True)
    code = fields.Char(string='Code', required=True, tracking=True)
    risk_level = fields.Selection([
        ('low', 'Low'),
        ('medium', 'Medium'),
        ('high', 'High'),
    ], string='Risk Level', default='high', tracking=True)
    assigned_officers = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        'keyword_tracking_officers',
        string="Officer(s) Responsible",
        tracking=True,
    )
    active = fields.Boolean(default=True, tracking=True)
    _sql_constraints = [
        ('name_uniq', 'unique (name)',
         "Keyword Name must be unique!"),
    ]


class KeywordAlertLog(models.Model):
    _name = 'keyword.alert.log'
    _description = 'Keyword Alert Logs'
    _order = 'id desc'
    _rec_name = "create_date"

    _inherit = ['mail.thread', 'mail.activity.mixin']

    # this should be related to the assigned_officers is keywords
    # user_id = fields.Many2one('res.users', string='Officers Alerted')
    name = fields.Char(string='Alert Reference', required=True, copy=False,
                       readonly=True, default=lambda self: 'New')
    status = fields.Selection([
        ('sent', 'Pending'),
        ('read', 'Seen')
    ], string='Status', default='sent', tracking=True,)
    # error_message = fields.Text(string='Error Message', readonly=True)
    create_date = fields.Datetime(string='Alert Date', readonly=True,        default=datetime.now().replace(microsecond=0),
                                  )
    alert_date = fields.Datetime(string='Alert Date', readonly=True,        default=datetime.now().replace(microsecond=0), tracking=True,
                                  )

    officers_alerted = fields.Many2many('res.users', string='Officers Responsible',
                                        compute='_compute_officers_alerted', store=True, ondelete='cascade', readonly=True)

    keyword_id = fields.Many2many(
        'keyword.tracking', string='Keyword(s) Found', required=True,
        tracking=True, index=True,  ondelete='cascade',      default=lambda self: self._default_keywords(), readonly=True, )

    document_id = fields.Many2one(
        'rulebook.title', string='Document', store=True, ondelete='cascade', readonly=True,tracking=True,)  # this should hold the value of the document that was proccessed

    risk_level = fields.Char(
        string='Risk Level', store=True, readonly=True)

    active = fields.Boolean(default=True, tracking=True,)

    match_text = fields.Text(string='Matched Context',
                             help="Text surrounding the matched keyword", readonly=True)
    ai_analysis = fields.Text(string='AI Analysis',
                              help="AI Analysis", readonly=True, tracking=True)
    formatted_alert_date = fields.Char(
        string="Create Date",
        compute="_compute_formatted_alert_date",
        # store=True
    )

    @api.depends("alert_date")
    def _compute_formatted_alert_date(self):
        for record in self:
            if record.alert_date:
                # Format the date as desired

                record.formatted_alert_date = self.env["reply.log"]._compute_formatted_date(
                    record.alert_date)

            else:
                record.formatted_alert_date = "N/A"

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
