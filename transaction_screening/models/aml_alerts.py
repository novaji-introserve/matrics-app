# -*- coding: utf-8 -*-
from odoo import api, fields, models

ALERT_STATE = [
    ('open', 'Open'),
    ('reviewed', 'Under Review'),
    ('escalated', 'Escalated'),
    ('closed', 'Closed'),
    ('false_positive', 'False Positive'),
]


class AMLVelocityAlert(models.Model):
    _name = 'res.aml.velocity.alert'
    _description = 'AML Velocity Alert'
    _order = 'created_at desc'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Alert Ref', readonly=True, copy=False, default='New')
    transaction_id = fields.Many2one(
        'res.customer.transaction', string='Transaction',
        required=True, ondelete='cascade', index=True,
    )
    customer_id = fields.Many2one('res.partner', string='Customer', required=True, index=True)
    window_hours = fields.Integer(string='Window (hours)')
    txn_count = fields.Integer(string='Transactions in Window')
    total_amount = fields.Float(string='Total Amount in Window', digits=(20, 2))
    risk_score = fields.Float(string='Risk Score', digits=(5, 2))
    state = fields.Selection(ALERT_STATE, default='open', required=True, tracking=True)
    created_at = fields.Datetime(string='Created', default=fields.Datetime.now, readonly=True)
    notes = fields.Text(string='Review Notes')

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('name', 'New') == 'New':
                vals['name'] = self.env['ir.sequence'].next_by_code('res.aml.velocity.alert') or 'VAL/NEW'
        return super().create(vals_list)


class AMLStructuringAlert(models.Model):
    _name = 'res.aml.structuring.alert'
    _description = 'AML Structuring / Smurfing Alert'
    _order = 'created_at desc'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Alert Ref', readonly=True, copy=False, default='New')
    transaction_id = fields.Many2one(
        'res.customer.transaction', string='Triggering Transaction',
        required=True, ondelete='cascade', index=True,
    )
    customer_id = fields.Many2one('res.partner', string='Customer', required=True, index=True)
    window_hours = fields.Integer(string='Window (hours)')
    txn_count = fields.Integer(string='Sub-Threshold Transactions in Window')
    total_amount = fields.Float(string='Total Amount in Window', digits=(20, 2))
    ctr_threshold = fields.Float(string='CTR Threshold at Detection', digits=(20, 2))
    risk_score = fields.Float(string='Risk Score', digits=(5, 2))
    state = fields.Selection(ALERT_STATE, default='open', required=True, tracking=True)
    created_at = fields.Datetime(string='Created', default=fields.Datetime.now, readonly=True)
    notes = fields.Text(string='Review Notes')

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('name', 'New') == 'New':
                vals['name'] = self.env['ir.sequence'].next_by_code('res.aml.structuring.alert') or 'SAL/NEW'
        return super().create(vals_list)


class AMLAnomalyAlert(models.Model):
    _name = 'res.aml.anomaly.alert'
    _description = 'AML Statistical Anomaly Alert'
    _order = 'created_at desc'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Alert Ref', readonly=True, copy=False, default='New')
    transaction_id = fields.Many2one(
        'res.customer.transaction', string='Transaction',
        required=True, ondelete='cascade', index=True,
    )
    customer_id = fields.Many2one('res.partner', string='Customer', required=True, index=True)
    transaction_amount = fields.Float(string='Transaction Amount', digits=(20, 2))
    customer_mean = fields.Float(string='Customer Mean Amount', digits=(20, 2))
    customer_stddev = fields.Float(string='Customer Std Dev', digits=(20, 2))
    zscore = fields.Float(string='Z-Score', digits=(10, 4))
    risk_score = fields.Float(string='Risk Score', digits=(5, 2))
    state = fields.Selection(ALERT_STATE, default='open', required=True, tracking=True)
    created_at = fields.Datetime(string='Created', default=fields.Datetime.now, readonly=True)
    notes = fields.Text(string='Review Notes')

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('name', 'New') == 'New':
                vals['name'] = self.env['ir.sequence'].next_by_code('res.aml.anomaly.alert') or 'AAL/NEW'
        return super().create(vals_list)
