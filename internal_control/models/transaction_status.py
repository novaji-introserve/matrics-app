# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class TransactionStatus(models.Model):
    _name = 'res.transaction.status'
    _description = 'Transaction Status'

    name = fields.Char(string="Transaction Status", readonly=True, index=True)
    code = fields.Char(string="Status Code", readonly=True, index=True, unique=True)
