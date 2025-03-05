
# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import json
import logging
import math
_logger = logging.getLogger(__name__)

class ETLDatabaseConnection(models.Model):
    _name = 'etl.database.connection'
    _description = 'ETL Database Connection'

    name = fields.Char('Connection Name', required=True)
    connection_type = fields.Selection([
        ('mssql', 'MS SQL Server'),
        ('postgres', 'PostgreSQL')
    ], string='Database Type', required=True)
    connection_string = fields.Text('Connection String', required=True)
    is_active = fields.Boolean('Active', default=True)
    batch_size = fields.Integer('Batch Size', default=2000)
