from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError


class SqlModel(models.Model):
    _name = 'process.sql'
    _description = "sql query for process"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    query = fields.Char(string="Query", required=True)
    name = fields.Char(string="Name",required=True)