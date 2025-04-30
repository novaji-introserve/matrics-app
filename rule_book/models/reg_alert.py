import logging
from odoo import models, fields, api
from odoo.exceptions import AccessError
from dateutil.relativedelta import relativedelta
from odoo.exceptions import UserError, ValidationError
from datetime import timedelta, datetime, time
import re
from dotenv import load_dotenv



class RegulatoryAlert(models.Model):
    _name = 'regulatory.alert'
    _description = 'Regulatory Alert'
    _order = 'id desc'
    _rec_name = "create_date"

    _inherit = ['mail.thread', 'mail.activity.mixin']

    alert_officers = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        'reg_alert_officers',
        string="Officer(s) Responsible",
        tracking=True,
    )
    active = fields.Boolean(default=True, tracking=True)
