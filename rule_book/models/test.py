from odoo import models, fields, api
import logging
from __future__ import print_function


_logger = logging.getLogger(__name__)

def today(self):
    today = fields.Date.context_today(self)
    return print(today)
    
today()
