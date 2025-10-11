import os
import logging
import re
from datetime import datetime
from odoo import models, fields, api, http
from odoo.exceptions import AccessError, UserError
from odoo.tools.config import config  

_logger = logging.getLogger(__name__)

class CustomerDigitalProductExtend(models.Model):
    _inherit  = 'customer.digital.product'
        
    ifuel = fields.Char(string='Ifuel', readonly=True)
    altpro = fields.Char(string='AltPro', readonly=True)
    altmall = fields.Char(string='AltMall', readonly=True)
    altinvest = fields.Char(string='AltInvest', readonly=True)
    altpower = fields.Char(string='AltPower', readonly=True)
    altdrive = fields.Char(string='AltDrive', readonly=True)
    chequebook = fields.Char(string='ChequeBook', readonly=True)