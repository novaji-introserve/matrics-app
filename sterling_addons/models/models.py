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
    _sql_constraints = [
        ('uniq_customer_id', 'unique(customer_id)',
         "Customer already exists. Customer must be unique!"),
    ]
        
    onebank = fields.Char(string='Uses One Bank', index=True, readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)


class CustomerDigitalProductMaterializedExtension(models.Model):
    _inherit = 'customer.digital.product.mat'
    
    onebank = fields.Char(string='Uses One Bank', readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)
