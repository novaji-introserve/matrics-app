# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class KYCLimit(models.Model):
    _name = 'res.partner.kyc.limit'
    _description = 'KYC Limit'
    _sql_constraints = [
        ('uniq_kyc_limit_code', 'unique(code)',
         "KYC Limit code already exists. Code must be unique!"),
    ]
    
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
