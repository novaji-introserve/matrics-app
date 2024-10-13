# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class IdentificationType(models.Model):
    _name = 'res.identification.type'
    _description = 'Identification Type'
    _sql_constraints = [
        ('uniq_identification_type_code', 'unique(code)',
         "Identification type code already exists. Code must be unique!"),
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
