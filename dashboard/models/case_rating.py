# -*- coding: utf-8 -*-

from odoo import models, fields, api

class CaseRating(models.Model):
    _name = 'case.rating'
    _description = 'case rating'
    _rec_name = 'name'
    
    name = fields.Char(string = "Name", required=True)
    ref = fields.Char(string = "ref", required=True)
    
    _sql_constraints = [
        ('ref_unique', 'UNIQUE(ref)', 'the ref must be unique'),
    ]