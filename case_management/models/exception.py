import logging
import csv
import os
from odoo import models, api, tools, fields

_logger = logging.getLogger(__name__)



class ExceptionCategory(models.Model):
    _name = 'exception.category'
    _description = 'Exception Category'

    name = fields.Char(string='Name', size=50, required=False)
    description = fields.Char(string='Description', size=50, required=False)
    code = fields.Char(string='Code', size=20, required=False)
    created_at = fields.Datetime(string='Created At', required=False)



# models/exception_models.py
from odoo import models, fields, api

class ExceptionProcessType(models.Model):
    _name = 'exception.process.type'
    _description = 'Exception Process Type'

    num_id = fields.Integer(string='ID', required=True)
    name = fields.Char(string='Name', required=True)
    
    def name_get(self):
        return [(record.id, f"{record.name}") for record in self]
    
    
class ExceptionProcess(models.Model):
    _name = 'exception.process'
    _description = 'Exception Process'
    
    name = fields.Char(string='Name', required=True)
    type_id = fields.Many2one('exception.process.type', string='Process Type', required=True)
    
    # This computed field is unnecessary if type_id is already a Many2one
    # If you need it for specific reasons, here's a safer implementation:
    process_type_id = fields.Many2one('exception.process.type', string='Process Type',
                                      compute='_compute_process_type_id', store=True)
    
    @api.depends('type_id')
    def _compute_process_type_id(self):
        for record in self:
            record.process_type_id = record.type_id.id if record.type_id else False
    
    def name_get(self):
        return [(record.id, f"{record.name}") for record in self]






class ComplianceRiskRating(models.Model):
    _name = 'compliance.risk.rating'
    _description = 'Compliance Risk Rating'

    name = fields.Selection([
        ('1', 'Low'),
        ('2', 'Medium'),
        ('3', 'High')
    ], string='Risk Level', required=True)
    
    description = fields.Char(string='Description', compute='_compute_description', store=True)
    
    @api.depends('name')
    def _compute_description(self):
        for record in self:
            risk_mapping = {'1': 'Low', '2': 'Medium', '3': 'High'}
            record.description = risk_mapping.get(record.name, '')




