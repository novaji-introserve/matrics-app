# -*- coding: utf-8 -*-

from odoo import models, fields, api


class CaseStatus(models.Model):
    _name = 'case.status'
    _description = 'case status'
    _rec_name = 'name'
    
    name = fields.Char(string = "Name", required=True)
    description = fields.Text(string = "Description", required=True)
    slug = fields.Char(string = "Slug", required=True)
    created_at = fields.Datetime(string="Created At", default=fields.Datetime.now())
    updated_at = fields.Datetime(string="Updated At", default=fields.Datetime.now())
    
    _sql_constraints = [
        ('slug_unique', 'UNIQUE(slug)', 'the slug must be unique'),
    ]
    
    def write(self, data):
        data['updated_at'] = fields.Datetime.now(())
        return super(CaseStatus, self).write(data)
    
