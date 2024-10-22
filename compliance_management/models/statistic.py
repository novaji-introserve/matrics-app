# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class Statistic(models.Model):
    _name = 'res.compliance.stat'
    _description = 'Compliance Statistics'
    _sql_constraints = [
        ('uniq_stats_code', 'unique(code)',
         "Stats code already exists. Value must be unique!"),
        ('uniq_stats_name', 'unique(name)',
         "Name already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    sql_query = fields.Text(string='SQL Query', required=True)
    scope = fields.Selection(string='Scope', selection=[(
        'bank', 'Bank Wide'), ('branch', 'Branch'), ('compliance', 'Compliance'),
        ('regulatory', 'Regulatory'),('risk','Risk Assessment')], default='bank')
    state = fields.Selection(string='State', selection=[(
        'active', 'Active'), ('key', 'Inactive')], default='active')
    val = fields.Float(string='Value', required=True, default=0.0)
    narration = fields.Text(string='Narration')
    
    def compute_stat(self):
        query = self.sql_query.lower()
        if 'delete' in query:
            pass
        elif 'update' in query:
            pass
        else:
            self.env.cr.execute(self.sql_query)
            rec = self.env.cr.fetchone()
            result = rec[0] if rec is not None else 0.0
            self.write({"val":result})
