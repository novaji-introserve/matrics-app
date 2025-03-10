# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class Branch(models.Model):
    _name = 'res.branch'
    _description = 'Branch'
    _sql_constraints = [
        ('uniq_branch_code', 'unique(code)',
         "Branch code already exists. Code must be unique!"),
    ]
    
    name = fields.Char(string="Branch")
    code = fields.Char(string="Code")
    users = fields.Many2many('res.users', 'res_branch_users_rel', 'branch_id', 'user_id')
