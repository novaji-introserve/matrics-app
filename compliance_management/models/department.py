# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class Branch(models.Model):
    _name = 'res.department'
    _description = 'Department'
    _sql_constraints = [
        ('uniq_branch_code', 'unique(code)',
         "Branch code already exists. Code must be unique!"),
    ]

    name = fields.Char(string="Branch", required=True)
    code = fields.Char(string="Code", required=True)
    users = fields.Many2many('res.users', 'res_branch_users_rel', 'branch_id', 'user_id')
