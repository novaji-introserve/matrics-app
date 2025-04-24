# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class Branch(models.Model):
    _name = 'res.branch'
    _description = 'Branch'
    _sql_constraints = [
        ('uniq_branch_name', 'unique(name)',
         "Branch Name already exists. Name must be unique!"),
    ]
    
    name = fields.Char(string="Branch")
    code = fields.Char(string="Code")
    co_code = fields.Char(string="Co-Code")
    users = fields.Many2many(
        'res.users', 'res_branch_users_rel', 'branch_id', 'user_id', required=False)
    region = fields.Char(string="Region", required=False)
    zone = fields.Char(string="Zone", required=False)
    address = fields.Char(string="Branch Address", required=False)
    state_located = fields.Char(string="State Located", required=False)
    town_area = fields.Char(string="Area",  required=False)
    
