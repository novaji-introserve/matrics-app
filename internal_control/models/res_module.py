from odoo import models, fields

class Module(models.Model):
    _name = 'res.module'
    _description = 'Res Module'

    moduledesc = fields.Char(string='Module Description', required=True, size=100)
    modulecode = fields.Char(string='Module Code', required=True, size=10)
    status = fields.Integer(string='Status')
    authid = fields.Char(string='Authorization ID', size=50)
    userid = fields.Char(string='User ID', size=50)

    _sql_constraints = [
        ('module_desc_unique', 'unique(moduledesc)', 'Module Description must be unique!')
    ]