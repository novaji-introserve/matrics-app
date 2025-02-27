from odoo import models, fields

class GLClassLV2(models.Model):
    _name = 'res.glclasslv2'
    _description = 'GL Class Level 2'

    gl_nodecode = fields.Char(string='GL Node Code', required=True, size=10)
    gl_nodename = fields.Char(string='GL Node Name', required=True, size=100)
    prodcode = fields.Char(string='Product Code', required=True, size=50)
    status = fields.Integer(string='Status')
    userid = fields.Char(string='User ID', size=50)
    authid = fields.Char(string='Authorization ID', size=50)
    createdate = fields.Datetime(string='Create Date')

    
    _sql_constraints = [
        ('glclasslv2_gl_nodecode_unique', 'unique(gl_nodecode)', 'GL_NodeCode must be unique!')
    ]