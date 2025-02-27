from odoo import models, fields

class GLClassLV3(models.Model):
    _name = 'res.glclasslv3'
    _description = 'GL Class Level 3'

    gl_classcode = fields.Char(string='GL Class Code', required=True, size=10)
    gl_classname = fields.Char(string='GL Class Name', required=True, size=100)
    nodecode = fields.Char(string='Node Code', required=True, size=50)
    status = fields.Integer(string='Status')
    userid = fields.Char(string='User ID', size=50)
    authid = fields.Char(string='Authorization ID', size=50)
    createdate = fields.Datetime(string='Create Date')
    lastnumber = fields.Integer(string='Last Number')

    _sql_constraints = [
        ('glclasslv3_gl_classcode_unique', 'unique(gl_classcode)', 'GL_classCode must be unique!')
    ]