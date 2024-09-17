from odoo import models, fields, api
# Exception Type Model
class ExceptionType(models.Model):
    _name = 'rulebook.exception_type'
    _description = 'Exception Type'
    _rec_name = 'name'
    
    name = fields.Char(string='Name', required=True, tracking=True)