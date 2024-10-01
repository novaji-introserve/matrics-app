from odoo import models, fields, api

# Exception Process Model
class ExceptionProcess(models.Model):
    _name = 'rulebook.exception_process'
    _description = 'Exception Process'
    _rec_name = 'name'
    
    name = fields.Char(string='Name', required=True, tracking=True)
    exception_type_id = fields.Many2one('rulebook.exception_type', string='Exception Type', required=True, tracking=True)
    expectation = fields.Text(string='Expectation', tracking=True)
    sanction = fields.Text(string='Sanction', tracking=True)