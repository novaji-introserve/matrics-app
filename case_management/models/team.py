from odoo import models, fields

class Team(models.Model):
    _name = 'team'
    _description = 'Team'

    name = fields.Char(string='Name', size=500, required=False)
    email = fields.Char(string='Email', size=50, required=False)
    staff_id = fields.Many2one('staff', string='Staff', required=False)
    created_at = fields.Datetime(string='Created At', required=False)
