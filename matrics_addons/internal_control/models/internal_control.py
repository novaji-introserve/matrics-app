from odoo import models, fields, api

class InternalControl(models.Model):
    _name = 'res.internal_control'
    _description = 'Internal Control'

    name = fields.Char(string="Name", required=True)
    description = fields.Text(string="Description")
    
    @api.model
    def create(self, vals):
        # Example override for the create method
        return super(InternalControl, self).create(vals)
