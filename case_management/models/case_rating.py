from odoo import models, fields, api

class CaseRating(models.Model):
    _name = 'case.rating'
    _description = 'Case Rating'

    ref = fields.Integer(string='Reference', required=True)
    
    name = fields.Char(string='Name', compute='_compute_name', store=True)
    
    @api.depends('ref')
    def _compute_name(self):
        for record in self:
            ref_mapping = {1: 'Low', 2: 'Medium', 3: 'High'}
            record.name = ref_mapping.get(record.ref, '')
