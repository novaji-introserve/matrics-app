from odoo import models, fields, api

class CaseStatus(models.Model):
    _name = 'case.status'
    _description = 'Case Status'

    name = fields.Selection([
        ('open', 'Open'),
        ('closed', 'Closed'),
        ('overdue', 'Overdue'),
        ('archived', 'Archived'),
    ], string='Status', required=True)

    slug = fields.Char(string='Slug', compute='_compute_slug', store=True)
    created_at = fields.Char(string='Created At', size=4000, required=False)
    updated_at = fields.Char(string='Updated At', size=4000, required=False)
    description = fields.Char(string='Description', size=4000, required=False)

    @api.depends('name')
    def _compute_slug(self):
        for record in self:
            record.slug = record.name if record.name else ''










