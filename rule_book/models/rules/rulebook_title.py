from odoo import models, fields, api

class RulebookTitle(models.Model):
    _name = 'rulebook.title'
    _description = 'Rulebook Titles'
    _rec_name = 'name'

    name = fields.Char(string='Title', required=True)
    file = fields.Binary(string='File', attachment=True, required=True)
    file_name = fields.Char(string='File Name')
    ref_number = fields.Char(string='Reference Number', required=False)
    released_date = fields.Date(string='Released Date', required=True)
    status = fields.Selection([
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('deleted', 'Deleted')
    ], string='Status', default='active', required=True)
    source_id = fields.Many2one('rulebook.sources', string='Source', required=True)
    created_on = fields.Datetime(string='Created On', default=fields.Datetime.now, required=True)
    created_by = fields.Many2one('res.users', string='Created By', default=lambda self: self.env.user, readonly=True)


    @api.model
    def create(self, vals):
        if not vals.get('created_by'):
            vals['created_by'] = self.env.user.id
        return super(RulebookTitle, self).create(vals)
