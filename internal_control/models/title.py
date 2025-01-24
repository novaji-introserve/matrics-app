from odoo import fields, models, api, _


class Title(models.Model):
    _inherit = 'res.partner.title'
    _sql_constraints = [
        ('uniq_title_code', 'unique(code)',
         "Title already exists. Value must be unique!"),
    ]

    code = fields.Char(string='Code',  readonly=True, index=True)
    title = fields.Char(string='Name',  readonly=True, index=True)
