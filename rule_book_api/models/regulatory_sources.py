from odoo import models, fields

class RegulatorySources(models.Model):
    _name = 'regulatory.sources'
    _description = 'Regulatory Sources'

    name = fields.Char(string='Name', required=True)
    description = fields.Text(string='Description')
    url_link = fields.Char(string='URL Link')
    create_uid = fields.Many2one('res.users', string='Created By', readonly=True)
    write_uid = fields.Many2one('res.users', string='Updated By', readonly=True)
