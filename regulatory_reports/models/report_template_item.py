from odoo import _, api, fields, models


class ReportTemplateItem(models.Model):
    _name = 'res.regulatory.report.template.item'
    _description = 'Regulatory Report Template Items'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string='Cell Reference',help="Cell location to place the value. eg A1",tracking=True)
    template_id = fields.Many2one(comodel_name='res.regulatory.report.template', string='Template',required=True,index=True,tracking=True)
    item_id = fields.Many2one(comodel_name='res.regulatory.report.item', string='Report Item',index=True,tracking=True)
    description = fields.Text(string='Description')
