from odoo import _, api, fields, models


class ReportTemplate(models.Model):
    _name = 'res.regulatory.report.template'
    _description = 'Regulatory Report Template'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char(string='Name', required=True)
    code = fields.Char(string='Code', required=True, unique=True,tracking=True)
    description = fields.Html(string='Description')
    entity_id = fields.Many2one(
        comodel_name='res.regulatory.report.entity',
        string='Reporting Entity',
        required=True,
        ondelete='cascade',
        index=True,
        help='The entity to which this report template belongs.',
        tracking=True
    )
    template_file = fields.Binary(string='Template File', help='Upload the report template file here.',required=True,tracking=True)
    template_file_name = fields.Char(string='File Name', help='Name of the uploaded file.')
    status  = fields.Selection(string='State', selection=[('active', 'Active'), ('inactive', 'Inactive')], default='active', help='Indicates whether the report template is active or inactive.')
    report_type = fields.Selection(
        string='Report Type',
        selection=[
            ('xls', 'Excel'),
            ('pdf', 'PDF'),
            ('csv', 'CSV'),
            ('docx', 'Word Document'),
            ('txt', 'Text File')
        ],
        default='xls',
        help='Type of report this template is designed for.',
        index=True
    )
    item_ids = fields.Many2many(
        'res.regulatory.report.item', 'res_report_template_items_rel', 'template_id', 'item_id', string='Report Items' ,required=False)