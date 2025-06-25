from odoo import _, api, fields, models


class ReportItem(models.Model):
    _name = 'res.regulatory.report.item'
    _description = 'Regulatory Report Item'
    _order = 'name'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string='Name', required=True)
    code = fields.Char(string='Code', required=True, unique=True)
    description = fields.Text(string='Description')
    source = fields.Selection(string='Source', selection=[('static', 'Static From Field'), (
        'sql', 'SQL Query returning single value')], default='static', help='Source of the report item data.')
    source_value = fields.Char(
        string='Source Value', help='The static value used to retrieve the data for this report item.')
    source_sql = fields.Text(string='Source SQL', help='SQL query to retrieve the data for this report item. The query should return a single value.')
    status = fields.Selection(string='State', selection=[('active', 'Active'), (
        'inactive', 'Inactive')], default='active', help='Indicates whether the report item is active or inactive.')
    report_template_ids = fields.Many2many(
        'res.regulatory.report.template', 'res_report_template_items_rel', 'item_id', 'template_id',string='Report Templates', required=False)
