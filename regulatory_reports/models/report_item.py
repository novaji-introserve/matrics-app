from odoo import _, api, fields, models


class ReportItem(models.Model):
    _name = 'res.regulatory.report.item'
    _description = 'Regulatory Report Item'
    _order = 'name'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string='Name', required=True)
    code = fields.Char(string='Code', required=True, unique=True,help="Tracking code",tracking=True)
    description = fields.Text(string='Description')
    source = fields.Selection(string='Source', selection=[('static', 'Static From Field'), (
        'sql_single', 'SQL Query returning single value'),('sql_multi','SQL Query returning multiple rows')], default='static', help='Source of the report item data.',tracking=True)
    source_value = fields.Char(
        string='Source Value', help='The static value used to retrieve the data for this report item.',tracking=True)
    source_sql = fields.Text(string='Source SQL', help='SQL query to retrieve the data for this report item. The query should return a single value.',tracking=True)
    status = fields.Selection(string='State', selection=[('active', 'Active'), (
        'inactive', 'Inactive')], default='active', help='Indicates whether the report item is active or inactive.',tracking=True)
    
    
    def get_value(self):
        if self.source == 'sql_single':
            self.env.cr.execute(self.source_sql)
            rec = self.env.cr.fetchone()
            if rec is not None:
                # we have a hit
                return rec[0]
        if self.source == 'sql_multi':
            self.env.cr.execute(self.source_sql)
            recs = self.env.cr.fetchall()
            if recs is not None:
                return recs
        return self.source_value
