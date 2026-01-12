from odoo import models, fields

class ResTransactionChannel(models.Model):
    _name = 'res.transaction.channel'
    _description = 'Transaction Channel'
    _rec_name = 'name'
    _sql_constraints = [
        ('uniq_code', 'unique(code)',
         "Channel already exists. Value must be unique!"),
    ] 

    code = fields.Char(required=True, string='Channel Code', index=True)
    name = fields.Char(required=True, string='Channel Name', index=True)
    description = fields.Text(string='Description')
