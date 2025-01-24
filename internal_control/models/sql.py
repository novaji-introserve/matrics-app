from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError


class SqlModel(models.Model):
    _name = 'process.sql'
    _description = "sql query for process"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    query = fields.Char(string="Query", required=True)
    name = fields.Char(string="Name",required=True)
    
    is_valid = fields.Boolean(default=False)

    def Validate(self):
        try:
            chk = self.query.strip().startswith('select')
            
            if chk:
                self.env.cr.execute(self.query)
                self.write({'is_valid': True})
            else:
                raise ValidationError(f'query not supported.\n Hint: start with select')
                
        except Exception as e:
            raise ValidationError(f'failed to execute query {str(e)}')
    
    def savetodb(self):
        pass