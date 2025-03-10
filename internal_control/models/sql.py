from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError
import re

class SqlModel(models.Model):
    _name = 'process.sql'
    _description = "sql query for process"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    query = fields.Char(string="Query", required=True)
    name = fields.Char(string="Name",required=True)
    

    def Validate(self):
        try:
           

            query = self.query.strip().lower()
            
            if not query.startswith('select') :
                raise ValidationError(f'query not supported.\n Hint: start with select')
            
            elif re.search(r"\w+\.\w+\s+AS\s+\w+", query, re.IGNORECASE):
                if not re.search(r"\w+\.branch_id", query, re.IGNORECASE):
                    raise ValidationError(f'Record are partition by branch so please specify a branch from appropriate table in your statement')
            else:
                return {
                    "type": "ir.actions.client",
                    "tag": "display_notification",
                    "params":{
                        "message": "Valid Sql",
                        "type": "success"
                    }
                }
                
                
        except Exception as e:
            raise ValidationError(f'failed to execute query {str(e)}')
    
    def savetodb(self):
        pass