from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError
import re

class SqlModel(models.Model):
    _name = 'process.sql'
    _description = "sql query for process"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    query = fields.Text(string="Query", required=True)
    name = fields.Char(string="Name",required=True)
    

    def Validate(self):
        try:
           

            query = self.query.strip().lower()
            
            if not query.startswith('select') :
                raise ValidationError(f'Query not supported.\n Hint: Query must start with SELECT')
            
            elif re.search(r"\w+\.\w+\s+AS\s+\w+", query, re.IGNORECASE):
                if not re.search(r"\w+\.branch_id", query, re.IGNORECASE):
                    raise ValidationError(f'Record are partition by branch so please specify a branch from appropriate table in your statement')
                else:
                    return {
                    "type": "ir.actions.client",
                    "tag": "display_notification",
                    "params":{
                        "message": "SQL Query validated successfully",
                        "type": "success"
                    }
                }
            else:
                
                try:
                    self.env.cr.execute(query)

                    return {
                        "type": "ir.actions.client",
                        "tag": "display_notification",
                        "params":{
                            "message": "SQL Query validated successfully",
                            "type": "success"
                        }
                    }
                except Exception as e:
                    raise ValidationError(f'Failed to execute query {str(e)}')



                
                
        except Exception as e:
            raise ValidationError(f'Failed to execute query {str(e)}')
    
    def savetodb(self):
        pass