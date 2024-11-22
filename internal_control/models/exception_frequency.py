from odoo import models, fields, api
from odoo.exceptions import ValidationError
class Exception_frequency(models.Model):
    _name = 'exception.frequency'
    _description = 'exception Frequency rate'
    
    def __str__(self):
        return f"{self.period} {self.name}"

    name = fields.Selection(
        [
            ("minutes", "Minutes"),
            ("hourly", "Hourly"),
            ("daily", "Daily"),
            ("weekly", "Weekly"),
            ("monthly", "Monthly"),
            ("yearly", "Yearly")
        ]
    )
    period = fields.Integer(string="period", required=True)
    date_created = fields.Datetime(string="created_at", default=fields.Datetime.now())

    
    @api.onchange("name")
    def change_period_to_zero(self):
        self.period = 1
        
    @api.onchange("period")
    def change_period(self):
        
        
    
        if self.name:
            if self.name == "minutes" and self.period > 60:
                raise ValidationError("period  must be between 1 and 60")
               
            elif self.name == "hourly" and self.period > 24:
                
                raise ValidationError("period  must be between 1 and 24")
    
               
            elif self.name == "daily" and self.period > 30:
                
                raise ValidationError("period  must be between 1 and 30")
    
               
            elif self.name == "weekly" and self.period > 4:
                
                raise ValidationError("period  must be between 1 and 4")
    
               
            elif self.name == "monthly" and self.period > 12:
                
                raise ValidationError("period  must be between 1 and 12")
    
               
            elif self.name == "yearly" and self.period > 12:
                
                raise ValidationError("period  must be between 1 and 12")
            
            elif self.period == 0:
                
                raise ValidationError("period  cannot be zero")
            
            
    
               