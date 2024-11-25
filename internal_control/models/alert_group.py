from odoo import models, fields, api
from odoo.exceptions import ValidationError
class Alert_group(models.Model):
    _name = 'alert.group'
    _description = 'alert Group for exception management'
    

    name = fields.Char(string="Name", required=True)
    email = fields.Char(string="Email", required=True)
    email_cc = fields.Char(string="Copy_Email")
    state = fields.Boolean(string="State",default=True, required=True)
    tag = fields.Char(string="Tag")
    date_created = fields.Datetime(string="created_at", default=fields.Datetime.now())
    email_list = fields.Text(string="email_list", compute="split_email")

    @api.depends('email')
    def split_email(self):
        for record in self:
            # Split the email string by commas and remove any empty entries (if any)
            if record.email:
                record.email_list = str(record.email.split(','))
            else:
                raise ValidationError('Provide Strings of Email separated by comma(,) for this group')