from odoo import models, fields

class MailMailInherit(models.Model):
    _inherit = 'mail.mail'

    # Example: Add a new field to mail.mail
    custom_field = fields.Char(string="Custom Field")

    # You can override methods or add new ones here
    def custom_method(self):
        # Custom logic here
        pass
