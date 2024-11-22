from odoo import models, fields, api

class Alert_group(models.Model):
    _name = 'alert.group'
    _description = 'alert Group for exception management'
    

    name = fields.Char(string="Name", required=True)
    email = fields.Char(string="Email", required=True)
    email_cc = fields.Char(string="Copy_Email")
    state = fields.Char(string="State", required=True)
    tag = fields.Char(string="Tag")
    date_created = fields.Datetime(string="created_at", default=fields.Datetime.now())


  