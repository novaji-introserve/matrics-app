from odoo import models, fields, api
from odoo.exceptions import ValidationError
class Alert_group(models.Model):
    _name = 'alert.group'
    _description = 'alert Group for exception management'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    

    name = fields.Char(string="Name", required=True, tracking=True)
    email = fields.Many2many('res.users', "alert_group_email_rel", "alert_group_id", "user_id", string="Email", required=True, tracking=True)
    email_cc = fields.Many2many('res.users', "alert_group_email_cc_rel", "alert_group_id", "user_id",  string="Email_cc", required=True, tracking=True)
    state = fields.Selection(
    [("active", "Active"), ("inactive", "inactive" )],
    default="active",  # The default value is an integer (1)
    string="State",
    tracking=True
    )
    tag = fields.Char(string="Slug", tracking=True)
   
    _sql_constraints = [
        ('unique_name', 'UNIQUE(name)', 'Alert Group name must be unique!'),
    ]


           
