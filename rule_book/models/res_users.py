# models/res_users.py
from odoo import models, fields

class ResUsers(models.Model):
    _inherit = "res.users"

    department = fields.Many2one("hr.department", string="Department")

