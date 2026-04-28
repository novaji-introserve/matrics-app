# models/res_users.py
# from odoo import models, fields
from odoo import models, fields, api


class ResUsers(models.Model):
    _inherit = "res.users"

    department = fields.Many2one("hr.department", string="Department")
    
        


