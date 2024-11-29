from odoo import models, fields

class Department(models.Model):
    _inherit = 'hr.department'  # The model name in Odoo
    _description = 'Department'

    # Define the fields based on the SQL table structure
    
    deptid = fields.Char(string="Department ID", required=True)
    deptname = fields.Char(string="Department Name", required=True)
    deptshortname = fields.Char(string="Department Short Name", nullable=True)
    status = fields.Integer(string="Status")
    userid = fields.Char(string="User ID")
    authid = fields.Char(string="Authorization ID", nullable=True)
    createdate = fields.Datetime(string="Create Date")

    # You can add additional logic, such as constraints, or use `default` to set values.
