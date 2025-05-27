from odoo import models, fields

class Staff(models.Model):
    _name = 'staff'
    _description = 'Staff'
    _rec_name = 'staff_name'

    staff_name = fields.Char(string='Staff Name', size=4000, required=False, readonly=True)
    department = fields.Char(string='Department', size=4000, required=False)
    email = fields.Char(string='Email', size=4000, required=False, readonly=True)
    dept_email = fields.Char(string='Department Email', size=4000, required=False, readonly=True)
    staff_id = fields.Char(string='Staff ID', size=4000, required=False, readonly=True)
    
    user_id = fields.Many2one('res.users', string='User', required=False, readonly=True)
    
    os_user = fields.Char(string='OS User', size=4000, required=False)
    firstname = fields.Char(string='First Name', size=200, required=False, readonly=True)
    lastname = fields.Char(string='Last Name', size=200, required=False, readonly=True)
    username = fields.Char(string='Username', size=1000, required=False, readonly=True)
