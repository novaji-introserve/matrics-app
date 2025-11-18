from odoo import models, fields, api


class ResImalUsers(models.Model):
    _name = 'res.imal.users'
    _description = 'IMAL Users'
    _rec_name = 'firstname'

    _sql_constraints = [
        ('unique_user_id', 'unique(user_id)', 'user id must be unique!'),
    ]

    user_id = fields.Char(string='User ID', index=True)
    pass_forward = fields.Char(string='Pass Forward')
    pass_reverse = fields.Char(string='Pass Reverse')
    user_group_id = fields.Char(string='User Group ID', index=True)
    user_group_description = fields.Char(string='User Group Description')
    user_created_date = fields.Date(string='User Created Date')
    user_valid_date = fields.Date(string='User Valid Date')
    email = fields.Char(string='Email', index=True)
    firstname = fields.Char(string='First Name', index=True)
    middle_name = fields.Char(string='Middle Name')
    last_name = fields.Char(string='Last Name', index=True)
    occupation = fields.Char(string='Occupation')
    suspended_by = fields.Char(string='Suspended By')
    suspend_reason = fields.Text(string='Suspend Reason')
    suspend_date = fields.Date(string='Suspend Date')
    activation_reason = fields.Text(string='Activation Reason')
    activation_date = fields.Date(string='Activation Date')
    ad_user_id = fields.Char(string='AD User ID', index=True)