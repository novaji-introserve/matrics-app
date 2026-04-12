from odoo import models, fields

class UserProfile(models.Model):
    _inherit = "hr.employee"
    _description = 'User Profile'
    _sql_constraints = [
        ('uniq_userid', 'unique(userid)',
         "User ID already exists. Value must be unique!"),
    ] 
    
    userid = fields.Char(required=True, string="User ID", index=True)
    branch_id = fields.Many2one("res.branch", string="Branch")
    ip = fields.Char(string="IP")
    role_id = fields.Char(string="Role ID")
    authoriser = fields.Char()
    staff_status = fields.Boolean(default=True)
    postgl_acctno = fields.Char(string="POSTGL ACCNO")
    passchange_date = fields.Date(string='Password Change Date')
    report_level = fields.Char(string="Report Level")
    lockcount = fields.Integer(0)
    authuserid = fields.Char()
    post_userid = fields.Char()
    dept_id = fields.Many2one("hr.department", string="Legacy Department")
