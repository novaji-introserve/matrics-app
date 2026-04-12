from odoo import api, fields, models

class UserProfile(models.Model):
    _inherit = "hr.employee"
    _description = 'User Profile'
    _sql_constraints = [
        ('uniq_userid', 'unique(userid)',
         "User ID already exists. Value must be unique!"),
    ] 
    
    userid = fields.Char(string="User ID", index=True)
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

    def _derive_userid(self, user):
        return user.login if user and user.login else False

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get("userid") and vals.get("user_id"):
                user = self.env["res.users"].browse(vals["user_id"])
                derived_userid = self._derive_userid(user)
                if derived_userid:
                    vals["userid"] = derived_userid
        return super().create(vals_list)

    def write(self, vals):
        result = super().write(vals)
        if vals.get("userid"):
            return result

        employees_to_sync = self.filtered(lambda emp: not emp.userid and emp.user_id and emp.user_id.login)
        for employee in employees_to_sync:
            super(UserProfile, employee).write({"userid": employee.user_id.login})
        return result

    def init(self):
        self.env.cr.execute(
            """
            UPDATE hr_employee he
               SET userid = ru.login
              FROM res_users ru
             WHERE he.user_id = ru.id
               AND ru.login IS NOT NULL
               AND COALESCE(he.userid, '') = ''
            """
        )
