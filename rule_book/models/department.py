from odoo import models, fields, api
# inherit the user class to ensure every new user is added to a department
from odoo import models, api, _
from odoo.exceptions import ValidationError


 
class Department(models.Model):
    _name = 'hr.department'
    # _description = 'Department'

    email = fields.Char(string="Department Email")
    
  
# class BranchDepartment(models.Model):
    
    
#     _name = 'rulebook.department'
#     _description = 'Department'

#     name = fields.Char(string="Department Name", required=True)
#     code = fields.Char(string="Department Code")
#     branch_ids = fields.Many2many('rulebook.branch', string="Branches", help="Branches that this department belongs to.")
#     user_ids = fields.Many2many('res.users', string="Users", help="Users assigned to this department.")
    # branch_ids = fields.Many2many('rulebook.branch', string="Branches")
    # user_ids = fields.Many2many('res.users', string="Users")
 
    # _name = 'rulebook.department'
    # _description = 'Department'
    
    # _sql_constraints = [
    #     ('name_branch_uniq', 'unique (name, branch_id)', 'Department name must be unique per branch!')
    # ]

    # name = fields.Char(string="Department Name", required=True)
    # code = fields.Char(string="Department Code")
    
    # # Many2one relationship to a branch
    # branch_id = fields.Many2one('rulebook.branch', string="Branch", required=True)
    
    # # Many2many relationship with users
    # user_ids = fields.Many2many('res.users', string="Users")
 
    # _name = 'rulebook.department'
    # _description = 'Branch Department'
    # _rec_name = 'name'
    # _inherit = ["mail.thread", "mail.activity.mixin"]
    # _sql_constraints = [
    #     ('name_uniq', 'unique (name)', 'Department name must be unique!')
    # ]

    # name = fields.Char(string="Department Name", required=True)
    # code = fields.Char(string="Department Code")
    # branch_id = fields.Many2one(
    #     'rulebook.branch', string="Branch", required=True, ondelete='cascade'
    # )  # One department to one branch
    # user_ids = fields.One2many(
    #     'res.users', 'department_id', string='Users'
    # )
      
# class BranchDepartment(models.Model):
#     _name = 'rulebook.department'
#     _description = 'Branch Department'
#     _rec_name = 'name',
#     _inherit = ["mail.thread", "mail.activity.mixin"],
#     _sql_constraints = [
#     ('name_uniq', 'unique (name)', 'Department name must be unique!')
#     ]

#     name = fields.Char(string="Department Name", required=True)
#     code = fields.Char(string="Department Code")
#     branch_id = fields.Many2one('rulebook.branch', string="Branch", required=True, ondelete='cascade')
#     user_ids = fields.Many2many('res.users', 'branch_department_user_rel', 'department_id', 'user_id', string='Users')





# class ResConfigSettings(models.TransientModel):
#     _inherit = 'res.config.settings'

#     module_attachment_indexation = fields.Boolean(string="Index Attachments")  
#     group_ir_attachment_user = fields.Boolean(
#         string='Attachment User Group',
#         implied_group='rule_book.group_compliance_officer'  # Changed to use our new group
#     )
    
#     module_document_page = fields.Boolean(string="Enable Document Pages")
#     module_mgmtsystem_quality = fields.Boolean(string="Enable Document Pages")
#     module_mgmtsystem_environment = fields.Boolean(string="Enable Document Pages")
#     module_document_page_approval = fields.Boolean(
#         string="Enable Document Page Approval",
#         help="Check this box to enable document page approval feature."
#     )
    



# class ResUsers(models.Model):
#     _inherit = 'res.users'
#     department_ids = fields.Many2many('rulebook.department', string='Departments',required=True,  help="Every user must be added to at least one department.")
    
#     @api.model
#     def create(self, vals):
        
#         # Check if department_ids are provided
#         if 'department_ids' not in vals or not vals.get('department_ids'):
#             raise ValidationError(_("You must assign the user to at least one department before creating them."))

#         user = super(ResUsers, self).create(vals)
#         return user