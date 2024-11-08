from odoo import models, fields, api




class User(models.Model):
    _inherit = 'res.users'

    # Foreign keys to Branch and Department models
    branch_ids = fields.Many2many('rulebook.branch', string="Branch")
    department_id = fields.Many2one('rulebook.department', string="Department")
    
   
# class ResUsers(models.Model):
#     _inherit = 'res.users'
    
    
#     branch_ids = fields.Many2many('rulebook.branch', string='Branches', help="Branches the user belongs to.")
#     department_ids = fields.Many2many('rulebook.department', 'department_user_rel',
#                                        'user_id', 'department_id', string="Departments", help="Departments the user belongs to.")

    # branch_ids = fields.Many2many('rulebook.branch', string='Branches')
    # # department_ids = fields.Many2many('rulebook.department', string='Departments')
    # department_ids = fields.Many2many('rulebook.department', 'department_user_rel',
    #                                 'user_id', 'department_id', string="Departments")
    
    
    
    
    # department_id = fields.Many2one(
    #     'rulebook.department', string="Department", ondelete='restrict'
    # )  
    
# class ResUsers(models.Model):
#     _inherit = 'res.users'

#     department_ids = fields.Many2many(
#         'rulebook.department', 
#         'branch_department_user_rel', 
#         'user_id', 'department_id', 
#         string='Departments'
#     )
