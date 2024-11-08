from odoo import models, fields, api




class Branch(models.Model):
    _name = 'rulebook.branch'
    _description = 'Branch'

    name = fields.Char(string="Branch Name", required=True)
    code = fields.Char(string="Branch Code")
    location = fields.Char(string="Branch Location")    
    # Relationship with Users
    user_ids = fields.Many2many('res.users', 'branch_id', string="Users")

# class Branch(models.Model):
    
#     _name = 'rulebook.branch'
#     _description = 'Branch'

#     name = fields.Char(string="Branch Name", required=True)
#     code = fields.Char(string="Branch Code")
#     location = fields.Char(string="Branch Location")
#     user_ids = fields.Many2many('res.users', string="Users")
  
    # _name = 'rulebook.branch'
    # _description = 'Branch'
    
    # _sql_constraints = [
    #     ('name_uniq', 'unique (name)', 'Branch name must be unique!')
    # ]

    # name = fields.Char(string="Branch Name", required=True)
    # location = fields.Char(string="Branch Location")
    
    # # Many2many relationship with users
    # user_ids = fields.Many2many('res.users', string="Users")
    
    # # One2many relationship with departments
    # department_ids = fields.One2many('rulebook.department', 'branch_id', string="Departments")
  
    # _name = 'rulebook.branch'
    # _description = 'Branch'
    # _sql_constraints = [
    #     ('name_uniq', 'unique (name)', 'Branch name must be unique!')
    # ]

    # name = fields.Char(string="Branch Name", required=True)
    # location = fields.Char(string="Branch Location")
    # department_id = fields.One2many(
    #     'rulebook.department', 'branch_id', string="Departments"
    # ) 
    
# class Branch(models.Model):
#     _name = 'rulebook.branch'
#     _description = 'Branch'
#     _sql_constraints = [
#     ('name_uniq', 'unique (name)', 'Branch name must be unique!')
# ]

#     name = fields.Char(string="Branch Name", required=True)
#     location = fields.Char(string="Branch Location")
#     department_ids = fields.Many2many(
#         'rulebook.department',
#         'rulebook_branch_rulebook_department_rel',  # This is the name of the join table
#         'branch_id',         # This is the column for the branch
#         'department_id',     # This is the column for the department
#         string="Departments"
#     )

    # name = fields.Char(string="Branch Name", required=True)
    # # code = fields.Char(string="Branch Code", help="Unique code for branch")
    # location = fields.Char(string="Branch Location")
    # # department_ids = fields.Many2many('rulebook.department', 'branch_id', string="Departments")
    # department_ids = fields.Many2many('rulebook.department', string="Departments")