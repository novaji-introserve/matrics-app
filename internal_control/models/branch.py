from odoo import models, fields, api

class Branch(models.Model):
    _name = 'tbl.branch'
    _description = 'branch table'

    branch_code = fields.Char(string="Branch Code", required=True)
    branch_name = fields.Char(string="Branch Name", required=True)
    SBranchCode = fields.Char(string="Branch Type")
    subbbranch = fields.Integer(string="subbbranch")
    users = fields.Many2many("res.users", "tbl_branch_users_rel", "branch_id", "user_id")
    
    _sql_constraints = [('branch_code_unique', 'unique(branch_code)', 'Branch Code must be unique!')]
    _rec_name = 'representation'

    @api.depends('branch_code', 'branch_name')
    def _compute_name_rep(self):
        for record in self:
            record.representation = f"{record.branch_code} - {record.branch_name}"

    representation = fields.Char(string='representation', compute='_compute_name_rep', store=True)
  