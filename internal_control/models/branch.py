from odoo import models, fields, api

class Branch(models.Model):
    _name = 'tbl.branch'
    _description = 'branch table'

    branch_code = fields.Char(string="Name", required=True)
    branch_name = fields.Char(string="Code", required=True)
    address = fields.Char(string="Address")
    address2 = fields.Char(string="Address2")
    address3 = fields.Char(string="Address3")
    phone = fields.Char(string="Phone")
    email = fields.Char(string="Email")
    fax = fields.Char(string="Fax")
    city = fields.Char(string="City")
    state = fields.Char(string="State")
    country = fields.Char(string="Country")
    status = fields.Boolean(string="Status", default=True)
    cashaccount = fields.Char(string="cash account")
    suspenseDR = fields.Char(string="suspense DR")
    interBranchGL = fields.Char(string="interBranch GL")
    BranchType = fields.Integer(string="Branch Type")
    MBranchCode = fields.Char(string="Branch Type")
    SBranchCode = fields.Char(string="Branch Type")
    User_id = fields.Char(string="Branch Type")
    create_dt = fields.Datetime(string="created_at", default=fields.Datetime.now())
    region = fields.Char(string="Region")
    authid = fields.Char(string="authid")
    subbbranch = fields.Integer(string="subbbranch")
    
    _sql_constraints = [('branch_code_unique', 'unique(branch_code)', 'Branch Code must be unique!')]
<<<<<<< HEAD

=======
    _rec_name = 'representation'

    @api.depends('branch_code', 'branch_name')
    def _compute_name_rep(self):
        for record in self:
            record.representation = f"{record.branch_code} - {record.branch_name}"

    representation = fields.Char(string='representation', compute='_compute_name_rep', store=True)
>>>>>>> main
  