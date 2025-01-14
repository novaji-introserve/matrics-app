from odoo import models, fields, api

class Branch(models.Model):
    _inherit = "res.branch"
    _description = 'branch table'
    
    branchname = fields.Char(string='Branch Name')
    branchcode = fields.Char(string='Branch Code')

    @api.onchange('branchname')
    def _onchange_branchname(self):
        if self.branchname:
            self.name = self.branchname

    @api.onchange('name')
    def _onchange_name(self):
        if self.name:
            self.branchname = self.name

    @api.onchange('branchcode')
    def _onchange_branchcode(self):
        if self.branchcode:
            self.code = self.branchcode

    @api.onchange('code')
    def _onchange_code(self):
        if self.code:
            self.branchcode = self.code