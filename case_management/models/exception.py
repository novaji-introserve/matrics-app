from odoo import models, fields, api

class ExceptionCategory(models.Model):
    _name = 'exception.category'
    _description = 'Exception Category'

    name = fields.Char(string='Name', size=50, required=False)
    description = fields.Char(string='Description', size=50, required=False)
    code = fields.Char(string='Code', size=20, required=False)
    created_at = fields.Datetime(string='Created At', required=False)


class ExceptionProcessType(models.Model):
    _name = 'exception.process.type'
    _description = 'Exception Process Type'

    name = fields.Char(string='Name', size=500, required=False)
    category_id = fields.Many2one('exception.category', string='Category', required=False)




class ComplianceRiskRating(models.Model):
    _name = 'compliance.risk.rating'
    _description = 'Compliance Risk Rating'

    name = fields.Selection([
        ('1', 'Low'),
        ('2', 'Medium'),
        ('3', 'High')
    ], string='Risk Level', required=True)
    
    description = fields.Char(string='Description', compute='_compute_description', store=True)
    
    @api.depends('name')
    def _compute_description(self):
        for record in self:
            risk_mapping = {'1': 'Low', '2': 'Medium', '3': 'High'}
            record.description = risk_mapping.get(record.name, '')




class ExceptionProcess(models.Model):
    _name = 'exception.process'
    _description = 'Exception Process'

    name = fields.Char(string='Name', required=False)
    sql_text = fields.Char(string='SQL Text', required=False)
    frequency = fields.Char(string='Frequency', required=False)
    email_to = fields.Char(string='Email To', required=False)
    state = fields.Char(string='State', required=False)
    
    category_id = fields.Many2one('exception.category', string='Category', required=False)
    alert_group_id = fields.Many2one('alert.group', string='Alert Group', required=False)
    branch_code = fields.Many2one('branch', string='Branch', required=False)
    type_id = fields.Many2one('exception.process.type', string='Type', required=False)
    risk_rating_id = fields.Many2one('compliance.risk.rating', string='Risk Rating', required=False)
    policy_id = fields.Many2one('policy', string='Policy', required=False)
    user_id = fields.Many2one('res.users', string='User', required=False)
    
    first_line_owner = fields.Integer(string='First Line Owner', required=False)
    second_line_owner = fields.Integer(string='Second Line Owner', required=False)
    first_line_owner_id = fields.Integer(string='First Line Owner ID', required=False)
    second_owner_id = fields.Integer(string='Second Owner ID', required=False)
    
    approved_at = fields.Datetime(string='Approved At', required=False)
