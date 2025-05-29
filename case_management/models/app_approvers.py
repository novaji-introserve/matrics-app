from odoo import models, fields


class AppApprovers(models.Model):
    _name = 'app.approvers'
    _description = 'App Approvers'

    row_id = fields.Char(string='Row ID', required=False)
    id = fields.Char(string='ID', required=False)
    mdp = fields.Char(string='MDP', required=False)
    name_cimp = fields.Char(string='Name CIMP', required=False)
    nif = fields.Integer(string='NIF', required=False)
    
    date_created = fields.Datetime(string='Date Created', required=False)
    last_updated = fields.Datetime(string='Last Updated', required=False)
    ddlv = fields.Datetime(string='DDLV', required=False)
    
    branch_code = fields.Many2one('branch', string='Branch', required=False)
    
    customer_id = fields.Char(string='Customer ID', required=False)
    puti = fields.Char(string='PUTI', required=False)
    port = fields.Integer(string='Port', required=False)
