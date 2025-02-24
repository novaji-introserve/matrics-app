# models/internal_control_screening_rule.py
from odoo import models, fields, api, _

class InternalControlScreeningRule(models.Model):
    _inherit = 'res.transaction.screening.rule'  # Inherit the original model
    
    
    def sync_branch_id_from_accounts(self):

        rules = self.env['res.transaction.screening.rule'].search(
            [('state', '=', 'active')], order='priority')

        if rules:
            for rule in rules:
                # try:
                    query = rule.sql_query
                        
                    self.env.cr.execute(query)
                   
                    records = self.env.cr.fetchall()
                    
                    for record in records:
                        record.rule_id = rule
                        record.risk_level = rule.risk_level
                
    