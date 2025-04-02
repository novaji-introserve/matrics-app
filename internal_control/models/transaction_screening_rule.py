# models/internal_control_screening_rule.py
from odoo import models, fields, api, _

class InternalControlScreeningRule(models.Model):
    _inherit = 'res.transaction.screening.rule'  # Inherit the original model
    
    
    # def screen_transactions(self):

    #     rules = self.env['res.transaction.screening.rule'].search(
    #         [('state', '=', 'active')], order='priority')

    #     if rules:
    #         for rule in rules:
    #             # try:
    #                 query = rule.sql_query
                        
    #                 self.env.cr.execute(query)
                   
    #                 records = self.env.cr.fetchall()
                    
    #                 for record in records:
    #                     record.rule_id = rule
    #                     record.risk_level = rule.risk_level
                
    
    def screen_transactions(self):
        rules = self.env['res.transaction.screening.rule'].search(
            [('state', '=', 'active')], order='priority')

        if rules:
            for rule in rules:
                query = rule.sql_query
                
                # Note: The query should now include "AND rule_id IS NULL" to only match unflagged transactions
                self.env.cr.execute(query)
                
                records = self.env.cr.fetchall()
                
                for rec in records:
                    # rec[0] should be the ID of the transaction
                    record = self.env['res.customer.transaction'].browse(rec[0])
                    
                    # Double-check that the record exists and isn't already flagged
                    if record.exists() and not record.rule_id:
                        record.write({
                            'rule_id': rule.id,
                            'risk_level': rule.risk_level,
                        })
        