# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import re

class Statistic(models.Model):
    _name = 'res.compliance.stat'
    _description = 'Compliance Statistics'
    _sql_constraints = [
        ('uniq_stats_code', 'unique(code)',
         "Stats code already exists. Value must be unique!"),
        ('uniq_stats_name', 'unique(name)',
         "Name already exists. Value must be unique!")
    ]
    _inherit = ['mail.thread','mail.activity.mixin']

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    sql_query = fields.Text(string='SQL Query', required=True)
    scope = fields.Selection(string='Scope', selection=[(
        'bank', 'Bank Wide'), ('branch', 'Branch'), ('compliance', 'Compliance'),
        ('regulatory', 'Regulatory'),('risk','Risk Assessment')], default='bank')
    state = fields.Selection(string='State', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active')
    val = fields.Char(string='Value')
    narration = fields.Text(string='Narration')
  
    
    scope_color = fields.Char()

    # @api.depends('sql_query')
    # def _compute_val(self):
    #     for record in self:
    #         if record.sql_query:
    #             try:
    #                 query = record.sql_query.strip().lower()
    #                 if not query.startswith('select'):
    #                     raise ValidationError('Query not supported.\nHint: Start with SELECT')
    #                 record.env.cr.execute(query)
    #                 aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
    #                 pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\("
    #                 match = re.search(pattern, query, re.IGNORECASE)
    #                 if match:
    #                     record.val = record.env.cr.fetchone()[0]
    #                 else:
    #                     records = record.env.cr.fetchall()
    #                     record.val = str(len(records)) # convert to string, because val is a Char.
    #             except Exception as e:
    #                 record.val = 'Error'
    #                 raise ValidationError(f'Invalid SQL query:\n{str(e)}')
    #         else:
    #             record.val = '0'


    @api.model
    def create(self, vals):
        sql_query = vals.get('sql_query')  # Get the sql_query from the values
        scope = vals.get('scope')

        if sql_query:  # Check if sql_query is provided
            try:
                query = sql_query.strip().lower()

                if not query.startswith('select'):
                    raise ValidationError('Query not supported.\nHint: Start with SELECT')
                
                self.env.cr.execute(query)

                aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
                pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\(" 
                match = re.search(pattern, query, re.IGNORECASE)

                if match:
                    count = self.env.cr.fetchone()[0]
                    self.val = count  # Store the count of records
                else:
                    records = self.env.cr.fetchall()
                    if records:
                        self.val = len(records)  # Store the length of the records
                    else:
                        self.val = 0  # Store 0 if no records


                # assign the color
                if scope:
                    # vals['scope_color'] = {
                    #     'bank': '#FEEEF1',
                    #     'branch': '#D9FAE7',
                    #     'compliance': '#8BB8D9',
                    #     'regulatory': '#F7E8BD',
                    #     'risk': '#FEF2DC',
                    # }.get(scope, '#00F2AD')
                    
                    vals['scope_color'] = {
                        'bank': '#FFD700',
                        'branch': '#66B2FF',
                        'compliance': '#C8102E',
                        'regulatory': '#4CAF50',
                        'risk': '#999999',
                    }.get(scope, '#FFD700')

            except Exception as e:
                self.env.cr.rollback() # Important: rollback on error
                raise ValidationError(f'Invalid SQL query:\n{str(e)}')
        
        return super(Statistic, self).create(vals)

    @api.onchange('sql_query')
    def _onchange_sql_query(self):
        
        try:
            if self.sql_query:

                print(self.val)
                print(self.sql_query)
               
                query = self.sql_query
                self.env.cr.execute(query)

                aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
                pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\(" 
                match = re.search(pattern, query, re.IGNORECASE)

                if match:
                    count = self.env.cr.fetchone()[0]
                    print(count)
                    self.write({'val': count}) # save the value to the database.
                else:
                    records = self.env.cr.fetchall()
                    if records:
                        self.write({'val': len(records)}) # save the value to the database.
                    else:
                       
                        self.write({'val': 0}) # save the value to the database.
                
               


        except Exception as e:
            raise ValidationError(str(e))



    def compute_stat(self):
        query = self.sql_query.lower()
        if 'delete' in query:
            pass
        elif 'update' in query:
            pass
        else:
            self.env.cr.execute(self.sql_query)
            rec = self.env.cr.fetchone()
            result = rec[0] if rec is not None else 0.0
            self.write({"val":result})

    



    def update_stat(self):
       
        try:
                
          statistic = self.env['res.compliance.stat'].search([])

          for stat in statistic:
              self.env.cr.execute(stat.sql_query)
        finally:
            self.env.cr.rollback() #Rollback the cursor to prevent any unintended changes

