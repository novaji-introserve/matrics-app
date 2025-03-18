# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError


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

                if "count('*')" in sql_query or 'count(*)' in sql_query:
                    count = self.env.cr.fetchone()[0]
                    vals['val'] = count  # Store the count of records
                else:
                    records = self.env.cr.fetchall()
                    if records:
                            vals['val'] = len(records)  # Store the length of the records
                    else:
                        vals['val'] = 0  # Store 0 if no records


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

