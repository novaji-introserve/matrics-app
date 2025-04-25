# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import re
import logging

_logger = logging.getLogger(__name__)

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
    # val = fields.Char(string='Value')
    val = fields.Char(string='Value', compute='_compute_val', store=True, readonly=True)
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
                query:str = sql_query.strip().lower()

                if not query.startswith('select'):
                    raise ValidationError('Query not supported.\nHint: Start with SELECT')
                
                if "res.partner" in query:

                    if query.endswith(";"):
                        query[:-1]
                    
                    query += " AND origin = ANY(['demo','test','prod'])"
                
                self.env.cr.execute(query)

                aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
                pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\(" 
                match = re.search(pattern, query, re.IGNORECASE)

                if match:
                    count = self.env.cr.fetchone()[0]
                    self.val = count if count is not None else '0'
                    
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

    @api.depends('sql_query')
    def _compute_val(self):
        for record in self:
            if not record.sql_query:
                record.val = '0'
                continue
                
            try:
                query = record.sql_query.strip().lower()
                
                if not query.startswith('select'):
                    raise ValidationError('Query not supported.\nHint: Start with SELECT')
                    
                record.env.cr.execute(query)
                
                aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
                pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\("
                match = re.search(pattern, query, re.IGNORECASE)
                
                if match:
                    result = record.env.cr.fetchone()
                    record.val = str(result[0]) if result and result[0] is not None else '0'
                else:
                    records = record.env.cr.fetchall()
                    record.val = str(len(records)) if records else '0'
                    
            except Exception as e:
                record.val = 'Error'
                # Logging the error might be better than raising here
                # since this is a computed field
                record.env.cr.rollback()
    
    @api.onchange('sql_query')
    def _onchange_sql_query(self):
        # This will update the field in the UI before saving
        self._compute_val()

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
        statistic = self.env['res.compliance.stat'].search([])
        
        # Define pattern outside the loop for better performance
        aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
        pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\("
        
        for stat in statistic:
            try:
                if not stat.sql_query:
                    continue
                    
                self.env.cr.execute(stat.sql_query)
                match = re.search(pattern, stat.sql_query, re.IGNORECASE)
                
                if match:
                    result = self.env.cr.fetchone()
                    stat.val = str(result[0]) if result and result[0] is not None else '0'
                else:
                    records = self.env.cr.fetchall()
                    stat.val = str(len(records)) if records else '0'
                    
            except Exception as e:
                _logger.error(f"Error updating stat {stat.name}: {str(e)}")
                
        
       

    
    
    def open_action(self):
        pass
        # query = self.env.context.get('query')
        # # Process any logic based on the stat_id
        
        # # Return action to open web page
        # return {
        #     'type': 'ir.actions.act_url',
        #     'url': '/your_controller_path/%s' % query,
        #     'target': 'self',  # or 'new' for a new window/tab
        # }

