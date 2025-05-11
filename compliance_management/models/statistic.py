# # -*- coding: utf-8 -*-

# from odoo import models, fields, api, _
# from odoo.exceptions import ValidationError
# import re
# import logging

# _logger = logging.getLogger(__name__)

# class Statistic(models.Model):
#     _name = 'res.compliance.stat'
#     _description = 'Compliance Statistics'
#     _sql_constraints = [
#         ('uniq_stats_code', 'unique(code)',
#          "Stats code already exists. Value must be unique!"),
#         ('uniq_stats_name', 'unique(name)',
#          "Name already exists. Value must be unique!")
#     ]
#     _inherit = ['mail.thread','mail.activity.mixin']

#     name = fields.Char(string="Name", required=True)
#     code = fields.Char(string="Code", required=True)
#     sql_query = fields.Text(string='SQL Query', required=True)
#     scope = fields.Selection(string='Scope', selection=[(
#         'bank', 'Bank Wide'), ('branch', 'Branch'), ('compliance', 'Compliance'),
#         ('regulatory', 'Regulatory'),('risk','Risk Assessment')], default='bank')
#     state = fields.Selection(string='State', selection=[(
#         'active', 'Active'), ('inactive', 'Inactive')], default='active')
#     # val = fields.Char(string='Value')
#     val = fields.Char(string='Value', compute='_compute_val', store=True, readonly=True)
#     narration = fields.Text(string='Narration')
#     scope_color = fields.Char()

#     def _prepare_and_validate_query(self, sql_query):
#         """Helper method to prepare and validate SQL query with consistent filtering"""
#         if not sql_query:
#             return None

#         pattern = r"\bres_partner\b"    
#         try:
#             # Keep original query for execution but use lowercase for checks
#             original_query = sql_query.strip()
#             query = original_query.lower()
            
#             if not query.startswith('select'):
#                 raise ValidationError('Query not supported.\nHint: Start with SELECT')
            
#             if re.search(pattern, query, re.IGNORECASE):
#                 # Remove trailing semicolon if present
#                 if query.endswith(";"):
#                     query = query[:-1]
#                     original_query = original_query[:-1]
                
#                 has_where = bool(re.search(r'\bwhere\b', query))
                
#                 # Determine the condition to add
#                 condition = " AND origin IN ('demo','test','prod')" if has_where else " WHERE origin IN ('demo','test','prod')"
                
#                 # Add the condition before any GROUP BY, ORDER BY, LIMIT, etc.
#                 for clause in ['group by', 'order by', 'limit', 'offset', 'having']:
#                     clause_pos = query.find(' ' + clause + ' ')
#                     if clause_pos > -1:
#                         # Insert condition before this clause in the original query
#                         original_query = original_query[:clause_pos] + condition + original_query[clause_pos:]
#                         break
#                 else:
#                     # No such clauses found, append at the end
#                     original_query += condition
                    
#             return original_query, query
            
#         except Exception as e:
#             self.env.cr.rollback()  # Important: rollback on error
#             raise ValidationError(f'Invalid SQL query:\n{str(e)}')
    
#     def _execute_query_and_get_value(self, original_query, query):
#         """Execute the query and return the appropriate value"""
#         self.env.cr.execute(original_query)
        
#         aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
#         pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\("
#         match = re.search(pattern, query, re.IGNORECASE)
        
#         if match:
#             result = self.env.cr.fetchone()
#             return str(result[0]) if result and result[0] is not None else '0'
#         else:
#             records = self.env.cr.fetchall()
#             return str(len(records)) if records else '0'
   
#     @api.model
#     def create(self, vals):
#         sql_query = vals.get('sql_query')
        
#         if sql_query:
#             try:
#                 original_query, query = self._prepare_and_validate_query(sql_query)
#                 self.val = self._execute_query_and_get_value(original_query, query)
#                 # We don't need to set vals['val'] here since it's a computed field
#             except Exception as e:
    
#                 raise ValidationError(f'Invalid SQL query:\n{str(e)}')
        
#         return super(Statistic, self).create(vals)

#     @api.depends('sql_query')
#     def _compute_val(self):
#         for record in self:
#             if not record.sql_query:
#                 record.val = '0'
#                 continue
                
#             try:
#                 original_query, query = record._prepare_and_validate_query(record.sql_query)
#                 if original_query:
#                     record.val = record._execute_query_and_get_value(original_query, query)
#             except Exception as e:
#                 record.val = 'Error'
#                 record.env.cr.rollback()
    
#     @api.onchange('sql_query')
#     def _onchange_sql_query(self):
#         # This will update the field in the UI before saving
#         self._compute_val()

    

    

#     #    def Validate_expression(self):
        
#     #     try:
           
#     #         query = self.sql_query.strip().lower()
            
#     #         if not query.startswith('select') :
#     #             raise ValidationError(f'query not supported.\n Hint: start with select')
            
#     #         else:
                
#     #             try:
#     #                 self.env.cr.execute(query)

#     #                 original_query, query = self._prepare_and_validate_query(self.sql_query)
#     #                 if original_query:
#     #                     self.val = self._execute_query_and_get_value(original_query, query)
#     #             except Exception as e:
#     #                     self.val = 'Error'
#     #                     return {
#     #                         "type": "ir.actions.client",
#     #                         "tag": "display_notification",
#     #                         "params":{
#     #                             "message": f'failed to execute query {str(e)}',
#     #                             "type": "danger"
#     #                         }
#     #                     }

            
#     #     except Exception as e:
#     #         return {
#     #                     "type": "ir.actions.client",
#     #                     "tag": "display_notification",
#     #                     "params":{
#     #                         "message": f'failed to execute query {str(e)}',
#     #                         "type": "danger"
#     #                     }
#     #                 }


#     # def update_stat(self):
#     #     statistic = self.env['res.compliance.stat'].search([])
        
#     #     # Define pattern outside the loop for better performance
#     #     aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
#     #     pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\("
        
#     #     for stat in statistic:
#     #         try:
#     #             if not stat.sql_query:
#     #                 continue
                    
#     #             self.env.cr.execute(stat.sql_query)
#     #             match = re.search(pattern, stat.sql_query, re.IGNORECASE)
                
#     #             if match:
#     #                 result = self.env.cr.fetchone()
#     #                 stat.val = str(result[0]) if result and result[0] is not None else '0'
#     #             else:
#     #                 records = self.env.cr.fetchall()
#     #                 stat.val = str(len(records)) if records else '0'
                    
#     #         except Exception as e:
#     #             _logger.error(f"Error updating stat {stat.name}: {str(e)}")
                
        
       

    
    
#     def open_action(self):
#         pass
#         # query = self.env.context.get('query')
#         # # Process any logic based on the stat_id
        
#         # # Return action to open web page
#         # return {
#         #     'type': 'ir.actions.act_url',
#         #     'url': '/your_controller_path/%s' % query,
#         #     'target': 'self',  # or 'new' for a new window/tab
#         # }


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
    val = fields.Char(string='Value', compute='_compute_val', store=True, readonly=True)
    narration = fields.Text(string='Narration')
    scope_color = fields.Char()

    def _prepare_and_validate_query(self, sql_query):
        """Helper method to prepare and validate SQL query with consistent filtering"""
        if not sql_query:
            return None

        pattern = r"\bres_partner\b"    
        try:
            # Keep original query for execution but use lowercase for checks
            original_query = sql_query.strip()
            query = original_query.lower()
            
            if not query.startswith('select'):
                raise ValidationError('Query not supported.\nHint: Start with SELECT')
            
            if re.search(pattern, query, re.IGNORECASE):
                # Remove trailing semicolon if present
                if query.endswith(";"):
                    query = query[:-1]
                    original_query = original_query[:-1]
                
                has_where = bool(re.search(r'\bwhere\b', query))
                
                # Determine the condition to add
                condition = " AND origin IN ('demo','test','prod')" if has_where else " WHERE origin IN ('demo','test','prod')"
                
                # Add the condition before any GROUP BY, ORDER BY, LIMIT, etc.
                for clause in ['group by', 'order by', 'limit', 'offset', 'having']:
                    clause_pos = query.find(' ' + clause + ' ')
                    if clause_pos > -1:
                        # Insert condition before this clause in the original query
                        original_query = original_query[:clause_pos] + condition + original_query[clause_pos:]
                        break
                else:
                    # No such clauses found, append at the end
                    original_query += condition
                    
            return original_query, query
            
        except Exception as e:
            self.env.cr.rollback()  # Important: rollback on error
            raise ValidationError(f'Invalid SQL query:\n{str(e)}')
    
    def _execute_query_and_get_value(self, original_query, query):
        """Execute the query and return the appropriate value"""
        self.env.cr.execute(original_query)
        
        aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
        pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\("
        match = re.search(pattern, query, re.IGNORECASE)
        
        if match:
            result = self.env.cr.fetchone()
            return str(result[0]) if result and result[0] is not None else '0'
        else:
            records = self.env.cr.fetchall()
            return str(len(records)) if records else '0'
   
    @api.model
    def create(self, vals):
        sql_query = vals.get('sql_query')
        
        if sql_query:
            try:
                original_query, query = self._prepare_and_validate_query(sql_query)
                # We don't set vals['val'] here since it's a computed field
                
                # Test if the query actually executes correctly
                self._execute_query_and_get_value(original_query, query)
            except Exception as e:
                # Only raise validation error for the SQL query here
                raise ValidationError(f'Invalid SQL query:\n{str(e)}')
        
        return super(Statistic, self).create(vals)
    
    def write(self, vals):
        # If updating SQL query, validate it before calling super
        if 'sql_query' in vals:
            sql_query = vals['sql_query']
            try:
                original_query, query = self._prepare_and_validate_query(sql_query)
                # Test if the query actually executes correctly
                self._execute_query_and_get_value(original_query, query)
            except Exception as e:
                # Only raise validation error for the SQL query here
                raise ValidationError(f'Invalid SQL query:\n{str(e)}')
                
        return super(Statistic, self).write(vals)

    @api.depends('sql_query')
    def _compute_val(self):
        for record in self:
            if not record.sql_query:
                record.val = '0'
                continue
                
            try:
                original_query, query = record._prepare_and_validate_query(record.sql_query)
                if original_query:
                    record.val = record._execute_query_and_get_value(original_query, query)
            except Exception as e:
                record.val = 'Error'
                # Don't rollback here as it can interfere with the form view
                _logger.error(f"Error computing value for stat {record.name}: {str(e)}")
    
    @api.onchange('sql_query')
    def _onchange_sql_query(self):
        # This will update the field in the UI before saving
        self._compute_val()
  