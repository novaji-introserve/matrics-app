# -*- coding: utf-8 -*-

from odoo import models, fields, api,_
from odoo import exceptions
import psycopg2
import re

class ResCharts(models.Model):
    _name = 'res.dashboard.charts'
    _description = 'Dashboard Charts'
    _sql_constraints = [
        ('uniq_chart_title', 'unique(title)',
         "Title already exists. It must be unique!"),
    ]
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char('Chart Name', required=True)
    description = fields.Text('Description')
    chart_type = fields.Selection([
        ('bar', 'Bar Chart'),
        ('line', 'Line Chart'),
        ('pie', 'Pie Chart'),
        ('doughnut', 'Doughnut Chart'),
        ('radar', 'Radar Chart'),
        ('polarArea', 'Polar Area Chart'),
    ], string='Chart Type', required=True)
    
    query = fields.Text('SQL Query', required=True, 
                        help="SQL query must return at least two columns: label and value")
    color_scheme = fields.Selection([
        ('default', 'Default'),
        ('cool', 'Cool Colors'),
        ('warm', 'Warm Colors'),
        ('rainbow', 'Rainbow'),
    ], string="Color Scheme", default='default')
    
    x_axis_field = fields.Char('X-Axis Field', help="Column name to use for X-axis labels")
    y_axis_field = fields.Char('Y-Axis Field', help="Column name to use for Y-axis values")
    
    date_filter = fields.Boolean('Enable Date Filter', default=True)
    date_field = fields.Char('Date Field Name', help="Name of date field in query to filter by")
    
    state = fields.Selection(
    [("active", "Active"), ("inactive", "inactive" )],
    default="active",  # The default value is an integer (1)
    string="State",
    tracking=True
    )

    @api.constrains('query')
    def _check_query_safety(self):
        """Validate query for safety"""
        for chart in self:
            print(chart.query)
            if not chart.query:
                continue
                
            # Check for dangerous SQL operations
            dangerous_patterns = [
                r'\b(CREATE|DROP|ALTER|TRUNCATE)\s+(TABLE|DATABASE|INDEX|VIEW)\b',
                r'\bINSERT\s+INTO\b',
                r'\bUPDATE\s+\w+\s+SET\b',
                r'\bDELETE\s+FROM\b',
                r'pg_\w+',
                r'information_schema\.\w+',
                r'--.*', # SQL Comments
                r'/\*.*\*/' #SQL Comments
            ]
                        
            for pattern in dangerous_patterns:
                if re.search(pattern, chart.query, re.IGNORECASE):
                    raise exceptions.ValidationError(_("Query contains potentially unsafe operations. Pattern detected: %s") % pattern)
            try:    
                self.env.cr.execute(chart.query)
                # Ensure the query is SELECT only
                if chart.query.strip().upper().startswith('SELECT'):
                    pass
                else:
                    raise exceptions.ValidationError(_("Query must start with SELECT statement"))
            except psycopg2.errors.SyntaxError as e:
                raise exceptions.ValidationError(f"SQL syntax error: {e}")
            except psycopg2.errors.UndefinedColumn as e:
                raise exceptions.ValidationError(f"Undefined column error: {e}")
            except Exception as e:
                raise exceptions.ValidationError(f"Database error: {e}")

    
    # def get_chart_data(self, start_date=None, end_date=None):
    #     self.ensure_one()
        
    #     try:
    #         params = []
    #         query = self.query
            
    #         # Apply date filter if enabled and dates provided
    #         if self.date_filter and start_date and end_date and self.date_field:
    #             if '%s' in query:
    #                 params = [start_date, end_date]
    #             else:
    #                 # If no parameters in query, add WHERE clause
    #                 if 'WHERE' in query.upper():
    #                     query = query.replace('WHERE', f"WHERE {self.date_field} BETWEEN %s AND %s AND ", 1)
    #                     params = [start_date, end_date]
    #                 else:
    #                     # Add WHERE clause before GROUP BY, ORDER BY, or at the end
    #                     for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']:
    #                         if clause in query.upper():
    #                             position = query.upper().find(clause)
    #                             query = query[:position] + f" WHERE {self.date_field} BETWEEN %s AND %s " + query[position:]
    #                             params = [start_date, end_date]
    #                             break
    #                     else:
    #                         query += f" WHERE {self.date_field} BETWEEN %s AND %s"
    #                         params = [start_date, end_date]
            
    #         self.env.cr.execute(query, params)
    #         results = self.env.cr.dictfetchall()
            
    #         if not results:
    #             return {'labels': [], 'datasets': [{'data': [], 'backgroundColor': []}]}
            
    #         # Extract labels and values
    #         x_field = self.x_axis_field or next(iter(results[0]))
    #         y_field = self.y_axis_field or next((k for k in results[0].keys() if k != x_field), None)
            
    #         if not y_field:
    #             return {'error': 'Cannot determine Y-axis field from query results'}
            
    #         labels = [str(r[x_field]) for r in results]
    #         values = [float(r[y_field]) if r[y_field] is not None else 0 for r in results]
            
    #         # Generate colors based on selected scheme
    #         colors = self._generate_colors(len(results))
            
    #         return {
    #             'labels': labels,
    #             'datasets': [{
    #                 'data': values,
    #                 'backgroundColor': colors,
    #                 'borderColor': colors if self.chart_type in ['line', 'radar'] else [],
    #                 'borderWidth': 1
    #             }]
    #         }
        
    #     except Exception as e:
    #         return {'error': str(e)}
    
    # def _generate_colors(self, count):
    #     """Generate colors based on the selected color scheme"""
    #     if self.color_scheme == 'cool':
    #         base_colors = ['#3366cc', '#66ccff', '#6666ff', '#3333cc', '#000099']
    #     elif self.color_scheme == 'warm':
    #         base_colors = ['#ff6600', '#ff9933', '#ffcc66', '#ff0000', '#cc0000']
    #     elif self.color_scheme == 'rainbow':
    #         base_colors = ['#ff0000', '#ff9900', '#ffff00', '#00ff00', '#0099ff', '#6633ff']
    #     else:  # default
    #         base_colors = ['#3366cc', '#dc3912', '#ff9900', '#109618', '#990099', '#0099c6']
        
    #     colors = []
    #     for i in range(count):
    #         colors.append(base_colors[i % len(base_colors)])
    #     return colors

