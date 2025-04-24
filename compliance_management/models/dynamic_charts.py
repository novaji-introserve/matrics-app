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
    ], string="Color Scheme", default='cool')
    
    x_axis_field = fields.Char('X-Axis Field', help="Column name to use for X-axis labels", required=True)
    y_axis_field = fields.Char('Y-Axis Field', help="Column name to use for Y-axis values",required=True)
    
    branch_filter = fields.Boolean('Enable Branch Filter', default=True)
    date_field = fields.Char('Date Field Name', help="Name of date field in query to filter by",required=True)
    branch_field = fields.Char('Branch Field Name', help="Name of branch field in query to filter by")
    column = fields.Selection([
        ('1', 'One'),
        ('2', 'Two'),
        ('3', 'Three'),
        ('4', 'Four'),
        ('5', 'Five'),
        ('6', 'Six'),
        ('7', 'Seven'),
        ('8', 'Eight'),
        ('9', 'Nine'),
        ('10', 'Ten'),
        ('11', 'Eleven'),
        ('12', 'Twelve'),
    ], string="Columns", default='4')
    
    state = fields.Selection(
    [("active", "Active"), ("inactive", "inactive" )],
    default="active",  # The default value is an integer (1)
    string="State",
    tracking=True
    )
    target_model_id = fields.Many2one(
    'ir.model',
    string='Target Model for Action',
    required=True,
    ondelete='cascade', 
    help="Select the model to use for this chart's action",
    # The domain will be defined here (as discussed previously)
    )
    target_model = fields.Char(
        related='target_model_id.model',
        string='Model Technical Name',
        store=True
    )
    # Add this field to your ResCharts model
    domain_field_id = fields.Many2one(
        'ir.model.fields',
        string='Domain Fields',
        help="Select fields from the target model to use as domain filters",
        domain="[('model_id', '=', target_model_id)]"
    )
    domain_field = fields.Char(
    related='domain_field_id.name',
    string='Domain Field Name',
    store=True
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

    