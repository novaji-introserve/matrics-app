from odoo import http
from odoo.http import request
import json
from datetime import datetime, timedelta

class DynamicChartController(http.Controller):
    
    @http.route('/dashboard/dynamic_charts', type='json', auth='user')
    def dynamic_charts_dashboard(self, cco, branches_id, datepicked, **kw):
        """Render the dynamic charts dashboard"""
        if not request.env.user.has_group('dynamic_charts.group_dynamic_chart_user'):
            return request.render('web.login', {})
        
        charts = request.env['dynamic.chart'].search([('active', '=', True)])
        return request.render('dynamic_charts.dashboard_template', {
            'charts': charts
        })
    
    @http.route('/dashboard/dynamic_charts/<int:chart_id>', type='json', auth='user')
    def get_chart_data(self, chart_id, cco, branches_id, datepicked, **kw):
        """Get chart data in JSON format"""
        # if not request.env.user.has_group('dynamic_charts.group_dynamic_chart_user'):
        #     return {'error': 'Access denied'}

       
        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date
        
        chart = request.env['res.dashboard.charts'].browse(chart_id)
        if not chart.exists():
            return {'error': 'Chart not found'}
        
        try:
            params = {}
            query = chart.query
            
            
            if 'WHERE' in query.upper():
                query = query.replace('WHERE', f"WHERE {chart.date_field} BETWEEN %(start_date)s AND %(end_date)s AND ", 1)
                params = {'start_date': prevDate, 'end_date': today}
            else:
                # Add WHERE clause before GROUP BY, ORDER BY, or at the end
                for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']: 
                    if clause in query.upper():
                        position = query.upper().find(clause)
                        query = query[:position] + f" WHERE {chart.date_field} BETWEEN %(start_date)s AND %(end_date)s " + query[position:]
                        params = {'start_date': prevDate, 'end_date': today}
                        break
                    else:
                        query += f" WHERE {chart.date_field} BETWEEN %(start_date)s AND %(end_date)s"
                        params = {'start_date': prevDate, 'end_date': today}
            
           
            request.env.cr.execute(query, params)
            results = request.env.cr.dictfetchall()

            if not results:
                return {'labels': [], 'datasets': [{'data': [], 'backgroundColor': []}]}
            
            # Extract labels and values
            x_field = chart.x_axis_field or next(iter(results[0]))
            y_field = chart.y_axis_field or next((k for k in results[0].keys() if k != x_field), None)

            
            if not y_field:
                return {'error': 'Cannot determine Y-axis field from query results'}
            
            labels = [str(r[x_field]) for r in results]
            values = [float(r[y_field]) if r[y_field] is not None else 0 for r in results]

            # Generate colors based on selected scheme
            colors = self._generate_colors(chart.color_scheme, len(results))

            return {
                'id': chart.id,
                'title': chart.name,
                'type': chart.chart_type,
                'labels': labels,
                'datasets': [{
                    'data': values,
                    'backgroundColor': colors,
                    'borderColor': colors if chart.chart_type in ['line', 'radar'] else [],
                    'borderWidth': 1
                }]
            }
        
        except Exception as e:
            return {'error': str(e)}
    
    def _generate_colors(self, color_scheme, count):
        """Generate colors based on the selected color scheme"""
        if color_scheme == 'cool':
            base_colors = ['#3366cc', '#66ccff', '#6666ff', '#3333cc', '#000099']
        elif color_scheme == 'warm':
            base_colors = ['#ff6600', '#ff9933', '#ffcc66', '#ff0000', '#cc0000']
        elif color_scheme == 'rainbow':
            base_colors = ['#ff0000', '#ff9900', '#ffff00', '#00ff00', '#0099ff', '#6633ff']
        else:  # default
            base_colors = ['#3366cc', '#dc3912', '#ff9900', '#109618', '#990099', '#0099c6']
        
        colors = []
        for i in range(count):
            colors.append(base_colors[i % len(base_colors)])
        return colors
    
    @http.route('/web/dynamic_charts/preview', type='json', auth='user')
    def preview_chart(self, chart_type, query, x_axis_field=None, y_axis_field=None, color_scheme='default'):
        """Preview chart without saving"""
        # if not request.env.user.has_group('dynamic_charts.group_dynamic_chart_manager'):
        #     return {'error': 'Access denied'}
        
        # Create a temporary chart for preview
        try:
            chart = request.env['res.dashboard.charts'].new({
                'chart_type': chart_type,
                'query': query,
                'x_axis_field': x_axis_field,
                'y_axis_field': y_axis_field,
                'color_scheme': color_scheme
            })
            
            # Run validation manually since it's a new record
            chart._check_query_safety()
            
            return self.get_chart_data(chart.id)
        except Exception as e:
            return {'error': str(e)}