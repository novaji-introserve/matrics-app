from odoo import http
from odoo.http import request
import json
from datetime import datetime, timedelta

class DynamicChartController(http.Controller):
 

    def _add_where_to_query(self, query, where_clause):
        
        if 'WHERE' in query.upper():
            # Find the position of WHERE
            where_pos = query.upper().find('WHERE')
            # Get everything after WHERE (the original conditions)
            where_content = query[where_pos + 5:].strip()
            
            # Determine whether we need to add an AND
            if where_content:
                # There are existing conditions, so add AND
                new_where = f"WHERE {where_clause} AND {where_content}"
            else:
                # No existing conditions after WHERE
                new_where = f"WHERE {where_clause}"
                
            return query[:where_pos] + new_where
        
        # If no WHERE clause exists, add it before GROUP BY, ORDER BY, etc.
        for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']:
            if clause in query.upper():
                position = query.upper().find(clause)
                return query[:position] + f" WHERE {where_clause} " + query[position:]
        
        # If no clauses found, add WHERE at the end
        return query + f" WHERE {where_clause}"

    # def _add_where_to_query(self, query, where_clause):
    #     # Initialize variable to store conditions with values (as a list)
    #     conditions_with_values = []
        
    #     if 'WHERE' in query.upper():
    #         # Find the position of WHERE
    #         where_pos = query.upper().find('WHERE')
            
    #         # Find the end of the WHERE clause
    #         end_pos = len(query)
    #         for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']:
    #             clause_pos = query.upper().find(clause, where_pos)
    #             if clause_pos != -1:
    #                 end_pos = min(end_pos, clause_pos)
            
    #         # Get everything after WHERE up to the next clause (the original conditions)
    #         where_content = query[where_pos + 5:end_pos].strip()
            
    #         # Store conditions with values
    #         if where_content:
    #             # Split by AND to handle multiple conditions
    #             conditions = where_content.split(' AND ')
    #             for condition in conditions:
    #                 condition = condition.strip()
    #                 if '=' in condition:
    #                     conditions_with_values.append(condition)
            
    #         # Determine whether we need to add an AND
    #         if where_content:
    #             # There are existing conditions, so add AND
    #             new_where = f"WHERE {where_clause} AND {where_content}"
    #         else:
    #             # No existing conditions after WHERE
    #             new_where = f"WHERE {where_clause}"
                
    #         return query[:where_pos] + new_where
        
    #     # If no WHERE clause exists, add it before GROUP BY, ORDER BY, etc.
    #     for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']:
    #         if clause in query.upper():
    #             position = query.upper().find(clause)
    #             return query[:position] + f" WHERE {where_clause} " + query[position:]
        
    #     # If no clauses found, add WHERE at the end
    #     return query + f" WHERE {where_clause}"
    

    def _process_query_results(self, chart, query):

        print(query)

       
        try:
            request.env.cr.execute(query)
            results = request.env.cr.dictfetchall()
        except Exception as e:
            print(e)

        if len(results) == 0:
            return {
                'title': '',
                'type': '',
                'labels': [],
                'datasets': [{'data': [], 'backgroundColor': []}]
            }
        
        # Extract labels and values
        x_field = chart.x_axis_field or next(iter(results[0]))
        y_field = chart.y_axis_field or next((k for k in results[0].keys() if k != x_field), None)
        # Try to find an ID field - common patterns might be 'id', '{table}_id', etc.
        id_field = next((k for k in results[0].keys() if k.endswith('_id') or k == 'id'), None)

        # If no obvious ID field is found, use the first field that's not x_field or y_field
        if not id_field:
            id_field = next((k for k in results[0].keys() if k != x_field and k != y_field), None)

        # Extract the IDs if we found a suitable field
        ids = [r[id_field] if id_field else None for r in results]

    
        
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
            'model_name': chart.target_model,
            'filter': chart.domain_field,
            'labels': labels,
            'ids': ids,
            'datefield': chart.date_field,
            'datasets': [{
                'data': values,
                'backgroundColor': colors,
                'borderColor': colors if chart.chart_type in ['line', 'radar'] else [],
                'borderWidth': 1
            }]
        }

    
    @http.route('/dashboard/dynamic_charts/', type='json', auth='user')
    def get_chart_data(self, cco, branches_id, datepicked, **kw):
        """Get chart data in JSON format"""
        
        charts = request.env['res.dashboard.charts'].search([('state', '=', 'active')])

        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        chartsData = []
        
        for chart in charts:

            
            
            chart = request.env['res.dashboard.charts'].browse(chart.id)
            if not chart.exists():
                return {'error': 'Chart not found'}
            
            try:


                # Build where clause based on conditions
                where_clause = f"{chart.date_field} BETWEEN '{prevDate}' AND '{today}'"
                
                # Add branch filtering if needed
                if not cco and chart.branch_filter and branches_id and len(branches_id) > 0:
            
                    # where_clause += f" AND {chart.branch_field} IN {tuple(branches_id)}"
                    if len(branches_id) == 1:
                        where_clause += f" AND {chart.branch_field} = {branches_id[0]}"
                       
                    else:
                        where_clause += f" AND {chart.branch_field} IN {tuple(branches_id)}"
                
                
                elif not cco and chart.branch_filter and len(branches_id) == 0:
                    where_clause += " AND 1 = 0"
                

                # Modify query to include WHERE clause
                query = self._add_where_to_query(chart.query, where_clause)

                
                # Execute query and process results
                result = self._process_query_results(chart, query)

                if result['title'] == '' and result['type'] == '' and result['labels'] == []:
                    pass
                    
                else:
                    chartsData.append(result)
                    

            except Exception as e:
                return {'error': str(e)}
        
        return chartsData

              
    
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