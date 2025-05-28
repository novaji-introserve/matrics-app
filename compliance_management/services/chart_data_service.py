import logging
import re
from odoo import tools
from odoo import api
from odoo.http import request
from ..utils.color_generator import ColorGenerator

_logger = logging.getLogger(__name__)

class ChartDataService:
    """Service for generating and processing chart data with improved security and performance"""

    def __init__(self):
        from ..utils.color_generator import ColorGenerator
        from ..services.branch_security import ChartSecurityService

        self.color_generator = ColorGenerator()
        self.security_service = ChartSecurityService()

    @staticmethod
    def _get_view_name_for_chart(chart_id):
        """Generate a consistent view name for a chart - accepts either chart object or ID"""
        if isinstance(chart_id, int):
            return f"dashboard_chart_view_{chart_id}"
        else:
            return f"dashboard_chart_view_{chart_id.id}"

    def _extract_chart_data(self, chart, results, query):
        """Extract chart data from query results"""
        if not results:
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
        x_field = chart.x_axis_field or "name"
        if x_field not in results[0]:
            x_field = next(iter(results[0]))
        y_field = chart.y_axis_field
        if not y_field or y_field not in results[0]:
            for field in results[0].keys():
                if (
                    field != x_field
                    and field != "id"
                    and isinstance(results[0][field], (int, float))
                ):
                    y_field = field
                    break
            if not y_field:
                y_field = next(
                    (k for k in results[0].keys() if k != x_field and k != "id"), None
                )
        id_field = "id"
        if id_field not in results[0]:
            id_field = next((k for k in results[0].keys() if k.endswith("_id")), None)
        ids = (
            [r.get(id_field) for r in results]
            if id_field and id_field in results[0]
            else []
        )
        if not y_field:
            _logger.error(f"Cannot determine Y-axis field for chart {chart.id}")
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "error": "Cannot determine Y-axis field from query results",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
        labels = [str(r.get(x_field, "")) for r in results]
        values = []
        for r in results:
            val = r.get(y_field)
            try:
                values.append(float(val) if val is not None else 0)
            except (ValueError, TypeError):
                values.append(0)
        additional_domain = []
        if chart.query:
            original_query = chart.query.upper()
            where_clause = ""
            if "WHERE" in original_query:
                where_start = original_query.find("WHERE") + 5
                where_end = -1
                for clause in ["GROUP BY", "ORDER BY", "LIMIT"]:
                    clause_pos = original_query.find(clause, where_start)
                    if clause_pos > -1 and (where_end == -1 or clause_pos < where_end):
                        where_end = clause_pos
                where_clause = original_query[
                    where_start : where_end if where_end > -1 else None
                ].strip()
                if where_clause:
                    conditions = where_clause.split("AND")
                    for condition in conditions:
                        condition = condition.strip()
                        if "=" in condition and "." in condition:
                            parts = condition.split("=")
                            field_part = parts[0].strip()
                            value_part = parts[1].strip()
                            if "." in field_part:
                                table, field = field_part.split(".")
                                if "'" in value_part:
                                    value = value_part.replace("'", "").lower()
                                    additional_domain.append(
                                        (field.lower(), "=", value)
                                    )
                                elif value_part.isdigit():
                                    value = int(value_part)
                                    additional_domain.append(
                                        (field.lower(), "=", value)
                                    )
        color_generator = ColorGenerator()
        colors = color_generator._generate_colors(chart.color_scheme, len(results))
        return {
            "id": chart.id,
            "title": chart.name,
            "type": chart.chart_type,
            "model_name": chart.target_model,
            "filter": chart.domain_field,
            "column": chart.column,
            "labels": labels,
            "ids": ids,
            "datefield": chart.date_field,
            "additional_domain": additional_domain,
            "datasets": [
                {
                    "data": values,
                    "backgroundColor": colors,
                    "borderColor": (
                        colors if chart.chart_type in ["line", "radar"] else []
                    ),
                    "borderWidth": 1,
                }
            ],
        }

    def _is_safe_query(self, query):
        """Check if a query is safe to execute"""
        if not query:
            return False
        if ";" in query and not query.strip().endswith(";"):
            return False
        unsafe_commands = [
            "UPDATE",
            "DELETE",
            "INSERT",
            "ALTER",
            "DROP",
            "TRUNCATE",
            "CREATE",
            "GRANT",
            "REVOKE",
            "SET ROLE",
        ]
        for cmd in unsafe_commands:
            if re.search(r"\b" + cmd + r"\b", query, re.IGNORECASE):
                return False
        return True
