# -*- coding: utf-8 -*-

from odoo import SUPERUSER_ID, api, fields

INTERNAL_CONTROL_SCOPE = "transaction_monitoring"
INTERNAL_CONTROL_CHART_CODES = (
    "transactions_by_branch",
    "transactions_by_currency",
    "transaction_volume_by_currency",
)
INTERNAL_CONTROL_STAT_CODES = (
    "tm_total_transactions",
    "tm_transaction_volume",
    "tm_high_risk_transactions",
    "tm_high_risk_accounts",
)

CHART_DEFINITIONS = [
    {
        "name": "All Transactions by Branch",
        "code": "transactions_by_branch",
        "description": "Transaction counts by branch for the selected period.",
        "display_summary": "Bar chart showing how transaction volume is distributed across branches in the selected review window.",
        "display_order": 5,
        "scope": INTERNAL_CONTROL_SCOPE,
        "is_visible": True,
        "refresh_mode": "scheduled",
        "cache_ttl_minutes": 60,
        "chart_type": "bar",
        "query": """
            SELECT rb.id AS branch_id, COALESCE(rb.name, 'Unassigned') AS name, COUNT(rct.id) AS transaction_count
            FROM res_customer_transaction rct
            LEFT JOIN res_branch rb ON rb.id = rct.branch_id
            WHERE rct.date_created >= (CURRENT_DATE - INTERVAL '30 days')
              AND rct.date_created < (CURRENT_DATE + INTERVAL '1 day')
            GROUP BY rb.id, rb.name
            ORDER BY transaction_count DESC, name ASC
            LIMIT 20
        """,
        "color_scheme": "brown",
        "x_axis_field": "name",
        "y_axis_field": "transaction_count",
        "navigation_value_field": "branch_id",
        "branch_filter": True,
        "date_filter": True,
        "date_field": "rct.date_created",
        "branch_field": "rct.branch_id",
        "column": "6",
        "state": "active",
        "navigation_filter_field": "branch_id",
        "navigation_domain": "[]",
        "apply_dashboard_date_filter": True,
        "navigation_date_field": "date_created",
        "apply_dashboard_branch_filter": True,
        "navigation_branch_field": "branch_id",
        "target_model_xmlid": "compliance_management.model_res_customer_transaction",
    },
    {
        "name": "All Transactions by Currency",
        "code": "transactions_by_currency",
        "description": "Transaction counts by currency for the selected period.",
        "display_summary": "Bar chart showing the currency mix of transaction activity in the selected review window.",
        "display_order": 6,
        "scope": INTERNAL_CONTROL_SCOPE,
        "is_visible": True,
        "refresh_mode": "scheduled",
        "cache_ttl_minutes": 60,
        "chart_type": "bar",
        "query": """
            SELECT COALESCE(rct.currency_id, 0) AS currency_id,
                   COALESCE(rc.name, rct.currency, 'Unknown') AS name,
                   COUNT(rct.id) AS transaction_count
            FROM res_customer_transaction rct
            LEFT JOIN res_currency rc ON rc.id = rct.currency_id
            WHERE rct.date_created >= (CURRENT_DATE - INTERVAL '30 days')
              AND rct.date_created < (CURRENT_DATE + INTERVAL '1 day')
            GROUP BY COALESCE(rct.currency_id, 0), COALESCE(rc.name, rct.currency, 'Unknown')
            ORDER BY transaction_count DESC, name ASC
            LIMIT 20
        """,
        "color_scheme": "cool",
        "x_axis_field": "name",
        "y_axis_field": "transaction_count",
        "navigation_value_field": "currency_id",
        "branch_filter": True,
        "date_filter": True,
        "date_field": "rct.date_created",
        "branch_field": "rct.branch_id",
        "column": "6",
        "state": "active",
        "navigation_filter_field": "currency_id",
        "navigation_domain": "[]",
        "apply_dashboard_date_filter": True,
        "navigation_date_field": "date_created",
        "apply_dashboard_branch_filter": True,
        "navigation_branch_field": "branch_id",
        "target_model_xmlid": "compliance_management.model_res_customer_transaction",
    },
    {
        "name": "All Transactions Volume by Currency",
        "code": "transaction_volume_by_currency",
        "description": "Total transaction amount by currency for the selected period.",
        "display_summary": "Line chart showing total transaction value by currency across the selected review window.",
        "display_order": 7,
        "scope": INTERNAL_CONTROL_SCOPE,
        "is_visible": True,
        "refresh_mode": "scheduled",
        "cache_ttl_minutes": 60,
        "chart_type": "line",
        "query": """
            SELECT COALESCE(rct.currency_id, 0) AS currency_id,
                   COALESCE(rc.name, rct.currency, 'Unknown') AS name,
                   COALESCE(SUM(rct.amount), 0) AS total_amount
            FROM res_customer_transaction rct
            LEFT JOIN res_currency rc ON rc.id = rct.currency_id
            WHERE rct.date_created >= (CURRENT_DATE - INTERVAL '30 days')
              AND rct.date_created < (CURRENT_DATE + INTERVAL '1 day')
            GROUP BY COALESCE(rct.currency_id, 0), COALESCE(rc.name, rct.currency, 'Unknown')
            ORDER BY name ASC
            LIMIT 20
        """,
        "color_scheme": "default",
        "x_axis_field": "name",
        "y_axis_field": "total_amount",
        "navigation_value_field": "currency_id",
        "branch_filter": True,
        "date_filter": True,
        "date_field": "rct.date_created",
        "branch_field": "rct.branch_id",
        "column": "6",
        "state": "active",
        "navigation_filter_field": "currency_id",
        "navigation_domain": "[]",
        "apply_dashboard_date_filter": True,
        "navigation_date_field": "date_created",
        "apply_dashboard_branch_filter": True,
        "navigation_branch_field": "branch_id",
        "target_model_xmlid": "compliance_management.model_res_customer_transaction",
    },
]


def _sync_internal_control_dashboard_scope(env):
    now = fields.Datetime.now()
    env.cr.execute(
        """
        UPDATE res_dashboard_charts
        SET scope = %s,
            cached_payload = NULL,
            cache_computed_at = NULL,
            cache_expires_at = NULL,
            write_uid = %s,
            write_date = %s
        WHERE code IN %s
        """,
        (INTERNAL_CONTROL_SCOPE, SUPERUSER_ID, now, INTERNAL_CONTROL_CHART_CODES),
    )
    env.cr.execute(
        """
        UPDATE res_compliance_stat
        SET scope = %s
        WHERE code IN %s
        """,
        (INTERNAL_CONTROL_SCOPE, INTERNAL_CONTROL_STAT_CODES),
    )


def _seed_dashboard_charts(env):
    _sync_internal_control_dashboard_scope(env)
    for definition in CHART_DEFINITIONS:
        values = dict(definition)
        target_model = env.ref(values.pop("target_model_xmlid"), raise_if_not_found=False)
        if not target_model:
            continue
        now = fields.Datetime.now()
        env.cr.execute(
            """
            SELECT id
            FROM res_dashboard_charts
            WHERE code = %(code)s
            ORDER BY id
            LIMIT 1
            """,
            {
                "code": values["code"],
            },
        )
        existing = env.cr.fetchone()
        params = {
            **values,
            "target_model_id": target_model.id,
            "user_id": SUPERUSER_ID,
            "now": now,
        }
        if existing:
            params["id"] = existing[0]
            env.cr.execute(
                """
                UPDATE res_dashboard_charts
                SET name = %(name)s,
                    code = %(code)s,
                    description = %(description)s,
                    display_summary = %(display_summary)s,
                    display_order = %(display_order)s,
                    scope = %(scope)s,
                    is_visible = %(is_visible)s,
                    refresh_mode = %(refresh_mode)s,
                    cache_ttl_minutes = %(cache_ttl_minutes)s,
                    chart_type = %(chart_type)s,
                    query = %(query)s,
                    color_scheme = %(color_scheme)s,
                    x_axis_field = %(x_axis_field)s,
                    y_axis_field = %(y_axis_field)s,
                    branch_filter = %(branch_filter)s,
                    date_field = %(date_field)s,
                    branch_field = %(branch_field)s,
                    "column" = %(column)s,
                    state = %(state)s,
                    target_model_id = %(target_model_id)s,
                    navigation_filter_field = %(navigation_filter_field)s,
                    navigation_value_field = %(navigation_value_field)s,
                    navigation_domain = %(navigation_domain)s,
                    apply_dashboard_date_filter = %(apply_dashboard_date_filter)s,
                    navigation_date_field = %(navigation_date_field)s,
                    apply_dashboard_branch_filter = %(apply_dashboard_branch_filter)s,
                    navigation_branch_field = %(navigation_branch_field)s,
                    date_filter = %(date_filter)s,
                    active = TRUE,
                    cached_payload = NULL,
                    cache_computed_at = NULL,
                    cache_expires_at = NULL,
                    write_uid = %(user_id)s,
                    write_date = %(now)s
                WHERE id = %(id)s
                """,
                params,
            )
            continue
        env.cr.execute(
            """
            INSERT INTO res_dashboard_charts (
                name, code, description, display_summary, display_order, scope,
                is_visible, refresh_mode, cache_ttl_minutes, chart_type, query,
                color_scheme, x_axis_field, y_axis_field, branch_filter, date_field,
                branch_field, "column", state, target_model_id, navigation_filter_field,
                navigation_value_field, navigation_domain, apply_dashboard_date_filter,
                navigation_date_field, apply_dashboard_branch_filter, navigation_branch_field,
                date_filter, active, create_uid, create_date, write_uid, write_date
            ) VALUES (
                %(name)s, %(code)s, %(description)s, %(display_summary)s, %(display_order)s, %(scope)s,
                %(is_visible)s, %(refresh_mode)s, %(cache_ttl_minutes)s, %(chart_type)s, %(query)s,
                %(color_scheme)s, %(x_axis_field)s, %(y_axis_field)s, %(branch_filter)s, %(date_field)s,
                %(branch_field)s, %(column)s, %(state)s, %(target_model_id)s, %(navigation_filter_field)s,
                %(navigation_value_field)s, %(navigation_domain)s, %(apply_dashboard_date_filter)s,
                %(navigation_date_field)s, %(apply_dashboard_branch_filter)s, %(navigation_branch_field)s,
                %(date_filter)s, TRUE, %(user_id)s, %(now)s, %(user_id)s, %(now)s
            )
            ON CONFLICT (code) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                display_summary = EXCLUDED.display_summary,
                display_order = EXCLUDED.display_order,
                scope = EXCLUDED.scope,
                is_visible = EXCLUDED.is_visible,
                refresh_mode = EXCLUDED.refresh_mode,
                cache_ttl_minutes = EXCLUDED.cache_ttl_minutes,
                chart_type = EXCLUDED.chart_type,
                query = EXCLUDED.query,
                color_scheme = EXCLUDED.color_scheme,
                x_axis_field = EXCLUDED.x_axis_field,
                y_axis_field = EXCLUDED.y_axis_field,
                branch_filter = EXCLUDED.branch_filter,
                date_field = EXCLUDED.date_field,
                branch_field = EXCLUDED.branch_field,
                "column" = EXCLUDED."column",
                state = EXCLUDED.state,
                target_model_id = EXCLUDED.target_model_id,
                navigation_filter_field = EXCLUDED.navigation_filter_field,
                navigation_value_field = EXCLUDED.navigation_value_field,
                navigation_domain = EXCLUDED.navigation_domain,
                apply_dashboard_date_filter = EXCLUDED.apply_dashboard_date_filter,
                navigation_date_field = EXCLUDED.navigation_date_field,
                apply_dashboard_branch_filter = EXCLUDED.apply_dashboard_branch_filter,
                navigation_branch_field = EXCLUDED.navigation_branch_field,
                date_filter = EXCLUDED.date_filter,
                active = EXCLUDED.active,
                write_uid = EXCLUDED.write_uid,
                write_date = EXCLUDED.write_date
            """,
            {
                **params,
            },
        )


def post_init_hook(cr, registry):
    env = api.Environment(cr, SUPERUSER_ID, {})
    _sync_internal_control_dashboard_scope(env)
    _seed_dashboard_charts(env)
    cr.commit()
