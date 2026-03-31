# -*- coding: utf-8 -*-
{
    "name": "FSDH Addons - FSDH IMPLEMENTATIONS",
    "summary": "Alert statistics and dashboard (cards, drill-down views).",
    "version": "16.0.1.0.0",
    "category": "icomply",
    "author": "Novaji Introserve",
    "depends": [
        "alert_management",
        "compliance_management",
        "web",
    ],
    "data": [
        "security/groups.xml",
        "security/ir.model.access.csv",
        "views/alert_stat_views.xml",
        "views/alert_dashboard.xml",
        "views/alert_chart.xml",
        "views/escalation_period_views.xml",
        "views/menus.xml",
        # Example data removed to avoid duplicate 'total_alerts' code on re-install
        # "data/alert_stat_example.xml",
    ],
    "assets": {
        "web.assets_backend": [
            "compliance_management/static/lib/chart.umd.min.js",
            "fsdh_addons/static/src/components/alert_dashboard/css/alert_dashboard.css",
            "fsdh_addons/static/src/components/alert_dashboard/xml/alert_dashboard.xml",
            "fsdh_addons/static/src/components/alert_dashboard/js/alert_dashboard.js",
            "fsdh_addons/static/src/components/alert_chart/css/alert_chart.css",
            "fsdh_addons/static/src/components/alert_chart/xml/alert_chart.xml",
            "fsdh_addons/static/src/components/alert_chart/js/alert_chart.js",
        ],
    },
    "installable": True,
    "application": False,
    "license": "LGPL-3",
}