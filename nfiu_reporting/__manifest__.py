# -*- coding: utf-8 -*-
{
    'name': "NFIU GoAML Reporting",

    'summary': """
         This module allows you to:
        * Manage financial transaction reports
        * Generate XML reports compliant with NFIU goAML schema
        * Validate XML against XSD schema
        * Submit reports to financial intelligence unit
        """,

    'description': """
        Generate NFIU goAML compliant XML reports for financial transactions
    """,

    'author': "Novaji Intrserve Ltd",
    'website': "https://www.novajii.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'Uncategorized',
    'version': '0.2',

    # any module necessary for this one to work correctly
    'depends': ['base','compliance_management','regulatory_reports','hr','case_management'],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'data/bank_account_type_data.xml',
        'data/interbank_dashboard_data.xml',
        'views/interbank_dashboard.xml',
        'views/nfiu_person_views.xml',
        'views/nfiu_entity_views.xml',
        'views/nfiu_entity_director_views.xml',
        'views/nfiu_indicators_views.xml',
        'views/nfiu_currency_threshold_views.xml',
        'views/res_config_settings_views.xml',
        'views/nfiu_report_views.xml',
        'views/nfiu_export_wizard_views.xml',
        'views/nfiu_transaction_views.xml',
        'views/nfiu_data.xml',
        'data/data.xml',
        'views/bank_transaction_views.xml',
        'views/menuitems.xml',
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
    'assets': {
        'web.assets_backend': [
            'nfiu_reporting/static/src/components/interbank_dashboard/chart/js/chart.js',
            'nfiu_reporting/static/src/components/interbank_dashboard/chart/xml/chart.xml',
            'nfiu_reporting/static/src/components/interbank_dashboard/dashboard/js/dashboard.js',
            'nfiu_reporting/static/src/components/interbank_dashboard/dashboard/xml/dashboard.xml',
            'nfiu_reporting/static/src/components/interbank_dashboard/dashboard/css/dashboard.css',
        ],
    },
    'license': 'LGPL-3',
}
