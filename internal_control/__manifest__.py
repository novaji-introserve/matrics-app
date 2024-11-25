# -*- coding: utf-8 -*-
{
    'name': "internal_control",

    'summary': """
        Internal Control Monitoring For Financial Institutions""",

    'description': """
           For Transaction monitoring    """,

    'author': "Novaji Introserve Ltd",
    'website': "https://www.novajii.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'iComply',
    'version': '0.3',

    # any module necessary for this one to work correctly
    'depends': ['icomply_dashboard', 'compliance_management'],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'views/actions.xml',
        'views/transaction_monitoring_views.xml',
        'views/account_monitoring_views.xml',
        'views/fraud_monitoring_views.xml',
        # 'data/cron_data.xml',
        'views/alert_rules.xml',
        'views/alert_group.xml',
        'views/process.xml',
        'views/process_category.xml',
        'views/alert_frequency.xml',
        'views/transaction_rule.xml',
        'views/tinymce.xml',
        'views/menus.xml',
        
    ],

    'images': [
    'static/description/icon.png',
    ],
   

    "installable": True,
    "application": True,
    "auto_install": False,
    'license': 'LGPL-3',
    "assets":{
        'web.assets_backend_legacy_lazy': [
           "internal_control/static/src/components/internal_control.js"
        ]
    }
}
