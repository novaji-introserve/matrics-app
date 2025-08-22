# -*- coding: utf-8 -*-
{
    'name': "Transaction Monitoring",

    'summary': """
        Transaction Monitoring For Financial Institutions""",

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
    'depends': ['icomply_dashboard', 'compliance_management', 'bi_sql_editor', 'psql_query_execute', 'alert_management'],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        # 'views/actions.xml',
        # 'views/branch_view.xml',
        # 'views/account_monitoring_views.xml',
        'views/screen_rules_cron_job.xml',
        'views/cron_schedule.xml',
        'views/transaction_rule.xml',
        'views/transaction_monitoring_views.xml',
        'views/customer.xml',
        'views/customer_account.xml',
        'views/tinymce.xml',
        'views/fraud_monitoring_views.xml',
        'views/process_view.xml',
        'views/process_category_view.xml',        
        # 'views/department.xml',
        'views/user_profile.xml',
        'views/emplpyee.xml',
        'views/sql_panel.xml', 
        'views/menus.xml',       
        'security/groups.xml',
        
     
        # cron job
        
        'data/schedule/cron_trans_screening.xml',
        'data/schedule/cron_branch_sync.xml',
        'data/schedule/account_trans_metrics.xml',
        'data/schedule/cron_sync_tran_bid.xml'
    ],

    'images': [
    'static/description/icon.png',
    ],
   

    "installable": True,
    "application": True,
    "auto_install": False,
    'license': 'LGPL-3',
    "assets":{
        'web.assets_backend': [
           "internal_control/static/src/components/**/*.js",
           "internal_control/static/src/css/**/*.css",
           "internal_control/static/src/components/**/*.xml"
        ]
    }
}
