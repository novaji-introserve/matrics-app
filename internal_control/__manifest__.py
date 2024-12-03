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
<<<<<<< HEAD
    'depends': ['base', 'contacts', 'hr', 'access_apps', 'muk_web_theme_default_sidebar_invisible', 'web_field_slider', 'spreadsheet_dashboard', 'hide_powered_by_odoo', 'hide_menu_user','web_widget_numeric_step','legion_hide_odoo','base_automation', 'web', 'website', 'icomply_dashboard'],
=======
    'depends': ['icomply_dashboard', 'compliance_management'],
>>>>>>> main

    # always loaded
    'data': [
        'security/ir.model.access.csv',
<<<<<<< HEAD
        'views/menus.xml',
        'views/alert_rules_tree.xml',
        'views/frequency_tree.xml',
        'views/tinymce.xml',
=======
        'views/actions.xml',
        'views/alert_rules_view.xml',
        'views/alert_group_view.xml',
        'views/alert_history.xml',
        'views/account_monitoring_views.xml',
        'views/transaction_rule.xml',
        'views/transaction_monitoring_views.xml',
        'views/tinymce.xml',
        'views/mail_template.xml',
        'views/fraud_monitoring_views.xml',
        'views/frequency_view.xml',
        'views/process_view.xml',
        'views/process_category_view.xml',
        'views/branch_view.xml',
        'views/department.xml',
        # 'views/user_profile.xml',
        'views/cron_job.xml',
        'views/sql_view.xml', 
        'views/emailbranch.xml',
        'views/menus.xml',
>>>>>>> main
        
        
    ],

    'images': [
    'static/description/icon.png',
    ],
   

    "installable": True,
    "application": True,
    "auto_install": False,
<<<<<<< HEAD
    "assets":{
        'web.assets_backend_legacy_lazy': [
=======
    'license': 'LGPL-3',
    "assets":{
        'web.assets_backend': [
           "internal_control/static/src/components/editor.js",
           "internal_control/static/src/components/editor.css",
<<<<<<< HEAD
           "internal_control/static/src/components/internal_control.js",
           "internal_control/static/src/css/style.css"
=======
>>>>>>> main
           "internal_control/static/src/components/internal_control.js"
>>>>>>> main
        ]
    }
}
