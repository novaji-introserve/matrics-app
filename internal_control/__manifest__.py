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
    'depends': ['base', 'contacts', 'hr', 'access_apps', 'muk_web_theme_default_sidebar_invisible', 'web_field_slider', 'spreadsheet_dashboard', 'hide_powered_by_odoo', 'hide_menu_user','web_widget_numeric_step','legion_hide_odoo','base_automation', 'icomply_dashboard', 'web_editor'],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'views/menus.xml',
        'views/alert_rules_view.xml',
        'views/alert_group_view.xml',
        'views/alert_status_view.xml',
        'views/frequency_view.xml',
        'views/process_view.xml',
        'views/process_category_view.xml',
        'views/branch_view.xml',
        'views/cron_job.xml',
        'views/mail_template.xml',
        'data/email_smtp.xml',
        
    ],

    'images': [
    'static/description/icon.png',
    ],
   

    "installable": True,
    "application": True,
    "auto_install": False,
    "assets":{
        'web.assets_backend': [
           "internal_control/static/src/components/editor.js",
           "internal_control/static/src/components/editor.css",
           "internal_control/static/src/components/internal_control.js"
        ]
    }
}
