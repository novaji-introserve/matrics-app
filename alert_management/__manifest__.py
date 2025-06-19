# -*- coding: utf-8 -*-
{
    'name': "Alert Management",

 'summary': """
    Comprehensive alert management system for tracking, scheduling, and notifying users of critical tasks or deadlines.
""",

'description': """
    This module provides a robust alert management system, allowing users to define, track, and manage alerts. 
    Alerts can be scheduled based on various triggers such as dates, task deadlines, or other important events.
    Users are notified through multiple channels (email, in-app notifications, etc.), ensuring timely awareness of critical events.
    Key features include:
    - Customizable alert triggers
    - Scheduling and escalation options
    - User notifications and reminders
    - Logs and audit trails for alert actions
    - Integration with other Odoo apps for workflow automation
    This module is ideal for managing reminders, task follow-ups, and ensuring deadlines are met.
"""
,

    'author': "Novaji Introserve",
    'website': "https://novajii.com",
    "sequence": -1000004,

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'icomply',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base','compliance_management', 'mail', 'web', 'bus', 'contacts', 'hr', 'access_apps', 'muk_web_theme_default_sidebar_invisible', 'web_field_slider', 'spreadsheet_dashboard', 'hide_powered_by_odoo', 'hide_menu_user','web_widget_numeric_step','legion_hide_odoo','base_automation'],
    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'views/alert_group.xml',
        'views/alert_history.xml',
        'views/alert_rules_view.xml',
        'views/control_officer.xml',
        'views/frequency_view.xml',
        'views/mail_template.xml',
        'views/sql_view.xml', 

        # demo data

        'data/hr/hr_dpt.xml',
        'data/hr/hr_job.xml',
        'data/alert.xml',
        # 'data/hr/hr_employee.xml',
        # 'data/users.xml',

        # menus
        'views/menus.xml',

        # cron job
        'data/schedule/cron_job.xml',
        # mail conf
        # 'data/email_smtp.xml',
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
    "installable": True,
    "application": True,
    "auto_install": False,
}
