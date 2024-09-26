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
    'depends': ['base',  'mail',],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'views/views.xml',
        'views/templates.xml',
         'views/mail_mail_views.xml'
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
    "installable": True,
    "application": True,
    "auto_install": False,
}
