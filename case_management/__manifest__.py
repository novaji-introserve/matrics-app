# -*- coding: utf-8 -*-
{
    'name': 'Case Management',
    'version': '2.0',
    'summary': 'A module for managing cases and issues',
    'description': """This module helps in managing cases.""",
    'author': 'Novaji',
    'website': 'https://novajii.com',
    "category": "iComply",
    # any module necessary for this one to work correctly
    'depends': ['base','web', 'mail', 'alert_management'],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'security/security.xml',
        'data/demo/exception_process_type.xml',
        'data/demo/exception_process.xml',
        'data/demo/settings.xml',
        'views/case.xml',
        'views/menu_items.xml',
        'views/exception.xml',
        'views/settings.xml',
        'data/sequence/case_sequence.xml',
        'data/emails/case_template.xml',
        'data/schedules/case_schedules.xml'
    ],
    # only loaded in demonstration mode
    'demo': [],
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
    'icon': 'case_management/static/description/icon.png',
    "assets": {"web.assets_backend": [
        'case_management/static/src/css/style.css',
        'case_management/static/src/components/case_dashboard/dashboard.xml',
        'case_management/static/src/components/case_dashboard/dashboard.js',
        'case_management/static/src/components/chart_renderer/chart_renderer.js',
        'case_management/static/src/components/chart_renderer/chart_renderer.xml',
        'case_management/static/src/components/kpi_card/kpi_card.js',
        'case_management/static/src/components/kpi_card/kpi_card.xml',

    ]},
}
