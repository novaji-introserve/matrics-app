# -*- coding: utf-8 -*-

{
    'name': "Hide Inbox Chatter",
    'license': 'AGPL-3',
    'summary': """Hide Chatter and Inbox Hide Discussion Menu and chatter from Odoo""",
    'description': """
        Hide Chatter & Inbox
        Hide Discussion Menu and chatter from Odoo Tray
    """,
    'version': "16.0",
    'author': "odoo.solution.vn",
    'support': 'odoo.solution.vn@gmail.com',
    'images': ['static/description/img.png'],
    'category': 'tools',
    'depends': ['base'],
    'data': [

    ],
    'assets': {
        'web.assets_backend': [
            'hide_inbox_chatter/static/src/xml/*.xml',
        ],
    },
    'installable': True,
    'application': False,
    'auto_install': False,

}
