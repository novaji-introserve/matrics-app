# -*- coding: utf-8 -*-
{
    'name': 'Redis Session',
    'summary': 'Redis Session',
    'description': 'Store sessions in Redis instead of the file system to increase Odoo performance.',
    'author': 'FPG',
    'category': 'Extra Tools',
    'version': '16.0.1.0.0',
    'depends': [
        'base'
    ],
    'data': [
        
    ],
    'external_dependencies': {
        'python': ['redis']
    },
    'installable': True,
    'application': False,
    'license': 'LGPL-3',
    'images': [
        'static/description/main_screenshot.png'
    ],
    'price': 15.45,
    'currency': 'USD',
}
