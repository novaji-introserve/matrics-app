{
    'name': 'Console Security Protection',
    'version': '1.0.0',
    'summary': 'Block console-based JavaScript execution attempts',
    'description': """
Console Security Protection
==========================
This module protects against console-based JavaScript execution attempts while
preserving all legitimate Odoo functionality.

Features:
---------
* Detects and blocks console-based code execution
* Whitelists all legitimate Odoo JavaScript patterns
* Configurable via system parameters
* Logging mode for testing
* Can be enabled/disabled without restart
    """,
    'author': 'Novaji Introserve Ltd',
    'website': 'https://www.novajii.com',
    'category': 'iComply',
    'depends': ['base', 'web'],
    'data': [
        'data/console_security_data.xml',
        'security/ir.model.access.csv',
    ],
    'assets': {
        'web.assets_backend': [
            'console_security/static/src/js/console_protection.js',
        ],
    },
    'installable': True,
    'application': False,
    'auto_install': False,
    'license': 'LGPL-3',
}




