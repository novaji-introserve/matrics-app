# -*- coding: utf-8 -*-
{
    'name': 'URL Encryption',
    'version': '1.0.0',
    'category': 'icomply',
    'summary': 'Encrypt sensitive URL parameters to prevent manipulation',
    'description': '''
        This module encrypts sensitive URL parameters like action IDs, menu IDs, etc.
        to prevent users from seeing or manipulating them directly in the URL.
    ''',
    'author': "Olumide Awodeji (Synth corp)",
    'website': "https://cybercraftsmen.tech",
    'depends': ['web', 'base'],
    'external_dependencies': {
        'python': ['cryptography'],
    },
    'data': [
        'security/ir.model.access.csv',
        'data/config.xml',
    ],
    'assets': {
        'web.assets_backend': [
            'url_encryption/static/src/js/url_encryption.js',
            'url_encryption/static/src/js/url_encryption_service.js',
            'url_encryption/static/src/js/url_masking.js',
            'url_encryption/static/src/js/navigation_interceptor.js',
            'url_encryption/static/src/js/action_hook.js',
            'url_encryption/static/src/js/rpc_handler.js',
        ],
    },
    
    'license': 'LGPL-3',
    'installable': True,
    'auto_install': False,
    'application': False,
    'price': 49.99,
    'currency': 'EUR',
    'images': ['static/description/banner.png'],
}
