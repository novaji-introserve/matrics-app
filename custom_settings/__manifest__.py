# -*- coding: utf-8 -*-
{
    'name': "Custom Settings Layout",

    'summary': 'Customizes the settings menu layout',
    
    'description': """
This module customizes the settings page by:
- Showing only General Settings in the sidebar
- Removing specified sections from General Settings
""",

    'author': "Olumide Awodeji (Synth corp)",
    'website': "https://cybercraftsmen.tech",

    'category': 'Technical',
    'version': '1.0.0',

    'depends': ['base', 'base_setup', 'web'],

    'data': [
        # 'views/views.xml',
    ],
    'assets': {
        'web.assets_backend': [
            'custom_settings/static/src/js/settings_form.js',
            'custom_settings/static/src/scss/custom_settings.scss',
        ],
    },
        
    'license': 'LGPL-3',
    'application': False,
    'installable': True,
    'auto_install': False,
    'price': 49.99,
    'currency': 'EUR',
    'images': ['static/description/banner.png'],
}
