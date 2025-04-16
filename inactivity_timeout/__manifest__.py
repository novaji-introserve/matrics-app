# -*- coding: utf-8 -*-
{
    'name': "Inactivity Timeout",
    
    'summary': 'Automatically log out inactive users',
        
    'description': """
Inactivity Timeout
==================
This module automatically logs out users after a period of inactivity.

Features:
---------
* Configurable timeout intervals through system parameters
* Warning notification before automatic logout
* Resets timer on user activity
* Easy administration through the system settings menu
* Follows SRP and DRY principles for maintainability

Technical Information:
---------------------
* Available system parameters:
  * inactivity_timeout.timeout: Total inactivity time before logout (seconds)
  * inactivity_timeout.warning: Warning time before logout (seconds)
    """,
    
    'author': "Olumide Awodeji (Synth corp)",
    'website': "https://cybercraftsmen.tech",
    
    'category': 'Technical',
    'version': '1.0.0',
    
    'depends': ['base', 'web'],
    
    'data': [
        'data/inactivity_data.xml',
        # 'views/inactivity_views.xml',
    ],
    
    'assets': {
        'web.assets_backend': [
            'inactivity_timeout/static/src/scss/inactivity_notification.scss',
            'inactivity_timeout/static/src/js/inactivity_timeout.js',
        ],
        'web.assets_qweb': [
            'inactivity_timeout/static/src/xml/inactivity_notification.xml',
        ],
    },
    
    'license': 'LGPL-3',
    'installable': True,
    'application': False,
    'auto_install': False,
    'price': 49.99,
    'currency': 'EUR',
    'images': ['static/description/banner.png'],
}