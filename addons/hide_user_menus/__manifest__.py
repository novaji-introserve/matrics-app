# -*- coding: utf-8 -*-
{
    'name': 'Hide User Menus (Top Right Corner)',
    'author': 'Odoo Hub',
    'category': 'Tools',
    'summary': 'This module hides the user menu from the top right corner of the Odoo interface, removing options like Documentation, Support, Shortcuts for a cleaner and more focused UI. Odoo Hub Apps remove top right menu Odoo remove toprightmenu Odoo remove top rightmenu Odoo remove top-right menu Odoo remove top-rightmenu Odoo remove right top menu Odoo remove top menu Odoo remove user menu Odoo Odoo remove menu items remove header menu Odoo remove menu icons Odoo hide user menu Odoo hide top right menu Odoo hide toprightmenu Odoo hide top-right menu Odoo hide top menu Odoo hide top right icons Odoo hide menu items Odoo hide Odoo top right menu hide right corner menu Odoo hide extra menu Odoo hide account menu Odoo clean ui Odoo clean user interface Odoo cleaninterface Odoo declutter ui Odoo declutter interface Odoo Odoo minimal UI minimal Odoo simplify Odoo interface streamline Odoo UI streamline user interface Odoo remove clutter Odoo simple UI Odoo Odoo remove unused menus Odoo UI customization Odoo UI cleanup Odoo hide default menu hide support menu Odoo hide onboarding menu Odoo hide documentation menu Odoo hide my odoo.com account Odoo hide my odoo account Odoo hide shortcuts menu Odoo remove support menu Odoo remove documentation menu Odoo remove onboarding menu Odoo remove my odoo.com Odoo remove shortcuts menu Odoo Odoo hide support and documentation menu Odoo remove support onboarding menu Odoo disable help menu items Odoo remove doc and shortcuts hide all user menu items Odoo Odoo user dropdown customization top right menu remover Odoo disable top user menu Odoo remove help links from Odoo hide all top menu Odoo ui customization menu cleanup user menu tweaks frontend tools non-technical easy to use enterprise compatible community support',
    'description': """
        This module modifies the Odoo user interface by removing or hiding elements in the top right user menu. 
        The menu items that will be hidden include:
        
        - Documentation
        - Support
        - Shortcuts
        - Onboarding
        - My Odoo.com account
        
        The main goal of this module is to streamline the user interface and make it less cluttered, particularly 
        for users who do not need these features in their daily use.
    """,
    'maintainer': 'Odoo Hub',
    'website': 'https://apps.odoo.com/apps/modules/browse?author=Odoo%20Hub',
    'version': '1.0',
    'depends': ['base', 'web'],
    'assets': {
        'web.assets_backend': [
            'hide_user_menus/static/src/js/user_menus.js',
        ],
    },
    'images': ['static/description/banner.png'],
    'live_test_url': 'https://youtu.be/IiCNMi-3bR4',
    'installable': True,
    'application': True,
    'auto_install': False,
    'license': 'LGPL-3',
}
