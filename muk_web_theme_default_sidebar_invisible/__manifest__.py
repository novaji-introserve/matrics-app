# -*- coding: utf-8 -*-
{
    'name': "MuK Web Theme Default Sidebar Invisible",

    'summary': """
       Make the default sibebar in MuK Backend Theme Invisible""",

    'description': """
        Make the default sibebar in MuK Backend Theme Invisible
    """,

    'author': "Jonathan Ogbimi & Olumide Awodeji",
    'website': "http://www.encom.com.ng",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/12.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'Uncategorized',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base', 'base_setup', 'web', 'muk_web_theme', 'mail'],

    # always loaded
    'data': [
        # 'security/ir.model.access.csv',
        'views/views.xml',
        # 'views/templates.xml',
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
    'assets': {
        'web.assets_backend': [
            'muk_web_theme_default_sidebar_invisible/static/src/js/chatter_position.js',
            'muk_web_theme_default_sidebar_invisible/static/src/scss/chatter_position.scss',
        ],
    },
}