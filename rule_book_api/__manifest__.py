# -*- coding: utf-8 -*-
{
    'name': "Rule Book API",
    'sequence': -10000,

    'summary': """
        Short (1 phrase/line) summary of the module's purpose, used as
        subtitle on modules listing or apps.openerp.com""",

    'description': """
          This module provides REST API endpoints for:
        - Retrieving rulebook files with download links
        - Downloading specific rulebook files
    """,

    'author': "Novaji",
    'website': "https://www.novajii.com",

    "category": "icomply",
    "version": "0.1",

    'depends': ["base", "web", 'hide_powered_by_odoo', 'legion_hide_odoo', 'muk_web_theme'],

    'data': [
        'security/ir.model.access.csv',

        "data/schedules/web_scraping.xml",
        "data/rule_book_sources.xml",

        "views/menu.xml",
        "views/rulebook_sources_views.xml",
        "views/api_request.xml",
    ],

    'demo': [
        'demo/demo.xml',
    ],

    'test': [
        'tests/test_sec_scraper_api.py',
    ],

    'assets': {
        'web.assets_backend': [
           
        ],
    },

    'installable': True,
    'application': True,
    'auto_install': False,
}
