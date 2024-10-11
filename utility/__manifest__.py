# -*- coding: utf-8 -*-
{
    'name': "iComply Utility",

    'summary': """
        Utility module for Icompliance management.
    """,

    'description': """
        This module provides various utilities to facilitate compliance management processes, enhancing operational efficiency and ensuring adherence to regulations.
    """,
     "sequence": -10008,

    'author': "Novaji Introserve",
    'website': "https://novajii.com",

    'category': 'iComply',
    'version': '0.1',

    'depends': ['base'],

    'data': [
        # security files
        'security/ir.model.access.csv',
        # views
        'views/views.xml',
        'views/templates.xml',
        "views/process_category_views.xml",
        "views/process_views.xml",
        "views/sla_severity_views.xml",
        # preloaded data
        "data/preloaded_data/process_category.xml",
        "data/preloaded_data/sla_severity.xml",
        "data/preloaded_data/process.xml"
    ],
    
    'demo': [
        'demo/demo.xml',
    ],

    'installable': True,
    'auto_install': False,
    "application":True
}