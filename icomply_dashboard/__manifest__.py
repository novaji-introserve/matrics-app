# -*- coding: utf-8 -*-
{
    'name': "icomply_dashboard",
    'sequence':2,
    'summary': " dashboard overview ",

    'description': """
        Long description of module's purpose
    """,
    
    'category': 'iComply',
    'version': '0.3',

    'author': "Novaji Introserve Ltd",
    'website': "https://www.novajii.com",


    # any module necessary for this one to work correctly
    'depends': ['base','web', 'board'],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'views/menu.xml',
        
    ],
    'demo': [
      
    ],
    'installable': True,
    'application': True,
    "auto_install": False,
    'assets':{
        'web.assets_backend': [
            'icomply_dashboard/static/src/css/style.css',
            'icomply_dashboard/static/src/components/**/*.js',
            'icomply_dashboard/static/src/components/**/*.xml',
            'icomply_dashboard/static/src/img/logov.png',
        ]
    }
}
