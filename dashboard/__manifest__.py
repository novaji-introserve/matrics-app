# -*- coding: utf-8 -*-
{
    'name': "Icomply_dashboard",
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
    'depends': ['base','web', 'sale', 'board'],

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
            'mydashboard/static/src/components/**/*.js',
            'mydashboard/static/src/components/**/*.xml',
        ]
    }
}
