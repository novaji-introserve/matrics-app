# -*- coding: utf-8 -*-
{
    'name': "Compliance Management",

    'summary': """
        Risk-based Compliance Management For Financial Institutions""",

    'description': """
        Customizations for Compliance Management:
        - add extra fields for customer
        - add Branch
        - add Account
    """,

    'author': "Novaji Introserve Ltd",
    'website': "https://www.novajii.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'iComply',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base','contacts','hr'],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'security/groups.xml',
        'views/configuration.xml',
        'views/templates.xml',
        'data/data.xml'
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
}
