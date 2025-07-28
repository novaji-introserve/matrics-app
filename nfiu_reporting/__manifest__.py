# -*- coding: utf-8 -*-
{
    'name': "NFIU GoAML Reporting",

    'summary': """
         This module allows you to:
        * Manage financial transaction reports
        * Generate XML reports compliant with NFIU goAML schema
        * Validate XML against XSD schema
        * Submit reports to financial intelligence unit
        """,

    'description': """
        Generate NFIU goAML compliant XML reports for financial transactions
    """,

    'author': "Novaji Intrserve Ltd",
    'website': "https://www.novajii.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'Uncategorized',
    'version': '0.1',

    # any module necessary for this one to work correctly
<<<<<<< HEAD
<<<<<<< HEAD
    'depends': ['base','compliance_management','regulatory_reports','hr','case_management_v2'],
=======
    'depends': ['base','compliance_management','regulatory_reports'],
>>>>>>> 816be76 (XML Schema Validator)
=======
    'depends': ['base','compliance_management','regulatory_reports','hr'],
>>>>>>> b75258c (Suspicious Transaction history)

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'views/nfiu_person_views.xml',
        'views/nfiu_entity_views.xml',
        'views/nfiu_entity_director_views.xml',
        'views/nfiu_indicators_views.xml',
        'views/nfiu_currency_threshold_views.xml',
        'views/nfiu_report_views.xml',
        'views/nfiu_export_wizard_views.xml',
        'views/nfiu_transaction_views.xml',
        'views/nfiu_data.xml',
        'data/data.xml'
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
}
