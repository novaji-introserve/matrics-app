# -*- coding: utf-8 -*-
{
    'name': "Transaction Screening",

    'summary': """
        Module for managing transaction screening rules , block transactions based on rules, and risk assessment plans.""",

    'description': """
       Screening transactions against defined rules, managing risk assessment plans, and handling transaction-related data. Block transactions based on rules, and risk assessment plans.
       Integration with external systesms for transaction screening and risk assessment.
       This module provides a framework for defining transaction screening rules, managing risk assessment plans, and handling transaction-related data.
       It allows users to create and manage rules that can block transactions based on specific conditions, and to define risk assessment plans that evaluate transactions based on various criteria.
    """,

    'author': "Novaji Intrsoserve Limited",
    'website': "https://www.novajii.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'Uncategorized',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base','compliance_management','nfiu_reporting','regulatory_reports'],

    # always loaded
    'data': [
        # 'security/ir.model.access.csv',
        'views/transaction_screening_rule.xml',
        'views/transaction.xml',
        'views/menuitems.xml'
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
}
