# -*- coding: utf-8 -*-
{
    'name': "Transaction Screening",

    'summary': """
        Module for managing transaction screening rules, block transactions based on rules,
        AML detection (velocity, structuring, anomaly) and risk assessment plans.""",

    'description': """
       Screening transactions against defined rules, managing risk assessment plans, and handling
       transaction-related data. Block transactions based on rules, and risk assessment plans.
       AML detection layer: velocity checks, structuring/smurfing detection, and statistical
       anomaly detection using Z-score against customer behavioral baselines.
       Dynamic CTR threshold configurable via AML Configuration.
    """,

    'author': "Novaji Intrsoserve Limited",
    'website': "https://www.novajii.com",

    'category': 'Uncategorized',
    'version': '0.1',

    'depends': ['base', 'mail', 'compliance_management', 'nfiu_reporting', 'regulatory_reports'],

    'data': [
        'security/ir.model.access.csv',
        'data/aml_sequences.xml',
        'security/aml_dormant_access.xml',
        'demo/demo.xml',
        'views/transaction_screening_rule.xml',
        'views/transaction.xml',
        'views/aml_config.xml',
        'views/aml_alerts.xml',
        'views/menuitems.xml',
    ],

    'demo': [],
    'license': 'LGPL-3',
}
