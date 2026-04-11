# -*- coding: utf-8 -*-
{
    'name': "Regulatory Reports",

    'summary': """
        SQL-based regulatory report runner""",

    'description': """
        Run validated SQL SELECT queries, preview the first 30 rows,
        and generate downloadable exports through queue_job.
    """,

    'author': "Novaji Introserve Ltd",
    'website': "https://www.novajii.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'Uncategorized',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base', 'mail', 'queue_job', 'compliance_management'],
    'external_dependencies': {
        'python': ['sqlparse'],
    },

    # always loaded
    # order of the files is important, declare model id before referencing them
    'data': [
        'security/security.xml',
        'security/ir.model.access.csv',
        'views/report.xml',
        'views/menuitems.xml',
    ],
    'demo': [],
    'license': 'LGPL-3',
}
