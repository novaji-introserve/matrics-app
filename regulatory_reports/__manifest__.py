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
        'views/report_entity.xml',
        'views/report_template.xml',
        'views/report_item.xml',
        'views/report_template_items.xml',
        'views/menuitems.xml',
        'data/report_entity_data.xml',
        'data/report_items_demo_data.xml',
        'data/report_template_data.xml',
        'data/report_template_items_data.xml',
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
    'license': 'LGPL-3',
}
