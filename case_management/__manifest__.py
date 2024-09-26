# -*- coding: utf-8 -*-
{
    "name": "Case Management",
    "summary": """
       Submodule of icomply""",
    "description": """
       Case Management is a systematic approach to managing and resolving issues, inquiries, or requests within an organization or service environment. It involves the coordination of processes and resources to ensure that cases are handled efficiently and effectively.
    """,
    "author": "Novaji Introserve",
    "website": "https://novajii.com",
    "sequence": -10008,
    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    "category": "Uncategorized",
    "version": "0.1",
    # any module necessary for this one to work correctly
    "depends": ["base",'utility','mail' ],
    # always loaded
    "data": [
        'security/ir.model.access.csv',
        "views/views.xml",
        "views/templates.xml",
        "views/case_management_views.xml",
    ],
    # only loaded in demonstration mode
    "demo": [
        "demo/demo.xml",
    ],
    "installable": True,
    "application": True,
    "auto_install": False,
}
