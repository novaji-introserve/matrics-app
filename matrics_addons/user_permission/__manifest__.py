# -*- coding: utf-8 -*-
{
   "name": "User Permission",
    "summary": "Manage user permissions effectively across your application.",
    "description": "This module provides comprehensive tools for defining and managing user permissions, ensuring that each user has the appropriate access rights for their role. Streamline your permission management and enhance security with this essential module.",
    "author": "Novaji Introserve",
    "website": "https://www.novajii.com",
     "sequence": -10002,
    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    "category": "MATRICS",
    "version": "0.1",
    # any module necessary for this one to work correctly
    "depends": ["base"],
    # always loaded
    "data": [
        'security/ir.model.access.csv',
        "views/views.xml",
        "views/user_permission_views.xml",
        "views/res_users_views.xml",
    ],
    # only loaded in demonstration mode
    "demo": [
        "demo/demo.xml",
    ],
    "installable": True,
    "application": True,
}
