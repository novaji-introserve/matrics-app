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
    'version': '0.3',

    # any module necessary for this one to work correctly
    'depends': ['base', 'contacts', 'hr', 'access_apps', 'muk_web_theme_default_sidebar_invisible', 'web_field_slider', 'spreadsheet_dashboard', 'hide_powered_by_odoo', 'hide_menu_user','web_widget_numeric_step','legion_hide_odoo','base_automation'],

    # always loaded
    'data': [
        'security/groups.xml',
        'security/ir.model.access.csv',
        # 'data/department.xml',
        'views/configuration.xml',
        'views/edd.xml',
        'views/kyc.xml',
        # 'data/data.xml',
        'views/risk_assessment.xml',
        'views/res_users.xml',
        'views/sanction_screening.xml',
        'views/customer.xml',
        'data/res.country.state.csv',
        'data/res.branch.csv',
        'security/security.xml',
        'views/statistics.xml',
        'views/dashboard.xml',
        'views/risk_assessment_plan.xml',
        'views/settings.xml',
        'views/customer_accounts.xml',
        'views/transaction.xml',
        'views/transaction_screening_rule.xml',
    ],
    # only loaded in demonstration mode
    'demo': [
        # 'demo/demo.xml',
    ],
    "installable": True,
    "application": True,
    "auto_install": False,
}
