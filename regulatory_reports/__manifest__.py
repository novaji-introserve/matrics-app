# -*- coding: utf-8 -*-
{
    'name': "Regulatory Reports",

    'summary': """
        Compliance Reports for Regulatory Bodies""",

    'description': """
        Compliance Reports for Regulatory Bodies:
        - Generate and manage regulatory reports
        - Customizable templates for various compliance requirements
        - Integration with existing compliance management systems
        - Support for multiple regulatory frameworks
        - Automated report generation and scheduling
        - User-friendly interface for report customization
        - Export options in various formats (PDF, Excel, etc.)
        - Audit trail and version control for reports
        - Compliance with data protection regulations
        - Role-based access control for report management
    """,

    'author': "Novaji Introserve Ltd",
    'website': "https://www.novajii.com",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'Uncategorized',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base','compliance_management', 'web', 'report_xlsx'],

    # always loaded
    'data': [
        'security/ir.model.access.csv',
        'views/report_entity.xml',
        'views/report_template.xml',
        'views/report_item.xml',
        'views/report.xml',
        'views/menuitems.xml',
        'data/report_entity_data.xml',
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
}
