# -*- coding: utf-8 -*-
# Part of Odoo. See COPYRIGHT & LICENSE files for full copyright and licensing details.

{
    'name': "HR Grade",
    'summary': """ HR Grade""",
    'description': """
         Job position wise Employee Grading details.
         Basic Information of this module like this:
            - Configure job position wise Grade
            - Define Grade on employee profile
    """,
    'author': 'Synconics Technologies Pvt. Ltd.',
    'website': 'http://www.synconics.com',
    'category': 'Generic Modules/Human Resources',
    'version': '1.0',
    'license': 'OPL-1',
    'depends': ['hr'],
    'data': [
        'security/ir.model.access.csv',
        'security/security.xml',
        'views/hr_grade_view.xml',
    ],
    'price': 10,
    'currency': 'EUR',
    'demo': [],
    'images': [
        'static/description/main_screen.jpg'
    ],
    'installable': True,
    'auto_install': False,
}
