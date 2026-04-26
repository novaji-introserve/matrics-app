# -*- coding: utf-8 -*-

{
    "name" : "Employee Recruitment",
    "author": "Edge Technologies",
    "version" : "16.0.1.0",
    "live_test_url":'https://youtu.be/qyYQnlh1Kpo',
    "images":["static/description/main_screenshot.png"],
    'summary': 'Apps helps to recruit multiple applicant by the hr according to the hr applicants helps to shortlist employees recruitment employee is designed to recruit multiple applicant with experience or fresher hr recruitment process employee recruitment process',
    "description": """This app is designed to recruit multiple applicant by the hr according to the applicants. helps to shortlist employees.""",
    "depends" : ['base','hr_recruitment','stock'],
    "data": [
        'security/staff_security.xml',
        'security/ir.model.access.csv',
        'data/data.xml',
        'views/applicant_inherit_views.xml',
        'views/staff_recruitment_views.xml',
    ],
    "auto_install": False,
    "installable": True,
    "price": 000,
    "currency": 'EUR',
    "category" : "Human Resources",
}
# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
