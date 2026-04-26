# -*- coding: utf-8 -*-
# Part of BrowseInfo. See LICENSE file for full copyright and licensing details.
{
    'name': 'Create Issue from Lead',
    'version': '16.0.0.1',
    'category': 'CRM',
    'license': 'OPL-1',
    'summary': 'This odoo app helps user to create project issue from crm lead.',
    'description': """
    Issue on Lead, Add Issue from lead, Issue Lead, Create Project Issue from Lead
""",
    'author': 'BrowseInfo',
    'website': 'https://www.browseinfo.com',
    'depends': ['base', 'crm', 'sale','project'],
    
    'data': [ 'security/ir.model.access.csv',
             'views/crm_lead_view.xml'
    ],
    'installable': True,
    'auto_install': False,
    'application': True,
    "live_test_url":'https://youtu.be/zopFJY4MxPU',
    "images":["static/description/Banner.gif"],
}

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
