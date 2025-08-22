{
    'name': 'Access Control',
    'version': '1.0',
    'summary': 'Control access to views and actions dynamically',
    'description': 'Control View Access',
    'author': 'Novaji',
    'website': 'https://novajii.com',
    "category": "iComply",
    'depends': ['base', 'web'],
    'data': [
        'security/security.xml',
        'security/ir.model.access.csv',
        'views/menu_actions.xml',
        'views/view_access_views.xml',
    ],
    'qweb': [],
    'installable': True,
    'license': 'LGPL-3',
    'application': False,
    'auto_install': False,
    'assets': {
        'web.assets_backend': []
    },
}
