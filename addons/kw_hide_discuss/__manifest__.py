{
    'name': 'Hide Discuss Menu',
    'summary': 'Control visibility of Discuss menu based on user groups',

    'author': 'Kitworks Systems',
    'website': 'https://github.com/kitworks-systems/addons',

    'category': 'Extra Tools',
    'license': 'LGPL-3',
    'version': '16.0.1.4.0',

    'depends': [
        'mail',
    ],

    'external_dependencies': {
        'python': [],
    },

    'data': [
        'security/security.xml',
        'views/menu_view.xml',
    ],
    'demo': [],

    'installable': True,
    'auto_install': False,
    'application': False,

    'images': [
        'static/description/cover.png',
        'static/description/icon.png',
    ],

    'price': 0,
    'currency': 'EUR',
}
