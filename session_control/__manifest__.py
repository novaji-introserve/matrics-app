{
    'name': 'Session Control',
    'version': '1.0',
    'summary': 'Prevent users from having multiple active sessions',
    'description': 'Control user sessions',
    'author': 'Novaji',
    'website': 'https://novajii.com',
    "category": "iComply",
    'depends': ['base', 'web'],
    'data': [
        "data/schedules/cron.xml",
        "security/ir.model.access.csv"
    ],
    'installable': True,
    'application': False,
    'auto_install': False,
    'assets': {
        'web.assets_backend': [
            # Styles
            'session_control/static/src/js/session_check.js',
            
        ]
    },
}
