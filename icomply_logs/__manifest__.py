{
    'name': 'Log Manager',
    'version': '1.0.0',
    'category': 'iComply',
    'summary': 'View system logs',
    'description': """
        This module allows administrators to view system logs.

        Features:
        - View log entries with filtering and search
        - Real-time log monitoring
        - Log level filtering (ERROR, WARNING, INFO, DEBUG)
        - Export logs functionality
    """,
    'author': 'Novaji Introserve',
    'depends': [
        'base',
        'web',
        'muk_web_theme'
    ],
    'data': [
        'security/groups.xml',
        'security/ir.model.access.csv',
        
        'data/log_monitor.xml',

        'views/menu_views.xml',
        'views/log_views.xml'
    ],
   "assets": {
    "web.assets_backend": [

        'icomply_logs/static/src/components/scss/csv_import.scss',
        "icomply_logs/static/src/components/js/terminal.js",
        "icomply_logs/static/src/components/js/terminal_component.js",
        "icomply_logs/static/src/components/xml/logs.xml",
    ],
},
    'installable': True,
    'application': True,
    'auto_install': False,
    'license': 'LGPL-3',
    # 'post_init_hook': 'post_init_hook'
}
