{
    'name': 'System Logs Viewer',
    'version': '17.0.1.0.0',
    'category': 'Administration',
    'summary': 'View system logs in Odoo UI',
    'description': """
        This module allows administrators to view system logs directly from the Odoo interface.
        Features:
        - View log entries with filtering and search
        - Real-time log monitoring
        - Log level filtering (ERROR, WARNING, INFO, DEBUG)
        - Export logs functionality
    """,
    'author': 'Novaji Introserve',
    'depends': ['base', 'web', "muk_web_theme"],
    'data': [
        'security/ir.model.access.csv',
        'views/menu_views.xml',
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
    'post_init_hook': 'post_init_hook',
}
