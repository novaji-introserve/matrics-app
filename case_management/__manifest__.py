{
    'name': 'Case Management',
    'version': '1.0',
    'summary': 'A module for managing cases',
    'description': """This module helps in managing cases.""",
    'author': 'Novaji',
    'website': 'https://novajii.com',
    "category": "iComply",
    'depends': ['base', 'web', 'mail', 'alert_management', 'compliance_management'],
    'data': [
        'data/exception_processes/exception_process_type_data.xml',
        'data/exception_processes/exception_process_data.xml',
        'data/email_templates/email_templates.xml',
        #'data/case_closure.xml',
        'security/case_access_rule.xml',
        'security/security_two.xml',
        'security/security.xml',
        'security/ir.model.access.csv',
        
        'views/new_case.xml',        # Then load views and connect to menus
       # 'views/case_form_inherit_disable_save_discard.xml', 
       # 'views/message_alert_views.xml',
        'views/all_cases_views.xml',
        'views/my_alerts_views.xml',
        'views/case_dashboard.xml',# Load menus first
        #'views/case_form_inherit.xml',
        'data/overdue_cronJob/cron.xml',
        # 'data/exception.process.type.csv',
        # 'data/exception.process.csv',
    ],
    'demo': [],
    'assets': {
        'web.assets_backend': [
            'case_management/static/src/js/case_form_action.js',
            'case_management/static/src/js/success_message.js',
            'case_management/static/src/components/kpi_card/kpi_card.js',
            'case_management/static/src/components/chart_renderer/chart_renderers.js',  # ADDED THIS
            'case_management/static/src/components/case_dashboard.js',
            'case_management/static/src/components/kpi_card/kpi_card.xml',
            'case_management/static/src/components/chart_renderer/chart_renderers.xml', # ADDED THIS
            'case_management/static/src/components/case_dashboard.xml',
            'case_management/static/src/css/case_form.css',
           # 'views/wizard_load_exception_data.xml',
            

        ],
    },
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
    'icon': 'case_management/static/description/icon.png',
}