# -*- coding: utf-8 -*-
{
    "name": "Compliance Management",
    "summary": """
        Risk-based Compliance Management For Financial Institutions""",
    "description": """
        Customizations for Compliance Management:
        - add extra fields for customer
        - add Branch
        - add Account
    """,
    "author": "Novaji Introserve Ltd",
    "website": "https://www.novajii.com",
    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    "category": "iComply",
    "version": "0.3",
    # any module necessary for this one to work correctly
    'depends': ['base', 'web', 'bus', 'contacts', 'hr', 'access_apps', 'muk_web_theme_default_sidebar_invisible', 'web_field_slider', 'spreadsheet_dashboard', 'hide_powered_by_odoo', 'hide_menu_user','web_widget_numeric_step','legion_hide_odoo','base_automation'],

    # always loaded
    'data': [
        "security/groups.xml",
        "security/ir.model.access.csv",
        "data/res.country.state.csv",
        "data/department.xml",
        "data/res.branch.csv",
        "data/media_keywords.xml",
        "security/security.xml",
        "data/schedules/adverse_media_cron.xml",
        "data/email_templates/adverse_media_alert_template.xml",
        "data/schedules/global_pep_list_cron.xml",
        "views/dynamic_charts.xml",
        "views/configuration.xml",
        "views/edd.xml",
        "views/kyc.xml",
        "views/adverse_media.xml",
        "views/adverse_media_logs.xml",
        "views/adverse_media_keywords.xml",
        "views/pep_source.xml",
        # "data/schedules/queue_job_config.xml",
        "data/data.xml",
        "views/risk_assessment.xml",
        "views/open_sanctions.xml",
        "views/res_users.xml",
        "views/sanction_screening.xml",
        "views/customer.xml",
        "views/statistics.xml",
        "data/schedules/update_statistic_cron.xml",
        "views/dashboard.xml",
        "views/risk_assessment_plan.xml",
        "views/settings.xml",
        "views/customer_accounts.xml",
        "views/transaction.xml",
        "views/transaction_screening_rule.xml",
        
        
        'security/groups.xml',
        'security/ir.model.access.csv',
        'data/department.xml',
        'data/res.country.state.csv',
        'data/res.branch.csv',
        'data/media_keywords.xml',
        'security/security.xml',
        'data/schedules/adverse_media_cron.xml',
        'data/schedules/run_risk_assessment_cron.xml',
        'data/email_templates/adverse_media_alert_template.xml',
        'views/configuration.xml',
        'views/edd.xml',
        'views/kyc.xml',
        'views/adverse_media.xml',
        'views/adverse_media_logs.xml',
        'views/adverse_media_keywords.xml',
        'data/data.xml',
        'views/risk_assessment.xml',
        'views/res_users.xml',
        'views/sanction_screening.xml',
        'views/customer.xml',
        'data/res.country.state.csv',
        'data/res.branch.csv',
        'security/security.xml',
        'views/statistics.xml',
        'views/dashboard.xml',
        'views/risk_assessment_plan.xml',
        'views/settings.xml',
        'views/customer_accounts.xml',
        'views/transaction.xml',
        'views/transaction_screening_rule.xml',
    ],
    # only loaded in demonstration mode
    "demo": [
        # 'demo/demo.xml',
    ],
    "installable": True,
    "application": True,
    "auto_install": False,
    'post_init_hook': 'post_init_hook',
    'uninstall_hook': 'uninstall_hook',
    'assets':{
        'web.assets_backend': [
            # Styles
            'compliance_management/static/src/components/file_upload/scss/csv_import.scss',
            'compliance_management/static/src/scss/custom_status_bar.scss',

            # Load templates first
            'compliance_management/static/src/components/file_upload/xml/csv_import.xml',
            
            # Then load base files
            'compliance_management/static/src/components/file_upload/js/terminal.js',
            'compliance_management/static/src/components/file_upload/js/chunked_uploader.js',
            'compliance_management/static/src/components/file_upload/js/terminal_component.js',
            'compliance_management/static/src/components/file_upload/js/import_form_component.js',
            
            # Finally load action registrations
            'compliance_management/static/src/components/file_upload/js/main.js',
        
            'compliance_management/static/src/css/style.css',
            'compliance_management/static/src/components/**/*.js',
            'compliance_management/static/src/components/**/*.xml',
            'compliance_management/static/src/components/**/*.css',


            'compliance_management/static/src/img/logov.png',
            'compliance_management/static/img/alt_bank_logo.png',
            'compliance_management/static/img/alt_bank_logo_.png',

        ]
    },
}