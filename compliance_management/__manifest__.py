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
    "version": "0.5.1",
    # any module necessary for this one to work correctly
    
    'depends': ['base', 'web', 'bus', 'hr', 'access_apps', 'web_field_slider', 'gamification', 'hide_powered_by_odoo','web_widget_numeric_step','legion_hide_odoo','base_automation', 'google_gmail', 'gamification','project',
        'mass_mailing',
        'utm',
        'contacts',
        'mail',
        'survey','legion_hide_odoo'],

    # always loaded
    'data': [
        "security/groups.xml",
        "security/ir.model.access.csv",
        "data/demo_data/location/res.country.state.csv",
        "data/settings/email_smtp.xml",
        "data/demo_data/department/department.xml",
        "data/demo_data/risk_assessment/risk_assessment_control_data.xml",
        "data/demo_data/branch/res.branch.csv", #BRANCH
        "data/demo_data/region/res.partner.region.csv",#REGION
        "data/demo_data/industries/customer.industry.csv",#INDUSTRY
        "data/demo_data/risk_assessment/risk_assessment_mitigation.xml",
        "data/demo_data/risk_assessment/res_risk_implication.xml",
        "security/security.xml",
        "views/adverse_media.xml",
        "views/configuration.xml",
        "data/demo_data/keyword/media_keywords.xml",
        "data/schedules/adverse_media_cron.xml",
        'data/schedules/run_risk_assessment_cron.xml',
        'data/schedules/clean_cache.xml',
        'data/schedules/refresh_charts_materialized_views.xml',
        'data/schedules/update_statistic_cron.xml',

        "data/email_templates/adverse_media_alert_template.xml",
        'data/email_templates/edd_notifications_template.xml',
        "data/email_templates/customer_screening_template.xml",


        "data/schedules/update_customer_risk_level.xml",
        "data/schedules/global_pep_list_cron.xml",
        "data/schedules/count-weight-avg.xml",
        "views/dynamic_charts.xml",
        "views/fcra_score.xml",
        "views/edd.xml",
        'views/wizard_view.xml',
        "views/kyc.xml",
        "views/adverse_media_logs.xml",
        "views/adverse_media_keywords.xml",
        "views/pep_source.xml",
        # "data/schedules/queue_job_config.xml",
        "data/demo_data/score/fcra_score.xml",    
        "data/data.xml",
        "data/demo_data/risk_assessment/category.xml",
        "data/demo_data/risk_assessment/risk_universe.xml",
        "data/demo_data/risk_assessment/risk_type.xml",         
        "data/demo_data/risk_assessment/risk_level.xml",
        "data/demo_data/risk_assessment/risk_subject.xml",
        "data/demo_data/risk_assessment/risk_subject_2.xml",
       
       
        "data/demo_data/risk_assessment/jurisdiction.xml",
        "data/demo_data/risk_assessment/delivery_channel.xml",
        "data/demo_data/risk_assessment/product_services.xml",
       

        "data/demo_data/risk_assessment/compliance_history.xml",
        "data/demo_data/risk_assessment/data_quality.xml",
        "data/demo_data/risk_assessment/transaction_behavior.xml",
       
        "data/demo_data/account/account_type.xml",
        "data/demo_data/education/educational_level.xml",
        "data/demo_data/tier/customer_tier.xml",
        "data/demo_data/customer/sector.xml",
        "data/demo_data/customer/products.xml",
        "data/demo_data/customer/region.xml",
        "data/demo_data/customer/account_officers.xml",
        "views/risk_assessment.xml",
        "views/sanction_screening.xml",
        "views/open_sanctions.xml",
        # remove unwanted partner/customer actions
        'views/remove_partner_actions.xml',
        "views/customer.xml",
        "views/statistics.xml",
        "views/resource_uri.xml",
        "views/dashboard.xml",
        "views/card_dashboard_template.xml",
        "views/risk_assessment_plan.xml",
        "views/settings.xml",
        "views/customer_accounts.xml",
        "views/transaction.xml",
        "views/transaction_screening_rule.xml",
        "data/transaction_type.xml",
        'data/transaction_screening_rule.xml',
        # "views/res_users.xml", # USERS
        "views/peplist.xml",
        'data/gender.xml',
        'data/identification_type.xml',
        'data/demo_data/stat/compliance_stats.xml',
        'data/demo_data/chart/charts.xml',
        'data/settings.xml',
        'data/resource_uri_data.xml',
        'data/demo_data/plan/risk_plan.xml',
        'data/demo_data/plan/jurisdiction_plan.xml',
       
       
        'data/demo_data/account/customer_product.xml',
        'data/demo_data/partner/sectors.xml',
        'data/blacklist.xml',
        "views/customer_screening.xml",
        'views/transaction_screening_history.xml',
        "views/change_data_capture.xml",
        "views/menu_actions.xml",
    ],
    # only loaded in demonstration mode
    "demo": [
        # 'demo/demo.xml',
    ],
    "installable": True,
    'license': 'LGPL-3',
    "application": True,
    "auto_install": False,
    'post_init_hook': 'post_init_hook',
    'uninstall_hook': 'uninstall_hook',
    'assets': {
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
            'compliance_management/static/src/css/slider.css',
            'compliance_management/static/src/components/**/*.js',
            'compliance_management/static/src/components/**/*.xml',
            'compliance_management/static/src/components/**/*.css',
            
            'compliance_management/static/src/components/**/**/js/*.js',
            'compliance_management/static/src/components/**/**/xml/*.xml',
            
            "compliance_management/static/lib/chart.umd.min.js",
            
            'compliance_management/static/src/js/pep_auto_refresh.js',
            'compliance_management/static/src/xml/pep_auto_refresh.xml',
            'compliance_management/static/src/js/custom_title_service.js',
            'compliance_management/static/src/js/cache_service.js',

            'compliance_management/static/src/img/logov.png',
            'compliance_management/static/img/alt_bank_logo.png',
        ]
    },
}
