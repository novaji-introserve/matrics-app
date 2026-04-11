# -*- coding: utf-8 -*-
{
    "name": "Rule Book",
    "sequence": -10000,
    "summary": """
        Short (1 phrase/line) summary of the module's purpose, used as
        subtitle on modules listing or apps.openerp.com""",
    "description": """
        Long description of module's purpose
    """,
    "author": "Novaji",
    "website": "https://www.novajii.com",
    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    "category": "MATRICS",
    "version": "0.1",
    # any module necessary for this one to work correctly
    "depends": ["base", "web", "mail", "calendar", 'hide_powered_by_odoo', 'legion_hide_odoo', 'hr', 'muk_web_theme',
                # "access_apps"
                ],
    # always loaded
    "data": [
        # department/reports
        # Load security groups first
        "security/security_groups.xml",

        "security/ir.model.access.csv",

        # "data/preloaded_data/rule_book_branch.xml",
        "views/alert_dashboard.xml",
        "views/menu.xml",
        "views/rules/rule_theme.xml",
        "views/rules/rulebook_sources_views.xml",
        "views/rules/rulebook_title_views.xml",
        # "security/ir.model.access.csv",
        "views/risk/risk_category.xml",
        "views/rules/rule_book.xml",
        #
        # "views/responsible.xml",
        "data/email_templates/reminder_due_date_email.xml",
        "data/email_templates/internal_due_date_email.xml",
        "data/email_templates/internal_due_date_for_rulebook.xml",
        "data/email_templates/rulebook_log_notification.xml",
        "data/email_templates/first_line_escalation.xml",
        "data/email_templates/share_document_template.xml",
        "data/email_templates/keyword_alert.xml",
        'data/email_templates/regulatory_alert.xml',

        "data/schedules/rule_book.xml",
        # "views/dashboard.xml",
        "data/settings/email_smtp.xml",
        "data/preloaded_data/risk_categories.xml",
        # "data/preloaded_data/users.xml",
        "data/preloaded_data/rule_book_sources.xml",
        # "data/email_templates/escalation_notification.xml",

        "views/public/rule_book_submission.xml",
        "views/rules/reply_log.xml",
        "views/rules/web_scaping.xml",
        "data/schedules/web_scraping.xml",
        "views/public/thank_you.xml",
        # "security/group_action.xml",
        "views/pdf_chat.xml",
        # "views/alert_report_rulebook.xml",
        "views/alert_dashboard.xml",
      
        'views/user_setting.xml',
        'views/keyword_tracking.xml',
        'data/sequence/keyword.xml',
        'views/keyword_logs.xml',
        'views/reg_alert.xml',


    ],
    # only loaded in demonstration mode
    "demo": [
        "data/demo/demo.xml",
        # "demo/demo_department.xml"
    ],
    "installable": True,
    "application": True,
    "auto_install": False,
    "images": ['static/src/img/boi.jpeg'],
    "assets": {"web.assets_backend": [
        "rule_book/static/src/js/my_script.js",
        'rule_book/static/src/components/*/*.js',
        'rule_book/static/src/components/*/*.xml',
        'rule_book/static/src/components/*/*.scss',
        'rule_book/static/src/slider_field.xml',
        'rule_book/static/src/slider_field.js',
        # Add JavaScript files
        'rule_book/static/src/components/**/*.js',
        # Add CSS files
        'rule_book/static/src/components/**/*.css',
        # Add XML files if applicable
        'rule_book/static/src/components/**/*.xml',


    ]},
}
