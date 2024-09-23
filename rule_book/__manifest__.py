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
    "category": "icomply",
    "version": "0.1",
    # any module necessary for this one to work correctly
    "depends": ["base", "mail", "calendar"],
    # always loaded
    "data": [
        "views/menu.xml",
        "views/rules/rule_theme.xml",
        "views/rules/rulebook_sources_views.xml",
        "views/rules/rulebook_title_views.xml",
        "security/ir.model.access.csv",
        "views/risk/risk_category.xml",
        "views/department.xml",
        "views/responsible.xml",
        "views/exception/exception_process.xml",
        "views/exception/exception_type.xml",
        "views/rules/rule_book.xml",
        "data/email_templates/rule_book.xml",
        "data/schedules/rule_book.xml",
        # "views/dashboard.xml",
        "data/settings/email_smtp.xml",
        "data/preloaded_data/risk_categories.xml",
        "data/preloaded_data/rule_book_sources.xml",
        "views/public/rule_book_submission.xml",
        "data/email_templates/escalation_email.xml",
        "views/rules/reply_log.xml",
        "views/rules/web_scaping.xml",
        "data/schedules/web_scraping.xml",
        "views/public/thank_you.xml",
        "security/group_action.xml",
    ],
    # only loaded in demonstration mode
    "demo": [
        "demo/demo.xml",
    ],
    "installable": True,
    "application": True,
    "auto_install": False,
    "assets": {"web.assets_backend": ["rule_book/static/src/js/my_script.js"]},
}
