{
    "name": "Custom Backend Theme",
    "version": "1.0",
    "summary": "Customizes the look and feel of the Odoo backend.",
    "category": "Icomply/Theme",
    "author": "Ked",
    "depends": ["web"],
    "data": [
        "views/icons.xml",
        "views/layout.xml",
        "views/theme.xml",
        "views/assets.xml",
        "security/ir.model.access.csv",
        "data/theme_data.xml",
    ],
    "assets": {
        "web.assets_backend": {
            "/custom_backend_theme/static/src/scss/theme.scss",
            "/custom_backend_theme/static/src/js/systray.js",
            "/custom_backend_theme/static/src/js/load.js",
            "/custom_backend_theme/static/src/js/chrome/sidebar_menu.js",
            "/custom_backend_theme/static/src/xml/systray.xml",
            "/custom_backend_theme/static/src/xml/top_bar.xml",
        },
        "web.assets_frontend": {
            "/custom_backend_theme/static/src/scss/login.scss",
            "/custom_backend_theme/static/src/scss/login.scss",
        },
    },
    "images": [
        "static/description/banner.png",
        "static/description/theme_screenshot.png",
    ],
    "installable": True,
    "application": False,
    "auto_install": False,
}
