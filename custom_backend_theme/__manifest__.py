{
    "name": "Custom Backend Theme",
    "version": "1.0",
    "summary": "Customizes the look and feel of the Odoo backend.",
    "category": "Custom",
    "author": "Your Name",
    "depends": ["web"],
    "data": [],
    "assets": {
        "web.assets_backend": [
            "/custom_backend_theme/static/src/scss/custom_backend.scss",
        ],
    },
    "installable": True,
    "application": False,
    "auto_install": False,
}
