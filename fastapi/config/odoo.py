import odoo_connect

from config.settings import get_settings


def get_odoo_config():
    return get_settings().odoo_config


def get_odoo_client():
    odoo_config = get_odoo_config()
    return odoo_connect.connect(
        url=odoo_config.base_url,
        database=odoo_config.database,
        username=odoo_config.username,
        password=odoo_config.password,
    )
