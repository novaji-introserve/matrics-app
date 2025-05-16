from . import models
from . import controllers  


from . import models
from odoo import SUPERUSER_ID


def load_exception_data(cr, registry):
    from odoo.api import Environment
    env = Environment(cr, SUPERUSER_ID, {})
    print("Post init hook: Loading exception data...")
    env['exception.data.loader'].load_exception_process_types()
    env['exception.data.loader'].load_exception_processes()
