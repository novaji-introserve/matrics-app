
from odoo import models, fields, api
from odoo.exceptions import UserError

class CloseCaseWizard(models.TransientModel):
    _name = 'close.case.wizard'
    _description = 'Close Case Wizard'

    