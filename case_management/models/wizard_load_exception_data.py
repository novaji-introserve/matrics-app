# # In a file models/wizard_load_exception_data.py
# from odoo import models, fields, api

# class WizardLoadExceptionData(models.TransientModel):
#     _name = 'wizard.load.exception.data'
#     _description = 'Wizard to Load Exception Data'

#     @api.model
#     def default_get(self, fields):
#         res = super(WizardLoadExceptionData, self).default_get(fields)
#         return res

#     def action_load_data(self):
#         self.env['exception.data.loader'].load_all_exception_data()
#         return {'type': 'ir.actions.act_window_close'}