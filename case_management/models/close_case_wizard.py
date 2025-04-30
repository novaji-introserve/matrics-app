
from odoo import models, fields, api
from odoo.exceptions import UserError

class CloseCaseWizard(models.TransientModel):
    _name = 'close.case.wizard'
    _description = 'Close Case Wizard'

    # narration = fields.Text(string="Narration / Final Notes")
    # case_id = fields.Many2one('case', string='Case', required=True)

    # def action_confirm_close(self):
    #     self.ensure_one()
    #     case = self.case_id

    #     if case.status_id.name != 'open':
    #         raise UserError("Only open cases can be closed.")

    #     # if self.env.uid != case.staff_id.user_id.id:
    #     #     raise UserError("Only the Staff Responsible can close this case.")

    #     case.write({
    #         'cases_action': self.narration,
    #         'status_id': self.env.ref('case_management.case_status_closed').id
    #     })
    #     return {'type': 'ir.actions.act_window_close'}


    