# models/rulebook_theme.py
from odoo import models, fields, api
from odoo.exceptions import UserError

class RulebookTheme(models.Model):
    _name = 'rulebook.theme'
    _description = 'Rulebook Theme'
    _inherit = ['mail.thread']  # Enable tracking

    name = fields.Char(string='Theme', required=True, tracking=True)
    description = fields.Text(string='Description', tracking=True)
    source = fields.Selection(
        [('manual', 'Manual Input'), ('ai', 'AI Generated')],
        string='Source',
        default='manual',
        required=True,
        tracking=True
    )

    @api.model
    def create(self, vals):
        theme = super(RulebookTheme, self).create(vals)
        theme.message_post(body="Rulebook theme created.")
        return theme

    def write(self, vals):
        res = super(RulebookTheme, self).write(vals)
        self.message_post(body="Rulebook theme updated.")
        return res

    def unlink(self):
        raise UserError("You are not allowed to delete a rulebook theme.")



# 1. spooling report from requlatory sites and also using ai to read the pdf, run search throught all the pdf
#  2. uploading existing rules in BOI 3. Sending out reminders and alert 4. Ai that can read document and tell what it is doing 