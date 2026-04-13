from odoo import models, fields, api
from odoo.exceptions import ValidationError

class EscalationMatrix(models.Model):
    _name = 'fsdh.escalation.matrix'
    _description = 'Escalation Matrix'
    _rec_name = 'subsidiary_id'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    subsidiary_id = fields.Many2one(
        'res.groups',
        string='Subsidiary',
        required=True,
        domain="[('category_id.name', '=', 'Subsidiaries')]",
        help="Select the subsidiary this matrix belongs to. "
             "Only subsidiaries defined in Settings > Users appear here.",
        tracking=True,
    )
    # initial_recipient = fields.Char(
    #     string='Initial Alert Recipient Email',
    #     required=True,
    #     help="First point of contact when an alert rule first breaches.",
    #     tracking=True,
    # )
    # description = fields.Text(string='Notes')
    step_ids = fields.One2many(
        'fsdh.escalation.step', 'matrix_id',
        string='Escalation Steps',
    )

    _sql_constraints = [
        ('unique_subsidiary', 'UNIQUE(subsidiary_id)',
         'An escalation matrix already exists for this subsidiary!'),
    ]

    @api.constrains('step_ids')
    def _check_steps(self):
        for matrix in self:
            if matrix.step_ids:
                sequences = matrix.step_ids.mapped('sequence')
                if len(sequences) != len(set(sequences)):
                    raise ValidationError("Each escalation level must have a unique sequence number.")
