from odoo import models, fields, api
from odoo.exceptions import ValidationError

class EscalationPeriod(models.Model):
    _name = 'fsdh.escalation.period'
    _description = 'Escalation Period'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _rec_name = "representation"

    name = fields.Selection(
        [
            ("hours", "Hours"),
            ("days", "Days"),
            ("weeks", "Weeks"),
            ("months", "Months"),
            ("years", "Years")
        ],
        string="Escalation Type"
    )
    escalation_cycle = fields.Integer(string="Escalation Period Cycle", required=True, default=1)
    date_created = fields.Datetime(string="Date Created", default=lambda self: fields.Datetime.now())
    
    @api.onchange("name")
    def _onchange_name(self):
        if self.name:
            self.escalation_cycle = 1
            
    @api.constrains("escalation_cycle", "name")
    def _check_escalation_cycle_limits(self):
        for record in self:
            if record.name:
                if record.escalation_cycle <= 0:
                    raise ValidationError("Escalation Period Cycle cannot be zero or negative.")
                
                if record.name == "days" and record.escalation_cycle > 365:
                    raise ValidationError("Escalation Period Cycle for days must be between 1 and 365.")
                
                elif record.name == "weeks" and record.escalation_cycle > 52:
                    raise ValidationError("Escalation Period Cycle for weeks must be between 1 and 52.")
                
                elif record.name == "hours" and record.escalation_cycle > 24:
                    raise ValidationError("Escalation Period Cycle for hours must be between 1 and 24.")
                
                elif record.name == "months" and record.escalation_cycle > 12:
                    raise ValidationError("Escalation Period Cycle for months must be between 1 and 12.")
                
                elif record.name == "years" and record.escalation_cycle > 10:
                    raise ValidationError("Escalation Period Cycle for years must be between 1 and 10.")
            
    @api.depends('escalation_cycle', 'name')
    def _compute_name_rep(self):
        for record in self:
            if record.escalation_cycle and record.name:
                record.representation = f"{record.escalation_cycle} {record.name}"
            else:
                record.representation = "New Escalation Period"

    representation = fields.Char(string='Escalation Period', compute='_compute_name_rep', store=True)
