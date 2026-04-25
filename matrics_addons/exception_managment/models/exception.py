from odoo import models, fields, api
from odoo.exceptions import UserError

class ExceptionModel(models.Model):
    _name = 'exception.model'
    _description = 'Exception Model'

    process_name = fields.Char(string="Process Name", required=True)
    sql_query = fields.Html(string="SQL Query", sanitize=False)
    narration = fields.Text(string="Narration")
    frequency = fields.Selection([
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('yearly', 'Yearly'),
    ], string="Frequency", default='daily')
    exception_type = fields.Selection([
        ('automated', 'Automated'),
        ('case', 'Case'),
    ], string="Exception Type", required=True)
    branch_id = fields.Many2one('res.branch', string="Branch")
    alert_group_id = fields.Many2one('alert.group', string="Alert Group")
    first_line_owner_id = fields.Many2one('res.users', string="First Line Owner")
    second_line_owner_id = fields.Many2one('res.users', string="Second Line Owner")
    created_by = fields.Many2one('res.users', string="Created By", default=lambda self: self.env.user)

    def action_generate_alert(self):
        for record in self:
            # Implement your alert generation logic here
            # For example, create an alert record or send notifications
            # Below is a placeholder for demonstration
            alert = self.env['alert.model'].create({
                'name': f'Alert for {record.process_name}',
                'exception_id': record.id,
                'message': record.narration or 'No narration provided.',
            })
            if alert:
                self.env.user.notify_info(f'Alert "{alert.name}" has been generated.')
            else:
                raise UserError('Failed to generate alert.')
