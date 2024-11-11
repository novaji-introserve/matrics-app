# models/alert_report.py
from odoo import models, fields, api, tools

class AlertReport(models.Model):
    _name = 'alert.report'
    _description = 'Alert Response Analysis'
    _auto = False
    _order = 'reply_date desc'

    rulebook_id = fields.Many2one('rulebook', string='Rulebook', readonly=True)
    rulebook_name = fields.Char(string='Alert Name', readonly=True)
    reply_date = fields.Date(string='Response Date', readonly=True)
    rulebook_compute_date = fields.Datetime(string='Due Date', readonly=True)
    submission_timing = fields.Selection([
        ('early', 'Early Submission'),
        ('on_time', 'Right on Time'),
        ('late', 'Late Submission'),
        ('not_responded', 'Not Responded')
    ], string='Response Timing', readonly=True)
    rulebook_status = fields.Selection([
        ('pending', 'Pending'),
        ('submitted', 'Submitted'),
        ('reviewed', 'Reviewed'),
        ('completed', 'Completed')
    ], string='Status', readonly=True)
    days_difference = fields.Integer(string='Days from Due Date', readonly=True)

    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute("""
            CREATE OR REPLACE VIEW %s AS (
                SELECT
                    rl.id AS id,
                    rl.rulebook_id,
                    rb.type_of_return AS rulebook_name,
                    rl.reply_date,
                    rl.rulebook_compute_date,
                    rl.submission_timing,
                    rl.rulebook_status,
                    CASE 
                        WHEN rl.reply_date IS NOT NULL 
                        THEN DATE_PART('day', rl.reply_date::timestamp - rl.rulebook_compute_date::timestamp)
                        ELSE DATE_PART('day', CURRENT_DATE::timestamp - rl.rulebook_compute_date::timestamp)
                    END AS days_difference
                FROM reply_log rl
                LEFT JOIN rulebook rb ON rl.rulebook_id = rb.id
                WHERE rl.rulebook_compute_date IS NOT NULL
            )
        """ % self._table)
        