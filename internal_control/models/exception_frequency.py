from odoo import models, fields, api

class Exception_frequency(models.Model):
    _name = 'exception.frequency'
    _description = 'exception Frequency rate'

    name = fields.Selection(
        [
            ("Minutes", "minutes"),
            ("Hourly", "hourly"),
            ("Daily", "daily"),
            ("Weekly", "weekly"),
            ("Monthly", "monthly"),
            ("Yearly", "yearly")
        ]
    )
    period = fields.Integer(string="Period", required=True)
    duration = fields.Integer(string="Duration", computes="calcaulate_duration", store=True)
    date_created = fields.Datetime(string="created_at", default=fields.Datetime.now())


    @api.depends('name', 'period')
    def calcaulate_duration(self):
        for record in self:
            if record.name == 'minutes':
                record.duration = record.period
            elif record.name == 'hours':
                record.duration = record.period * 60
            elif record.name == 'day':
                record.duration = record.period * (60 * 24)
            elif record.name == 'week':
                record.duration = record.period * (60 * 24 * 7)
            elif record.name == 'month':
                record.duration = record.period * (60 * 24 * 30)
            elif record.name == 'year':
                record.duration = record.period * (60 * 24 * 365)