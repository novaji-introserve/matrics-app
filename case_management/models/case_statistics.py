from odoo import models, fields, api

class CaseStatistics(models.Model):
    _name = 'case.statistics'
    _description = 'Case Statistics'

    all_cases = fields.Integer(string='All Cases', compute='_compute_all_cases', store=True)
    open_cases = fields.Integer(string='Open Cases', required=False)
    close_cases = fields.Integer(string='Close Cases', required=False)
    overdue_cases = fields.Integer(string='Overdue Cases', required=False)
    
    open_case_rate = fields.Float(string='Open Case Rate', compute='_compute_rates', store=True)
    close_case_rate = fields.Float(string='Close Case Rate', compute='_compute_rates', store=True)
    
    created_at = fields.Datetime(string='Created At', required=False)
    updated_at = fields.Datetime(string='Updated At', required=False)
    
    @api.depends('open_cases', 'close_cases', 'overdue_cases')
    def _compute_all_cases(self):
        for record in self:
            record.all_cases = (record.open_cases or 0) + (record.close_cases or 0) + (record.overdue_cases or 0)
    
    @api.depends('open_cases', 'close_cases', 'all_cases')
    def _compute_rates(self):
        for record in self:
            total_cases = record.all_cases or 1  # Avoiding division by zero
            record.open_case_rate = (record.open_cases or 0) / total_cases * 100
            record.close_case_rate = (record.close_cases or 0) / total_cases * 100
