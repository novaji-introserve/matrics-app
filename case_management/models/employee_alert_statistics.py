from odoo import models, fields

class EmployeeAlertStatistics(models.Model):
    _name = 'employee.alert.statistics'
    _description = 'Employee Alert Statistics'

    staff_name = fields.Char(string='Staff Name', required=False)
    total_cases = fields.Integer(string='Total Cases', required=False)
    open_cases = fields.Integer(string='Open Cases', required=False)
    closed_cases = fields.Integer(string='Closed Cases', required=False)
    overdue_cases = fields.Integer(string='Overdue Cases', required=False)
    
    date_of_last_open_case = fields.Datetime(string='Date of Last Open Case', required=False)
    date_of_last_closed_case = fields.Datetime(string='Date of Last Closed Case', required=False)
    created_at = fields.Datetime(string='Created At', required=False)
    updated_at = fields.Datetime(string='Updated At', required=False)
