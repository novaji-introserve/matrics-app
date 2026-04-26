# -*- encoding: utf-8 -*-

from odoo import api, fields, models, _


class kra_wizard(models.TransientModel):
    _name = "kra.wizard"
    _description = "KRA Wizard"

    year = fields.Many2one('employee.year', 'Year', required=True)
    month = fields.Selection([('1', 'January'), ('2', 'February'), ('3', 'March'), ('4', 'April'), ('5', 'May'), ('6', 'June'), ('7', 'July'), ('8', 'August'), ('9', 'September'), ('10', 'october'), ('11', 'November'), ('12', 'December') ], "Month", default=1, required=True)
    quarterly = fields.Selection([('1', 'First Quarter'), ('2', 'Second Quarter'), ('3', 'Third Quarter'), ('4', 'Fourth Quarter')], "KRA Quarter")
    employee_ids = fields.Many2many('hr.employee', 'hr_employee_kra_wizard', 'emp_id', 'wiz_id', 'Employees')
    all_employee = fields.Boolean('All Employees')
    

    def create_kra(self):
        EmployeeObj = self.env['hr.employee']
        EmpKraObj = self.env['employee.kra']
        EmpKraQueObj = self.env['employee.kra.question']
        KraObj = self.env['hr.kra']
        if self.all_employee:
            employees = EmployeeObj.search([('job_id','!=', False), ('job_id.kra_id','!=', False)])

        else:
            employees = self.employee_ids

        for emp in employees:
            emp_kra_id = EmpKraObj.create({
                'year':  self.year.id,
                'name':  self.month,
                'employee_id': emp.id,
                'quarterly': self.quarterly,
            })
            for que in emp.job_id.kra_id.kra_question_ids:
                EmpKraQueObj.create({
                    'employee_id': emp.id,
                    'name': que.name,
                    'description': que.description,
                    'weightage': que.weightage,
                    'kra_question_id': que.id, 
                    'employee_kra_id': emp_kra_id.id,
                    'hint': que.hint,
                    'sequence': que.sequence,
                    'display_type': que.display_type,
                    'section_id': que.section_id and que.section_id.id or False,
                })

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4: