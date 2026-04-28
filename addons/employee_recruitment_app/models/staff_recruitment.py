# -*- coding: utf-8 -*-

from odoo import api, fields, models, _
from datetime import date
from odoo.exceptions import UserError, ValidationError


class StaffRequitment(models.Model):
    _name = 'staff.recruitment'
    _description = "Employee recruitment"
    _inherit = ['mail.thread']
    _rec_name = "sequence"

    applicant_id = fields.Many2one('res.users', string="Applicant", required=True)
    department_id = fields.Many2one('hr.department', string="Department", required=True)
    operation_id = fields.Many2one('stock.location', string="Operation")
    experience = fields.Selection([('exp', 'Experience'), ('fresh', 'Fresher')], default='fresh')
    exper_start_date = fields.Date('Start Date')
    exper_end_date = fields.Date('End Date')
    date = fields.Date(string="Date")
    experience_year = fields.Float("Years", compute='compute_years')
    schedule_id = fields.Many2one('resource.calendar', string="Schedule")
    job_id = fields.Many2one('hr.job', string="Job", required=True)
    state = fields.Selection(
        [('draft', 'Draft'), ('approve', 'Approved'), ('confirm', 'Confirmed'), ('cancel', 'Cancel')], string='State',
        default='draft')
    quantity = fields.Integer(string="Quantity")
    department_text = fields.Text(string="Text")
    count = fields.Integer(string="Count", compute='applicant_count')
    sequence = fields.Char(string="Name", default=lambda self: _('New'))

    def action_confirm(self):
        res_obj = self.env['hr.applicant'].sudo()
        res_obj.create(
            {
                'job_id': self.job_id.id,
                'name': self.job_id.name + '/' + self.sequence,
            }
        )
        self.write({'state': 'confirm'})
        return

    def compute_years(self):
        self.experience_year = 0.0
        if self.exper_start_date and self.exper_end_date:
            new_date = self.exper_start_date
            old_date = self.exper_end_date
            delta = old_date - new_date
            self.update({'experience_year': (delta.days) / 365})

    def action_approve(self):
        self.write({'state': 'approve'})
        return

    def action_cancel(self):
        self.write({'state': 'cancel'})
        return

    def action_draft(self):
        self.write({'state': 'draft'})
        return

    @api.model
    def create(self, vals):
        if vals.get('sequence', _('New')) == _('New'):
            seq = self.env['ir.sequence'].next_by_code('staff.recruitment') or '/'
            vals['sequence'] = seq
        return super(StaffRequitment, self).create(vals)
    #
    def action_application(self):
        self.ensure_one()
        return {
            'name': 'Applicants',
            'type': 'ir.actions.act_window',
            'view_mode': 'tree,form',
            'res_model': 'hr.applicant',
            'domain': [('name', '=', self.job_id.name + '/' + self.sequence)],
        }

    def applicant_count(self):
        for recuitment in self:
            recuitment.count = 0
            if recuitment.state == 'confirm':
                recuitment.update({'count': self.env['hr.applicant'].search_count(
                    [('name', '=', self.job_id.name + '/' + self.sequence)])})


class Applicant(models.Model):
    _inherit = 'hr.applicant'

    exist_new_employee = fields.Selection([('existing', 'Existing Experience'), ('new', 'New Employee')], default='new')
    employees_id = fields.Many2one('hr.employee', string="Existing Employee")

    partner_id = fields.Many2one('res.partner', string='Contact')
    person_reference = fields.Char(string="Referred By")

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
