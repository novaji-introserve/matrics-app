# -*- coding: utf-8 -*-

from odoo import api, fields, models, _
from datetime import datetime, date
from odoo.exceptions import UserError, ValidationError


mapping = ['sys_process','follow_instr','flexible','plan','job_knowledge','skill','learn_skill','accuracy','reliability','cust_sati','work_comple','pressure','handling','relationship','prob_solv','dec_mak','time_mng','express','share_know',
              'seeks','open_ideas','enthu','trust','ettiquttes','punctuality','descipline','attendance','team_work','team_build','strategy', 'participation']
mapping_avg = ['sys_process','follow_instr','flexible','plan','job_knowledge','skill','learn_skill','accuracy','reliability','cust_sati','work_comple','pressure','handling']


class hr_job(models.Model):
    _inherit = 'hr.job'

    kra_id = fields.Many2one('hr.kra', 'KRA')
 

class hr_employee(models.Model):
    _inherit = 'hr.employee'

    def _kra_count(self):
        for rec in self:
            kras = self.env['employee.kra'].search([('employee_id', '=', rec.id)])
            rec.kra_count = len(kras)

    def _value_rating_count(self):
        for rec in self:
            value_ratings = self.env['value.rating'].search([('employee_id', '=', rec.id)])
            rec.value_rating_count = len(value_ratings)

    kra_id = fields.Many2one('hr.kra', related='job_id.kra_id', string="KRA", readonly=True)
    employee_code = fields.Integer('Employee Code')
    kra_count = fields.Integer(compute='_kra_count', string="KRA #")
    value_rating_count = fields.Integer(compute='_value_rating_count', string="Value Ratings")
    
    def action_kra_tree_view(self):
        action = self.env["ir.actions.actions"]._for_xml_id("employee_kra.action_employee_kra")
        action['domain'] = [('employee_id','=',self.id)]
        action['context'] = {'default_employee_id': self.id}
        return action

    def action_value_rating_tree_view(self):
        action = self.env["ir.actions.actions"]._for_xml_id("employee_kra.action_employee_value_rating")
        action['domain'] = [('employee_id','=',self.id)]
        action['context'] = {'default_employee_id': self.id}
        return action


class employee_kra(models.Model):
    _name = 'employee.kra'
    _description = 'Employee KRA'
    _inherit = ['mail.thread']
    _rec_name = 'employee_id'

    name = fields.Selection([('1', 'January'), ('2', 'February'), ('3', 'March'), ('4', 'April'), ('5', 'May'), ('6', 'June'), ('7', 'July'), ('8', 'August'), ('9', 'September'), ('10', 'october'), ('11', 'November'), ('12', 'December') ], "KRA Month", required=True)
    quarterly = fields.Selection([('1', 'First Quarter'), ('2', 'Second Quarter'), ('3', 'Third Quarter'), ('4', 'Fourth Quarter')], "KRA Quarter")
    year = fields.Many2one('employee.year', 'Year', required=True)
    employee_id = fields.Many2one('hr.employee', 'Employee', required=True)
    kra_id = fields.Many2one('hr.kra', related='employee_id.kra_id', string="KRA", readonly=True)
    kra_question_ids = fields.One2many('employee.kra.question', 'employee_kra_id', 'KRA Question')
    date = fields.Date("Date", default=fields.Date.today)
    state = fields.Selection([('draft', 'Draft'), ('submit', 'Submited To Supervisor'), ('cancel', 'Cancelled'), ('done', 'Done'), ], "State", tracking=True, default='draft')
    
    def unlink(self):
        for rec in self:
            if rec.state not in ('draft', 'cancel'):
                raise UserError(_('You cannot delete KRA which is not draft or cancelled.'))
        return super(employee_kra, self).unlink()
    
    def action_submit(self):
        self.state = 'submit'

    def action_cancel(self):
        self.state = 'cancel'

    def action_done(self):
        self.state = 'done'

    @api.onchange('employee_id')
    def onchange_employee(self):
        data = []
        for que in self.employee_id.job_id.kra_id.kra_question_ids:
            data.append((0,0,{
                'employee_id': self.employee_id.id,
                'name': que.name,
                'description': que.description,
                'weightage': que.weightage,
                'kra_question_id': que.id, 
                #'employee_kra_id': self.id,
                'sequence': que.sequence,
                'hint': que.hint,
                'section_id': que.section_id and que.section_id.id or False,
                'display_type': que.display_type}))
        self.kra_question_ids = data


class employee_kra_question(models.Model):
    _name = 'employee.kra.question'
    _description = 'Employee KRA Question'
    _order = 'sequence'

    @api.depends('manager_rating')
    def _compute_total(self):
        for que in self:
            que.final_score = (que.weightage * que.manager_rating) / 10

    @api.constrains('employee_rating', 'manager_rating')
    def _check_max_limit(self):
        for que in self:
            if (que.employee_rating < 0.0 or que.employee_rating > 10.0):
                raise ValidationError((_("Rating in between 0-10 only")))
            if (que.manager_rating < 0.0 or que.manager_rating > 10.0):
                raise ValidationError((_("Rating in between 0-10 only")))

    def _acs_is_manager(self):
        is_hr_manager = self.env.user.has_group('hr.group_hr_user')
        for rec in self:
            rec.is_hr_manager = is_hr_manager

    is_hr_manager = fields.Boolean(compute=_acs_is_manager)
    name = fields.Char('Question')
    sequence = fields.Integer('Sr.No')
    description = fields.Text('Description')
    hint = fields.Char('Hint')
    employee_kra_id = fields.Many2one('employee.kra', 'KRA', ondelete='cascade')
    employee_id = fields.Many2one('hr.employee', string="Employee")
    kra_question_id = fields.Many2one('kra.question', 'KRA Question')
    employee_remark = fields.Char('Employee Remark')
    manager_remark = fields.Char('Manager Remark')
    employee_rating = fields.Float('Employee Rating')
    manager_rating = fields.Float('Manager Rating')
    weightage = fields.Integer('Weightage')
    final_score = fields.Float(compute='_compute_total', string='Final Score', store=True,readonly='1')
    display_type = fields.Selection([
        ('line_section', "Section")], default=False, help="Technical field for UX purpose.")
    section_id = fields.Many2one('kra.question.section', 'Section', ondelete='cascade')


class hr_kra(models.Model):
    _name = 'hr.kra'
    _description = 'HR KRA'
    _inherit = ['mail.thread']

    @api.constrains('kra_question_ids')
    def _check_allocation(self):
        total = 0.0
        for percentage in self:
            for amount in percentage.kra_question_ids:
                total += amount.weightage
            if total == 100 or total == 0:
                return
            else:
                raise ValidationError((_("Warning!| The total Weightage distribution should be 100%.")))
        return

    name = fields.Char('Name', required=True)
    kra_question_ids = fields.One2many('kra.question', 'kra_id', 'KRA Question')


class kra_question(models.Model):
    _name = 'kra.question'
    _description = 'KRA Question'
    _order = 'sequence'

    sequence = fields.Integer('Sr.No')
    kra_id = fields.Many2one('hr.kra', 'KRA', ondelete='cascade')
    section_id = fields.Many2one('kra.question.section', 'Section', ondelete='cascade')
    name = fields.Char('Question')
    description = fields.Text('Description')
    hint = fields.Char('Hint')
    weightage = fields.Integer('Weightage')
    display_type = fields.Selection([
        ('line_section', "Section")], default=False, help="Technical field for UX purpose.")


class KraQuestionSections(models.Model):
    _name = 'kra.question.section'
    _description = 'KRA Section'
    _order = 'sequence'

    sequence = fields.Integer('Sr.No')
    name = fields.Char('Section Name')
    description = fields.Text('Description')


class value_rating(models.Model):
    _name = 'value.rating'
    _description = 'Value Rating'
    _inherit = ['mail.thread']
    _rec_name = 'employee_id'

    @api.constrains('sys_process','follow_instr','flexible','plan','job_knowledge','skill','learn_skill','accuracy','reliability','cust_sati','work_comple','pressure','handling','relationship','prob_solv','dec_mak','time_mng','express','share_know',
                                                                'seeks','open_ideas','enthu','trust','ettiquttes','punctuality','descipline','attendance','team_work','team_build','strategy', 'participation')
    def _check_max_limit(self):
        for values in self:
            for val in mapping:
                if (values[val] < 0.0 or values[val] > 5.0):
                    raise ValidationError((_("Value Rating in between 0-5 only")))
        return

    def calculate_avg(self):
        res = 0.0
        for rec in self:
            total = 0.0
            for val in mapping_avg:
                total += rec[val]
            rec.score_leader =  round((total /len(mapping_avg)), 2)

    def total_average(self):
        for rec in self:
            total = 0.0
            for val in mapping:
                total += rec[val]
            rec.total_avg =  round((total /len(mapping)), 2)

    def _is_manager(self):
        for rec in self:
            if self.env.user.has_group('hr.group_hr_manager'):
                rec.is_manager = True

            elif rec.employee_id.parent_id and rec.employee_id.parent_id.user_id:
                rec.is_manager = True if rec.employee_id.parent_id.user_id==self.env.user else False

    is_manager = fields.Boolean(compute='_is_manager', default=True, string="Is Manager")

    employee_id = fields.Many2one('hr.employee', 'Employee Name', required=True)
    employee_code = fields.Integer(related='employee_id.employee_code', string="Employee Code" ,readonly=True)
    month = fields.Selection([('1', 'January'), ('2', 'February'), ('3', 'March'), ('4', 'April'), ('5', 'May'), ('6', 'June'),
                              ('7', 'July'), ('8', 'August'), ('9', 'September'), ('10', 'October'), ('11', 'November'), ('12', 'December')], 'Month', required=True)
    year = fields.Many2one('employee.year', 'Year', required=True)
    designation = fields.Many2one('hr.job', related='employee_id.job_id', string='Designation', readonly=True)
    appraiser_id = fields.Many2one('hr.employee', related='employee_id.parent_id', string="Appraiser", store=True, readonly=True)
    sys_process = fields.Float('System and Processes')
    follow_instr = fields.Float('Follow Instructions')
    flexible = fields.Float('Adaptable and Flexible')
    plan = fields.Float('Ability To Plan')
    job_knowledge = fields.Float('Job Knowledge')
    skill = fields.Float('Skill To Handle Work')
    learn_skill = fields.Float('Learn New Skill')
    accuracy = fields.Float('Accuracy')
    reliability = fields.Float('Reliability')
    cust_sati = fields.Float('Client Satisfaction')
    work_comple = fields.Float('Work Completion On Time')
    pressure = fields.Float('Ability to work under pressure')
    handling = fields.Float('Handling new portfolio')
    score_leader = fields.Float(compute="calculate_avg" , string='Leadership Score', readonly='1',
        help="This shows avg value for fields of foru sections: Approach To Work, Technical Skills, Quality Of work, Handling Targets")
    relationship = fields.Float('Relationship with co-workers')
    prob_solv = fields.Float('Problem solving')
    dec_mak = fields.Float('Decision making')
    time_mng = fields.Float('Time management')
    express = fields.Float('Oral and written expression')
    share_know = fields.Float('Sharing of knowledge')
    seeks = fields.Float('Seeks T & D')
    open_ideas = fields.Float('Open to ideas')
    enthu = fields.Float('Enthusiastic')
    trust = fields.Float('Trustworthy')
    ettiquttes = fields.Float('Work Place ettiquttes')
    punctuality = fields.Float('Punctuality')
    descipline = fields.Float('Descipline')
    attendance = fields.Float('Attendance')
    team_work = fields.Float('Team work')
    team_build = fields.Float('Team Building')
    strategy = fields.Float('New Strategy and direction')
    participation = fields.Float('Participation in HR activities')
    total_avg = fields.Float(compute='total_average' , string='Total average', readonly='1')
    state = fields.Selection([('draft', 'Draft'), ('cancel', 'Cancelled'), ('done', 'Done'), ], "State" , tracking=True,default='draft')

    def unlink(self):
        for rec in self:
            if rec.state not in ('draft', 'cancel'):
                raise UserError(_('You cannot delete Record which is not draft or cancelled.'))
        return super(value_rating, self).unlink()

    def action_submit(self):
        self.state = 'submit'
    
    def action_cancel(self):
        self.state = 'cancel'
    
    def action_done(self):
        self.state = 'done'


class employee_year(models.Model):
    _name = 'employee.year'
    _description = 'Employee Year'

    name = fields.Char('Year', size=4)

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4: