# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskAssessment(models.Model):
    _name = 'res.risk.assessment'
    _description = 'Risk Assessment'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    # _sql_constraints = [
    #     ('uniq_risk_assessment_name', 'unique(name)',
    #      "Risk Assessment Name already exists. Value must be unique!")
    # ]
    _order = "name"

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", index=True)
    state = fields.Char(string="State")
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
    risk_rating = fields.Float(
        string='Risk Rating', digits=(10, 2), default=0.0)
    narration = fields.Html(string='Narration')
    subject_id = fields.Many2one(
        comodel_name='res.risk.subject', string='Risk Subject', index=True)
    universe_id = fields.Many2one(
        comodel_name='res.risk.universe', string='Risk Universe', index=True)
    recommendation = fields.Html(string='Recommendation')
    assessment_type_id = fields.Many2one(
        comodel_name='res.risk.assessment.type', string='Assessment Type')
    type_id = fields.Many2one(comodel_name='res.risk.type', string='Risk Type')
    partner_id = fields.Many2one(comodel_name='res.partner', string='Partner')
    line_ids = fields.One2many(comodel_name='res.risk.assessment.line',
                               inverse_name='risk_assessment_id', string='Risk Assessment Lines')
    total_risk_lines = fields.Integer(
        string='Total Risk Lines', _compute='_compute_total_risk_lines', store=True)
    internal_category = fields.Selection(string='Internal Category', selection=[('inst', 'Institutional'), ('cp', 'Counter Party')],default='inst')
    is_default = fields.Boolean(string='Is Default',tracking=True)

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
    @api.model
    def create(self, vals):
        record = super(RiskAssessment, self).create(vals)
        for e in record:
            e.action_update_risk_score()
        return record

    def _compute_total_risk_lines(self):
        self.total_risk_lines = len(self.line_ids)

    def write(self, vals):
       
        for e in self:
            score = e.compute_risk_score_from_lines()
            vals['risk_rating'] = score
        record = super(RiskAssessment, self).write(vals)
        return record

    def action_update_risk_score(self):
        for rec in self:
            score = self.compute_risk_score_from_lines()
            rec.write({"risk_rating": score})     

    
    
    def compute_risk_score_from_lines(self):
        # Set a default value BEFORE the search. 'max' is a good default
        # based on your if/else logic.
        plan_setting = 'avg' 
        
        setting = self.env['res.compliance.settings'].search([('code','=','risk_assessment_computation')], limit=1)
        
        # Use a simple 'if' since you only expect one record.
        # If a setting IS found, this will overwrite the default value.
        if setting:
            plan_setting = setting.val.strip().lower()

        # Now, 'plan_setting' is GUARANTEED to have a value here.
        if plan_setting == 'avg':
            self.env.cr.execute("SELECT avg(residual_risk_score) FROM res_risk_assessment_line WHERE risk_assessment_id = %s", (self.id,))
        else:
            self.env.cr.execute("SELECT max(residual_risk_score) FROM res_risk_assessment_line WHERE risk_assessment_id = %s", (self.id,))
        
        rec = self.env.cr.fetchone()
        result = 0.0
        
        # Your try/except block here is good for handling None results.
        # Small improvement to handle the case where rec[0] is None.
        try:
            if rec and rec[0] is not None:
                result = float(f"{rec[0]:.2f}")
        except (TypeError, ValueError):
            result = 0.0
            
        return result

    @api.depends('line_ids')
    def _compute_risk_score(self):
        score = self.compute_risk_score_from_lines()
        for rec in self:
    
             rec.write({"risk_rating": score}) 
    
    @api.onchange('line_ids')
    def _onchange_line_ids(self):
        #score = self.compute_risk_score_from_lines()
        #for rec in self:
        #     rec.write({"risk_rating": score})
        pass
    
    def action_total_risk_lines(self):
        self.ensure_one()
        return {
            "type": "ir.actions.act_window",
            "res_model": "res.risk.assessment",
            "views": [[False, "tree"], [False, "form"]],
            "domain": [["id", "=", self.id]],
        }
        
    # filter subject based on universe

    @api.onchange('universe_id')
    def filter_subjects(self):
        for rec in self:
            return {'domain': {'subject_id': [('universe_id', '=', rec.universe_id.id)]}}
        