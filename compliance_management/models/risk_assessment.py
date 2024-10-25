# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskAssessment(models.Model):
    _name = 'res.risk.assessment'
    _description = 'Risk Assessment'
    _inherit = ['mail.thread','mail.activity.mixin']
    _sql_constraints = [
        ('uniq_risk_assessment_name', 'unique(name)',
         "Risk Assessment Name already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
    risk_rating = fields.Float(
        string='Risk Rating', digits=(10, 2), required=True,default=0.0)
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
        string='Total Risk Lines', _compute='_compute_total_risk_lines',store=True)

    @api.model
    def create(self, vals):
        record = super(RiskAssessment, self).create(vals)
        score = record.compute_risk_score_from_lines()
        record.write({"risk_rating": score})
        return record
    
    def _compute_total_risk_lines(self):
        self.total_risk_lines = len(self.line_ids)

    def write(self, vals):
        vals['risk_rating'] = self.compute_risk_score_from_lines()
        record = super(RiskAssessment, self).write(vals)
        return record
    
    def action_update_risk_score(self):
        for rec in self:
            score = self.compute_risk_score_from_lines()
            rec.write({"risk_rating": score})
            

    def compute_risk_score_from_lines(self):
        self.env.cr.execute(
            "SELECT avg(residual_risk_score) FROM res_risk_assessment_line WHERE risk_assessment_id = %s", (self.id,))
        rec = self.env.cr.fetchone()
        return f"{rec[0]:.2f}" if rec is not None else 0.0

    @api.depends('line_ids')
    def _compute_risk_score(self):
        score = self.compute_risk_score_from_lines()
        for rec in self:
            rec.risk_rating = score
  
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
