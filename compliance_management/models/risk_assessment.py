# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskAssessment(models.Model):
    _name = 'res.risk.assessment'
    _description = 'Risk Assessment'
    _sql_constraints = [
        ('uniq_risk_assessment_name', 'unique(name)',
         "Risk Assessment Name already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
    risk_rating = fields.Float(
        string='Risk Rating', digits=(10, 2), required=True)
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
    
     # filter subject based on universe
    @api.onchange('universe_id')
    def filter_subjects(self):
        for rec in self:
            return {'domain': {'subject_id': [('universe_id', '=', rec.universe_id.id)]}}
