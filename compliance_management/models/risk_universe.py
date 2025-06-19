# -*- coding: utf-8 -*-

from odoo import models, fields, api, _, tools


class RiskUniverse(models.Model):
    _name = 'res.risk.universe'
    _description = 'Risk Universe'
    _sql_constraints = [
        ('uniq_risk_universe_code', 'unique(code)',
         "Risk Universe code already exists. Value must be unique!"),
        ('uniq_risk_universe_name', 'unique(name)',
         "Risk Universe Name already exists. Value must be unique!")
    ]
    _order = "name"
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
    weight_percentage = fields.Float(string='Weight Percentage', digits=(5, 2), default=0.0,
                                     help="Weight percentage used in composite risk calculation")
    is_included_in_composite = fields.Boolean(string='Include in Composite', default=True,
                                              help="If checked, this risk universe will be included in composite risk calculations")
 
class PartnerCompositePlanLine(models.Model):
    _name = 'res.partner.composite.plan.line'
    _description = 'Partner Composite Risk Plan Line'

    partner_id = fields.Many2one(
        'res.partner', string='Partner', ondelete='cascade', index=True)
    plan_id = fields.Many2one(
        'res.compliance.risk.assessment.plan', string='Risk Plan', index=True)
    universe_id = fields.Many2one(
        'res.risk.universe', string='Risk Universe', index=True)
    subject_id = fields.Many2one(
        'res.risk.subject', string='Risk Subject')
    matched = fields.Boolean(string='Matched', default=False,
                             help="Whether this plan matched the SQL criteria")
    risk_score = fields.Float(string='Risk Score', digits=(10, 2))
    assessment_id = fields.Many2one(
        'res.risk.assessment', string='Risk Assessment')
    name = fields.Char(related='plan_id.name',
                       string='Plan Name', store=True, readonly=True)
    # Add these new fields
    universe_weight_percentage = fields.Float(related='universe_id.weight_percentage',
                                              string='Universe Weight %', store=False, readonly=True)
    weighted_score = fields.Float(string='Weighted Score', digits=(10, 2),
                                  compute='_compute_weighted_score', store=False)

    @api.depends('risk_score', 'universe_id.weight_percentage')
    def _compute_weighted_score(self):
        for record in self:
            if record.universe_id and record.risk_score:
                record.weighted_score = record.risk_score * \
                    (record.universe_id.weight_percentage / 100.0)
            else:
                record.weighted_score = 0.0
        

