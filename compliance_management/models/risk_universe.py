# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


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
   
class PartnerRiskUniverseWeight(models.Model):
    _name = 'res.partner.risk.universe.weight'
    _description = 'Partner Risk Universe Weight'

    
    partner_id = fields.Many2one(
        'res.partner', string='Partner', ondelete='cascade', index=True)
    universe_id = fields.Many2one(
        'res.risk.universe', string='Risk Universe', index=True)
    weight_percentage = fields.Float(string='Weight %', digits=(5, 2))
    universe_score = fields.Float(string='Risk Score', digits=(10, 2))
    weighted_score = fields.Float(string='Weighted Score', digits=(10, 2))
    name = fields.Char(related='universe_id.name',
                       string='Universe', store=True, readonly=True)
    code = fields.Char(related='universe_id.code',
                    string='Code', store=True, readonly=True)
    subject_id = fields.Many2one(
        'res.risk.subject', string='Risk Subject', readonly=True)
    assessment_id = fields.Many2one(
        'res.risk.assessment', string='Risk Assessment', readonly=True)
    assigned_score = fields.Float(string='Risk Score', digits=(10, 2),
                                  help="The original risk assessment score assigned to this universe")

    
class PartnerRiskUniverseWeightReport(models.Model):
    _name = 'partner.risk.universe.weight.report'
    _description = 'Partner Risk Universe Weight Report'
    _auto = False
    _order = 'weight_percentage desc'

    partner_id = fields.Many2one(
        'res.partner', string='Partner', readonly=True)
    universe_id = fields.Many2one(
        'res.risk.universe', string='Risk Universe', readonly=True)
    subject_id = fields.Many2one(
        'res.risk.subject', string='Risk Subject', readonly=True)
    name = fields.Char(string='Universe Name', readonly=True)
    code = fields.Char(string='Universe Code', readonly=True)
    weight_percentage = fields.Float(string='Weight %', readonly=True)
    universe_score = fields.Float(string='Risk Score', readonly=True)
    weighted_score = fields.Float(string='Weighted Score', readonly=True)
    assigned_score = fields.Float(string='Risk Score', readonly=True)


    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute('''
            CREATE OR REPLACE VIEW %s AS (
                SELECT
                    pruw.id as id,
                    pruw.partner_id as partner_id,
                    pruw.universe_id as universe_id,
                    pruw.subject_id as subject_id,
                    ru.name as name,
                    ru.code as code,
                    pruw.weight_percentage as weight_percentage,
                    pruw.universe_score as universe_score,
                    pruw.weighted_score as weighted_score,
                    pruw.assigned_score  as assigned_score 
                FROM
                    res_partner_risk_universe_weight pruw
                JOIN
                    res_risk_universe ru ON ru.id = pruw.universe_id
                WHERE
                    ru.is_included_in_composite = True
            )
        ''' % (self._table,))

        self._cr.execute(
            "CREATE INDEX IF NOT EXISTS res_risk_universe_composite_idx ON res_risk_universe(id) WHERE is_included_in_composite=True")
        self._cr.execute(
            "CREATE INDEX IF NOT EXISTS res_partner_risk_universe_weight_universe_id_idx ON res_partner_risk_universe_weight(universe_id) ")
        
