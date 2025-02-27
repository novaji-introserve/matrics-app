from odoo import models, fields, api

class ReActivation(models.Model):
    _name = 'res.reactivation'
    _description = "reactivation"
    # _inherit = ['mail.thread', 'mail.activity.mixin']
    reactivation_id = fields.Char(string="ID")
    accountnumber = fields.Char()
    acctstatus = fields.Integer()
    chargeDue = fields.Float(string='Charge Due', digits=(10, 2))  # Precision 10, Scale 2
    userid = fields.Char()
    authid = fields.Char()
    systemdate = fields.Datetime()
    actualdate = fields.Datetime()

    _sql_constraints = [
        ('reactivation_id_unique', 'unique(reactivation_id)', 'ID must be unique!')
    ]
   

   