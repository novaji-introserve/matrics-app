# -*- coding: utf-8 -*-
from odoo import _, api, fields, models


class ReportEntities(models.Model):
    _name = 'res.regulatory.report.entity'
    _description = 'Regulatory Report Entity'

    name = fields.Char(string='Name', required=True)
    code = fields.Char(string='Code', required=True, unique=True)
    description = fields.Text(string='Description')
    
    def action_save(self):
        """        Placeholder method for saving the report entity.
        """
        pass
    
    def action_cancel(self):
        """        Placeholder method for canceling the report entity.
        """
        pass    
