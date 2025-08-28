from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


class NFIUExportWizard(models.TransientModel):
    _name = 'nfiu.export.wizard'
    _description = 'NFIU Export Wizard'

    report_ids = fields.Many2many('nfiu.report', string='Reports to Export')
    date_from = fields.Date(string='Date From')
    date_to = fields.Date(string='Date To')
    report_type = fields.Selection([
        ('STR', 'Suspicious Transaction Report'),
        ('CTR', 'Currency Transaction Report'),
        ('UTR', 'Unusual Transaction Report'),
    ], string='Report Type')

    def action_export(self):
        """Export selected reports to XML"""
        reports = self.report_ids
        if not reports:
            # If no specific reports selected, use filter criteria
            domain = []
            if self.date_from:
                domain.append(('submission_date', '>=', self.date_from))
            if self.date_to:
                domain.append(('submission_date', '<=', self.date_to))
            if self.report_type:
                domain.append(('report_code', '=', self.report_type))
            reports = self.env['nfiu.report'].search(domain)

        # Generate XML for all reports
        for report in reports:
            report.generate_xml()
            report.validate_xml()

        return {
            'type': 'ir.actions.act_window',
            'name': 'NFIU Reports',
            'res_model': 'nfiu.report',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', reports.ids)],
        }
