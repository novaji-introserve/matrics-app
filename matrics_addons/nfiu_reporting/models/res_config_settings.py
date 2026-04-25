from odoo import fields, models


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    nfiu_xsd_attachment_id = fields.Many2one(
        'ir.attachment',
        string='NFIU XSD Attachment',
        domain=[('type', '=', 'binary')],
        help='Schema attachment used to validate generated NFIU XML files.',
    )
    nfiu_xsd_module_path = fields.Char(
        string='Bundled XSD Path',
        help='Fallback path inside the nfiu_reporting module when no attachment is configured.',
    )

    def get_values(self):
        res = super().get_values()
        params = self.env['ir.config_parameter'].sudo()
        attachment_id = params.get_param('nfiu_reporting.xsd_attachment_id')
        res.update(
            nfiu_xsd_attachment_id=int(attachment_id) if attachment_id else False,
            nfiu_xsd_module_path=params.get_param(
                'nfiu_reporting.xsd_module_path',
                default='data/NFIU_goAML_4_5_Schema.xsd',
            ),
        )
        return res

    def set_values(self):
        super().set_values()
        params = self.env['ir.config_parameter'].sudo()
        params.set_param(
            'nfiu_reporting.xsd_attachment_id',
            self.nfiu_xsd_attachment_id.id or '',
        )
        params.set_param(
            'nfiu_reporting.xsd_module_path',
            self.nfiu_xsd_module_path or 'data/NFIU_goAML_4_5_Schema.xsd',
        )
