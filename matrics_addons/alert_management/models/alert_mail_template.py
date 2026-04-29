from odoo import models, fields


class AlertMailTemplate(models.Model):
    _name = 'alert.mail.template'
    _description = 'Alert Mail Template'

    name = fields.Char(string='Name', required=True)
    code = fields.Char(string='Code', required=True)
    html_header = fields.Html(string='HTML Header', sanitize=False)
    inline_style = fields.Text(string='Inline Style (CSS)')
    html_body = fields.Html(string='HTML Body', sanitize=False)
    html_footer = fields.Html(string='HTML Footer', sanitize=False)

    _sql_constraints = [
        ('unique_code', 'UNIQUE(code)', 'Alert template code must be unique!'),
    ]
