import logging
from odoo import models, fields

_logger = logging.getLogger(__name__)


class _SafeDict(dict):
    """Return empty string for any missing placeholder key."""
    def __missing__(self, key):
        return ''


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

    def render(self, **kwargs):
        """Return the full assembled email HTML with placeholders interpolated.

        Missing placeholders render as empty string so a partially-filled
        template never crashes. ``{inline_style}`` in html_header is resolved
        from the record's own ``inline_style`` field before any caller kwargs
        are applied.
        """
        self.ensure_one()
        safe = _SafeDict(inline_style=self.inline_style or '', **kwargs)
        header = (self.html_header or '').format_map(safe)
        body = (self.html_body or '').format_map(safe)
        footer = self.html_footer or ''
        return header + body + footer
