from odoo import models, fields

class CaseReplyLog(models.Model):
    _name = 'case.reply.log'
    _description = 'Case Reply Log'

    case_id = fields.Many2one('case.management', string="Case", required=True)
    reply_message = fields.Text(string="Reply Message", required=True)
    reply_date = fields.Datetime(string="Reply Date", default=fields.Datetime.now)
