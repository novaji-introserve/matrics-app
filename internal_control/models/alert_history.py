from odoo import models, fields, api

class alert_history(models.Model):
    _name = 'alert.history'
    _description = "alert history"
    _rec_name = "alert_id"
    _order = 'id desc'

    alert_id = fields.Char(string="alert_id", required=True)
    attachment = fields.Many2one("ir.attachment")
    html_body = fields.Html(string="html body")
    alert_rule_id = fields.Many2one("alert.rules")
    process_category = fields.Char()
    last_checked = fields.Char()
    risk_rating = fields.Char()
    process_id = fields.Char()
    date_created = fields.Char()
    narration = fields.Char()
    email = fields.Char()
    
   
    def generate_csv(self):
        
            id = self.attachment.id
            if id:
                # Perform the download action
                attachment_url = '/web/content/%s?download=true' % id
                return {
                    'type': 'ir.actions.act_url',
                    'url': attachment_url,
                    'target': 'self',
                }
 