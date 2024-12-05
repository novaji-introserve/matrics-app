from odoo import models, fields, api

class alert_history(models.Model):
    _name = 'alert.history'
    _description = "alert history"
    _rec_name = "alert_id"
    _order = 'id desc'

    alert_id = fields.Char(string="alert_id", required=True)
    attachment_data = fields.Char()
    attachment_link = fields.Char()
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
        
            url = self.attachment_link
            if url:
                # Perform the download action
    
                return {
                    'type': 'ir.actions.act_url',
                    'url': url,
                    'target': 'self',
                }
 