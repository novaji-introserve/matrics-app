from odoo import models, fields, api

class CaseManagement(models.Model):
    _name = 'case.management'
    _description = 'Case Management'
    
    process_category = fields.Selection([
        ('category1', 'Category 1'),
        ('category2', 'Category 2'),
    ], string="Process Category", required=True)

    process_status = fields.Selection([
        ('open', 'Open'),
        ('closed', 'Closed'),
        ('overdue', 'Overdue'),
    ], string="Process Status", required=True)
    

    event_date = fields.Date(string="Event Date", required=True)
    narration = fields.Text(string="Narration")
    data_source = fields.Char(string="Data Source", default="default")
    # customer_id = fields.Many2one('res.', string="Customer", required=True)
    transaction_reference = fields.Char(string="Transaction Reference")
    recommended_action = fields.Text(string="Recommended Action")
    
    severity_id = fields.Many2one('sla.severity', string="Case Severity")
    
    branch_staff_responsible_id = fields.Many2one('res.users', string="Branch Staff Responsible")
    # responsible_department = fields.Many2one('hr.department', string="Responsible Department/Unit")
    supervisor_one_id = fields.Many2one('res.users', string="Supervisor One")
    supervisor_two_id = fields.Many2one('res.users', string="Supervisor Two")
    supervisor_three_id = fields.Many2one('res.users', string="Supervisor Three")
    further_description = fields.Text(string="Further Description")
    attachment = fields.Binary(string="Attachment")
    
    risk_rating = fields.Selection([
        ('low', 'Low'),
        ('medium', 'Medium'),
        ('high', 'High'),
    ], string="Risk Rating", required=True)
    
    reply_log = fields.One2many('case.reply.log', 'case_id', string="Reply Log")
    
    @api.model
    def create(self, vals):
        record = super(CaseManagement, self).create(vals)
        # Add additional logic if needed
        return record

  
    def action_reply_log(self):
        print(self.branch_staff_responsible_id)
        """ This method will open the reply log related to the case """
        self.ensure_one()
        return {
            'name': 'Reply Log',
            'type': 'ir.actions.act_window',
            'res_model': 'case.reply.log',
            'view_mode': 'tree,form',
            'domain': [('case_id', '=', self.id)],
            'context': dict(self.env.context),
        }