# models/report_rulebook.py
from odoo import models, fields, api

class ReportRulebook(models.Model):
    _name = "report.rulebook"
    _description = "Report Rulebook"
    _inherit = ["mail.thread", "mail.activity.mixin"]
    _rec_name = "name"

    name = fields.Char("Report Name", required=True, tracking=True)
    department_id = fields.Many2one('hr.department', string='Department')
    user_department = fields.Many2one('hr.department', string="User Department", related='user_id.department', store=True)

    user_id = fields.Many2one(
        "res.users",
        string="Uploaded by",
        default=lambda self: self.env.user,
        tracking=True,
    )
    content = fields.Text("Content", tracking=True)
    document = fields.Binary("Document", attachment=True)
    document_filename = fields.Char("Document Filename")
    date_created = fields.Datetime("Created Date", default=fields.Datetime.now)
    date_submitted = fields.Datetime("Submission Date")
    date_approved = fields.Datetime("Approval Date")
    due_date = fields.Datetime("Due Date", required=True)
    response_status = fields.Selection([
        ('on_time', 'On Time'),
        ('late', 'Late'),
        ('not_responded', 'Not Responded')
    ], compute='_compute_response_status', store=True, string='Response Status')
    state = fields.Selection([
        ('draft', 'Draft'),
        ('submitted', 'Submitted'),
        ('approved', 'Approved')
    ], string='Status', default='draft', tracking=True)

    @api.model
    def default_get(self, fields_list):
        res = super(ReportRulebook, self).default_get(fields_list)
        if 'department' in fields_list and not res.get('department'):
            user = self.env.user
            res['department'] = user.department
        return res

    @api.depends('date_submitted', 'due_date')
    def _compute_response_status(self):
        for record in self:
            if record.due_date: 
                if fields.Datetime.now() > record.due_date:
                    record.response_status = 'late'
                else:
                    record.response_status = 'on_time'
            else:
                record.response_status = 'on_time' 

    def action_submit(self):
        self.write({
            'state': 'submitted',
            'date_submitted': fields.Datetime.now()
        })

    def action_approve(self):
        self.write({
            'state': 'approved',
            'date_approved': fields.Datetime.now()
        })

    def action_draft(self):
        self.write({'state': 'draft'})

        
