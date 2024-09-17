from odoo import models, fields, api

# Department Model
class Department(models.Model):
    _name = 'rulebook.department'
    _description = 'Department'
    _rec_name = 'name'
    _inherit = ["mail.thread", "mail.activity.mixin"]

    name = fields.Char(string='Name', required=True, tracking=True)
    user_ids = fields.Many2many('res.users', string='Users', tracking=True)




class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    module_attachment_indexation = fields.Boolean(string="Index Attachments")  
    group_ir_attachment_user = fields.Boolean(string="Attachment User Group")  
    module_document_page = fields.Boolean(string="Enable Document Pages")
    module_mgmtsystem_quality = fields.Boolean(string="Enable Document Pages")
    module_mgmtsystem_environment = fields.Boolean(string="Enable Document Pages")
    module_document_page_approval = fields.Boolean(
        string="Enable Document Page Approval",
        help="Check this box to enable document page approval feature."
    )