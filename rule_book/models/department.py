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


# inherit the user class to ensure every new user is added to a department
from odoo import models, api, _
from odoo.exceptions import ValidationError

class ResUsers(models.Model):
    _inherit = 'res.users'
    department_ids = fields.Many2many('rulebook.department', string='Departments',required=True,  help="Every user must be added to at least one department.")
    
    @api.model
    def create(self, vals):
        
        # Check if department_ids are provided
        if 'department_ids' not in vals or not vals.get('department_ids'):
            raise ValidationError(_("You must assign the user to at least one department before creating them."))

        user = super(ResUsers, self).create(vals)
        return user