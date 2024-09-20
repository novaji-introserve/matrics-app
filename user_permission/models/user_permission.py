from odoo import models, fields, api


class UserPermission(models.Model):
    _name = "user.permission"
    _description = "Custom User Permissions"

    name = fields.Char(string="Permission Name")
    user_id = fields.Many2one(
        "res.users", string="User", required=True, ondelete="cascade"
    )
    model_id = fields.Many2one("ir.model", ondelete="set null", string="Model")
    can_create = fields.Boolean(string="Can Create", default=False)
    can_read = fields.Boolean(string="Can Read", default=True)
    can_write = fields.Boolean(string="Can Edit", default=False)
    can_delete = fields.Boolean(string="Can Delete", default=False)

    @api.model
    def create(self, vals):
        """Create custom permission logic."""
        return super(UserPermission, self).create(vals)

    @api.model
    def check_user_permissions(self, model_name):
        """Check the logged-in user's permissions for a specific model."""
        current_user = self.env.user
        permissions = self.search(
            [("user_id", "=", current_user.id), ("model_id.model", "=", model_name)]
        )
        return {
            "can_create": permissions.can_create,
            "can_read": permissions.can_read,
            "can_write": permissions.can_write,
            "can_delete": permissions.can_delete,
        }

    @api.model
    def check_user_permission(self, model_name, action_type):
        """Check and raise errors based on user permissions."""
        user_permissions = self.check_user_permissions(model_name)
        if action_type == "create" and not user_permissions["can_create"]:
            raise AccessError(
                "You don't have permission to create records in %s." % model_name
            )
        if action_type == "read" and not user_permissions["can_read"]:
            raise AccessError(
                "You don't have permission to read records in %s." % model_name
            )
        if action_type == "write" and not user_permissions["can_write"]:
            raise AccessError(
                "You don't have permission to write records in %s." % model_name
            )
        if action_type == "delete" and not user_permissions["can_delete"]:
            raise AccessError(
                "You don't have permission to delete records in %s." % model_name
            )


class ResUsers(models.Model):
    _inherit = "res.users"

    custom_permission_ids = fields.One2many(
        "user.permission",  # Corrected the model name
        "user_id",  # The foreign key field in user.permission model
        string="Permissions",
    )
