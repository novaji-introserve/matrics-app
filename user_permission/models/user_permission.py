from odoo import models, fields, api
from odoo.exceptions import AccessError
from odoo import SUPERUSER_ID
from odoo.exceptions import ValidationError


class UserPermission(models.Model):
    _name = "user.permission"
    _description = "Custom User Permissions"

    name = fields.Char(string="Permission Name")
    user = fields.Many2one(
        "res.users", string="User", required=True, ondelete="cascade"
    )
    partner = fields.Many2one(related="user.partner_id", store=True)
    model_id = fields.Many2one(
        "ir.model",
        ondelete="set null",
        string="Model",
        domain=lambda self: self._get_custom_model_domain(),
    )

    # @api.model
    # def _get_custom_model_domain(self):
    #     # Search for models that are part of the specified module and are not inherited
    #     modelsd = self.env["ir.model"].search(
    #         [
    #             (
    #                 "modules",
    #                 "ilike",
    #                 "rule_book",
    #             ),  # Case-insensitive search for module_name
    #             ("inherited_model_ids", "=", False),
    #         ]
    #     )
    #     # Filter the models where the 'modules' field exactly matches 'rule_book'
    #     filtered_models = modelsd.filtered(
    #         lambda m: "rule_book" in m.modules.split(", ")
    #     )
    #     model_names = filtered_models.mapped("id")  # Get the model field names
    #     return [("id", "in", model_names)]

    remove_model = ["ir.model.access", "res.company","custom.tree.model","res.config.settings"]

    @api.model
    def _get_custom_model_domain(self):
        # Search for models that are part of the specified module and are not inherited
        modelsd = self.env["ir.model"].search(
            [
                (
                    "modules",
                    "ilike",
                    "rule_book",
                ),  # Case-insensitive search for module_name
                ("inherited_model_ids", "=", False),
            ]
        )
        # Filter the models where the 'modules' field exactly matches 'rule_book'
        filtered_models = modelsd.filtered(
            lambda m: "rule_book" in m.modules.split(", ") and m.model not in self.remove_model
        )
        model_names = filtered_models.mapped("id")  # Get the model field names
        # name = filtered_models.mapped("model")  # Get the model field names
        # print(name)
        # print(model_names)
        return [("id", "in", model_names)]

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
        """
        Check and return permissions for the current user on the given model.
        Disables access checks while retrieving the permissions.
        """
        # Check if the current user is the superuser (admin)
        if self.env.uid == SUPERUSER_ID:
            # Admin has full permissions
            return {
                "can_create": True,
                "can_read": True,
                "can_write": True,
                "can_delete": True,
            }

        # Use sudo to bypass access rights checks for the ir.model lookup
        ir_model_sudo = self.env["ir.model"].sudo()

        # Get the model record from ir.model based on the model name
        model_record = ir_model_sudo.search([("model", "=", model_name)], limit=1)

        # If the model does not exist, return no permissions
        if not model_record:
            return {
                "can_create": False,
                "can_read": False,
                "can_write": False,
                "can_delete": False,
            }

        # Bypass access rules for checking user permissions
        user_permissions = self.sudo().search(
            [("model_id", "=", model_record.id), ("user", "=", self.env.uid)], limit=1
        )

        # If no specific permissions are set for the user, deny all permissions
        if not user_permissions:
            return {
                "can_create": False,
                "can_read": False,
                "can_write": False,
                "can_delete": False,
            }

        # Return the specific permissions for the user
        return {
            "can_create": user_permissions.can_create,
            "can_read": user_permissions.can_read,
            "can_write": user_permissions.can_write,
            "can_delete": user_permissions.can_delete,
        }

    @api.constrains('user', 'model_id')
    def _check_unique_user_model(self):
        """Ensure that a user doesn't have multiple records for the same model."""
        for record in self:
            domain = [('user', '=', record.user.id), ('model_id', '=', record.model_id.id), ('id', '!=', record.id)]
            existing = self.search(domain)
            if existing:
                raise ValidationError(
                    'The same user cannot have more than one permission record for the same model.'
                )

    @api.constrains('name')
    def _check_unique_name(self):
        """Ensure the name is unique."""
        for record in self:
            domain = [('name', '=', record.name), ('id', '!=', record.id)]
            existing = self.search(domain)
            if existing:
                raise ValidationError(
                    'Permission Name must be unique.'
                )

    @api.model
    def get_base_models_from_module(self, module_name):
        # Search for models that are part of the specified module and are not inherited
        models = self.env["ir.model"].search(
            [
                (
                    "modules",
                    "ilike",
                    module_name,
                ),  # Case-insensitive search for module_name
                ("inherited_model_ids", "=", False),
            ]
        )
        # Filter the models where the 'modules' field exactly matches 'rule_book'
        filtered_models = models.filtered(
            lambda m: module_name in m.modules.split(", ")
        )
        model_names = filtered_models.mapped("model")  # Get the model field names
        return model_names


class ResUsers(models.Model):
    _inherit = "res.users"

    custom_permission_ids = fields.One2many(
        "user.permission",  # Corrected the model name
        "user",  # The foreign key field in user.permission model
        string="Permissions",
    )
