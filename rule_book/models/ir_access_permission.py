from odoo import models, api
from odoo.exceptions import AccessError


class IrModelAccess(models.Model):
    _inherit = "ir.model.access"

    remove_model = [
        "ir.model.access",
        "res.company",
        "custom.tree.model",
        "res.config.settings"
        "ir.model"
    ]

    model_list = [
        # "rulebook.department",
        "rulebook",
        "rulebook.report",
        "rulebook.theme",
        "rulebook.exception_process",
        "rulebook.exception_type",
        "external.resource",
        "reply.log",
        "rulebook.responsible",
        "rulebook.risk_category",
        "rulebook.sources",
        "rulebook.title",
        # "rulebook.branch"
        "pdf.chat.log"
    ]

    @api.model
    def _get_custom_model_domain(self, model_name=None):
        if model_name in self.remove_model:
            return []
        # Search for models that are part of the "rule_book" module and are not inherited
        models = self.env["ir.model"].search(
            [
                ("modules", "ilike", "rule_book"),  # Case-insensitive search for "rule_book"
                ("inherited_model_ids", "=", False),
            ]
        )
        print(models)
        # Filter models with modules exactly matching 'rule_book' and exclude unwanted models
        filtered_models = models.filtered(
            lambda m: "rule_book" in m.modules.split(", ") 
            # and m.model not in self.remove_model
        )
        model_name = filtered_models.mapped("name")  # Get the model ids
        # print(model_ids)
        return model_name

    # model_name != "ir.model" is what causes recurring dept
    # @api.model
    # def check(self, model_name, operation, raise_exception=True):

    #     # Print model_name for debugging purpose
    #     # custom_domain = self._get_custom_model_domain(model_name)
    #     # print(custom_domain)
    #     # print(self._get_custom_model_domain())
    #     if self.env.user.has_group("base.group_system") == False:
    #         # Enforce custom permissions for a specific model, e.g., "rulebook"
    #         if model_name in self.model_list:
    #             # Fetch custom user permissions for the model
    #             user_permissions = self.env["user.permission"].check_user_permissions(
    #                 model_name
    #             )

    #             # Check specific operations and permissions
    #             if operation == "create" and not user_permissions.get("can_create", False):
    #                 if raise_exception:
    #                     raise AccessError(
    #                         "You don't have permission to create records in %s."
    #                         % model_name
    #                     )
    #                 return False
    #             if operation == "read" and not user_permissions.get("can_read", False):
    #                 if raise_exception:
    #                     raise AccessError(
    #                         "You don't have permission to read records in %s." % model_name
    #                     )
    #                 return False
    #             if operation == "write" and not user_permissions.get("can_write", False):
    #                 if raise_exception:
    #                     raise AccessError(
    #                         "You don't have permission to write records in %s." % model_name
    #                     )
    #                 return False
    #             if operation == "unlink" and not user_permissions.get("can_delete", False):
    #                 if raise_exception:
    #                     raise AccessError(
    #                         "You don't have permission to delete records in %s."
    #                         % model_name
    #                     )
    #                 return False

    #         # For other models, continue with default behavior
    #     return super(IrModelAccess, self).check(model_name, operation, raise_exception)
