# Copyright 2016 Antonio Espinosa
# Copyright 2020 Tecnativa - João Marques
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).

from odoo import _, api, models
from odoo.exceptions import ValidationError


class ResPartner(models.Model):
    _inherit = "res.partner"

    @api.constrains("name")
    def _check_ref(self):
        for partner in self.filtered("name"):
            # Skip partners linked to internal users (e.g. Administrator, Public User)
            if partner.user_ids:
                continue
            # Skip private addresses (employee personal contacts created by hr module)
            if partner.type == "private":
                continue
            domain = [
                ("id", "!=", partner.id),
                ("name", "=ilike", partner.name),
                ("user_ids", "=", False),
                ("type", "!=", "private"),
            ]
            other = self.search(domain)
            if other:
                raise ValidationError(_("This Name Already Exist For to partner '%s'") % other[0].display_name)
