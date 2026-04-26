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
            domain = [("id", "!=", partner.id),("name", "=ilike", partner.name),]

            other = self.search(domain)
            if other :
                raise ValidationError(_("This Name Already Exist For to partner '%s'")% other[0].display_name)
