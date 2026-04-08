# -*- coding: utf-8 -*-
from odoo import api, fields, models

# Subsidiary group IDs — ordered from lowest to highest access level
_SUBSIDIARY_IDS = [46, 47, 48, 49]

_SUBSIDIARY_SELECTION = [
    ('46', 'Business Control'),
    ('47', 'IT Control'),
    ('48', 'Fsdh Capital'),
    ('49', 'Fsdh Asset Mgt'),
]


class ResUsersSubsidiaries(models.Model):
    """Adds a persistent 'Subsidiaries' selection field to res.users.

    Unlike the auto-generated sel_groups_* virtual fields (which depend on
    fragile caching and active/share flags), this field is a first-class ORM
    field that is always available.
    """
    _inherit = 'res.users'

    subsidiary_group = fields.Selection(
        selection=_SUBSIDIARY_SELECTION,
        string='Subsidiaries',
        compute='_compute_subsidiary_group',
        inverse='_inverse_subsidiary_group',
        store=False,
        required=True,
    )

    @api.depends('groups_id')
    def _compute_subsidiary_group(self):
        """Return the highest-level subsidiary group the user belongs to."""
        for user in self:
            assigned = set(user.groups_id.ids)
            user.subsidiary_group = False
            for gid in reversed(_SUBSIDIARY_IDS):
                if gid in assigned:
                    user.subsidiary_group = str(gid)
                    break

    def _inverse_subsidiary_group(self):
        """Remove all subsidiary groups then add the selected one."""
        for user in self:
            current = set(user.groups_id.ids) & set(_SUBSIDIARY_IDS)
            ops = [(3, gid) for gid in current]
            if user.subsidiary_group:
                ops.append((4, int(user.subsidiary_group)))
            if ops:
                user.sudo().write({'groups_id': ops})
