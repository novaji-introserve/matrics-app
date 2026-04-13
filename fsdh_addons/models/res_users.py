# -*- coding: utf-8 -*-
from odoo import api, fields, models

_SUBSIDIARY_XML_IDS = [
    'fsdh_addons.group_subsidiary_fsdh_capital',      # Business Control
    'fsdh_addons.group_subsidiary_fsdh_asset_mgt',    # IT Control
    'fsdh_addons.group_subsidiary_business_control',  # Fsdh Capital
    'fsdh_addons.group_subsidiary_it_control',        # Fsdh Asset Mgt
]

_SUBSIDIARY_SELECTION = [
    ('fsdh_addons.group_subsidiary_fsdh_capital', 'Business Control'),
    ('fsdh_addons.group_subsidiary_fsdh_asset_mgt', 'IT Control'),
    ('fsdh_addons.group_subsidiary_business_control', 'Fsdh Capital'),
    ('fsdh_addons.group_subsidiary_it_control', 'Fsdh Asset Mgt'),
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
            for xml_id in reversed(_SUBSIDIARY_XML_IDS):
                group = self.env.ref(xml_id, raise_if_not_found=False)
                if group and group.id in assigned:
                    user.subsidiary_group = xml_id
                    break

    def _inverse_subsidiary_group(self):
        """Remove all subsidiary groups then add the selected one."""
        sub_group_ids = []
        for xml_id in _SUBSIDIARY_XML_IDS:
            group = self.env.ref(xml_id, raise_if_not_found=False)
            if group:
                sub_group_ids.append(group.id)

        for user in self:
            current = set(user.groups_id.ids) & set(sub_group_ids)
            ops = [(3, gid) for gid in current]
            if user.subsidiary_group:
                selected_group = self.env.ref(user.subsidiary_group, raise_if_not_found=False)
                if selected_group:
                    ops.append((4, selected_group.id))
            if ops:
                user.sudo().write({'groups_id': ops})
