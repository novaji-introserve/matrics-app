# -*- coding: utf-8 -*-

from odoo import fields, models


class HrEmployee(models.Model):
    _inherit = "hr.employee"

    compliance_partner_id = fields.Many2one(
        "res.partner",
        string="Associated Partner",
        related="user_id.partner_id",
        readonly=True,
    )
    risk_score = fields.Float(
        string="Risk Score",
        related="user_id.partner_id.risk_score",
        readonly=True,
    )
    risk_level = fields.Char(
        string="Risk Level",
        related="user_id.partner_id.risk_level",
        readonly=True,
    )
    identity_verification = fields.Selection(
        related="user_id.partner_id.identity_verification",
        string="Identity Verification",
        readonly=True,
    )

    def get_risk_level(self):
        self.ensure_one()
        return self._action_open_compliance_partner()

    def get_risk_score(self):
        self.ensure_one()
        return self._action_open_compliance_partner()

    def _action_open_compliance_partner(self):
        self.ensure_one()
        partner = self.compliance_partner_id
        if not partner:
            return False
        action = self.env.ref(
            "compliance_management.contacts_action_contacts_inherit",
            raise_if_not_found=False,
        )
        if action:
            action_vals = action.read()[0]
        else:
            action_vals = {
                "type": "ir.actions.act_window",
                "res_model": "res.partner",
            }

        action_vals.update(
            {
                "name": partner.display_name,
                "res_id": partner.id,
                "view_mode": "form",
                "views": [(self.env.ref("base.view_partner_form").id, "form")],
                "domain": [("id", "=", partner.id)],
                "target": "current",
                "context": {"form_view_initial_mode": "readonly"},
            }
        )
        action_vals.pop("search_view_id", None)
        return action_vals
