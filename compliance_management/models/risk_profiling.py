# -*- coding: utf-8 -*-

from odoo import models, fields, api
import requests
import logging
import json
from datetime import datetime, timedelta
import urllib.parse

_logger = logging.getLogger(__name__)


class HighRiskJurisdiction(models.Model):
    _name = "res.high.risk.jurisdiction"
    _description = "High Risk Jurisdiction"
    _order = "risk_score desc"

    name = fields.Char("Country/Region Name", required=True)
    country_code = fields.Char("Country Code", size=3)
    risk_score = fields.Float("Risk Score", default=0.0)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    risk_level = fields.Selection(
        [
            ("low", "Low Risk"),
            ("medium", "Medium Risk"),
            ("high", "High Risk"),
            ("extreme", "Extreme Risk"),
        ],
        string="Risk Level",
        compute="_compute_risk_level",
        store=True,
    )
    last_updated = fields.Datetime("Last Updated")
    factors = fields.Text("Risk Factors")
    notes = fields.Text("Notes")
    active = fields.Boolean("Active", default=True)

    @api.depends("risk_score")
    def _compute_risk_level(self):
        for record in self:
            if record.risk_score < 30:
                record.risk_level = "low"
            elif record.risk_score < 60:
                record.risk_level = "medium"
            elif record.risk_score < 80:
                record.risk_level = "high"
            else:
                record.risk_level = "extreme"


class NewsearchAPISettings(models.Model):
    _name = "icomply.newsearch.settings"
    _description = "Newsearch API Settings"

    api_key = fields.Char("API Key", required=True)
    api_endpoint = fields.Char(
        "API Endpoint", default="https://api.newsapi.org"
    )
    update_frequency = fields.Selection(
        [
            ("daily", "Daily"),
            ("weekly", "Weekly"),
            ("monthly", "Monthly"),
        ],
        string="Update Frequency",
        default="weekly",
    )
    last_update = fields.Datetime("Last Update")

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    @api.model
    def get_settings(self):
        settings = self.search([], limit=1)
        if not settings:
            settings = self.create(
                {
                    "api_key": "demo_key",
                    "api_endpoint": "https://newsapi.org/v2/everything",
                }
            )
        return settings


class RiskProfileUpdate(models.Model):
    _name = "res.risk.profile.update"
    _description = "Risk Profile Update"
    _order = "date desc"

    date = fields.Datetime("Update Date", default=fields.Datetime.now)
    user_id = fields.Many2one(
        "res.users", "User", default=lambda self: self.env.user.id
    )
    update_type = fields.Selection(
        [
            ("manual", "Manual"),
            ("automatic", "Automatic"),
        ],
        string="Update Type",
        default="manual",
    )
    status = fields.Selection(
        [
            ("pending", "Pending"),
            ("in_progress", "In Progress"),
            ("completed", "Completed"),
            ("failed", "Failed"),
        ],
        string="Status",
        default="pending",
    )
    result = fields.Text("Result")

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    def construct_newsapi_url(self,country):
        """
            Constructs a NewsAPI.org URL for crime-related news in a specific country.

            Args:
                country (str): The 2-letter country code (e.g., "us", "gb", "ng").
                keywords (list): A list of keywords related to crime (e.g., ["crime", "theft"]).

            Returns:
                str: The constructed NewsAPI.org URL.
        """
        settings = self.env["icomply.newsearch.settings"].get_settings()
        api_key = settings.api_key
        base_url = settings.api_endpoint

        # Construct the query string
        query_parts = []
        for keyword in ["crime", "robbery", "arrest"]:
            query_parts.append(keyword)
        query = " ".join(query_parts)

        url = f"{base_url}q={urllib.parse.quote_plus(query)}&country={country}&apiKey={api_key}"
        return url


    def update_risk_profiles(self):
        self.status = "in_progress"

        try:
            url = self.construct_newsapi_url("ng")
            print(url)
            # settings = self.env["icomply.newsearch.settings"].get_settings()
            # headers = {
            #     "Authorization": f"Bearer {settings.api_key}",
            #     "Content-Type": "application/json",
            # }

            # response = requests.get(settings.api_endpoint, headers=headers)

            # if response.status_code == 200:
            #     data = response.json()
            #     updated_count = 0
            #     new_count = 0

#                 for jurisdiction_data in data.get("jurisdictions", []):
#                     country_name = jurisdiction_data.get("name")
#                     country_code = jurisdiction_data.get("code")
#                     risk_score = jurisdiction_data.get("risk_score", 0)
#                     factors = json.dumps(jurisdiction_data.get("factors", {}))

#                     jurisdiction = self.env["icomply.high.risk.jurisdiction"].search(
#                         [("country_code", "=", country_code)], limit=1
#                     )

#                     if jurisdiction:
#                         jurisdiction.write(
#                             {
#                                 "risk_score": risk_score,
#                                 "factors": factors,
#                                 "last_updated": fields.Datetime.now(),
#                             }
#                         )
#                         updated_count += 1
#                     else:
#                         self.env["icomply.high.risk.jurisdiction"].create(
#                             {
#                                 "name": country_name,
#                                 "country_code": country_code,
#                                 "risk_score": risk_score,
#                                 "factors": factors,
#                                 "last_updated": fields.Datetime.now(),
#                             }
#                         )
#                         new_count += 1

#                 settings.last_update = fields.Datetime.now()
#                 result = f"Update completed successfully. Updated {updated_count} existing jurisdictions and added {new_count} new jurisdictions."

#                 self.write({"status": "completed", "result": result})

#             else:
#                 error_msg = f"API Error: {response.status_code} - {response.text}"
#                 self.write({"status": "failed", "result": error_msg})

        except Exception as e:
            self.write({"status": "failed", "result": f"Exception: {str(e)}"})


# class ResPartner(models.Model):
#     _inherit = "res.partner"

#     jurisdiction_id = fields.Many2one(
#         "icomply.high.risk.jurisdiction", string="Jurisdiction"
#     )
#     risk_score = fields.Float(
#         related="jurisdiction_id.risk_score", string="Risk Score", store=True
#     )
#     risk_level = fields.Selection(
#         related="jurisdiction_id.risk_level", string="Risk Level", store=True
#     )
#     requires_enhanced_due_diligence = fields.Boolean(
#         "Requires Enhanced Due Diligence",
#         compute="_compute_enhanced_due_diligence",
#         store=True,
#     )

#     @api.depends("risk_level")
#     def _compute_enhanced_due_diligence(self):
#         for partner in self:
#             partner.requires_enhanced_due_diligence = partner.risk_level in [
#                 "high",
#                 "extreme",
#             ]


# class AccountMove(models.Model):
#     _inherit = "account.move"

#     partner_risk_level = fields.Selection(
#         related="partner_id.risk_level", string="Partner Risk Level", store=True
#     )
#     requires_risk_review = fields.Boolean(
#         "Requires Risk Review", compute="_compute_requires_risk_review", store=True
#     )
#     risk_review_state = fields.Selection(
#         [
#             ("not_required", "Not Required"),
#             ("pending", "Pending"),
#             ("approved", "Approved"),
#             ("rejected", "Rejected"),
#         ],
#         string="Risk Review Status",
#         default="not_required",
#     )

#     @api.depends("partner_risk_level")
#     def _compute_requires_risk_review(self):
#         for move in self:
#             move.requires_risk_review = move.partner_risk_level in ["high", "extreme"]
#             if move.requires_risk_review and move.risk_review_state == "not_required":
#                 move.risk_review_state = "pending"


# class AutomaticRiskUpdateCron(models.Model):
#     _name = "icomply.automatic.risk.update"
#     _description = "Automatic Risk Update Cron"

#     @api.model
#     def run_scheduled_update(self):
#         settings = self.env["icomply.newsearch.settings"].get_settings()
#         last_update = settings.last_update

#         if not last_update:
#             self._create_and_run_update()
#             return

#         frequency = settings.update_frequency
#         current_time = fields.Datetime.now()

#         if frequency == "daily" and current_time >= last_update + timedelta(days=1):
#             self._create_and_run_update()
#         elif frequency == "weekly" and current_time >= last_update + timedelta(days=7):
#             self._create_and_run_update()
#         elif frequency == "monthly" and current_time >= last_update + timedelta(
#             days=30
#         ):
#             self._create_and_run_update()

#     def _create_and_run_update(self):
#         update = self.env["icomply.risk.profile.update"].create(
#             {
#                 "update_type": "automatic",
#             }
#         )
#         update.update_risk_profiles()
