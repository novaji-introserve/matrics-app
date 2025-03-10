from odoo import models, fields, api
import requests
import json
from datetime import datetime


class CustomTreeModel(models.TransientModel):
    _name = "custom.tree.model"
    _description = "Custom Tree Model"

    unique_id = fields.Char(string="Unique ID")
    type = fields.Selection(
        [("others", "Others"), ("phone", "Phone"), ("email", "Email")], string="Type"
    )
    updated_at = fields.Datetime(string="Updated At")
    created_at = fields.Datetime(string="Created At")

    # @api.model
    def fetch_data_from_api(self):
        api_url = "http://soft-token.novajii.com/api/test/users/generate-all-user"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer Sem9q92MRfRNh3r4wo9qDuCYgTeBj6xYRuH7nwLRlj4Gi1F7lVPRxvLhgapI",
            "Accept": "application/json"
        }
        response = requests.get(api_url, headers=headers)

        if response.status_code == 200:

            data = response.json().get("data", [])
            print(data)
            print('testing')
            self.env["custom.tree.model"].search([]).unlink()

            for item in data['data']:
                self.create(
                    {
                        "unique_id": item["unique_id"],
                        "type": item["type"],
                        "updated_at": datetime.strptime(item["updated_at"], "%Y-%m-%dT%H:%M:%S.%fZ"),
                        "created_at":datetime.strptime(item["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ") ,
                    }
                )
        else:
            print(response.json())
            print(response)
            raise Exception("Failed to fetch data from API")

    def create_new_user(self):
        api_url = "http://soft-token.novajii.com/api/test/users/generate-user"
        payload = {
            "unique_id": self.unique_id,
            "type": self.type,
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(api_url, data=json.dumps(payload), headers=headers)

        if response.status_code == 200:
            # Code to send an email immediately after creation
            self.env["mail.mail"].create(
                {
                    "subject": "New User Created",
                    "body_html": f"<p>User {self.unique_id} has been created.</p>",
                    "email_to": "recipient@example.com",  # Replace with the actual recipient
                }
            ).send()
        else:
            raise Exception("Failed to create user via API")

    @api.model
    def get_view(self, view_id=None, view_type="form", **options):
        view = super().get_view(view_id, view_type, **options)
        self.fetch_data_from_api()
        return view        
