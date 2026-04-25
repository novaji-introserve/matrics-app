import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from odoo import fields, models, api
from dotenv import load_dotenv
import base64
from urllib.parse import unquote, urlparse
from os.path import basename
import json

load_dotenv()


class ExternalResource(models.Model):
    _name = "external.resource"
    _description = "External Resource Model"

    mime_type = fields.Char(
        string="MIME Type", help="The MIME type of the external resource."
    )
    name = fields.Char(
        string="Name", required=True, help="The name of the external resource."
    )
    filename = fields.Char(
        string="Filename", help="The original filename of the resource."
    )
    ref_number = fields.Char(
        string="Reference Number", help="A unique reference number for this resource."
    )
    release_date = fields.Date(
        string="Release Date", help="The date when the resource was released."
    )
    source_id = fields.Many2one(
        "res.partner",
        string="Source",
        help="The source or origin of the external resource.",
    )
    created_by = fields.Many2one(
        "res.users",
        string="Created By",
        default=lambda self: self.env.user,
        help="The user who created the resource.",
    )
    channel = fields.Char(
        string="Channel", help="The channel through which the resource is available."
    )
    external_resource_url = fields.Char(
        string="External Resource URL", help="URL linking to the external resource."
    )

    # Optional: Adding auto-generated timestamp fields
    create_date = fields.Datetime(string="Created On", readonly=True)
    write_date = fields.Datetime(string="Last Updated On", readonly=True)

    