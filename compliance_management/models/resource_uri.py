# -*- coding: utf-8 -*-

from odoo import fields, models


class ResourceURI(models.Model):
    _name = "res.resource.uri"
    _description = "Resource URI"
    _order = "name asc, id asc"

    name = fields.Char(string="Name", required=True, index=True)
    model_uri = fields.Char(string="Model URI", required=True, index=True)
