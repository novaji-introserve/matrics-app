from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


class NFIUEntityDirector(models.Model):
    _name = 'nfiu.entity.director'
    _description = 'NFIU Entity Director'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    entity_id = fields.Many2one(
        'nfiu.entity', string='Entity', required=True, ondelete='cascade',tracking=True)
    person_id = fields.Many2one('nfiu.person', string='Person', required=True,tracking=True)
    name = fields.Char(string='Name',related='person_id.name',tracking=True)
    role = fields.Selection([
        ('A', 'Authorized Signatory'),
        ('B', 'Board Member'),
        ('C', 'Chairman'),
        ('E', 'Executive Director'),
        ('F', 'Financial Officer'),
        ('N', 'Non-Executive Director'),
        ('P', 'President/CEO'),
        ('S', 'Secretary'),
        ('U', 'Unspecified'),
    ], string='Role', required=True, default='B',tracking=True)

    start_date = fields.Date(string='Start Date',tracking=True)
    end_date = fields.Date(string='End Date',tracking=True)
