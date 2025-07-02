from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


<<<<<<< HEAD
class Customer(models.Model):
    _inherit = 'res.partner'
    prefix = fields.Char(string='Prefix', size=100)
    birth_place = fields.Char(string='Birth Place', size=255)
    mothers_name = fields.Char(string="Mother's Name", size=100)
    alias = fields.Char(string='Alias', size=100)
    ssn = fields.Char(string='SSN', size=25, tracking=True)
    passport_number = fields.Char(
        string='Passport Number', size=255, tracking=True)
    passport_country = fields.Char(
        string='Passport Country', size=2, tracking=True)
    id_number = fields.Char(
        string='ID Number', size=255, tracking=True)
    nationality1 = fields.Char(
        string='Primary Nationality', size=2, default='NG')
    nationality2 = fields.Char(string='Secondary Nationality', size=2)
    nationality3 = fields.Char(string='Third Nationality', size=2)
    residence = fields.Char(
        string='Country of Residence', size=2, default='NG')
    occupation = fields.Char(
        string='Occupation', size=255, tracking=True)
    employer_name = fields.Char(
        string='Employer Name', size=255, tracking=True)
    source_of_wealth = fields.Char(string='Source of Wealth', size=255)


class NFIUPerson(models.Model):
    _name = 'nfiu.person'
    _description = 'Reporting Person'
=======
class NFIUPerson(models.Model):
    _name = 'nfiu.person'
    _description = 'NFIU Person'
>>>>>>> 816be76 (XML Schema Validator)
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string='Full Name', compute='_compute_name', store=True)
    gender = fields.Selection([
        ('-', 'Not Specified'),
        ('F', 'Female'),
        ('M', 'Male'),
    ], string='Gender', required=True, default='-')

<<<<<<< HEAD
    title = fields.Char(string='Title', size=30, tracking=True)
    first_name = fields.Char(
        string='First Name', required=True, size=100, tracking=True)
    middle_name = fields.Char(string='Middle Name', size=100)
    prefix = fields.Char(string='Prefix', size=100)
    last_name = fields.Char(
        string='Last Name', required=True, size=100, tracking=True)
    birthdate = fields.Date(string='Birth Date',
                            related='employee_id.birthday', tracking=True)
    birth_place = fields.Char(string='Birth Place',
                              related='employee_id.place_of_birth', size=255)
    mothers_name = fields.Char(string="Mother's Name", size=100)
    alias = fields.Char(string='Alias', size=100)
    ssn = fields.Char(string='SSN', size=25, tracking=True)

    passport_number = fields.Char(
        string='Passport Number', related='employee_id.passport_id', size=255, tracking=True)
    passport_country = fields.Char(
        string='Passport Country', size=2, tracking=True)
    id_number = fields.Char(
        string='ID Number', related='employee_id.identification_id', size=255, tracking=True)
=======
    title = fields.Char(string='Title', size=30)
    first_name = fields.Char(string='First Name', required=True, size=100,tracking=True)
    middle_name = fields.Char(string='Middle Name', size=100)
    prefix = fields.Char(string='Prefix', size=100)
    last_name = fields.Char(string='Last Name', required=True, size=100,tracking=True)

    birthdate = fields.Date(string='Birth Date',tracking=True)
    birth_place = fields.Char(string='Birth Place', size=255)
    mothers_name = fields.Char(string="Mother's Name", size=100)
    alias = fields.Char(string='Alias', size=100)
    ssn = fields.Char(string='SSN', size=25,tracking=True)

    passport_number = fields.Char(string='Passport Number', size=255,tracking=True)
    passport_country = fields.Char(string='Passport Country', size=2,tracking=True)
    id_number = fields.Char(string='ID Number', size=255,tracking=True)
>>>>>>> 816be76 (XML Schema Validator)

    nationality1 = fields.Char(
        string='Primary Nationality', size=2, default='NG')
    nationality2 = fields.Char(string='Secondary Nationality', size=2)
    nationality3 = fields.Char(string='Third Nationality', size=2)
    residence = fields.Char(
        string='Country of Residence', size=2, default='NG')

<<<<<<< HEAD
    occupation = fields.Char(
        string='Occupation', related='employee_id.job_id.name', size=255, tracking=True)
    employer_name = fields.Char(
        string='Employer Name', related='employee_id.address_id.name', size=255, tracking=True)
    source_of_wealth = fields.Char(string='Source of Wealth', size=255)

    email = fields.Char(
        string='Email', related='employee_id.work_email', size=255, tracking=True)
    phone = fields.Char(
        string='Phone', related='employee_id.work_phone', size=50, tracking=True)
=======
    occupation = fields.Char(string='Occupation', size=255,tracking=True)
    employer_name = fields.Char(string='Employer Name', size=255,tracking=True)
    source_of_wealth = fields.Char(string='Source of Wealth', size=255)

    email = fields.Char(string='Email', size=255,tracking=True)
    phone = fields.Char(string='Phone', size=50,tracking=True)
>>>>>>> 816be76 (XML Schema Validator)

    address_ids = fields.One2many(
        'nfiu.address', 'person_id', string='Addresses')

    comments = fields.Text(string='Comments', size=4000)
<<<<<<< HEAD
    employee_id = fields.Many2one(
        'hr.employee', string='Related Employee', ondelete='set null', tracking=True)
=======
>>>>>>> 816be76 (XML Schema Validator)

    @api.depends('first_name', 'middle_name', 'last_name')
    def _compute_name(self):
        for person in self:
            name_parts = [person.first_name or '']
            if person.middle_name:
                name_parts.append(person.middle_name)
            name_parts.append(person.last_name or '')
            person.name = ' '.join(filter(None, name_parts))
