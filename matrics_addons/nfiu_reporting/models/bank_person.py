from odoo import models, fields, api


class BankPerson(models.Model):
    _name = 'bank.person'
    _description = 'Bank Person'
    _rec_name = 'full_name'

    # Personal Information
    title = fields.Char(string='Title')
    first_name = fields.Char(string='First Name', required=True)
    last_name = fields.Char(string='Last Name', required=True)
    full_name = fields.Char(string='Full Name',
                       compute='_compute_full_name', store=True)
    gender = fields.Selection(
        [('M', 'Male'), ('F', 'Female')], string='Gender')
    birthdate = fields.Date(string='Birth Date')

    # Nationality and Residence
    nationality1 = fields.Char(string='Primary Nationality')
    nationality2 = fields.Char(string='Secondary Nationality')
    residence = fields.Char(string='Country of Residence')

    # Professional Information
    occupation = fields.Char(string='Occupation')
    source_of_wealth = fields.Char(string='Source of Wealth')

    # Tax Information
    tax_number = fields.Char(string='Tax Number')
    tax_reg_number = fields.Char(string='Tax Registration Number')

    # Related Records
    phone_ids = fields.One2many(
        'person.phone', 'person_id', string='Phone Numbers')
    address_ids = fields.One2many(
        'person.address', 'person_id', string='Addresses')
    identification_ids = fields.One2many(
        'person.identification', 'person_id', string='Identifications')
    signatory_ids = fields.One2many(
        'account.signatory', 'person_id', string='Account Signatories')

    @api.depends('first_name', 'last_name')
    def _compute_full_name(self):
        for record in self:
            if record.first_name and record.last_name:
                record.full_name = f"{record.first_name} {record.last_name}"
            else:
                record.full_name = record.first_name or record.last_name or ''


class PersonPhone(models.Model):
    _name = 'person.phone'
    _description = 'Person Phone Number'

    person_id = fields.Many2one(
        'bank.person', string='Person', required=True, ondelete='cascade')
    contact_type = fields.Selection([
        ('P', 'Personal'),
        ('B', 'Business'),
        ('H', 'Home'),
        ('W', 'Work')
    ], string='Contact Type')
    communication_type = fields.Selection([
        ('M', 'Mobile'),
        ('L', 'Landline'),
        ('F', 'Fax')
    ], string='Communication Type')
    country_prefix = fields.Char(string='Country Prefix')
    phone_number = fields.Char(string='Phone Number', required=True)
    full_number = fields.Char(string='Full Number',
                              compute='_compute_full_number', store=True)

    @api.depends('country_prefix', 'phone_number')
    def _compute_full_number(self):
        for record in self:
            if record.country_prefix and record.phone_number:
                record.full_number = f"{record.country_prefix}{record.phone_number}"
            else:
                record.full_number = record.phone_number or ''


class PersonAddress(models.Model):
    _name = 'person.address'
    _description = 'Person Address'

    person_id = fields.Many2one(
        'bank.person', string='Person', required=True, ondelete='cascade')
    address_type = fields.Selection([
        ('P', 'Permanent'),
        ('T', 'Temporary'),
        ('B', 'Business'),
        ('M', 'Mailing')
    ], string='Address Type')
    address = fields.Text(string='Address', required=True)
    city = fields.Char(string='City')
    state = fields.Char(string='State/Province')
    country_code = fields.Char(string='Country Code')
    postal_code = fields.Char(string='Postal Code')


class PersonIdentification(models.Model):
    _name = 'person.identification'
    _description = 'Person Identification'

    person_id = fields.Many2one(
        'bank.person', string='Person', required=True, ondelete='cascade')
    identification_type = fields.Selection([
        ('B', 'BVN'),
        ('P', 'Passport'),
        ('N', 'National ID'),
        ('D', 'Driver License'),
        ('V', 'Voter ID'),
        ('I', 'International Passport')
    ], string='Identification Type')
    number = fields.Char(string='ID Number', required=True)
    issue_date = fields.Date(string='Issue Date')
    expiry_date = fields.Date(string='Expiry Date')
    issue_country = fields.Char(string='Issue Country')
    issuing_authority = fields.Char(string='Issuing Authority')
