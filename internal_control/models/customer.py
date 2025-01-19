from odoo import models, fields, api

class Customer(models.Model):
    _inherit = "res.partner" 
    
    customerid = fields.Char(string="Customer ID", index=True, tracking=True)
    # title = fields.Char(string="title", index=True, tracking=True)
    surname = fields.Char(string='surname')
    othername = fields.Char(string='Other Name')
    fullname = fields.Char(string="fullname", index=True, tracking=True)
    customertype = fields.Char(index=True, tracking=True)
    sex = fields.Char(index=True, tracking=True)
    nationality = fields.Char(index=True, tracking=True)
    edulevel = fields.Char(index=True, tracking=True)
    statecode = fields.Char(index=True, tracking=True)
    occupation = fields.Char(index=True, tracking=True)
    address = fields.Char(index=True, tracking=True)
    residentstatecode = fields.Char(index=True, tracking=True)
    residenttypecode = fields.Char(index=True, tracking=True)
    idtype = fields.Char(index=True, tracking=True)
    pep1 = fields.Char(index=True, tracking=True)
    idissuedate = fields.Char(index=True, tracking=True)
    idexpirydate = fields.Char(index=True, tracking=True)
    nin = fields.Char(index=True, tracking=True)
    status = fields.Boolean(index=True, tracking=True)
    phone1 = fields.Char(index=True, tracking=True)
    