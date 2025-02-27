from odoo import models, fields

class MandatePicts(models.Model):
    _name = 'res.mandatepicts'
    _description = 'MandatePicts'

    mandatepicts_id = fields.Char(string="ID")
    accountnumber = fields.Char(string='Account Number', size=22)
    customerimage = fields.Binary(string='Customer Image')
    signatureimage = fields.Binary(string='Signature Image')
    image_type = fields.Char(string='Image Type', size=30)
    image_type2 = fields.Char(string='Image Type 2', size=30)
    mandateid = fields.Integer(string='Mandate ID')
    userid = fields.Char(string='User ID', size=50)
    authorisedby = fields.Char(string='Authorised By', size=30)
    status = fields.Integer(string='Status')
    serial = fields.Integer(string='Serial')
    signatoryname = fields.Char(string='Signatory Name', size=100)
    designation = fields.Char(string='Designation', size=50)
    mandatedesc1 = fields.Char(string='Mandate Description 1', size=100)
    createdate = fields.Datetime(string='Create Date')

    _sql_constraints = [
        ('mandatepicts_id_unique', 'unique(mandatepicts_id)', 'ID must be unique!')
    ]
   