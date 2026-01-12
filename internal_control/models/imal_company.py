from odoo import models, fields, api


class ResImalCompanies(models.Model):
    _name = 'res.imal.companies'
    _description = 'IMAL Companies'
    _rec_name = 'name'
    
    _sql_constraints = [
    ('uniq_comp_code', 'unique(comp_code)',
        "Company code exists. Value must be unique!"),
] 

    comp_code = fields.Char(
        string='Company Code',
        required=True,
        help='Company Code'
    )
    
    
    name = fields.Char(
        string='Company Name',
        required=True
        )