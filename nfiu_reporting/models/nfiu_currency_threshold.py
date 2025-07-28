from odoo import _, api, fields, models


class NFIUCurrencyThreshold(models.Model):
    _name = 'nfiu.currency.threshold'
<<<<<<< HEAD
<<<<<<< HEAD
    _description = 'Currency Threshold'
=======
    _description = 'NFIU Currency Threshold'
>>>>>>> 816be76 (XML Schema Validator)
=======
    _description = 'Currency Threshold'
>>>>>>> b75258c (Suspicious Transaction history)
    _inherit = ['mail.thread', 'mail.activity.mixin']
        
    threshold = fields.Float(string='Threshold Limit', digits=(10,2),help='Limit that must be reported to NFIU',required=True, tracking=True)
    description = fields.Text(string='Description')
    def set_domain_currency(self):
        """Set domain for currency to get on the ones we need."""
        return [('name', 'in', ['NGN','USD','GBP','CAD','EUR','CNY'])]
    currency_id = fields.Many2one(comodel_name='res.currency', string='Currency',required=True, tracking=True,domain=set_domain_currency)
    name = fields.Char(string='Name',related='currency_id.full_name',required=True, tracking=True)
<<<<<<< HEAD
<<<<<<< HEAD
    shortname = fields.Char(string='Short Name',tracking=True,required=True,default='NG')
=======
>>>>>>> 816be76 (XML Schema Validator)
=======
    shortname = fields.Char(string='Short Name',tracking=True,required=True,default='NG')
>>>>>>> b75258c (Suspicious Transaction history)
