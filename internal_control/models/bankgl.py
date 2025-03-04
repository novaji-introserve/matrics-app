from odoo import models, fields

class ResBankGL(models.Model):
    _name = 'res.bankgl'  # Use a module-style name
    _description = 'Bank GL Accounts'

    glnumber = fields.Char(string='GL Number', required=True)
    acctname = fields.Char(string='Account Name')
    branchcode = fields.Char(string='Branch Code', size=10)
    gl_classcode = fields.Char(string='GL Class Code', size=10)
    currencycode = fields.Char(string='Currency Code', size=10)
    dateopened = fields.Datetime(string='Date Opened')
    dt_lst_month = fields.Datetime(string='Last Month Date')
    last_month_balance = fields.Float(string='Last Month Balance') # Using float for decimal
    status = fields.Integer(string='Status')
    last_night_balance = fields.Float(string='Last Night Balance')
    bkbalance = fields.Float(string='Bank Balance')
    tpostdebit = fields.Float(string='Total Post Debit')
    tpostcredit = fields.Float(string='Total Post Credit')
    blocked = fields.Char(string='Blocked', size=1)
    closed = fields.Char(string='Closed', size=1)
    reconlen = fields.Integer(string='Reconciliation Length')
    post = fields.Integer(string='Post')
    bbf = fields.Float(string='BBF')
    prodtype = fields.Char(string='Product Type', size=10)
    pointing = fields.Integer(string='Pointing')
    typep = fields.Char(string='Type P', size=2)
    userid = fields.Char(string='User ID', size=50)
    authid = fields.Char(string='Authorization ID', size=50)
    createdate = fields.Datetime(string='Create Date')
    populate = fields.Integer(string='Populate')
    oldglno = fields.Char(string='Old GL Number', size=22)
    last_night_balance2 = fields.Float(string='Last Night Balance 2')
    swing = fields.Integer(string='Swing')
    last_month_balance2 = fields.Float(string='Last Month Balance 2')
    last_month_balance1 = fields.Float(string='Last Month Balance 1')
    last_eom2 = fields.Datetime(string='Last EOM 2')
    last_eom1 = fields.Datetime(string='Last EOM 1')
    currmondiff = fields.Float(string='Current Month Difference')
    lastmondiff = fields.Float(string='Last Month Difference')
    oldglno2 = fields.Char(string='Old GL Number 2', size=22)
    oldglno3 = fields.Char(string='Old GL Number 3', size=22)

    _sql_constraints = [
        ('glnumber_unique', 'unique(glnumber)', 'GL Number must be unique!') # mimicking primary key
    ]