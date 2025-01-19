from odoo import models, fields, api
import logging
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

class TransactionMonitoring(models.Model):
    _rec_name = "refno"
    _inherit = ["res.customer.transaction"]
    _sql_constraints = [
        ('uniq_refno', 'unique(refno)',
         "Ref No already exists. Value must be unique!"),
    ] 

    # id = fields.Integer(string="id", readonly=True)
    refno = fields.Char(string="Ref Number", readonly=True, index=True)
    postseq = fields.Char(string="Post Sequence", readonly=True, index=True)
    trandate = fields.Char(string="Transaction Date", readonly=True, index=True)
    tranamount = fields.Char(string="Transaction Amount", readonly=True, index=True)
    trancode = fields.Char(string="Transaction Code", readonly=True, index=True)
    valuedate = fields.Char(string="Value Date", readonly=True, index=True)
    actualdate = fields.Char(string="Actual Date", readonly=True, index=True)
    userid = fields.Char(string="User ID", readonly=True, index=True)
    # narration = fields.Char(string="Narration", readonly=True, index=True)
    trancurrency = fields.Char(string="Transaction Currency", readonly=True, index=True)
    crossrate = fields.Char(string="Cross Rate", readonly=True, index=True)
    charge = fields.Char(string="Charge", readonly=True, index=True)
    reversal = fields.Char(string="Reversal Flag", readonly=True, index=True)
    accountmodule = fields.Char(string="Account Module", readonly=True, index=True)
    chknum = fields.Char(string="Check Number", readonly=True, index=True)
    tellerno = fields.Char(string="Teller Number", readonly=True, index=True)
    subbranchcode = fields.Char(string="Sub Branch Code", readonly=True, index=True)
    deptcode = fields.Char(string="Department Code", readonly=True, index=True)
    authid = fields.Char(string="Auth ID", readonly=True, index=True)
    status = fields.Char(string="Status", readonly=True, index=True)
    batchno = fields.Char(string="Batch Number", readonly=True, index=True)
    accountnumber = fields.Char(string="Account Number", readonly=True, index=True)
    tran_channel = fields.Char(string="Transaction Channel", readonly=True, index=True)
    request_id = fields.Char(string="Request ID", readonly=True, index=True)
    rrn = fields.Char(string="RRN", readonly=True, index=True)
    rem = fields.Char(string="REM", readonly=True, index=True)
    overideid = fields.Char(string="Override ID", readonly=True, index=True)
    

    @api.model
    def open_transactions(self):
    

        
    
        return {
            'name': 'Transactions To Review',
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('subbranchcode', 'in', [e.branchcode.strip() for e in self.env.user.branches_id]),  ('state', '=', 'new')],
           'context': {'search_default_group_branch': 1, 'default_state': 'new'}
        }

    @api.model
    def open_transactions_done(self):
        return {
            'name': 'Reviewed Transactions',
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('state', '=', 'done')],
            'domain': [('subbranchcode', 'in', [e.branchcode.strip() for e in self.env.user.branches_id]),  ('state', '=', 'done')],
            # 'context': {'search_default_group_by': ['subbranchcode']},
            'context': {'search_default_group_branch': 1, 'default_state': 'new'}
           
        }

    @api.model
    def open_transactions_all(self):

        branch_ids = [e.id for e in self.env.user.branches_id]
    
      
        if(len(branch_ids) > 0):
    
         return {
            'name': 'All Transactions',
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('subbranchcode', 'in', [e.branchcode.strip() for e in self.env.user.branches_id])],
            'context': {'search_default_group_branch': 1, 'default_state': 'new'}
        }
    
    def action_screen(self):
     
        rules = self.env['res.transaction.screening.rule'].search(
            [('state', '=', 'active')], order='priority')

        if rules:
            for rule in rules:
                # try:
                    query = rule.sql_query
                    char_to_replace = {'#AMOUNT#': f"{self.amount}",
                                    '#ACCOUNT_ID#': f"{self.account_id.id}",
                                    "#CUSTOMER_ID#": f"{self.customer_id.id}",
                                    "#TRAN_DATE#": f"{self.date_created}",
                                    "#BRANCH_ID#": f"{self.branch_id.id}",
                                    "#CURRENCY_ID#": f"{self.currency_id.id}"}
                    # Iterate over all key-value pairs in dictionary
                    for key, value in char_to_replace.items():
                        # Replace key character with value character in string
                        query = query.replace(key, value)
                        
                    self.env.cr.execute(query)
                   
                    records = self.env.cr.fetchall()
                    
                    
                    for rec in records:

                        record = self.env['res.customer.transaction'].browse(rec[0])  # rec[0] contains the ID of the record
    
                        # Make sure the record exists and then update it
                        if record.exists() and not record.rule_id:
                            record.write({
                                'rule_id': rule.id,  # Assuming 'rule' is a record
                                'risk_level': rule.risk_level,
                            })
                            print(f"Record {record.id}: rule_id updated to {rule.id}, risk_level updated to {rule.risk_level}, rule name: {rule.name}")
                     