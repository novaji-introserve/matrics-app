from odoo import models, fields, api
import logging

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
    postseq = fields.Integer(string="Post Sequence", readonly=True)
    trandate = fields.Char(string="Transaction Date", readonly=True)
    tranamount = fields.Char(string="Transaction Amount", readonly=True)
    valuedate = fields.Char(string="Value Date", readonly=True)
    actualdate = fields.Char(string="Actual Date", readonly=True)
    userid = fields.Char(string="User ID", readonly=True)
    # narration = fields.Char(string="Narration", readonly=True)
    trancurrency = fields.Float(string="Transaction Currency", readonly=True)
    crossrate = fields.Integer(string="Cross Rate", readonly=True)
    charge = fields.Float(string="Charge", readonly=True)
    reversal = fields.Integer(string="Reversal Flag", readonly=True)
    accountmodule = fields.Integer(string="Account Module", readonly=True)
    chknum = fields.Integer(string="Check Number", readonly=True)
    tellerno = fields.Char(string="Teller Number", readonly=True)
    subbranchcode = fields.Char(string="Sub Branch Code", readonly=True)
    deptcode = fields.Char(string="Department Code", readonly=True)
    authid = fields.Char(string="Auth ID", readonly=True)
    status = fields.Integer(string="Status", readonly=True)
    batchno = fields.Char(string="Batch Number", readonly=True)
    accountnumber = fields.Char(string="Account Number", readonly=True)
    tran_channel = fields.Integer(string="Transaction Channel", readonly=True)
    request_id = fields.Char(string="Request ID", readonly=True)
    rrn = fields.Char(string="RRN", readonly=True)
    rem = fields.Char(string="REM", readonly=True)
    overideid = fields.Integer(string="Override ID", readonly=True)
    
    # Computed fields to show "NULL" if data is missing
    # refno_null = fields.Char(string="Reference Number", compute="_compute_null_values")
    # tran_date_null = fields.Char(string="Transaction Date", compute="_compute_null_values")
    # valuedate_null = fields.Char(string="Value Date", compute="_compute_null_values")
    # actual_date_null = fields.Char(string="Actual Date", compute="_compute_null_values")
    # # narration_null = fields.Char(string="Narration", compute="_compute_null_values")
    # tran_currency_null = fields.Char(string="Transaction Currency", compute="_compute_null_values")
    # crossrate_null = fields.Char(string="Cross Rate", compute="_compute_null_values")
    # charge_null = fields.Char(string="Charge", compute="_compute_null_values")
    # reversal_null = fields.Char(string="Reversal Flag", compute="_compute_null_values")
    # account_module_null = fields.Char(string="Account Module", compute="_compute_null_values")
    # chknum_null = fields.Char(string="Check Number", compute="_compute_null_values")
    # tellerno_null = fields.Char(string="Teller Number", compute="_compute_null_values")
    # subbranchcode_null = fields.Char(string="Sub Branch Code", compute="_compute_null_values")
    # deptcode_null = fields.Char(string="Department Code", compute="_compute_null_values")
    # authid_null = fields.Char(string="Auth ID", compute="_compute_null_values")
    # status_null = fields.Char(string="Status", compute="_compute_null_values")
    # batchno_null = fields.Char(string="Batch Number", compute="_compute_null_values")
    # accountnumber_null = fields.Char(string="Account Number", compute="_compute_null_values")
    # tran_channel_null = fields.Char(string="Transaction Channel", compute="_compute_null_values")
    # request_id_null = fields.Char(string="Request ID", compute="_compute_null_values")
    # rrn_null = fields.Char(string="RRN", compute="_compute_null_values")
    # rem_null = fields.Char(string="REM", compute="_compute_null_values")
    # overideid_null = fields.Char(string="Override ID", compute="_compute_null_values")

    # @api.depends('refno', 'TranDate', 'Valuedate', 'ActualDate', 'Narration', 'TranCurrency', 'crossrate',
    #              'charge', 'Reversal', 'AccountModule', 'chkNUm', 'tellerno', 'subbranchcode', 'deptcode',
    #              'authid', 'status', 'batchno', 'AccountNumber', 'Tran_Channel', 'Request_ID', 'RRN', 'REM', 'overideid')
    # def _compute_null_values(self):
    #     for record in self:
    #         record.refno_null = self._get_null_value(record.refno)
    #         record.tran_date_null = self._get_null_value(record.TranDate)
    #         record.valuedate_null = self._get_null_value(record.Valuedate)
    #         record.actual_date_null = self._get_null_value(record.ActualDate)
    #         record.narration_null = self._get_null_value(record.Narration)
    #         record.tran_currency_null = self._get_null_value(record.TranCurrency)
    #         record.crossrate_null = self._get_null_value(record.crossrate)
    #         record.charge_null = self._get_null_value(record.charge)
    #         record.reversal_null = self._get_null_value(record.Reversal)
    #         record.account_module_null = self._get_null_value(record.AccountModule)
    #         record.chknum_null = self._get_null_value(record.chkNUm)
    #         record.tellerno_null = self._get_null_value(record.tellerno)
    #         record.subbranchcode_null = self._get_null_value(record.subbranchcode)
    #         record.deptcode_null = self._get_null_value(record.deptcode)
    #         record.authid_null = self._get_null_value(record.authid)
    #         record.status_null = self._get_null_value(record.status)
    #         record.batchno_null = self._get_null_value(record.batchno)
    #         record.accountnumber_null = self._get_null_value(record.AccountNumber)
    #         record.tran_channel_null = self._get_null_value(record.Tran_Channel)
    #         record.request_id_null = self._get_null_value(record.Request_ID)
    #         record.rrn_null = self._get_null_value(record.RRN)
    #         record.rem_null = self._get_null_value(record.REM)
    #         record.overideid_null = self._get_null_value(record.overideid)

    # def _get_null_value(self, field_value):
    #     """Helper function to return 'NULL' if field is empty or False"""
    #     return "NULL" if not field_value else field_value
    
    # @api.model
    # def _fetch_transactions(self):
    #     """Fetch transactions from the database using raw SQL (only read data)"""
    #     _logger.info("Executing raw SQL query to fetch transactions...")
    #     try:
    #         self.env.cr.execute("""
    #             SELECT 
    #                 "id", "refno", "TranCurrency", "TranDate", "Valuedate", 
    #                 "Userid", "Narration", "Postseq", "Reversal", "AccountModule",
    #                 "chkNUm", "tellerno", "subbranchcode", "deptcode", "authid",
    #                 "status", "batchno", "AccountNumber", "Tran_Channel", "Request_ID",
    #                 "RRN", "ActualDate", "crossrate", "charge", "REM", "overideid"
    #             FROM 
    #                 "tbl_transactions"
    #         """)
        #     transactions = self.env.cr.fetchall()
        #     _logger.info(f"Fetched {len(transactions)} transactions.")
        #     return transactions
        # except Exception as e:
        #     _logger.error(f"Error fetching transactions: {str(e)}")
        #     return []

    # @api.model
    # def post_init(self):
    #     """Automatically fetch transactions when the server starts, without modifying the table"""
    #     _logger.info("Transaction Monitoring: Running post-init to fetch transactions...")
        
        # Fetch the transactions (This is a read-only operation)
    #     transactions = self._fetch_transactions()
        
    #     # Trigger the mismatched transactions server action
    #     self.invoke_mismatched_transactions_action()
        
    #     if transactions:
    #         _logger.info(f"Successfully fetched {len(transactions)} transactions.")
    #     else:
    #         _logger.warning("No transactions fetched or error occurred.")

    # @api.model
    # def get_date_mismatched_transactions(self):
    #     """Fetch transactions where TranDate and Valuedate dates are different."""
    #     _logger.info("Starting get_date_mismatched_transactions method...")
    #     try:
    #         # Query to extract and compare only the date part of TranDate and Valuedate
    #         self.env.cr.execute("""
    #             SELECT 
    #                 id, 
    #                 refno, 
    #                 "TranDate", 
    #                 "Valuedate", 
    #                 LEFT("TranDate", 10) AS tran_date_only,
    #                 LEFT("Valuedate", 10) AS value_date_only
    #             FROM "tbl_transactions"
    #             WHERE 
    #                 LEFT("TranDate", 10) != LEFT("Valuedate", 10)
    #         """)
    #         mismatched_transactions = self.env.cr.fetchall()

    #         _logger.info(f"Found {len(mismatched_transactions)} transactions with date mismatches.")

    #         if mismatched_transactions:
    #             for txn in mismatched_transactions[:10]:  # Log first 10 mismatches
    #                 _logger.info(f"Mismatch - ID: {txn[0]}, RefNo: {txn[1]}, TranDate: {txn[2]}, ValueDate: {txn[3]}")

    #             # Return an action to display these transactions in the UI
    #             mismatched_ids = [txn[0] for txn in mismatched_transactions]
    #             return {
    #                 'type': 'ir.actions.act_window',
    #                 'name': 'Transactions with Date Mismatch',
    #                 'res_model': 'tbl_transactions',
    #                 'view_mode': 'tree,form',
    #                 'domain': [('id', 'in', mismatched_ids)],
    #                 'target': 'current',
    #             }
    #         else:
    #             _logger.info("No transactions with mismatched dates found.")
    #             return {
    #                 'type': 'ir.actions.client',
    #                 'tag': 'display_notification',
    #                 'params': {
    #                     'type': 'info',
    #                     'message': 'No transactions with mismatched dates found.',
    #                     'sticky': False,
    #                 }
    #             }
    #     except Exception as e:
    #         _logger.error(f"Error detecting date mismatches: {str(e)}")
    #         return {
    #             'type': 'ir.actions.client',
    #             'tag': 'display_notification',
    #             'params': {
    #                 'type': 'danger',
    #                 'message': f'Error detecting date mismatches: {str(e)}',
    #                 'sticky': True,
    #             }
    #         }

    # @api.model
    # def invoke_mismatched_transactions_action(self):
    #     """Invoke the server action to fetch mismatched transactions"""
    #     _logger.info("Invoking the mismatched transactions action programmatically...")
        
    #     # Get the server action using its XML ID
    #     action = self.env.ref('internal_control.action_mismatched_transactions')
        
    #     # Trigger the action
    #     return action.read()[0]


    @api.model
    def open_transactions(self):
    
        return {
            'name': 'Transactions To Review',
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('subbranchcode', 'in', [e.id for e in self.env.user.branches_id]),  ('state', '=', 'new')],
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
            'domain': [('subbranchcode', 'in', [e.id for e in self.env.user.branches_id]),  ('state', '=', 'done')],
            # 'context': {'search_default_group_by': ['subbranchcode']},
            'context': {'search_default_group_branch': 1, 'default_state': 'new'}
           
        }

    @api.model
    def open_transactions_all(self):
    
        return {
            'name': 'All Transactions',
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('subbranchcode', 'in', [e.id for e in self.env.user.branches_id])],
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
                     