from odoo import models, fields, api
import logging
from odoo.exceptions import ValidationError
from datetime import datetime, timedelta


_logger = logging.getLogger(__name__)

class TransactionMonitoring(models.Model):
    _inherit = 'res.customer.transaction'
    _sql_constraints = [
        ('uniq_refno', 'unique(refno)',
         "Transaction already exists. Value must be unique!"),
    ] 

    # id = fields.Integer(string="id", readonly=True)
    refno = fields.Char(string="Ref Number", readonly=True, index=True)
    valuedate = fields.Char(string="Value Date", readonly=True, index=True)
    actualdate = fields.Char(string="Actual Date", readonly=True, index=True)
    userid = fields.Char(string="User ID", readonly=True, index=True)
    reversal = fields.Char(string="Reversal Flag", readonly=True, index=True)
    accountmodule = fields.Char(string="Account Module", readonly=True, index=True)
    tellerno = fields.Char(string="Teller Number", readonly=True, index=True)
    deptcode = fields.Many2one(comodel_name='hr.department',
                              string='Dept. Code', index=True)
    status = fields.Many2one(comodel_name='res.transaction.status',
                              string='Trans. Status', index=True)
    tran_channel = fields.Char(string="Transaction Channel", readonly=True, index=True)
    request_id = fields.Char(string="Request ID", readonly=True, index=True)
    trans_id = fields.Char(string="Transaction ID", readonly=True, index=True)
    

    # @api.model
    # def search(self, args, offset=0, limit=None, order=None, count=False):
    #     # Default filter for last 7 days if no date filter present
    #     if not any(arg[0] == 'valuedate' for arg in args):
    #         date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
    #         args.append(('valuedate', '>=', date_from))
    #     return super().search(args, offset=offset, limit=limit, order=order, count=count)

# class TransactionMonitoring(models.Model):
#     _inherit = 'res.customer.transaction'

    # @api.model
    # def read_group(self, domain, fields, groupby, offset=0, limit=None, orderby=False, lazy=True):
    #     _logger.info(f"""
    #     Read Group Called:
    #     Domain: {domain}
    #     Fields: {fields}
    #     Groupby: {groupby}
    #     Offset: {offset}
    #     Limit: {limit}
    #     """)
    #     return super().read_group(domain, fields, groupby, offset=offset, limit=limit, orderby=orderby, lazy=lazy)

    # @api.model
    # def search_read(self, domain=None, fields=None, offset=0, limit=None, order=None):
    #     _logger.info(f"""
    #     Search Read Called:
    #     Domain: {domain}
    #     Fields: {fields}
    #     Offset: {offset}
    #     Limit: {limit}
    #     Order: {order}
    #     """)
    #     return super().search_read(domain=domain, fields=fields, offset=offset, limit=limit, order=order)
    

    @api.model
    def read_group(self, domain, fields, groupby, offset=0, limit=None, orderby=False, lazy=True):
        try:
            _logger.info(f"Read Group Called: Domain: {domain}, Fields: {fields}, Groupby: {groupby}")
            return super().read_group(domain, fields, groupby, offset=offset, limit=limit, orderby=orderby, lazy=lazy)
        except AttributeError as e:
            # Log detailed information about Many2one fields and their invalid data
            many2one_fields = [f for f in self._fields if self._fields[f].type == 'many2one']
            invalid_fields = {}
            
            for field in many2one_fields:
                for record in self.search(domain):  # Iterate over matching records
                    value = getattr(record, field, None)
                    if value and not value.exists():  # Check if the record exists
                        invalid_fields.setdefault(field, []).append(record.id)

            if invalid_fields:
                _logger.error(f"Invalid Many2one field(s) causing issue: {invalid_fields}")
            
            _logger.error(f"Error in read_group: {e}")
            raise ValidationError("A Many2one field contains invalid or missing data. Check the logs for more details.")


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
                     