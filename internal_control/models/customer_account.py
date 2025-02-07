from odoo import models, fields, api, _

class CustomerAccount(models.Model):
    _inherit = "res.partner.account"
    _rec_name = 'accounttitle'
    
    officercode = fields.Many2one(
        comodel_name='res.account.officer', string='Account Officer', index=True)
    sectorcode = fields.Many2one(
        comodel_name='res.partner.sector', string='Sector', index=True)
    product_type_id = fields.Many2one(
        comodel_name='res.partner.account.product', string='Product Type',index=True)
    lnbalance = fields.Char(string="Ln Bal.")
    bkbalance = fields.Char(string="Bk Bal.")
    unclearedbal = fields.Char(string="Total Creadit")
    holdbal = fields.Char(string="Hold Bal.")
    totdebit = fields.Char(string="Total Debit")
    totcredit = fields.Char(string="Total Creadit")
    last_month_balance = fields.Char(string="Last Month Bal.")
    lien = fields.Char(string="Lien")
    account_tier = fields.Many2one(
        comodel_name='res.partner.tier', string='Account Tier', index=True)
    source_account_id = fields.Char(string="Source Account ID", index=True)
    Status = fields.Boolean(default=False)
    accounttitle = fields.Char(string="Account Title")
    product_id = fields.Many2one(
        comodel_name='res.bank.product', string='Product',index=True)
 

    @api.model
    def open_accounts_tier_1(self):
        tier_1 = self.env['res.partner.tier'].search([('code', '=', '001')], limit=1)
        if not tier_1:
            return False

        domain = [('account_tier', '=', tier_1.id)]  # Start with the base domain
        
         # Check if the current user is a Chief Compliance Officer
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        
        if not is_cco:  # Only apply branch filtering if not a CCO
            domain.append(('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]))
        
        return {
            'name': _('Tier 1 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_accounts_tier_2(self):
        tier_2 = self.env['res.partner.tier'].search([('code', '=', '002')], limit=1)
        if not tier_2:
            return False
        
        domain = [('account_tier', '=', tier_2.id)]  # Start with the base domain
        
         # Check if the current user is a Chief Compliance Officer
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        
        if not is_cco:  # Only apply branch filtering if not a CCO
            domain.append(('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]))

        return {
            'name': _('Tier 2 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_accounts_tier_3(self):
        tier_3 = self.env['res.partner.tier'].search([('code', '=', '003')], limit=1)
        if not tier_3:
            return False
        
        domain = [('account_tier', '=', tier_3.id)]  # Start with the base domain
        
         # Check if the current user is a Chief Compliance Officer
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        
        if not is_cco:  # Only apply branch filtering if not a CCO
            domain.append(('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]))

        return {
            'name': _('Tier 3 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }
    
    def update_account_statistics(self):
            """
            Update transaction statistics for all accounts 
            Separate calculations for credit and debit transactions
            """
            self.env.cr.execute("""
                WITH account_dates AS (
                    -- First get the latest transaction date for each account
                    SELECT 
                        account_id,
                        MAX(date_created::date) as reference_date
                    FROM res_customer_transaction
                    GROUP BY account_id
                ),
                stats AS (
                    SELECT 
                        t.account_id,
                        -- Credit Transactions - Last 6 months
                        COUNT(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '180 days') AND 
                            rtt.trantype = 'C' THEN 1 END) as num_credit_6m,
                        COALESCE(AVG(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '180 days') AND 
                            rtt.trantype = 'C' THEN amount END), 0) as avg_credit_6m,
                        COALESCE(MAX(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '180 days') AND 
                            rtt.trantype = 'C' THEN amount END), 0) as max_credit_6m,
                        COALESCE(SUM(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '180 days') AND 
                            rtt.trantype = 'C' THEN amount END), 0) as tot_credit_6m,
                        
                        -- Debit Transactions - Last 6 months
                        COUNT(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '180 days') AND 
                            rtt.trantype = 'D' THEN 1 END) as num_debit_6m,
                        COALESCE(AVG(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '180 days') AND 
                            rtt.trantype = 'D' THEN amount END), 0) as avg_debit_6m,
                        COALESCE(MAX(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '180 days') AND 
                            rtt.trantype = 'D' THEN amount END), 0) as max_debit_6m,
                        COALESCE(SUM(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '180 days') AND 
                            rtt.trantype = 'D' THEN amount END), 0) as tot_debit_6m,
                        
                        -- Credit Transactions - Last 1 year
                        COUNT(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '365 days') AND 
                            rtt.trantype = 'C' THEN 1 END) as num_credit_1y,
                        COALESCE(AVG(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '365 days') AND 
                            rtt.trantype = 'C' THEN amount END), 0) as avg_credit_1y,
                        COALESCE(MAX(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '365 days') AND 
                            rtt.trantype = 'C' THEN amount END), 0) as max_credit_1y,
                        COALESCE(SUM(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '365 days') AND 
                            rtt.trantype = 'C' THEN amount END), 0) as tot_credit_1y,
                        
                        -- Debit Transactions - Last 1 year
                        COUNT(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '365 days') AND 
                            rtt.trantype = 'D' THEN 1 END) as num_debit_1y,
                        COALESCE(AVG(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '365 days') AND 
                            rtt.trantype = 'D' THEN amount END), 0) as avg_debit_1y,
                        COALESCE(MAX(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '365 days') AND 
                            rtt.trantype = 'D' THEN amount END), 0) as max_debit_1y,
                        COALESCE(SUM(CASE WHEN 
                            t.date_created::date >= (ad.reference_date - INTERVAL '365 days') AND 
                            rtt.trantype = 'D' THEN amount END), 0) as tot_debit_1y
                    FROM res_customer_transaction t
                    JOIN account_dates ad ON t.account_id = ad.account_id
                    JOIN res_transaction_type rtt ON t.tran_type = rtt.id
                    GROUP BY t.account_id
                )
                UPDATE res_partner_account rpa
                SET 
                    -- 6 months credit stats
                    num_credit_last6m = stats.num_credit_6m,
                    avg_credit_last6m = ROUND(stats.avg_credit_6m::numeric, 2),
                    max_credit_last6m = stats.max_credit_6m,
                    tot_credit_last6m = stats.tot_credit_6m,
                    
                    -- 6 months debit stats
                    num_debit_last6m = stats.num_debit_6m,
                    avg_debit_last6m = ROUND(stats.avg_debit_6m::numeric, 2),
                    max_debit_last6m = stats.max_debit_6m,
                    tot_debit_last6m = stats.tot_debit_6m,
                    
                    -- 1 year credit stats
                    num_credit_last1y = stats.num_credit_1y,
                    avg_credit_last1y = ROUND(stats.avg_credit_1y::numeric, 2),
                    max_credit_last1y = stats.max_credit_1y,
                    tot_credit_last1y = stats.tot_credit_1y,
                    
                    -- 1 year debit stats
                    num_debit_last1y = stats.num_debit_1y,
                    avg_debit_last1y = ROUND(stats.avg_debit_1y::numeric, 2),
                    max_debit_last1y = stats.max_debit_1y,
                    tot_debit_last1y = stats.tot_debit_1y,
                    
                    write_date = NOW()
                FROM stats
                WHERE rpa.id = stats.account_id
            """)