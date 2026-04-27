from odoo import models, fields, api, _

class CustomerAccount(models.Model):
    _inherit = "res.partner.account"
    _rec_name = 'accounttitle'
    
    officercode = fields.Many2one(
        comodel_name='res.account.officer', string='Account Officer', index=True)
    sectorcode = fields.Many2one(
        comodel_name='res.partner.sector', string='Sector', index=True)
    product_type_id = fields.Many2one(
        comodel_name='res.partner.account.product', string='Product Type', index=True)
    lnbalance = fields.Char(string="Ln Bal.")
    bkbalance = fields.Char(string="Bk Bal.")
    unclearedbal = fields.Char(string="Total Credit")
    holdbal = fields.Char(string="Hold Bal.")
    totdebit = fields.Char(string="Total Debit")
    totcredit = fields.Char(string="Total Credit")
    last_month_balance = fields.Char(string="Last Month Bal.")
    lien = fields.Char(string="Lien")
    account_tier = fields.Many2one(
        comodel_name='res.partner.tier', string='Account Tier', index=True)
    source_account_id = fields.Char(string="Source Account ID", index=True)
    Status = fields.Boolean(default=False)
    accounttitle = fields.Char(string="Account Title")
    product_id = fields.Many2one(
        comodel_name='res.bank.product', string='Product', index=True)
    date_closed = fields.Date(string='Date Closed')
    account_class = fields.Char(string='Account Class')
    freeze_code = fields.Char(string='Freeze Code')
    cleared_balance = fields.Char(string='Cleared Balance')
    bvn = fields.Char(string='BVN')

    @api.model
    def _get_accounts_by_tier_sql(self, tier_codes, branch_ids=None, limit=100):
        """
        SQL-based account filtering by tier codes
        Works perfectly with ETL data
        """
        # Build base query
        query = """
            SELECT rpa.id 
            FROM res_partner_account rpa
            JOIN res_partner_tier rpt ON rpa.account_tier = rpt.id
            WHERE rpt.code IN %s
        """
        
        params = [tuple(tier_codes)]
        
        # Add branch filtering if needed
        if branch_ids:
            placeholders = ','.join(['%s'] * len(branch_ids))
            query += f" AND rpa.branch_id IN ({placeholders})"
            params.extend(branch_ids)
        
        # Add ordering and limit
        query += " ORDER BY rpa.id DESC LIMIT %s"
        params.append(limit)
        
        # Execute query
        self.env.cr.execute(query, params)
        result_ids = [row[0] for row in self.env.cr.fetchall()]
        
        return result_ids

    @api.model
    def _get_branch_ids_for_user(self):
        """Helper method to get branch IDs for current user"""
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        if is_cco:
            return None  # CCO sees all branches
        else:
            return self.env.user.branches_id.ids

    @api.model
    def open_accounts_tier_1(self):
        branch_ids = self._get_branch_ids_for_user()
        tier_1_codes = ['SAV025', 'SAV011']
        account_ids = self._get_accounts_by_tier_sql(tier_1_codes, branch_ids, limit=100)
        
        return {
            'name': _('Tier 1 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', account_ids)],
            'limit': 100,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_accounts_tier_2(self):
        branch_ids = self._get_branch_ids_for_user()
        tier_2_codes = ['SAV023']
        account_ids = self._get_accounts_by_tier_sql(tier_2_codes, branch_ids, limit=100)
        
        return {
            'name': _('Tier 2 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', account_ids)],
            'limit': 100,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_accounts_tier_3(self):
        branch_ids = self._get_branch_ids_for_user()
        tier_3_codes = [
            'CUR028', 'CUR027', 'CUR050', 'CUR061', 'CUR053', 'CUR054', 'CUR056', 'CUR034', 'CUR029',
            'CUR041', 'CUR039', 'CUR040', 'CUR012', 'CUR013', 'CUR017', 'CUR019', 'CUR022', 'CUR023',
            'CUR087', 'CUR024', 'CUR025', 'CUR026', 'CUR030', 'CUR031', 'CUR033', 'CUR036', 'CUR042',
            'CUR046', 'CUR049', 'CUR051', 'CUR052', 'CUR057', 'CUR062', 'CUR063', 'CUR098', 'CUR118',
            'CUR119', 'CUR122', 'CUR123', 'CUR124', 'CUR125', 'CUR126', 'CUR128', 'CUR129', 'CUR130',
            'CUR131', 'CUR132', 'CUR137', 'CUR138', 'CUR154', 'CUR139', 'CUR153', 'CUR162', 'CUR161',
            'CUR160', 'CUR173', 'HYB004', 'HYB016', 'SAV0010', 'CUR148', 'SAV014', 'SAV015', 'SAV016',
            'SAV017', 'SAV018', 'SAV019', 'SAV022', 'SAV026', 'SAV027', 'SAV028', 'SAV030', 'SAV031',
            'SAV032', 'SAV033', 'CUR117', 'CUR116', 'SAV034', 'SAV035', 'SAV036', 'SAV061', 'SAV062',
            'SAV063', 'SAV064', 'SAV065', 'SAV067', 'SAV068', 'SAV069', 'HYB013', 'HYB012', 'SAV073',
            'HYB0010', 'CUR113', 'CUR112', 'CUR111', 'CUR110', 'CUR108', 'CUR107', 'CUR106', 'CUR105',
            'CUR104', 'CUR0100', 'CUR099', 'SAV075', 'SAV095', 'CUR095', 'CUR092', 'CUR090', 'CUR089',
            'SAV076', 'SAV077', 'SAV078', 'SAV079', 'SAV087', 'SAV088', 'SAV089', 'SAV091', 'SAV092',
            'SAV094', 'SAV102', 'SAV103', 'SAV104', 'SAV105', 'SAV110', 'SAV112', 'SAV114', 'SAV116',
            'SAV117', 'SAV118', 'SAV119', 'SAV121', 'SAV127', 'SAV128', 'SAV148', 'SAV149', 'SAV150',
            'SAV151', 'SAV157', 'SAV158'
        ]
        account_ids = self._get_accounts_by_tier_sql(tier_3_codes, branch_ids, limit=100)
        
        return {
            'name': _('Tier 3 Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', account_ids)],
            'limit': 100,
            'context': {'search_default_group_branch': 1}
        }

    def update_account_statistics(self):
        """Update transaction statistics for all accounts"""
        self.env.cr.execute("""
            WITH account_dates AS (
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