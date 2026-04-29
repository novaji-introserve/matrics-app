# -*- coding: utf-8 -*-

from odoo import models, fields, api, _, tools
import logging
from collections import defaultdict
_logger = logging.getLogger(__name__)


class CustomerAccount(models.Model):
    _name = 'res.partner.account'
    _description = 'Account'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _sql_constraints = [
        ('uniq_account_id', 'unique(name)',
         "Account Number already exists. Value must be unique!"),
    ]

    _order = 'id desc'
    _ACCOUNT_STATS_LOCK_KEY = 62016421
    _ACCOUNT_STATS_DEFAULT_BATCH_SIZE = 2000
    _ACCOUNT_STATS_MAX_BATCH_SIZE = 10000

    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', index=True)  # customer
    name = fields.Char(string="Account Number", index=True)
    account_name = fields.Char(
        string='Account Name', index=True)  # account_title1
    account_position = fields.Char(string='Account Position', required=False)
    account_type = fields.Char(string='Account Type', required=False)
    account_code = fields.Char(string='Account Code', required=False)
    account_status = fields.Char(string='Account Status', required=False)
    high_transactions_account = fields.Boolean(
        string="High Transaction Account", required=False)
    last_transaction_date = fields.Date(
        string='Last Transaction Date', required=False)
    opening_date = fields.Date(
        string='Opening Date', required=False, index=True)
    # date_created = fields.Date(
    #     string='Date Created', required=False, index=True)

    account_officer_id = fields.Many2one(
        comodel_name='account.officers', string='Account Officer', required=False)  # acct_officer

    currency = fields.Char(
        string='Currency', required=False, index=True)  # currency

    category = fields.Char(string='Category', required=False,index=True)  # category

    category_description = fields.Char(
        string='Category Description', required=False,index=True)  # category_desc

    is_joint_account = fields.Boolean(
        string='Is Joint Account', required=False)  # category_desc

    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)

    product_id = fields.Many2one(
        comodel_name='res.partner.account.product', string='Product', index=True)
    
    ledger_id = fields.Many2one(
        comodel_name='res.ledger', string='Ledger', index=True)
    ledger_type_id = fields.Many2one(
        comodel_name='res.ledger.type',
        string='Ledger Type',
        related='ledger_id.ledger_type_id',
        store=True,
        readonly=True,
    )
    closure_status = fields.Selection(string='Closure Status', selection=[
                                      ('N', 'No'), ('Y', 'Yes')])
    branch_id = fields.Many2one(
        comodel_name='res.branch', string='Branch', index=True)
    balance = fields.Float(string='Balance', digits=(15, 4))  # working_balance
    account_type_id = fields.Many2one(
        comodel_name='res.partner.account.type', string='Account Type', index=True)

    risk_assessment = fields.Many2one(
        comodel_name='res.risk.assessment', string='Risk Assessment')
    
    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)
    branch_code = fields.Char(string="Branch Code")

    # num_tran_last6m_credit = fields.Integer(string='Transactions - Last 6m')
    # avg_tran_last6m_credit = fields.Float(string='Avg. Transaction Amount - Last 6m', digits=(10,2))
    # max_tran_last6m_credit = fields.Float(string='Max. Transaction - Last 6m', digits=(10,2))
    # tot_tran_last6m_credit = fields.Float(string='Total Transaction Amount - Last 6m', digits=(15,2))
    # num_tran_last1y_credit = fields.Integer(string='Transactions - Last 1Y')
    # avg_tran_last1y_credit = fields.Float(string='Avg. Transaction Amount - Last 1Y', digits=(10,2))
    # max_tran_last1y_credit = fields.Float(string='Max. Transaction - Last 1Y', digits=(10,2))
    # tot_tran_last1y_credit = fields.Float(string='Total Transaction Amount - Last 1Y', digits=(15,2))
    # num_tran_last6m_debit = fields.Integer(string='Transactions - Last 6m')
    # avg_tran_last6m_debit = fields.Float(string='Avg. Transaction Amount - Last 6m', digits=(10,2))
    # max_tran_last6m_debit = fields.Float(string='Max. Transaction - Last 6m', digits=(10,2))
    # tot_tran_last6m_debit = fields.Float(string='Total Transaction Amount - Last 6m', digits=(15,2))
    # num_tran_last1y_debit = fields.Integer(string='Transactions - Last 1Y')
    # avg_tran_last1y_debit = fields.Float(string='Avg. Transaction Amount - Last 1Y', digits=(10,2))
    # max_tran_last1y_debit = fields.Float(string='Max. Transaction - Last 1Y', digits=(10,2))
    # tot_tran_last1y_debit = fields.Float(string='Total Transaction Amount - Last 1Y', digits=(15,2))
    # 6 months credit stats
    num_credit_last6m = fields.Integer(string='Credit Transactions - Last 6m')
    avg_credit_last6m = fields.Float(
        string='Avg. Credit Amount - Last 6m', digits=(10, 2))
    max_credit_last6m = fields.Float(
        string='Max. Credit - Last 6m', digits=(10, 2))
    tot_credit_last6m = fields.Float(
        string='Total Credit Amount - Last 6m', digits=(15, 2))

    # 6 months debit stats
    num_debit_last6m = fields.Integer(string='Debit Transactions - Last 6m')
    avg_debit_last6m = fields.Float(
        string='Avg. Debit Amount - Last 6m', digits=(10, 2))
    max_debit_last6m = fields.Float(
        string='Max. Debit - Last 6m', digits=(10, 2))
    tot_debit_last6m = fields.Float(
        string='Total Debit Amount - Last 6m', digits=(15, 2))

    # 1 year credit stats
    num_credit_last1y = fields.Integer(string='Credit Transactions - Last 1Y')
    avg_credit_last1y = fields.Float(
        string='Avg. Credit Amount - Last 1Y', digits=(10, 2))
    max_credit_last1y = fields.Float(
        string='Max. Credit - Last 1Y', digits=(10, 2))
    tot_credit_last1y = fields.Float(
        string='Total Credit Amount - Last 1Y', digits=(15, 2))

    # 1 year debit stats
    num_debit_last1y = fields.Integer(string='Debit Transactions - Last 1Y')
    avg_debit_last1y = fields.Float(
        string='Avg. Debit Amount - Last 1Y', digits=(10, 2))
    max_debit_last1y = fields.Float(
        string='Max. Debit - Last 1Y', digits=(10, 2))
    tot_debit_last1y = fields.Float(
        string='Total Debit Amount - Last 1Y', digits=(15, 2))
    risk_score = fields.Float(string='Risk Score', digits=(
        10, 2), related="customer_id.risk_score",index=True)
    risk_level = fields.Char(string='Risk Rating',
                             related="customer_id.risk_level",index=True)

    state = fields.Selection(string='Status', 
                             selection=[('Active', 'Active'), 
                                        ('Inactive', 'Inactive'), 
                                        ('Dormant', 'Dormant'), 
                                        ('Flagged', 'Flagged'), 
                                        ('Opened', 'Opened'), 
                                        ('Suspended', 'Suspended'), 
                                        ('To be Reactivated', 'To be Reactivated'), 
                                        ('To be suspended', 'To be suspended'), 
                                        ('Unknown', 'Unknown'), 
                                        ('Applied for closure', 'Applied for closure'), 
                                        ('Closed', 'Closed')
                                        ], 
                             tracking=True, default='Active', required=False,index=True)  # sta_code
    
    customer = fields.Char(string='Customer Id', index=True)
    max_debit_daily = fields.Float(string='Max. Debit - Daily', digits=(10, 2))
    overdraft_limit = fields.Float(string='OverDraft Limit', digits=(10, 2))
    uncleared_balance = fields.Float(
        string='Uncleared Balance', digits=(10, 2))
    start_year_balance = fields.Float(
        string='Start Year Balance', digits=(10, 2))
    date_last_credit_customer = fields.Char(string='Date Last Credit Customer')
    amount_last_credit_customer = fields.Char(
        string='Amount Last Credit Customer')
    date_last_debit_customer = fields.Char(string='Date Last Dedit Customer')
    
    tier_level = fields.Selection([
        ('1', 'Tier 1'),
        ('2', 'Tier 2'),
        ('3', 'Tier 3')
    ], string="Tier Level",index=True, compute='_compute_tier_info', search='_search_tier_level', store=False)

    tier_name = fields.Char(
        string="Account Tier", index=True, compute='_compute_tier_info', store=False)

   
    @api.model
    def customer_account_triggers_and_indexes(self):
        """Initialize database triggers when module is installed/updated"""

        # Create index on res_partner_account (only if it doesn't exist)
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_partner_account_id_idx ON res_partner_account (id)")
        

        # Check if the trigger exists
        self.env.cr.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.triggers 
                WHERE trigger_name = 'update_customer_id_field' 
                AND event_object_table = 'res_partner_account'
            )
        """)
        trigger_exists = self.env.cr.fetchone()[0]

        # Always replace the function so stale database definitions are repaired on module update.
        self.env.cr.execute("""
            CREATE OR REPLACE FUNCTION update_customer_id_field_func()
            RETURNS TRIGGER AS $$
            BEGIN
                -- Keep the denormalized customer text field in sync with the relation.
                IF (NEW.customer IS NULL OR TRIM(NEW.customer) = '') AND NEW.customer_id IS NOT NULL THEN
                    NEW.customer = NEW.customer_id::TEXT;
                END IF;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        # Only create the trigger if it doesn't exist
        if not trigger_exists:
            self.env.cr.execute("""
                CREATE TRIGGER update_customer_id_field
                BEFORE INSERT OR UPDATE ON res_partner_account
                FOR EACH ROW
                EXECUTE FUNCTION update_customer_id_field_func();
            """)

        # Update existing records with empty customer field
        self.env.cr.execute("""
            UPDATE res_partner_account
            SET customer = customer_id::TEXT
            WHERE (customer IS NULL OR TRIM(customer) = '')
            AND customer_id IS NOT NULL;
        """)

    @api.model
    def _account_stat_field_names(self):
        return [
            'num_credit_last6m', 'avg_credit_last6m', 'max_credit_last6m', 'tot_credit_last6m',
            'num_debit_last6m', 'avg_debit_last6m', 'max_debit_last6m', 'tot_debit_last6m',
            'num_credit_last1y', 'avg_credit_last1y', 'max_credit_last1y', 'tot_credit_last1y',
            'num_debit_last1y', 'avg_debit_last1y', 'max_debit_last1y', 'tot_debit_last1y',
        ]

    @api.model
    def _normalize_account_stats_batch_size(self, batch_size=None):
        batch_size = int(batch_size or self._ACCOUNT_STATS_DEFAULT_BATCH_SIZE)
        return max(100, min(batch_size, self._ACCOUNT_STATS_MAX_BATCH_SIZE))

    @api.model
    def _acquire_account_stats_lock(self):
        self.env.cr.execute("SELECT pg_try_advisory_lock(%s)", (self._ACCOUNT_STATS_LOCK_KEY,))
        return bool(self.env.cr.fetchone()[0])

    @api.model
    def _release_account_stats_lock(self):
        self.env.cr.execute("SELECT pg_advisory_unlock(%s)", (self._ACCOUNT_STATS_LOCK_KEY,))

    @api.model
    def _iter_account_stats_batches(self, batch_size, account_ids=None):
        if account_ids:
            unique_ids = sorted({int(account_id) for account_id in account_ids if account_id})
            for index in range(0, len(unique_ids), batch_size):
                yield unique_ids[index:index + batch_size]
            return

        last_seen_id = 0
        while True:
            self.env.cr.execute(
                """
                    SELECT id
                    FROM res_partner_account
                    WHERE id > %s
                    ORDER BY id
                    LIMIT %s
                """,
                (last_seen_id, batch_size),
            )
            batch_ids = [row[0] for row in self.env.cr.fetchall()]
            if not batch_ids:
                break
            yield batch_ids
            last_seen_id = batch_ids[-1]

    #  Use:
    # - All accounts: env['res.partner.account'].queue_account_stat_refresh()
    # - Specific accounts: env['res.partner.account'].browse(account_ids).queue_account_stat_refresh()

    @api.model
    def queue_account_stat_refresh(self, batch_size=None, account_ids=None):
        batch_size = self._normalize_account_stats_batch_size(batch_size)
        scoped_account_ids = account_ids or self.ids
        lock_acquired = False

        if not scoped_account_ids:
            lock_acquired = self._acquire_account_stats_lock()
            if not lock_acquired:
                _logger.info('Account statistics refresh is already running; skipping duplicate request.')
                return False

        try:
            batch_count = 0
            use_queue = hasattr(self, 'with_delay')

            for batch_count, batch_ids in enumerate(
                self._iter_account_stats_batches(batch_size, scoped_account_ids),
                start=1,
            ):
                if use_queue:
                    self.with_delay(
                        priority=30,
                        description=_(
                            'Account statistics refresh batch %(batch)s (%(size)s accounts)'
                        ) % {'batch': batch_count, 'size': len(batch_ids)},
                    ).job_refresh_account_stat_batch(batch_ids)
                else:
                    self.job_refresh_account_stat_batch(batch_ids)
                    self.env.cr.commit()

            _logger.info(
                'Queued account statistics refresh for %s batches using batch size %s',
                batch_count,
                batch_size,
            )
        finally:
            if lock_acquired:
                self._release_account_stats_lock()

        return batch_count

    @api.model
    def cron_queue_account_stat_refresh(self, batch_size=None):
        try:
            self.queue_account_stat_refresh(batch_size=batch_size)
        except Exception as exc:
            _logger.error('Cron job failed for account statistics refresh: %s', exc)
            return False
        return True

    @api.model
    def job_refresh_account_stat_batch(self, account_ids):
        batch_ids = sorted({int(account_id) for account_id in (account_ids or []) if account_id})
        if not batch_ids:
            return 0

        self.env.cr.execute(
            """
                WITH selected_accounts AS (
                    SELECT UNNEST(%s::int[]) AS account_id
                ),
                aggregated AS (
                    SELECT
                        sa.account_id,
                        COUNT(t.id) FILTER (
                            WHERE t.transaction_type = 'C'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 months')
                        )::integer AS num_credit_last6m,
                        COALESCE(ROUND((AVG(t.amount) FILTER (
                            WHERE t.transaction_type = 'C'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 months')
                        ))::numeric, 2), 0)::double precision AS avg_credit_last6m,
                        COALESCE(ROUND((MAX(t.amount) FILTER (
                            WHERE t.transaction_type = 'C'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 months')
                        ))::numeric, 2), 0)::double precision AS max_credit_last6m,
                        COALESCE(ROUND((SUM(t.amount) FILTER (
                            WHERE t.transaction_type = 'C'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 months')
                        ))::numeric, 2), 0)::double precision AS tot_credit_last6m,
                        COUNT(t.id) FILTER (
                            WHERE t.transaction_type = 'D'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 months')
                        )::integer AS num_debit_last6m,
                        COALESCE(ROUND((AVG(t.amount) FILTER (
                            WHERE t.transaction_type = 'D'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 months')
                        ))::numeric, 2), 0)::double precision AS avg_debit_last6m,
                        COALESCE(ROUND((MAX(t.amount) FILTER (
                            WHERE t.transaction_type = 'D'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 months')
                        ))::numeric, 2), 0)::double precision AS max_debit_last6m,
                        COALESCE(ROUND((SUM(t.amount) FILTER (
                            WHERE t.transaction_type = 'D'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '6 months')
                        ))::numeric, 2), 0)::double precision AS tot_debit_last6m,
                        COUNT(t.id) FILTER (
                            WHERE t.transaction_type = 'C'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                        )::integer AS num_credit_last1y,
                        COALESCE(ROUND((AVG(t.amount) FILTER (
                            WHERE t.transaction_type = 'C'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                        ))::numeric, 2), 0)::double precision AS avg_credit_last1y,
                        COALESCE(ROUND((MAX(t.amount) FILTER (
                            WHERE t.transaction_type = 'C'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                        ))::numeric, 2), 0)::double precision AS max_credit_last1y,
                        COALESCE(ROUND((SUM(t.amount) FILTER (
                            WHERE t.transaction_type = 'C'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                        ))::numeric, 2), 0)::double precision AS tot_credit_last1y,
                        COUNT(t.id) FILTER (
                            WHERE t.transaction_type = 'D'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                        )::integer AS num_debit_last1y,
                        COALESCE(ROUND((AVG(t.amount) FILTER (
                            WHERE t.transaction_type = 'D'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                        ))::numeric, 2), 0)::double precision AS avg_debit_last1y,
                        COALESCE(ROUND((MAX(t.amount) FILTER (
                            WHERE t.transaction_type = 'D'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                        ))::numeric, 2), 0)::double precision AS max_debit_last1y,
                        COALESCE(ROUND((SUM(t.amount) FILTER (
                            WHERE t.transaction_type = 'D'
                              AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                        ))::numeric, 2), 0)::double precision AS tot_debit_last1y
                    FROM selected_accounts sa
                    LEFT JOIN res_customer_transaction t
                        ON t.account_id = sa.account_id
                       AND t.date_created IS NOT NULL
                       AND t.date_created >= (CURRENT_TIMESTAMP - INTERVAL '1 year')
                    GROUP BY sa.account_id
                )
                UPDATE res_partner_account account
                SET
                    num_credit_last6m = aggregated.num_credit_last6m,
                    avg_credit_last6m = aggregated.avg_credit_last6m,
                    max_credit_last6m = aggregated.max_credit_last6m,
                    tot_credit_last6m = aggregated.tot_credit_last6m,
                    num_debit_last6m = aggregated.num_debit_last6m,
                    avg_debit_last6m = aggregated.avg_debit_last6m,
                    max_debit_last6m = aggregated.max_debit_last6m,
                    tot_debit_last6m = aggregated.tot_debit_last6m,
                    num_credit_last1y = aggregated.num_credit_last1y,
                    avg_credit_last1y = aggregated.avg_credit_last1y,
                    max_credit_last1y = aggregated.max_credit_last1y,
                    tot_credit_last1y = aggregated.tot_credit_last1y,
                    num_debit_last1y = aggregated.num_debit_last1y,
                    avg_debit_last1y = aggregated.avg_debit_last1y,
                    max_debit_last1y = aggregated.max_debit_last1y,
                    tot_debit_last1y = aggregated.tot_debit_last1y
                FROM aggregated
                WHERE account.id = aggregated.account_id
            """,
            (batch_ids,),
        )

        self.invalidate_cache(fnames=self._account_stat_field_names(), ids=batch_ids)
        _logger.info('Refreshed transaction statistics for %s accounts', len(batch_ids))
        return len(batch_ids)

    @api.model
    def open_accounts(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = []
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id])

            ]

        return {
            'name': _('Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': domain,
            'limit': 3000000,
            'context': {'search_default_group_branch': 1}
        }

    def get_balance(self):
        return '{0:.2f}'.format(self.balance)

    def get_risk_score(self):
        return self.risk_score

    def get_risk_level(self):
        return self.risk_level

    def compute_aggregate_risk_scores(self):
        """
        Compute aggregate risk scores grouped by branch, product, currency, account type, and state.
        Includes high/medium/low account counts and computes a count-weighted average risk score.
        Only runs if total customer accounts is 200 or less.
        """
        try:
            total_accounts = self.search_count([])

            if total_accounts <= 200:
                self.env.cr.execute("DELETE FROM account_agg_risk_score;")

                self.env.cr.execute("""
                    INSERT INTO account_agg_risk_score (
                        branch_id,
                        product_id,
                        state,
                        weighted_avg_risk_score,
                        total_accounts,
                        high_count,
                        medium_count,
                        low_count
                    )
                    SELECT
                        rpa.branch_id,
                        rpa.product_id,
                        rpa.state,

                        ROUND(
                            (
                                SUM(CASE WHEN rp.risk_level = 'high' THEN rp.risk_score ELSE 0 END) +
                                SUM(CASE WHEN rp.risk_level = 'medium' THEN rp.risk_score ELSE 0 END) +
                                SUM(CASE WHEN rp.risk_level = 'low' THEN rp.risk_score ELSE 0 END)
                            ) / NULLIF(COUNT(rp.id), 0),
                            2
                        ) AS weighted_avg_risk_score,

                        COUNT(rp.id) AS total_accounts,

                        COUNT(CASE WHEN rp.risk_level = 'high' THEN rp.id ELSE NULL END) AS high_count,
                        COUNT(CASE WHEN rp.risk_level = 'medium' THEN rp.id ELSE NULL END) AS medium_count,
                        COUNT(CASE WHEN rp.risk_level = 'low' THEN rp.id ELSE NULL END) AS low_count

                    FROM res_partner_account rpa
                    LEFT JOIN res_partner rp ON rpa.customer_id = rp.id
                    WHERE rp.risk_level IN ('high', 'medium', 'low') 

                    GROUP BY
                        rpa.branch_id,
                        rpa.product_id,
                        rpa.state;
                """)

                self.env.cr.commit()
                _logger.info(
                    f"Aggregate risk scores computed for {total_accounts} accounts")

            else:
                _logger.info(
                    f"Skipped: {total_accounts} accounts exceeds 200 limit")

        except Exception as e:
            self.env.cr.rollback()
            _logger.error(f"Error computing aggregate risk scores: {str(e)}")
            raise

    @api.model
    def cron_compute_aggregate_risk_scores(self):
        """
        Cron job wrapper method to compute aggregate risk scores.
        This method should be called by the scheduled action.
        """
        try:
            # Just call the method on the model - no need to fetch records
            # The count check is already handled inside compute_aggregate_risk_scores
            self.compute_aggregate_risk_scores()

        except Exception as e:
            _logger.error(
                f"Cron job failed for aggregate risk scores: {str(e)}")
            # Don't raise the exception to prevent cron job from failing completely
            return False

        return True

    def read_group(self, domain, fields, groupby, offset=0, limit=None, orderby=False, lazy=True):
        """
        Enhanced read_group method using pre-aggregated data from account.agg.risk.score.
        Falls back to standard read_group if no aggregated data is available.
        """
        return super().read_group(domain, fields, groupby, offset, limit, orderby, lazy)


        if not groupby:
            return super().read_group(domain, fields, groupby, offset, limit, orderby, lazy)

        # Extract groupby field name without optional modifiers like :day
        groupby_field = groupby[0].split(':')[0]

        # Fields that have pre-aggregated data
        aggregated_fields = ['branch_id', 'currency_id',
                             'product_id', 'account_type_id', 'state']

        # Check if we should use aggregated data
        should_use_aggregated = (
            groupby_field in aggregated_fields and
            'risk_score' in fields
        )

        if should_use_aggregated:
            return self._read_group_from_aggregated_data(domain, fields, groupby_field, offset, limit, orderby)
        else:
            return super().read_group(domain, fields, groupby, offset, limit, orderby, lazy)

    def _read_group_from_aggregated_data(self, domain, fields, groupby_field, offset=0, limit=None, orderby=False):
        """
        Read group data from account.agg.risk.score table instead of calculating on-the-fly.
        """
        try:
            # Get aggregated data
            agg_model = self.env['account.agg.risk.score']

            # Convert domain to match aggregated table structure
            agg_domain = self._convert_domain_for_aggregated_data(domain)

            # Read aggregated records
            agg_records = agg_model.search(agg_domain)

            if not agg_records:
                # Fallback to standard read_group if no aggregated data
                _logger.warning(
                    "No aggregated data found, falling back to standard read_group")
                return super().read_group(domain, [groupby_field], [groupby_field], offset, limit, orderby, True)

            # Group the aggregated data by the requested field
            grouped_data = self._group_aggregated_records(
                agg_records, groupby_field)

            # Format results
            formatted_results = self._format_aggregated_results(
                grouped_data, groupby_field, domain)

            # Apply ordering
            if orderby:
                formatted_results = self._apply_ordering(
                    formatted_results, orderby, groupby_field)

            # Apply pagination
            if offset or limit:
                end_index = (offset + limit) if limit else None
                formatted_results = formatted_results[offset:end_index]

            return formatted_results

        except Exception as e:
            _logger.error(
                f"Error reading aggregated data for {groupby_field}: {str(e)}")
            # Fallback to standard read_group
            return super().read_group(domain, [groupby_field], [groupby_field], offset, limit, orderby, True)

    def _convert_domain_for_aggregated_data(self, domain):
        """
        Convert the original domain to work with account.agg.risk.score fields.
        """
        if not domain:
            return []

        converted_domain = []

        for condition in domain:
            if isinstance(condition, (list, tuple)) and len(condition) == 3:
                field, operator, value = condition

                # Map fields that exist in both models
                field_mapping = {
                    'branch_id': 'branch_id',
                    'product_id': 'product_id',
                    # 'currency_id': 'currency_id',
                    # 'account_type_id': 'account_type_id',
                    'state': 'state'
                }

                if field in field_mapping:
                    converted_domain.append(
                        [field_mapping[field], operator, value])
                # Skip fields that don't exist in aggregated table

            else:
                # Keep logical operators (AND, OR, NOT)
                converted_domain.append(condition)

        return converted_domain

    def _group_aggregated_records(self, agg_records, groupby_field):
        """
        Group aggregated records by the specified field.
        """
        grouped = {}

        for record in agg_records:
            group_key = getattr(record, groupby_field)

            # Handle Many2one fields
            if hasattr(group_key, 'id'):
                key = group_key.id
            else:
                key = group_key

            if key not in grouped:
                grouped[key] = {
                    'records': [],
                    'total_accounts': 0,
                    'weighted_sum': 0.0,
                    'group_value': group_key
                }

            grouped[key]['records'].append(record)
            grouped[key]['total_accounts'] += record.total_accounts
            # Calculate weighted sum: risk_score * customer_count
            grouped[key]['weighted_sum'] += (
                record.weighted_avg_risk_score * record.total_accounts)

        return grouped

    def _format_aggregated_results(self, grouped_data, groupby_field, original_domain):
        """
        Format grouped aggregated data into Odoo's expected read_group format.
        """
        formatted_results = []

        for group_key, group_data in grouped_data.items():
            total_accounts = group_data['total_accounts']

            # Calculate overall weighted average for this group
            if total_accounts > 0:
                weighted_avg_risk_score = group_data['weighted_sum'] / \
                    total_accounts
            else:
                weighted_avg_risk_score = 0.0

            # Format the group key based on field type
            if groupby_field == 'state':
                display_key = group_key or 'Unknown'
                group_result = {
                    groupby_field: display_key,
                    f'{groupby_field}_count': total_accounts,
                    '__count': total_accounts,
                    '__domain': [(groupby_field, '=', group_key)] + original_domain
                }
            else:
                # For Many2one fields
                group_value = group_data['group_value']
                if hasattr(group_value, 'name_get'):
                    display_name = group_value.name_get(
                    )[0][1] if group_value else f'No {groupby_field.replace("_", " ").title()}'
                    display_key = (group_key, display_name)
                else:
                    display_key = group_key

                group_result = {
                    groupby_field: display_key,
                    f'{groupby_field}_count': total_accounts,
                    '__count': total_accounts,
                    '__domain': [(groupby_field, '=', group_key)] + original_domain
                }

            # Add risk score
            group_result['risk_score'] = round(weighted_avg_risk_score, 2)

            formatted_results.append(group_result)

        return formatted_results

    def _apply_ordering(self, results, orderby, groupby_field):
        """
        Apply ordering to the results.
        """
        if not orderby:
            return results

        # Parse orderby
        order_parts = [part.strip() for part in orderby.split(',')]

        for part in order_parts:
            part_lower = part.lower()
            reverse = 'desc' in part_lower

            if 'risk_score' in part_lower:
                results.sort(key=lambda x: x.get(
                    'risk_score', 0), reverse=reverse)
                break
            elif groupby_field in part_lower:
                if groupby_field == 'state':
                    results.sort(key=lambda x: x.get(
                        groupby_field, ''), reverse=reverse)
                else:
                    # For Many2one fields, sort by display name
                    results.sort(key=lambda x: x.get(groupby_field, (0, ''))[1] if isinstance(
                        x.get(groupby_field), tuple) else str(x.get(groupby_field, '')), reverse=reverse)
                break
            else:
                # Default to count
                results.sort(key=lambda x: x.get(
                    '__count', 0), reverse=reverse)
                break

        return results

    @api.depends('category')
    def _compute_tier_info(self):
        """Resolve tier information directly from active tier definitions."""
        categories = {record.category for record in self if record.category}
        tier_map = self._get_active_tier_map(categories)

        for record in self:
            data = tier_map.get(
                record.category, {'tier_level': '3', 'tier_name': 'Tier 3'})
            record.tier_level = data['tier_level']
            record.tier_name = data['tier_name']

    @api.model
    def _search_tier_level(self, operator, value):
        """Search method for tier_level using active tier definitions."""
        if operator not in ('=', '!=', 'in', 'not in'):
            return []

        active_tiers = self.env['res.partner.tier'].search([('status', '=', 'active')])
        codes_by_level = {'1': [], '2': [], '3': []}
        for tier in active_tiers:
            codes_by_level.setdefault(tier.tier_level, []).append(tier.code)
        all_active_codes = [code for codes in codes_by_level.values() for code in codes]

        if operator == '=':
            if value == '3':
                return ['|', ('category', 'in', codes_by_level['3']), ('category', 'not in', all_active_codes)]
            return [('category', 'in', codes_by_level.get(value, []))]

        elif operator == '!=':
            if value == '3':
                return [('category', 'in', codes_by_level['1'] + codes_by_level['2'])]
            return [('category', 'not in', codes_by_level.get(value, []))]

        elif operator == 'in':
            requested = set(value)
            matching_codes = []
            include_default_tier = '3' in requested
            for tier in requested:
                matching_codes.extend(codes_by_level.get(tier, []))
            if include_default_tier:
                return ['|', ('category', 'in', matching_codes), ('category', 'not in', all_active_codes)]
            return [('category', 'in', matching_codes)]

        elif operator == 'not in':
            requested = set(value)
            excluded_codes = []
            include_default_tier = '3' in requested
            for tier in requested:
                excluded_codes.extend(codes_by_level.get(tier, []))
            if include_default_tier:
                return [('category', 'in', codes_by_level['1'] + codes_by_level['2'])]
            return [('category', 'not in', excluded_codes)]

        return []

    @api.model
    def _get_active_tier_map(self, categories):
        tiers = self.env['res.partner.tier'].search(
            [('status', '=', 'active'), ('code', 'in', list(categories))]
        )
        tier_map = {}
        for tier in tiers:
            if tier.code not in tier_map:
                tier_map[tier.code] = {
                    'tier_level': tier.tier_level or '3',
                    'tier_name': tier.name or 'Tier 3',
                }
        return tier_map

class CustomerAccountOfficer(models.Model):
    _name = 'account.officers'
    _description = 'Account Officer'
    _sql_constraints = [
        ('uniq_account_code', 'unique(code)',
         "Account Officer already exists. Code must be unique!"),
    ]
    _order = "name"
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", )
    area = fields.Char(string="Area", )
    email = fields.Char(string="Email", )


class CustomerAccountDetails(models.Model):
    _name = 'customer.account.details'
    _description = 'Account Details'

    _sql_constraints = [
        ('uniq_account_number', 'unique(account_number)',
         "Account Number already exists. Account Number must be unique!"),
    ]
    
    account_name = fields.Char(string="Account Name", required=True)
    account_number = fields.Char(string="Account Number", index=True)
    account_status = fields.Integer(string="Account Status")
    cleared_balance = fields.Float(string="Cleared Balance")
    has_breach = fields.Boolean(string="Has Breach")
    last_transaction_time = fields.Date(string="Last Transaction Time")
    is_collection_account = fields.Boolean(string="Is Collection Account")
    high_transaction_account = fields.Boolean(
        string="Is High Transaction Account")
    

    def init(self):

        self.env.cr.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'customer_account_details'
        )
    """)
        table_exists = self.env.cr.fetchone()[0]

        if table_exists:

            self.env.cr.execute("""
                CREATE INDEX IF NOT EXISTS customer_account_details_id_idx
                ON customer_account_details (id)
            """)
            
