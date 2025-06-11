# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
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

    _order = "name"
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', index=True)  # customer
    name = fields.Char(string="Account Number")
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
        string='Opening Date', required=False)

    account_officer_id = fields.Many2one(
        comodel_name='account.officers', string='Account Officer', required=False)  # acct_officer

    currency = fields.Char(string='Currency', required=False)  # currency

    category = fields.Char(string='Category', required=False)  # category

    category_description = fields.Char(
        string='Category Description', required=False)  # category_desc

    is_joint_account = fields.Boolean(
        string='Is Joint Account', required=False)  # category_desc

    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)

    product_id = fields.Many2one(
        comodel_name='res.partner.account.product', string='Product', index=True)
    date_created = fields.Date(
        string='Date Created', index=True)  # date_created
    ledger_id = fields.Many2one(
        comodel_name='res.partner.account.ledger', string='Ledger', index=True)
    closure_status = fields.Selection(string='Closure Status', selection=[
                                      ('N', 'No'), ('Y', 'Yes')])
    branch_id = fields.Many2one(
        comodel_name='res.branch', string='Branch', index=True)
    balance = fields.Float(string='Balance', digits=(15, 4))  # working_balance
    account_type_id = fields.Many2one(
        comodel_name='res.partner.account.type', string='Account Type', index=True)

    risk_assessment = fields.Many2one(
        comodel_name='res.risk.assessment', string='Risk Assessment', index=True)
    risk_score = fields.Float(string='Risk Score', digits=(
        10, 2), related="customer_id.risk_score")
    risk_level = fields.Char(string='Risk Rating',
                             related="customer_id.risk_level")
    account_type_id = fields.Many2one(
        comodel_name='res.partner.account.type', string='Account Type', index=True)
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
        10, 2), related="customer_id.risk_score")
    risk_level = fields.Char(string='Risk Rating',
                             related="customer_id.risk_level")

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

    state = fields.Selection(string='Status', selection=[('Active', 'Active'), ('Inactive', 'Inactive'), ('Dormant', 'Dormant'), (
        'Flagged', 'Flagged'), ('Closed', 'Closed')], tracking=True, default='Active', required=False)  # sta_code
    active = fields.Boolean(default=True, tracking=True)
    customer = fields.Char(string='Customer Id')
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
    account_tier = fields.Selection([
        ('tier_1', 'Tier 1'),
        ('tier_2', 'Tier 2'),
        ('tier_3', 'Tier 3'),
    ], string='Account Tier', compute='_compute_account_tier', index=True, search='_search_account_tier')

    def init(self):
        """Initialize database triggers when module is installed/updated"""
        # Drop existing trigger if it exists
        self.env.cr.execute(
            "DROP TRIGGER IF EXISTS update_customer_id_field ON res_partner_account;")

        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_partner_account_id_idx ON res_partner_account (id)")

        # Create new trigger
        self.env.cr.execute("""
            CREATE OR REPLACE FUNCTION update_customer_id_field_func()
            RETURNS TRIGGER AS $$
            BEGIN
                -- Check if customer field is empty and customer_id is set
                IF (NEW.customer IS NULL OR TRIM(NEW.customer) = '') AND NEW.customer_id IS NOT NULL THEN
                    -- Set customer field to the ID value from customer_id
                    NEW.customer = NEW.customer_id::TEXT;                    
                END IF;
                
                IF NEW.active IS NULL THEN
                    -- Set active field to True
                    NEW.active = True;
                END IF;
                
                
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Create the trigger
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
        # Update existing records where active field is NULL
        self.env.cr.execute("""
            UPDATE res_partner_account
            SET active = TRUE
            WHERE active IS NULL;
        """)

        # self.compute_aggregate_risk_scores()

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
                        currency_id,
                        account_type_id,
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
                        rpa.currency_id,
                        rpa.account_type_id,
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
                        rpa.currency_id,
                        rpa.account_type_id,
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
        print(fields)
        print(domain)
        """
        Enhanced read_group method using pre-aggregated data from account.agg.risk.score.
        Falls back to standard read_group if no aggregated data is available.
        """

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
                    'currency_id': 'currency_id',
                    'account_type_id': 'account_type_id',
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
    def _compute_account_tier(self):
        for record in self:
            if not record.category:
                record.account_tier = False
                continue

            if record.category in ['SAV025', 'SAV146']:
                record.account_tier = 'tier_1'
            elif record.category in ['SAV023', 'SAV019']:
                record.account_tier = 'tier_2'
            else:
                record.account_tier = 'tier_3'
                
    def _search_account_tier(self, operator, value):
        """Custom search method for account_tier computed field"""
        if operator == '=' and value == 'tier_1':
            return [('category', 'in', ['SAV025', 'SAV146'])]
        elif operator == '=' and value == 'tier_2':
            return [('category', 'in', ['SAV023', 'SAV019'])]
        elif operator == '=' and value == 'tier_3':
            return [('category', 'not in', ['SAV025', 'SAV146', 'SAV023', 'SAV019']), ('category', '!=', False)]
        elif operator == '!=' and value == 'tier_1':
            return ['|', ('category', 'not in', ['SAV025', 'SAV146']), ('category', '=', False)]
        elif operator == '!=' and value == 'tier_2':
            return ['|', ('category', 'not in', ['SAV023', 'SAV019']), ('category', '=', False)]
        elif operator == '!=' and value == 'tier_3':
            return ['|', ('category', 'in', ['SAV025', 'SAV146', 'SAV023', 'SAV019']), ('category', '=', False)]
        elif operator == 'in' and isinstance(value, list):
            domain = []
            for tier in value:
                if tier == 'tier_1':
                    domain.append(('category', 'in', ['SAV025', 'SAV146']))
                elif tier == 'tier_2':
                    domain.append(('category', 'in', ['SAV023', 'SAV019']))
                elif tier == 'tier_3':
                    domain.append([('category', 'not in', ['SAV025', 'SAV146', 'SAV023', 'SAV019']), ('category', '!=', False)])
            return ['|'] * (len(domain) - 1) + domain if len(domain) > 1 else domain[0] if domain else []
        else:
            return []


class CustomerAccountOfficer(models.Model):
    _name = 'account.officers'
    _description = 'Account Officer'
    _sql_constraints = [
        ('uniq_account_code', 'unique(code)',
         "Account Officer already exists. Code must be unique!"),
    ]
    _order = "name"
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    area = fields.Char(string="Area", required=True)


class CustomerAccountDetails(models.Model):
    _name = 'customer.account.details'
    _description = 'Account Details'

    _sql_constraints = [
        ('uniq_account_number', 'unique(account_number)',
         "Account Number already exists. Account Number must be unique!"),
    ]
    # is_office_account = fields.Boolean(string="Is Office Account")
    # enable_sms_alert = fields.Boolean(string="Enabled Sms alert")
    # last_transaction_date = fields.Date(string="Last Transaction Date")
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
