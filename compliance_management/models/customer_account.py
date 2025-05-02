# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class CustomerAccount(models.Model):
    _name = 'res.partner.account'
    _description = 'Account'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _sql_constraints = [
        # ('uniq_account_name', 'unique(name)',
        #  "Account Name already exists. Value must be unique!"),
        ('uniq_account_id', 'unique(customer_id)',
         "Customer ID already exists. Value must be unique!"),
    ]
    _order = "name" 
    name = fields.Char(string="Account Number")
    account_name = fields.Char(string='Account Name', index=True) #account_title1
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
        comodel_name='account.officers', string='Account Officer', required=False) #acct_officer
    
    currency = fields.Char(string='Currency', required=False)#currency
    
    category = fields.Char(string='Category', required=False) #category
    
    category_description = fields.Char(string='Category Description', required=False) #category_desc
    
    is_joint_account = fields.Boolean(
        string='Is Joint Account', required=False)  # category_desc
    
    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)

    product_id = fields.Many2one(
        comodel_name='res.partner.account.product', string='Product',index=True)
    date_created = fields.Date(string='Date Created', index=True) #date_created
    ledger_id = fields.Many2one(comodel_name='res.partner.account.ledger', string='Ledger',index=True)
    closure_status = fields.Selection(string='Closure Status', selection=[('N', 'No'), ('Y', 'Yes')])
    customer_id = fields.Many2one(comodel_name='res.partner', string='Customer',index=True) #customer
    branch_id = fields.Many2one(comodel_name='res.branch', string='Branch',index=True)
    balance = fields.Float(string='Balance', digits=(15,4)) #working_balance
    account_type_id = fields.Many2one(comodel_name='res.partner.account.type', string='Account Type',index=True)

    risk_assessment = fields.Many2one(comodel_name='res.risk.assessment', string='Risk Assessment',index=True)
    risk_score = fields.Float(string='Risk Score', digits=(10,2),related="customer_id.risk_score")
    risk_level = fields.Char(string='Risk Rating',related="customer_id.risk_level")
    account_type_id = fields.Many2one(comodel_name='res.partner.account.type', string='Account Type',index=True)
    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)
    
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
    avg_credit_last6m = fields.Float(string='Avg. Credit Amount - Last 6m', digits=(10,2))
    max_credit_last6m = fields.Float(string='Max. Credit - Last 6m', digits=(10,2))
    tot_credit_last6m = fields.Float(string='Total Credit Amount - Last 6m', digits=(15,2))

    # 6 months debit stats
    num_debit_last6m = fields.Integer(string='Debit Transactions - Last 6m')
    avg_debit_last6m = fields.Float(string='Avg. Debit Amount - Last 6m', digits=(10,2))
    max_debit_last6m = fields.Float(string='Max. Debit - Last 6m', digits=(10,2))
    tot_debit_last6m = fields.Float(string='Total Debit Amount - Last 6m', digits=(15,2))

    # 1 year credit stats
    num_credit_last1y = fields.Integer(string='Credit Transactions - Last 1Y')
    avg_credit_last1y = fields.Float(string='Avg. Credit Amount - Last 1Y', digits=(10,2))
    max_credit_last1y = fields.Float(string='Max. Credit - Last 1Y', digits=(10,2))
    tot_credit_last1y = fields.Float(string='Total Credit Amount - Last 1Y', digits=(15,2))

    # 1 year debit stats
    num_debit_last1y = fields.Integer(string='Debit Transactions - Last 1Y')
    avg_debit_last1y = fields.Float(string='Avg. Debit Amount - Last 1Y', digits=(10,2))
    max_debit_last1y = fields.Float(string='Max. Debit - Last 1Y', digits=(10,2))
    tot_debit_last1y = fields.Float(string='Total Debit Amount - Last 1Y', digits=(15,2))
    risk_score = fields.Float(string='Risk Score', digits=(10,2),related="customer_id.risk_score")
    risk_level = fields.Char(string='Risk Rating',related="customer_id.risk_level")
    
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
    avg_credit_last6m = fields.Float(string='Avg. Credit Amount - Last 6m', digits=(10,2))
    max_credit_last6m = fields.Float(string='Max. Credit - Last 6m', digits=(10,2))
    tot_credit_last6m = fields.Float(string='Total Credit Amount - Last 6m', digits=(15,2))

    # 6 months debit stats
    num_debit_last6m = fields.Integer(string='Debit Transactions - Last 6m')
    avg_debit_last6m = fields.Float(string='Avg. Debit Amount - Last 6m', digits=(10,2))
    max_debit_last6m = fields.Float(string='Max. Debit - Last 6m', digits=(10,2))
    tot_debit_last6m = fields.Float(string='Total Debit Amount - Last 6m', digits=(15,2))

    # 1 year credit stats
    num_credit_last1y = fields.Integer(string='Credit Transactions - Last 1Y')
    avg_credit_last1y = fields.Float(string='Avg. Credit Amount - Last 1Y', digits=(10,2))
    max_credit_last1y = fields.Float(string='Max. Credit - Last 1Y', digits=(10,2))
    tot_credit_last1y = fields.Float(string='Total Credit Amount - Last 1Y', digits=(15,2))

    # 1 year debit stats
    num_debit_last1y = fields.Integer(string='Debit Transactions - Last 1Y')
    avg_debit_last1y = fields.Float(string='Avg. Debit Amount - Last 1Y', digits=(10,2))
    max_debit_last1y = fields.Float(string='Max. Debit - Last 1Y', digits=(10,2))
    tot_debit_last1y = fields.Float(string='Total Debit Amount - Last 1Y', digits=(15,2))

    state = fields.Selection(string='Status', selection=[('Active', 'Active'), ('Inactive', 'Inactive'), ('Dormant', 'Dormant'), ('Flagged','Flagged'), ('Closed', 'Closed')],tracking=True,default='active',required=False) #sta_code
    active = fields.Boolean(default=True, tracking=True)
    customer = fields.Char(string='Customer Id')
    max_debit_daily = fields.Float(string='Max. Debit - Daily', digits=(10,2))
    overdraft_limit = fields.Float(string='OverDraft Limit', digits=(10,2))
    uncleared_balance = fields.Float(string='Uncleared Balance', digits=(10,2))
    start_year_balance = fields.Float(string='Start Year Balance', digits=(10,2))
    date_last_credit_customer = fields.Char(string='Date Last Credit Customer')
    amount_last_credit_customer = fields.Char(string='Amount Last Credit Customer')
    date_last_debit_customer = fields.Char(string='Date Last Dedit Customer')
    _sql_constraints = [
        ('customer_unique', 'unique(customer)', 'Customer ID must be unique!'),
    ]

    
    def init(self):
        """Initialize database triggers when module is installed/updated"""
        # Drop existing trigger if it exists
        self.env.cr.execute(
            "DROP TRIGGER IF EXISTS update_customer_id_field ON res_partner_account;")

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

    @api.model
    def open_accounts(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer:
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
        return  '{0:.2f}'.format(self.balance)
    
    def get_risk_score(self):
        return self.risk_score

    def get_risk_level(self):
        return self.risk_level


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
