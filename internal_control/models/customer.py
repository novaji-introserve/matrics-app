from odoo import models, fields, api, _
import logging
from datetime import timedelta

_logger = logging.getLogger(__name__)

class Customer(models.Model): 
    _inherit = "res.partner"

    def init(self):
        """Automatically setup customer triggers when model initializes"""
        super().init()
        try:
            # Check if triggers already exist to avoid recreating them
            if not self._verify_customer_triggers():
                _logger.info("Customer triggers not found, setting up production system...")
                self._setup_customer_indexes()
                self._setup_customer_triggers()
                _logger.info("Customer production system initialized automatically")
        except Exception as e:
            _logger.warning(f"Auto-setup failed, manual setup may be required: {str(e)}")
            # Don't raise the exception to prevent module loading failure
            pass

    customertype = fields.Many2one(
        comodel_name='res.customer.type', string='Customer Type', tracking=True, index=True)
    nationality = fields.Many2one(
        comodel_name='res.country', string='Country', tracking=True, index=True)
    state_id = fields.Many2one(
        comodel_name='res.country.state', string='State', tracking=True, index=True)
    occupation = fields.Char(string='Occupation', index=True, tracking=True)
    date_opened = fields.Date(string='Date Opened', index=True, tracking=True)
    address = fields.Char(string='Address', index=True, tracking=True)
    nin = fields.Char(string='NIN', index=True, tracking=True)
    status = fields.Many2one(
        comodel_name='res.user.status', string='Status', tracking=True, index=True)
    phone1 = fields.Char(string='Phone', index=True, tracking=True)
    identification_issue_date = fields.Date(string='identification Issue Date', index=True, tracking=True)
    town_id = fields.Many2one(
        comodel_name='res.partner.town', string='Town', index=True)
    officer_code = fields.Many2one(
        comodel_name='res.account.officer', string='Account Officer', index=True, tracking=True)

    @api.model
    def _get_customers_sql(self, start_date, end_date, branch_ids=None, limit=100):
        """Universal SQL-based customer filtering for ETL data"""
        query = """
            SELECT rp.id 
            FROM res_partner rp
            WHERE rp.customer_id IS NOT NULL
            AND rp.origin = 'prod'
            AND rp.date_opened >= %s
            AND rp.date_opened <= %s
        """
        
        params = [start_date, end_date]
        
        if branch_ids:
            placeholders = ','.join(['%s'] * len(branch_ids))
            query += f" AND rp.branch_id IN ({placeholders})"
            params.extend(branch_ids)
        
        query += " ORDER BY rp.date_opened DESC, rp.id DESC LIMIT %s"
        params.append(limit)
        
        self.env.cr.execute(query, params)
        result_ids = [row[0] for row in self.env.cr.fetchall()]
        
        return result_ids

    @api.model
    def _get_branch_ids_for_user(self):
        """Helper method to get branch IDs for current user"""
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        if is_cco:
            return None
        else:
            return self.env.user.branches_id.ids

    @api.model
    def open_all_customers_today(self):
        today = fields.Date.today()
        branch_ids = self._get_branch_ids_for_user()
        
        customer_ids = self._get_customers_sql(today, today, branch_ids, limit=100)
        
        return {
            'name': _('Customers - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', customer_ids)],
            'limit': 100,
            'order': 'date_opened desc',
            'context': {'search_default_group_branch': 1}
        }
        
    @api.model
    def open_all_customers(self):
        # Use a wide date range to get all customers
        today = fields.Date.today()
        very_old_date = today - timedelta(days=36500)  # ~100 years ago
        branch_ids = self._get_branch_ids_for_user()
        
        customer_ids = self._get_customers_sql(very_old_date, today, branch_ids, limit=1000)
        
        return {
            'name': _('All Customers'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', customer_ids)],
            'limit': 1000,
            'order': 'date_opened desc',
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_all_customers_last_7days(self):
        today = fields.Date.today()
        seven_days_ago = today - timedelta(days=7)
        branch_ids = self._get_branch_ids_for_user()
        
        customer_ids = self._get_customers_sql(seven_days_ago, today, branch_ids, limit=100)
        
        return {
            'name': _('Customers - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', customer_ids)],
            'limit': 100,
            'order': 'date_opened desc',
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_all_customers_last_30days(self):
        today = fields.Date.today()
        thirty_days_ago = today - timedelta(days=30)
        branch_ids = self._get_branch_ids_for_user()
        
        customer_ids = self._get_customers_sql(thirty_days_ago, today, branch_ids, limit=100)
        
        return {
            'name': _('Customers - Last 30 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', customer_ids)],
            'limit': 100,
            'order': 'date_opened desc',
            'context': {'search_default_group_branch': 1}
        }


    @api.model
    def _setup_customer_indexes(self):
        """Setup production indexes for customer operations"""
        indexes = [
            """CREATE INDEX IF NOT EXISTS idx_partner_customer_origin 
               ON res_partner(customer_id, origin) 
               WHERE customer_id IS NOT NULL""",
            
            """CREATE INDEX IF NOT EXISTS idx_partner_branch_sync 
               ON res_partner(customer_id, branch_id, origin) 
               WHERE customer_id IS NOT NULL AND branch_id IS NULL""",
            
            """CREATE INDEX IF NOT EXISTS idx_partner_account_branch_lookup 
               ON res_partner_account(customer_id, branch_id) 
               WHERE branch_id IS NOT NULL""",
            
            """CREATE INDEX IF NOT EXISTS idx_partner_date_opened_prod 
               ON res_partner(date_opened, origin, branch_id) 
               WHERE customer_id IS NOT NULL AND origin = 'prod'"""
        ]
        
        for index_sql in indexes:
            try:
                self.env.cr.execute(index_sql)
                _logger.info("Customer index created successfully")
            except Exception as e:
                _logger.warning(f"Index creation warning: {str(e)}")

    @api.model
    def _setup_customer_triggers(self):
        """Setup production database triggers for customer data synchronization"""
        try:
            # Origin Assignment Trigger
            origin_trigger_sql = """
                CREATE OR REPLACE FUNCTION set_customer_origin()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.customer_id IS NOT NULL AND (NEW.origin IS NULL OR NEW.origin != 'prod') THEN
                        NEW.origin := 'prod';
                    END IF;
                    RETURN NEW;
                EXCEPTION 
                    WHEN OTHERS THEN
                        RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS trigger_set_customer_origin ON res_partner;
                CREATE TRIGGER trigger_set_customer_origin
                    BEFORE INSERT OR UPDATE OF customer_id
                    ON res_partner
                    FOR EACH ROW
                    WHEN (NEW.customer_id IS NOT NULL)
                    EXECUTE FUNCTION set_customer_origin();
            """

            # Customer Activation Trigger
            activation_trigger_sql = """
                CREATE OR REPLACE FUNCTION activate_customer()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.customer_id IS NOT NULL AND NEW.origin = 'prod' AND 
                       (NEW.active IS NULL OR NEW.active = false) THEN
                        NEW.active := true;
                    END IF;
                    RETURN NEW;
                EXCEPTION 
                    WHEN OTHERS THEN
                        RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS trigger_activate_customer ON res_partner;
                CREATE TRIGGER trigger_activate_customer
                    BEFORE INSERT OR UPDATE OF customer_id, origin
                    ON res_partner
                    FOR EACH ROW
                    WHEN (NEW.customer_id IS NOT NULL AND NEW.origin = 'prod')
                    EXECUTE FUNCTION activate_customer();
            """

            # Branch Sync Trigger
            branch_sync_trigger_sql = """
                CREATE OR REPLACE FUNCTION sync_customer_branch()
                RETURNS TRIGGER AS $$
                DECLARE
                    account_branch_id INTEGER;
                BEGIN
                    IF NEW.customer_id IS NOT NULL AND NEW.origin = 'prod' AND NEW.branch_id IS NULL THEN
                        SELECT branch_id INTO account_branch_id
                        FROM res_partner_account
                        WHERE customer_id = NEW.id 
                        AND branch_id IS NOT NULL
                        LIMIT 1;
                        
                        IF account_branch_id IS NOT NULL THEN
                            NEW.branch_id := account_branch_id;
                        END IF;
                    END IF;
                    RETURN NEW;
                EXCEPTION 
                    WHEN OTHERS THEN
                        RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS trigger_sync_customer_branch ON res_partner;
                CREATE TRIGGER trigger_sync_customer_branch
                    BEFORE INSERT OR UPDATE OF customer_id, origin, branch_id
                    ON res_partner
                    FOR EACH ROW
                    WHEN (NEW.customer_id IS NOT NULL AND NEW.origin = 'prod' AND NEW.branch_id IS NULL)
                    EXECUTE FUNCTION sync_customer_branch();
            """

            # Execute trigger creation
            self.env.cr.execute(origin_trigger_sql)
            self.env.cr.execute(activation_trigger_sql)
            self.env.cr.execute(branch_sync_trigger_sql)
            
            _logger.info("Customer triggers setup completed successfully")
            
        except Exception as e:
            _logger.error(f"Error setting up customer triggers: {str(e)}")
            raise

    @api.model
    def setup_customer_production_system(self):
        """Setup complete customer production system with triggers and indexes"""
        try:
            _logger.info("Setting up customer production system...")
            
            # Setup indexes first
            self._setup_customer_indexes()
            
            # Setup triggers
            self._setup_customer_triggers()
            
            # Verify installation
            if self._verify_customer_triggers():
                self.env.cr.commit()
                _logger.info("Customer production system setup completed successfully")
                return True
            else:
                raise Exception("Trigger verification failed")
                
        except Exception as e:
            self.env.cr.rollback()
            _logger.error(f"Customer production setup failed: {str(e)}")
            raise

    @api.model
    def create_concurrent_indexes(self):
        """Create concurrent indexes for better production performance (run manually)"""
        indexes = [
            """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partner_customer_origin_concurrent 
               ON res_partner(customer_id, origin) 
               WHERE customer_id IS NOT NULL""",
            
            """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partner_branch_sync_concurrent 
               ON res_partner(customer_id, branch_id, origin) 
               WHERE customer_id IS NOT NULL AND branch_id IS NULL""",
            
            """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partner_account_branch_lookup_concurrent 
               ON res_partner_account(customer_id, branch_id) 
               WHERE branch_id IS NOT NULL""",
            
            """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partner_date_opened_prod_concurrent 
               ON res_partner(date_opened, origin, branch_id) 
               WHERE customer_id IS NOT NULL AND origin = 'prod'"""
        ]
        
        _logger.info("Creating concurrent indexes for better performance...")
        for index_sql in indexes:
            try:
                # This should be run outside of a transaction
                self.env.cr.execute(index_sql)
                _logger.info("Concurrent index created successfully")
            except Exception as e:
                _logger.warning(f"Concurrent index creation failed: {str(e)}")
        
        _logger.info("Concurrent index creation completed")

    @api.model  
    def _verify_customer_triggers(self):
        """Verify that customer triggers are properly installed"""
        try:
            self.env.cr.execute("""
                SELECT trigger_name
                FROM information_schema.triggers 
                WHERE trigger_name IN ('trigger_set_customer_origin', 'trigger_activate_customer', 'trigger_sync_customer_branch')
                AND event_object_table = 'res_partner'
            """)
            
            active_triggers = [row[0] for row in self.env.cr.fetchall()]
            expected_triggers = ['trigger_set_customer_origin', 'trigger_activate_customer', 'trigger_sync_customer_branch']
            
            if len(active_triggers) == 3:
                _logger.info("All 3 customer triggers are active and verified")
                return True
            else:
                missing = set(expected_triggers) - set(active_triggers)
                _logger.warning(f"Missing triggers: {missing}")
                return False
        except Exception as e:
            _logger.warning(f"Trigger verification failed: {str(e)}")
            return False

    @api.model
    def remove_customer_triggers(self):
        """Remove customer triggers if needed"""
        try:
            cleanup_sql = """
                DROP TRIGGER IF EXISTS trigger_set_customer_origin ON res_partner;
                DROP TRIGGER IF EXISTS trigger_activate_customer ON res_partner;
                DROP TRIGGER IF EXISTS trigger_sync_customer_branch ON res_partner;
                DROP FUNCTION IF EXISTS set_customer_origin();
                DROP FUNCTION IF EXISTS activate_customer();
                DROP FUNCTION IF EXISTS sync_customer_branch();
            """
            self.env.cr.execute(cleanup_sql)
            self.env.cr.commit()
            _logger.info("Customer triggers removed successfully")
        except Exception as e:
            self.env.cr.rollback()
            _logger.error(f"Failed to remove customer triggers: {str(e)}")
            raise