# -*- coding: utf-8 -*-
from odoo import models, fields, api, tools, _
from datetime import timedelta
import logging
import gc
import random



_logger = logging.getLogger(__name__)




class CustomerDigitalProduct(models.Model):
    _name = 'customer.digital.product'
    _sql_constraints = [
        ('uniq_customer_id', 'unique(customer_id)',
         "Customer already exists. Customer must be unique!"),
    ]

    customer_id = fields.Text(string='Customer ID',
                              index=True, readonly=True)  # customer,
    customer_name = fields.Char(string='Name', readonly=True)
    customer_segment = fields.Char(
        string='Customer Segment', readonly=True)
    ussd = fields.Char(string='Uses USSD', index=True, readonly=True)
    onebank = fields.Char(string='Uses One Bank', index=True, readonly=True)
    carded_customer = fields.Char(
        string='Has A Card', index=True, readonly=True)
    alt_bank = fields.Char(string='Is On Alt Bank', readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)
    

  
    def init(self):
        # Minimal initialization for fast loading
        self.env.cr.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'customer_digital_product'
            )
        """)
        table_exists = self.env.cr.fetchone()[0]

        if table_exists:
            # Only create essential index
            try:
                self.env.cr.execute("""
                    CREATE INDEX IF NOT EXISTS customer_digital_product_customer_id_idx
                    ON customer_digital_product (customer_id)
                """)
            except Exception as e:
                _logger.warning(f"Index creation skipped: {e}")



# view model to display Customer digital products
class PartnerDigitalProductView(models.Model):
    _name = 'res.partner.digital.product.view'
    _description = 'Partner Digital Products View'
    _auto = False  # This is a database view

    partner_id = fields.Many2one(
        'res.partner', string='Partner', readonly=True)
    customer_id = fields.Char(string='Customer ID', readonly=True)
    ussd = fields.Char(string='Uses USSD', readonly=True)
    onebank = fields.Char(string='Uses One Bank', readonly=True)
    carded_customer = fields.Char(string='Has A Card', readonly=True)
    alt_bank = fields.Char(string='Is On Alt Bank', readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)
    customer_segment = fields.Char(string='Customer Segment', readonly=True)

    
    def init(self):
        """Defer view creation to avoid slow loading"""
        _logger.info(
            "Partner digital product view initialization deferred for performance")
        # View will be created manually when needed


   
class DigitalDeliveryChannel(models.Model):
    """Optimized model for handling millions of records"""
    _name = 'digital.delivery.channel'
    _description = 'Digital Delivery Channel'
    _sql_constraints = [
        ('uniq_channel_code', 'unique(code)',
         "Channel code already exists. Code must be unique!"),
    ]
    _order = "name"

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True, index=True,
                       help="Technical code for the channel (e.g., 'ussd', 'onebank')")
    description = fields.Text(string="Description")
    status = fields.Selection(string='Status', selection=[
        ('active', 'Active'),
        ('inactive', 'Inactive')
    ], default='active', index=True)
    
                        

    def init(self):
        """Ultra-fast initialization - minimal operations only"""
        _logger.info(
            "Starting fast initialization for digital delivery channels...")

        # Only check and create channels - no heavy operations
        self._cr.execute("SELECT COUNT(*) FROM digital_delivery_channel")
        count = self._cr.fetchone()[0]

        if count == 0:
            # self._create_initial_channels_fast()
            _logger.info("Skipped Creating initial channels from model")


    def _create_initial_channels_fast(self):
        """Fast channel creation without heavy operations"""
        channels_data = [
            ('One Bank', 'onebank', 'One Bank Mobile App', 'active'),
            ('Sterling Pro', 'sterling_pro', 'Sterling Pro Services', 'active'),
            ('USSD', 'ussd', 'USSD Banking Services', 'active'),
            ('Card', 'carded_customer', 'Card Services', 'active'),
            ('Alt Bank', 'alt_bank', 'Alternative Banking', 'active'),
            ('Banca', 'banca', 'Banca Services', 'active'),
            ('Doubble', 'doubble', 'Doubble Services', 'active'),
            ('Specta', 'specta', 'Specta Services', 'active'),
        ]

        # Simple bulk insert
        values = []
        for name, code, desc, status in channels_data:
            values.append(f"('{name}', '{code}', '{desc}', '{status}')")

        self._cr.execute(f"""
            INSERT INTO digital_delivery_channel (name, code, description, status)
            VALUES {','.join(values)}
            ON CONFLICT (code) DO NOTHING
        """)

    def action_setup_indexes_and_migration(self):
        """Heavy operations moved here - run manually when ready"""
        _logger.info("Starting index creation and migration setup...")

        try:
            # Step 1: Create all indexes 
            self._create_all_performance_indexes()

            # Step 2: Setup for migration
            self._prepare_for_migration()

            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Setup Complete',
                    'message': 'Indexes created and migration prepared. You can now run migration safely.',
                    'type': 'success',
                }
            }

        except Exception as e:
            error_msg = f"Setup failed: {e}"
            _logger.error(error_msg)
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Setup Failed',
                    'message': error_msg,
                    'type': 'danger',
                }
            }

    def _create_all_performance_indexes(self):
        """Create all indexes needed for 10M+ records performance"""
        _logger.info("Creating performance indexes for 10M+ records...")

        indexes = [
            ('digital_delivery_channel_code_idx',
             'digital_delivery_channel', ['code']),
            ('digital_delivery_channel_status_idx',
             'digital_delivery_channel', ['status']),
            ('customer_channel_subscription_customer_idx',
             'customer_channel_subscription', ['customer_id']),
            ('customer_channel_subscription_channel_idx',
             'customer_channel_subscription', ['channel_id']),
            ('customer_channel_subscription_partner_idx',
             'customer_channel_subscription', ['partner_id']),
            ('customer_channel_subscription_value_idx',
             'customer_channel_subscription', ['value']),
            ('res_partner_customer_id_idx', 'res_partner', ['customer_id']),
        ]

        for idx_name, table, columns in indexes:
            try:
                _logger.info(f"Creating index {idx_name}...")
                self._cr.execute(f"""
                    CREATE INDEX IF NOT EXISTS {idx_name} 
                    ON {table} ({','.join(columns)})
                """)
                _logger.info(f"✅ Index {idx_name} created")
            except Exception as e:
                _logger.warning(f"❌ Index {idx_name} failed: {e}")

        # composite indexes for performance
        composite_indexes = [
            ('customer_channel_subscription_customer_channel_idx',
             'customer_channel_subscription', ['customer_id', 'channel_id']),
            ('customer_channel_subscription_partner_channel_idx',
             'customer_channel_subscription', ['partner_id', 'channel_id']),
        ]

        for idx_name, table, columns in composite_indexes:
            try:
                _logger.info(f"Creating composite index {idx_name}...")
                self._cr.execute(f"""
                    CREATE INDEX IF NOT EXISTS {idx_name} 
                    ON {table} ({','.join(columns)})
                """)
                _logger.info(f"✅ Composite index {idx_name} created")
            except Exception as e:
                _logger.warning(f"❌ Composite index {idx_name} failed: {e}")

        # Partial index for non-null customer_ids
        try:
            _logger.info("Creating partial index for non-null customer_ids...")
            self._cr.execute("""
                CREATE INDEX IF NOT EXISTS res_partner_customer_id_not_null_idx 
                ON res_partner (customer_id) 
                WHERE customer_id IS NOT NULL AND customer_id != ''
            """)
            _logger.info("✅ Partial index created")
        except Exception as e:
            _logger.warning(f"❌ Partial index failed: {e}")

        _logger.info("Performance indexes creation completed")

    def _prepare_for_migration(self):
        """Prepare database for migration"""
        # Analyze tables for better query planning
        tables_to_analyze = [
            'customer_channel_subscription',
            'res_partner',
            'digital_delivery_channel',
            'customer_digital_product'
        ]

        for table in tables_to_analyze:
            try:
                self._cr.execute(f"ANALYZE {table}")
                _logger.info(f"✅ Analyzed {table}")
            except Exception as e:
                _logger.warning(f"❌ Could not analyze {table}: {e}")

    def _bulk_ensure_customer_channels_optimized(self):
        """Ultra-optimized BATCHED customer-channel sync for 10M+ records"""
        # Get counts first
        self._cr.execute(
            "SELECT COUNT(*) FROM res_partner WHERE customer_id IS NOT NULL AND customer_id != ''")
        customer_count = self._cr.fetchone()[0]

        self._cr.execute(
            "SELECT COUNT(*) FROM digital_delivery_channel WHERE status = 'active'")
        channel_count = self._cr.fetchone()[0]

        _logger.info(
            f"Processing {customer_count:,} customers with {channel_count} channels")

        # Check how many already exist
        self._cr.execute("SELECT COUNT(*) FROM customer_channel_subscription")
        existing_count = self._cr.fetchone()[0]

        if existing_count > 0:
            _logger.info(f"Found {existing_count:,} existing subscriptions")

        # BATCHED PROCESSING - Process customers in chunks
        batch_size = 50000  # Process 50K customers at a time
        total_inserted = 0

        # Get total number of customers
        self._cr.execute("""
            SELECT COUNT(*) FROM res_partner 
            WHERE customer_id IS NOT NULL AND customer_id != ''
        """)
        total_customers = self._cr.fetchone()[0]

        # Process in batches
        offset = 0
        batch_number = 1

        while offset < total_customers:
            _logger.info(
                f"Processing batch {batch_number}: customers {offset:,} to {min(offset + batch_size, total_customers):,}")

            # Get a batch of customer IDs
            self._cr.execute("""
                SELECT id, customer_id 
                FROM res_partner 
                WHERE customer_id IS NOT NULL AND customer_id != ''
                ORDER BY id
                LIMIT %s OFFSET %s
            """, (batch_size, offset))

            customer_batch = self._cr.fetchall()

            if not customer_batch:
                break

            _logger.info(
                f"Retrieved {len(customer_batch):,} customers for batch {batch_number}")

            # Create a temporary table for this batch
            self._cr.execute(
                "CREATE TEMP TABLE temp_customer_batch (partner_id INTEGER, customer_id TEXT)")

            # Insert batch customers into temp table
            batch_values = []
            for partner_id, customer_id in customer_batch:
                batch_values.append(f"({partner_id}, '{customer_id}')")

            if batch_values:
                self._cr.execute(f"""
                    INSERT INTO temp_customer_batch (partner_id, customer_id)
                    VALUES {','.join(batch_values)}
                """)

                # Now do the CROSS JOIN for just this batch
                self._cr.execute("""
                    INSERT INTO customer_channel_subscription (customer_id, partner_id, channel_id, value, last_updated)
                    SELECT 
                        tcb.customer_id,
                        tcb.partner_id,
                        dc.id,
                        'NO',
                        NOW()
                    FROM temp_customer_batch tcb
                    CROSS JOIN digital_delivery_channel dc
                    WHERE dc.status = 'active'
                    AND NOT EXISTS (
                        SELECT 1 FROM customer_channel_subscription ccs
                        WHERE ccs.customer_id = tcb.customer_id 
                        AND ccs.channel_id = dc.id
                    )
                """)

                batch_inserted = self._cr.rowcount
                total_inserted += batch_inserted

                _logger.info(
                    f"Batch {batch_number}: Inserted {batch_inserted:,} new subscriptions (Total: {total_inserted:,})")

                # Drop temp table
                self._cr.execute("DROP TABLE temp_customer_batch")

                # Commit this batch
                self._cr.commit()

                # Show progress percentage
                progress = (min(offset + batch_size, total_customers) /
                            total_customers) * 100
                _logger.info(f"Progress: {progress:.1f}% complete")

            # Move to next batch
            offset += batch_size
            batch_number += 1

            # Memory cleanup
            gc.collect()

        _logger.info(f"Completed! Created {total_inserted:,} new subscriptions")
        return f"{total_inserted:,} new subscriptions"
    
    def _bulk_migrate_legacy_data_optimized(self):
        """BATCHED legacy data migration"""
        # Check if legacy table exists
        self._cr.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'customer_digital_product'
            )
        """)

        if not self._cr.fetchone()[0]:
            _logger.info("No legacy table found")
            return "No legacy data"

        # Get channel mappings
        legacy_mappings = {
            'ussd': 'ussd', 'onebank': 'onebank', 'carded_customer': 'carded_customer',
            'alt_bank': 'alt_bank', 'sterling_pro': 'sterling_pro', 'banca': 'banca',
            'doubble': 'doubble', 'specta': 'specta', 'switch': 'switch'
        }

        self._cr.execute(
            "SELECT code, id FROM digital_delivery_channel WHERE status = 'active'")
        channel_ids = dict(self._cr.fetchall())

        total_migrated = 0
        batch_size = 100000  # Process 100K records at a time

        for channel_code, legacy_field in legacy_mappings.items():
            if channel_code not in channel_ids:
                continue

            channel_id = channel_ids[channel_code]

            # Get total count for this channel
            try:
                self._cr.execute(f"""
                    SELECT COUNT(*) FROM customer_digital_product
                    WHERE {legacy_field} IS NOT NULL AND {legacy_field} != ''
                """)
                total_for_channel = self._cr.fetchone()[0]

                if total_for_channel == 0:
                    continue

                _logger.info(
                    f"Migrating {total_for_channel:,} records for {channel_code} in batches")

                # Process in batches
                offset = 0
                batch_num = 1
                channel_migrated = 0

                while offset < total_for_channel:
                    _logger.info(
                        f"Channel {channel_code} - Batch {batch_num}: processing {offset:,} to {min(offset + batch_size, total_for_channel):,}")

                    # Update in batches using LIMIT and OFFSET
                    self._cr.execute(f"""
                        UPDATE customer_channel_subscription ccs
                        SET value = subq.new_value, last_updated = NOW()
                        FROM (
                            SELECT customer_id, {legacy_field} as new_value
                            FROM customer_digital_product
                            WHERE {legacy_field} IS NOT NULL AND {legacy_field} != ''
                            ORDER BY customer_id
                            LIMIT %s OFFSET %s
                        ) subq
                        WHERE ccs.customer_id = subq.customer_id
                        AND ccs.channel_id = %s
                    """, (batch_size, offset, channel_id))

                    batch_updated = self._cr.rowcount
                    channel_migrated += batch_updated

                    _logger.info(
                        f"Channel {channel_code} - Batch {batch_num}: Updated {batch_updated:,} records")

                    # Commit each batch
                    self._cr.commit()
                    gc.collect()

                    # Move to next batch
                    offset += batch_size
                    batch_num += 1

                    # Show progress
                    progress = (min(offset, total_for_channel) /
                                total_for_channel) * 100
                    _logger.info(
                        f"Channel {channel_code} progress: {progress:.1f}% complete")

                total_migrated += channel_migrated
                _logger.info(
                    f"Completed {channel_code}: {channel_migrated:,} records migrated")

            except Exception as e:
                _logger.warning(f"Migration failed for {channel_code}: {e}")

        return f"{total_migrated:,} legacy records migrated"
    
    def _bulk_update_partner_relationships_optimized(self):
        """Optimized partner relationship updates"""
        self._cr.execute("""
            UPDATE customer_channel_subscription ccs
            SET partner_id = rp.id, last_updated = NOW()
            FROM res_partner rp
            WHERE ccs.customer_id = rp.customer_id
            AND ccs.partner_id IS NULL
            AND rp.customer_id IS NOT NULL
            AND rp.customer_id != ''
        """)

        updated_count = self._cr.rowcount
        _logger.info(f"Updated {updated_count:,} partner relationships")
        self._cr.commit()

        return f"{updated_count:,} partner links updated"

    def action_run_full_migration(self):
        """Batch-wise migration: All 4 steps per batch for better progress tracking"""
        _logger.info("Starting batch-wise optimized migration for 10M+ records...")

        try:
            # Check if indexes exist first
            self._cr.execute("""
                SELECT COUNT(*) FROM pg_indexes 
                WHERE indexname = 'customer_channel_subscription_customer_channel_idx'
            """)

            if self._cr.fetchone()[0] == 0:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': 'Setup Required',
                        'message': 'Please run "Setup Indexes & Migration" first to create performance indexes.',
                        'type': 'warning',
                    }
                }

            # Get migration parameters
            migration_params = self._get_migration_parameters()

            if migration_params['total_customers'] == 0:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': 'No Customers Found',
                        'message': 'No customers with customer_id found for migration.',
                        'type': 'info',
                    }
                }

            # Initialize counters
            total_results = {
                'subscriptions_created': 0,
                'legacy_migrated': 0,
                'partner_links_updated': 0,
                'batches_completed': 0
            }

            _logger.info(
                f"Migration plan: {migration_params['total_customers']:,} customers in {migration_params['total_batches']} batches of {migration_params['batch_size']:,}")

            # Process each batch with all 4 steps
            for batch_num in range(1, migration_params['total_batches'] + 1):
                _logger.info(
                    f"\n=== BATCH {batch_num}/{migration_params['total_batches']} ===")

                batch_results = self._process_single_batch(
                    batch_num, migration_params)

                # Update totals
                total_results['subscriptions_created'] += batch_results['subscriptions']
                total_results['legacy_migrated'] += batch_results['legacy']
                total_results['partner_links_updated'] += batch_results['partners']
                total_results['batches_completed'] += 1

                # Calculate and log progress
                progress_percent = (
                    batch_num / migration_params['total_batches']) * 100
                customers_processed = min(
                    batch_num * migration_params['batch_size'], migration_params['total_customers'])

                _logger.info(
                    f"✅ Batch {batch_num} COMPLETE - Progress: {progress_percent:.1f}% ({customers_processed:,}/{migration_params['total_customers']:,} customers)")
                _logger.info(
                    f"   Batch results: {batch_results['subscriptions']:,} subscriptions, {batch_results['legacy']:,} legacy, {batch_results['partners']:,} partner links")
                _logger.info(
                    f"   Total so far: {total_results['subscriptions_created']:,} subscriptions, {total_results['legacy_migrated']:,} legacy, {total_results['partner_links_updated']:,} partner links")

            # Final step: Create materialized view (only once at the end)
            _logger.info(f"\n=== FINAL STEP: Creating Materialized View ===")
            view_result = self._create_materialized_view_optimized()

            # Final summary
            summary = f"Completed {total_results['batches_completed']} batches: {total_results['subscriptions_created']:,} subscriptions created, {total_results['legacy_migrated']:,} legacy records migrated, {total_results['partner_links_updated']:,} partner links updated, {view_result}"
            _logger.info(f"🎉 MIGRATION COMPLETE: {summary}")

            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Migration Complete!',
                    'message': summary,
                    'type': 'success',
                }
            }

        except Exception as e:
            error_msg = f"Migration failed: {e}"
            _logger.error(error_msg)
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Migration Failed',
                    'message': error_msg,
                    'type': 'danger',
                }
            }

    def _get_migration_parameters(self):
        """Get migration parameters and counts"""
        # Get total customer count
        self._cr.execute("""
            SELECT COUNT(*) FROM res_partner 
            WHERE customer_id IS NOT NULL AND customer_id != ''
        """)
        total_customers = self._cr.fetchone()[0]

        # Get channel count
        self._cr.execute(
            "SELECT COUNT(*) FROM digital_delivery_channel WHERE status = 'active'")
        channel_count = self._cr.fetchone()[0]

        # Get existing subscription count
        self._cr.execute("SELECT COUNT(*) FROM customer_channel_subscription")
        existing_subscriptions = self._cr.fetchone()[0]

        # Determine batch size based on customer count
        if total_customers > 1000000:  # 1M+
            batch_size = 50000
        elif total_customers > 100000:  # 100K+
            batch_size = 25000
        else:
            batch_size = 10000

        total_batches = (total_customers + batch_size -
                        1) // batch_size  # Ceiling division

        return {
            'total_customers': total_customers,
            'channel_count': channel_count,
            'existing_subscriptions': existing_subscriptions,
            'batch_size': batch_size,
            'total_batches': total_batches
        }

    def _process_single_batch(self, batch_num, params):
        """Process all 4 steps for a single batch"""
        offset = (batch_num - 1) * params['batch_size']
        batch_size = params['batch_size']

        batch_results = {
            'subscriptions': 0,
            'legacy': 0,
            'partners': 0
        }

        # Step 1: Create customer-channel subscriptions for this batch
        _logger.info(f"  Step 1/4: Creating subscriptions for batch {batch_num}")
        subscriptions_created = self._create_subscriptions_for_batch(
            offset, batch_size)
        batch_results['subscriptions'] = subscriptions_created

        # Step 2: Migrate legacy data for this batch
        _logger.info(f"  Step 2/4: Migrating legacy data for batch {batch_num}")
        legacy_migrated = self._migrate_legacy_for_batch(offset, batch_size)
        batch_results['legacy'] = legacy_migrated

        # Step 3: Update partner relationships for this batch
        _logger.info(f"  Step 3/4: Updating partner links for batch {batch_num}")
        partners_updated = self._update_partners_for_batch(offset, batch_size)
        batch_results['partners'] = partners_updated

        # Step 4: Commit and cleanup for this batch
        _logger.info(f"  Step 4/4: Committing batch {batch_num}")
        self._cr.commit()
        gc.collect()

        return batch_results


    def _create_subscriptions_for_batch(self, offset, batch_size):
        """Step 1: Create subscriptions for a specific batch of customers"""
        # Get batch of customers
        self._cr.execute("""
            SELECT id, customer_id 
            FROM res_partner 
            WHERE customer_id IS NOT NULL AND customer_id != ''
            ORDER BY id
            LIMIT %s OFFSET %s
        """, (batch_size, offset))

        customer_batch = self._cr.fetchall()

        if not customer_batch:
            return 0

        # Create temporary table for this batch
        self._cr.execute(
            "CREATE TEMP TABLE IF NOT EXISTS temp_batch_customers (partner_id INTEGER, customer_id TEXT)")
        self._cr.execute("TRUNCATE TABLE temp_batch_customers")

        # Insert batch customers
        batch_values = []
        for partner_id, customer_id in customer_batch:
            batch_values.append(f"({partner_id}, '{customer_id}')")

        if batch_values:
            self._cr.execute(f"""
                INSERT INTO temp_batch_customers (partner_id, customer_id)
                VALUES {','.join(batch_values)}
            """)

            # Create subscriptions for this batch
            self._cr.execute("""
                INSERT INTO customer_channel_subscription (customer_id, partner_id, channel_id, value, last_updated)
                SELECT 
                    tbc.customer_id,
                    tbc.partner_id,
                    dc.id,
                    'NO',
                    NOW()
                FROM temp_batch_customers tbc
                CROSS JOIN digital_delivery_channel dc
                WHERE dc.status = 'active'
                AND NOT EXISTS (
                    SELECT 1 FROM customer_channel_subscription ccs
                    WHERE ccs.customer_id = tbc.customer_id 
                    AND ccs.channel_id = dc.id
                )
            """)

            created_count = self._cr.rowcount
            return created_count

        return 0


    def _migrate_legacy_for_batch(self, offset, batch_size):
        """Step 2: Migrate legacy data for customers in this batch"""
        # Check if legacy table exists
        self._cr.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'customer_digital_product'
            )
        """)

        if not self._cr.fetchone()[0]:
            return 0

        # Get channel mappings
        legacy_mappings = {
            'ussd': 'ussd', 'onebank': 'onebank', 'carded_customer': 'carded_customer',
            'alt_bank': 'alt_bank', 'sterling_pro': 'sterling_pro', 'banca': 'banca',
            'doubble': 'doubble', 'specta': 'specta', 'switch': 'switch'
        }

        self._cr.execute(
            "SELECT code, id FROM digital_delivery_channel WHERE status = 'active'")
        channel_ids = dict(self._cr.fetchall())

        total_updated = 0

        # Get customer IDs for this batch
        self._cr.execute("""
            SELECT customer_id
            FROM res_partner 
            WHERE customer_id IS NOT NULL AND customer_id != ''
            ORDER BY id
            LIMIT %s OFFSET %s
        """, (batch_size, offset))

        batch_customer_ids = [row[0] for row in self._cr.fetchall()]

        if not batch_customer_ids:
            return 0

        # Create customer ID list for SQL
        customer_ids_str = "'" + "','".join(batch_customer_ids) + "'"

        # Update legacy data for each channel for this batch of customers
        for channel_code, legacy_field in legacy_mappings.items():
            if channel_code not in channel_ids:
                continue

            channel_id = channel_ids[channel_code]

            try:
                self._cr.execute(f"""
                    UPDATE customer_channel_subscription ccs
                    SET value = cdp.{legacy_field}, last_updated = NOW()
                    FROM customer_digital_product cdp
                    WHERE ccs.customer_id = cdp.customer_id
                    AND ccs.channel_id = %s
                    AND cdp.{legacy_field} IS NOT NULL 
                    AND cdp.{legacy_field} != ''
                    AND ccs.customer_id IN ({customer_ids_str})
                """, (channel_id,))

                updated_count = self._cr.rowcount
                total_updated += updated_count

            except Exception as e:
                _logger.warning(
                    f"Legacy migration failed for {channel_code} in this batch: {e}")

        return total_updated


    def _update_partners_for_batch(self, offset, batch_size):
        """Step 3: Update partner relationships for customers in this batch"""
        # Get customer IDs for this batch
        self._cr.execute("""
            SELECT customer_id
            FROM res_partner 
            WHERE customer_id IS NOT NULL AND customer_id != ''
            ORDER BY id
            LIMIT %s OFFSET %s
        """, (batch_size, offset))

        batch_customer_ids = [row[0] for row in self._cr.fetchall()]

        if not batch_customer_ids:
            return 0

        customer_ids_str = "'" + "','".join(batch_customer_ids) + "'"

        # Update partner relationships for this batch
        self._cr.execute(f"""
            UPDATE customer_channel_subscription ccs
            SET partner_id = rp.id, last_updated = NOW()
            FROM res_partner rp
            WHERE ccs.customer_id = rp.customer_id
            AND ccs.partner_id IS NULL
            AND rp.customer_id IS NOT NULL
            AND rp.customer_id != ''
            AND ccs.customer_id IN ({customer_ids_str})
        """)

        updated_count = self._cr.rowcount
        return updated_count
        
    def _create_materialized_view_optimized(self):
        """Create materialized view without blocking"""
        try:
            # Check if view exists
            self._cr.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_matviews
                    WHERE matviewname = 'customer_digital_product_mat'
                )
            """)

            if self._cr.fetchone()[0]:
                _logger.info(
                    "Materialized view already exists, skipping creation")
                return "View already exists"

            # Only create if legacy table exists
            self._cr.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'customer_digital_product'
                )
            """)

            if not self._cr.fetchone()[0]:
                _logger.info("No legacy table, skipping materialized view")
                return "No legacy table for view"

            # Create view with minimal data first
            _logger.info("Creating materialized view...")
            self._cr.execute("""
                CREATE MATERIALIZED VIEW customer_digital_product_mat AS (
                    SELECT 
                        ROW_NUMBER() OVER (ORDER BY customer_id) as id,
                        customer_id,
                        'Placeholder' as customer_name,
                        'Standard' as customer_segment,
                        NULL as ussd, NULL as onebank, NULL as carded_customer,
                        NULL as alt_bank, NULL as sterling_pro, NULL as banca,
                        NULL as doubble, NULL as specta, NULL as switch
                    FROM (SELECT DISTINCT customer_id FROM customer_channel_subscription LIMIT 1000) t
                )
            """)

            # Create indexes
            self._cr.execute(
                "CREATE UNIQUE INDEX customer_digital_mat_id_idx ON customer_digital_product_mat(id)")
            self._cr.execute(
                "CREATE INDEX customer_digital_mat_customer_idx ON customer_digital_product_mat(customer_id)")

            _logger.info("Materialized view created with basic structure")
            return "View created (basic)"

        except Exception as e:
            _logger.error(f"Materialized view creation failed: {e}")
            return f"View error: {e}"
    
    def action_create_dynamic_demo_subscriptions(self):
        """Create random channel subscriptions dynamically for any channels without hardcoding"""

        # Find partners with customer_id but no channel subscriptions
        self._cr.execute("""
            SELECT rp.id, rp.customer_id, rp.name
            FROM res_partner rp
            WHERE rp.customer_id IS NOT NULL 
            AND rp.customer_id != ''
            AND NOT EXISTS (
                SELECT 1 FROM customer_channel_subscription ccs
                WHERE ccs.customer_id = rp.customer_id
            )
            LIMIT 1000
        """)

        partners_without_subscriptions = self._cr.fetchall()

        if not partners_without_subscriptions:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'No Partners Found',
                    'message': 'All partners already have channel subscriptions',
                    'type': 'info',
                }
            }

        # Get all active channels dynamically
        channels = self.search([('status', '=', 'active')], order='name')

        if not channels:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'No Channels Found',
                    'message': 'No active delivery channels found. Please create channels first.',
                    'type': 'warning',
                }
            }

        _logger.info(
            f"Creating dynamic subscriptions for {len(partners_without_subscriptions)} partners with {len(channels)} channels")

        total_created = 0

        for partner_id, customer_id, partner_name in partners_without_subscriptions:
            subscriptions_to_create = []

            for channel in channels:
                # Dynamic probability: 60% chance of YES, 40% chance of NO
                # You can adjust this percentage easily
                yes_probability = 40
                value = 'YES' if random.randint(
                    1, 100) <= yes_probability else 'NO'

                subscriptions_to_create.append({
                    'customer_id': customer_id,
                    'partner_id': partner_id,
                    'channel_id': channel.id,
                    'value': value,
                    'last_updated': fields.Datetime.now(),
                })

            # Bulk create subscriptions for this partner
            self.env['customer.channel.subscription'].create(
                subscriptions_to_create)
            total_created += len(subscriptions_to_create)

            # Log progress every 100 partners
            if (total_created // len(channels)) % 100 == 0:
                _logger.info(
                    f"Created subscriptions for {total_created // len(channels)} partners...")

            # Commit every 50 partners to avoid long transactions
            if (total_created // len(channels)) % 50 == 0:
                self._cr.commit()

        _logger.info(
            f"Dynamic demo subscription creation completed: {total_created} subscriptions created for {len(partners_without_subscriptions)} partners")

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Demo Subscriptions Created',
                'message': f'Created {total_created} random subscriptions for {len(partners_without_subscriptions)} partners across {len(channels)} channels',
                'type': 'success',
            }
        }
    
    

class CustomerChannelSubscription(models.Model):
    """Optimized subscription model for large datasets"""
    _name = 'customer.channel.subscription'
    _description = 'Customer Channel Subscription'
    _sql_constraints = [
        ('uniq_customer_channel', 'unique(customer_id, channel_id)',
         "This customer already has this channel registered!"),
    ]

    customer_id = fields.Char(string='Customer ID', required=True, index=True)
    partner_id = fields.Many2one('res.partner', string='Partner',
                                 index=True, readonly=False)
    channel_id = fields.Many2one('digital.delivery.channel', string='Channel',
                                 required=True, index=True, ondelete='restrict')
    value = fields.Char(string='Value', index=True,
                        help="The value/status of this channel for the customer")

    subscription_date = fields.Date(string='Subscription Date')
    last_updated = fields.Datetime(
        string='Last Updated', default=fields.Datetime.now, index=True)


    @api.model_create_multi
    def create(self, vals_list):
        """Optimized batch create for large datasets"""
        # Set last_updated for all records
        for vals in vals_list:
            vals['last_updated'] = fields.Datetime.now()
        return super().create(vals_list)

    def write(self, vals):
        """Optimized write with minimal overhead"""
        vals['last_updated'] = fields.Datetime.now()
        return super().write(vals)

   
class CustomerDigitalProductMaterialized(models.Model):
    """Materialized view for efficient delivery channel lookups"""
    _name = 'customer.digital.product.mat'
    _description = 'Customer Digital Products Materialized'
    _auto = False  # This is not a regular table

    id = fields.Integer(readonly=True)
    customer_id = fields.Char(string='Customer ID', readonly=True, index=True)
    customer_name = fields.Char(string='Name', readonly=True)
    customer_segment = fields.Char(string='Customer Segment', readonly=True)
    ussd = fields.Char(string='Uses USSD', readonly=True)
    onebank = fields.Char(string='Uses One Bank', readonly=True)
    carded_customer = fields.Char(string='Has A Card', readonly=True)
    alt_bank = fields.Char(string='Is On Alt Bank', readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)

    def init(self):
        """Minimal initialization - no heavy operations"""
        _logger.info("Materialized view model initialized (deferred creation)")

    @api.model
    def refresh_view(self):
        """Safe refresh method"""
        try:
            self._cr.execute(
                "REFRESH MATERIALIZED VIEW customer_digital_product_mat")
            return True
        except Exception as e:
            _logger.error(f"Failed to refresh materialized view: {e}")
            return False
        