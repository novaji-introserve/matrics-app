#!/usr/bin/env python3
import os
import sys
import random
import logging
import psycopg2
import time
from datetime import datetime
from dotenv import load_dotenv

# Set up logging to console AND file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def create_dynamic_demo_subscriptions():
    """Create random channel subscriptions dynamically for any channels without hardcoding"""

    # Start timing
    start_time = time.time()
    logger.info("Script started")

    # Load environment variables from .env file
    load_dotenv()

    # Get database credentials from .env file
    db_host = os.getenv("HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB")  # DATABASE NAME
    db_user = os.getenv("USERNAME")  # DATABASE USERNAME
    db_password = os.getenv("PASSWORD")

    # Validate that required credentials are present
    if not all([db_name, db_user, db_password]):
        logger.error("Missing required database credentials in .env file")
        return

    logger.info(
        f"Attempting to connect to database: {db_name} on {db_host}:{db_port}")

    conn = None
    try:
        # Connect to the database with timeout
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            connect_timeout=10  # 10 seconds timeout for connection
        )

        logger.info("Successfully connected to database")

        # Create a cursor
        cr = conn.cursor()

        # Set statement timeout to 2 minutes instead of 30 seconds
        # 120 seconds timeout for queries
        cr.execute("SET statement_timeout = 120000")

        # Get all active channels first (this should be fast)
        logger.info("Fetching active channels...")
        cr.execute("""
            SELECT id, name 
            FROM digital_delivery_channel 
            WHERE status = 'active'
            ORDER BY name
        """)

        channels = cr.fetchall()

        if not channels:
            logger.warning(
                "No active delivery channels found. Please create channels first.")
            conn.close()
            return

        logger.info(f"Found {len(channels)} active channels")

        # Use a more efficient approach - first get all customer_ids that already have subscriptions
        logger.info("Building list of customers with existing subscriptions...")
        cr.execute("""
            SELECT DISTINCT customer_id 
            FROM customer_channel_subscription
        """)

        existing_customer_ids = set(row[0] for row in cr.fetchall())
        logger.info(
            f"Found {len(existing_customer_ids)} customers with existing subscriptions")

        # Process in smaller batches to avoid timeout
        batch_size = 100000
        offset = 0
        total_processed = 0
        total_created = 0

        while True:
            # Use a more efficient query that doesn't use NOT EXISTS
            logger.info(
                f"Fetching batch of partners (offset {offset}, limit {batch_size})...")
            cr.execute("""
                SELECT rp.id, rp.customer_id, rp.name
                FROM res_partner rp
                WHERE rp.customer_id IS NOT NULL 
                AND rp.customer_id != ''
                ORDER BY rp.id
                LIMIT %s OFFSET %s
            """, (batch_size, offset))

            batch_partners = cr.fetchall()

            if not batch_partners:
                logger.info("No more partners to process")
                break

            logger.info(f"Processing batch of {len(batch_partners)} partners")

            # Filter out partners that already have subscriptions
            partners_to_process = [
                partner for partner in batch_partners
                if partner[1] not in existing_customer_ids
            ]

            logger.info(
                f"Found {len(partners_to_process)} partners without subscriptions in this batch")

            if not partners_to_process:
                logger.info(
                    "No partners without subscriptions in this batch, moving to next batch")
                offset += batch_size
                continue

            # Process the filtered batch
            for partner_id, customer_id, partner_name in partners_to_process:
                partner_subscriptions = 0

                try:
                    for channel_id, channel_name in channels:
                        # Dynamic probability: 40% chance of YES, 60% chance of NO
                        yes_probability = 40
                        value = 'YES' if random.randint(
                            1, 100) <= yes_probability else 'NO'

                        # Insert subscription
                        cr.execute("""
                            INSERT INTO customer_channel_subscription (
                                customer_id, partner_id, channel_id, value, last_updated
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (
                            customer_id,
                            partner_id,
                            channel_id,
                            value,
                            datetime.now()
                        ))

                        total_created += 1
                        partner_subscriptions += 1

                    # Add to our set of processed customer_ids
                    existing_customer_ids.add(customer_id)

                    logger.info(
                        f"Created {partner_subscriptions} subscriptions for partner {partner_name}")
                    total_processed += 1

                    # Commit after each partner
                    conn.commit()

                except Exception as e:
                    logger.error(
                        f"Error processing partner {partner_name}: {e}")
                    conn.rollback()

            # Calculate overall progress
            elapsed = time.time() - start_time
            logger.info(
                f"Completed batch. Processed {total_processed} partners, created {total_created} subscriptions.")
            logger.info(f"Time elapsed: {elapsed:.1f}s.")

            # Move to next batch
            offset += batch_size

        # Final stats
        elapsed = time.time() - start_time
        logger.info(
            f"COMPLETED: Created {total_created} subscriptions for {total_processed} partners across {len(channels)} channels")
        logger.info(f"Total time: {elapsed:.1f} seconds")

    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {e}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())

    finally:
        # Always try to close the connection
        if conn is not None and not conn.closed:
            logger.info("Closing database connection")
            conn.close()

        logger.info("Script finished")


if __name__ == "__main__":
    print("Starting subscription creation script...")
    try:
        create_dynamic_demo_subscriptions()
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting.")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        print("Script execution complete.")
