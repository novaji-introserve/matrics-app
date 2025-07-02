#!/usr/bin/env python3
import configparser
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import sys
import os
import argparse
import time
import traceback
from datetime import datetime
from contextlib import contextmanager

DEFAULT_CONFIG_FILE = "/data/odoo/ETL_script/update_script/settings.conf"

# Configure logging
def setup_logging():
    log_file = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), 'UpdateScript.log')

    # Check if file exists and exceeds size limit
    if os.path.exists(log_file) and os.path.getsize(log_file) >= 30 * 1024 * 1024:
        open(log_file, 'w').close()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )
    return logging.getLogger("__COMPUTE_AGGREGATE_RISK_SCORES__")

# Get database configuration
def get_db_config(config_file=DEFAULT_CONFIG_FILE):
    """Get database configuration from settings.conf."""
    try:
        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Look for config file in the script directory
        config_path = os.path.join(script_dir, config_file)

        # Check if config file exists
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Config file not found: {config_file}. Expected at: {config_path}")

        config = configparser.ConfigParser()
        config.read(config_path)

        # Check if database section exists
        if 'database' not in config:
            raise KeyError(
                f"Missing [database] section in {config_path}. File contains sections: {config.sections()}")

        # Check for required fields
        required_fields = ['host', 'port', 'user', 'password', 'dbname']
        missing_fields = [
            field for field in required_fields if field not in config['database']]

        if missing_fields:
            raise ValueError(
                f"Missing required fields in [database] section: {missing_fields}")

        return dict(config['database'])
    except Exception as e:
        logger.error(f"Failed to read config file: {str(e)}")
        raise


def get_db_connection(db_config):
    """Create a database connection using provided credentials."""
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            dbname=db_config['dbname']
        )
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to database: {str(e)}")
        raise


def check_total_accounts(conn):
    """Check total number of customer accounts."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM res_partner_account rpa
                LEFT JOIN res_partner rp ON rpa.customer_id = rp.id
                WHERE rp.risk_level IN ('high', 'medium', 'low')
            """)
            result = cursor.fetchone()
            return result[0] if result else 0
    except psycopg2.Error as e:
        logger.error(f"Error checking total accounts: {str(e)}")
        raise


def compute_aggregate_risk_scores(config_file=DEFAULT_CONFIG_FILE):
    """
    Compute aggregate risk scores grouped by branch, product, currency, account type, and state.
    Includes high/medium/low account counts and computes a count-weighted average risk score.
    """
    start_time = time.time()
    logger.info("Starting aggregate risk score computation")

    db_config = get_db_config(config_file)
    conn = None

    try:
        # Create a connection
        conn = get_db_connection(db_config)

        # Check total number of accounts
        total_accounts = check_total_accounts(conn)
        logger.info(f"Total customer accounts found: {total_accounts}")

        # Clear existing records with proper error handling
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM account_agg_risk_score")
                deleted_count = cursor.rowcount
                conn.commit()
            logger.info(
                f"Cleared {deleted_count} existing aggregate risk score records")
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"Failed to clear existing records: {str(e)}")
            raise

        # Compute and insert aggregate risk scores
        try:
            with conn.cursor() as cursor:
                logger.info("Computing aggregate risk scores...")

                insert_query = """
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
                        rpa.state
                """

                cursor.execute(insert_query)
                inserted_count = cursor.rowcount
                conn.commit()

                logger.info(
                    f"Successfully inserted {inserted_count} aggregate risk score records")

        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"Failed to compute aggregate risk scores: {str(e)}")
            raise

        # Get summary statistics
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_groups,
                        SUM(total_accounts) as total_accounts_processed,
                        SUM(high_count) as total_high_risk,
                        SUM(medium_count) as total_medium_risk,
                        SUM(low_count) as total_low_risk,
                        AVG(weighted_avg_risk_score) as overall_avg_score 
                        FROM account_agg_risk_score
                """)

                summary = cursor.fetchone()
                if summary:
                    logger.info("Aggregate computation summary:")
                    logger.info(
                        f"  - Total groupings created: {summary['total_groups']}")
                    logger.info(
                        f"  - Total accounts processed: {summary['total_accounts_processed']}")
                    logger.info(
                        f"  - High risk accounts: {summary['total_high_risk']}")
                    logger.info(
                        f"  - Medium risk accounts: {summary['total_medium_risk']}")
                    logger.info(
                        f"  - Low risk accounts: {summary['total_low_risk']}")
                    logger.info(
                        f"  - Overall average score: {summary['overall_avg_score']}")

        except psycopg2.Error as e:
            logger.warning(f"Failed to get summary statistics: {str(e)}")

        # Calculate and log performance metrics
        total_time = time.time() - start_time
        logger.info(f"Completed aggregate risk score computation")
        logger.info(f"Performance summary:")
        logger.info(f"  - Total time: {total_time:.2f} seconds")
        logger.info(f"  - Accounts processed: {total_accounts}")
        if total_time > 0:
            logger.info(
                f"  - Processing rate: {total_accounts/total_time:.2f} accounts/second")

    except Exception as e:
        logger.error(f"Error computing aggregate risk scores: {str(e)}")
        logger.error(traceback.format_exc())
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # Setup logging
    logger = setup_logging()

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Compute aggregate risk scores for customer accounts')
    parser.add_argument('--config', default='settings.conf',
                        help='Path to config file')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Resolve config path relative to script directory if not absolute
    if not os.path.isabs(args.config):
        config_path = os.path.join(script_dir, args.config)
    else:
        config_path = args.config

    try:
        # Print start information
        logger.info(
            f"Starting compute_aggregate_risk_scores")

        # Try to run with specified config file
        compute_aggregate_risk_scores(args.config)

        logger.info("Aggregate risk score computation completed successfully")
    except FileNotFoundError as e:
        logger.error(f"ERROR: {e}")
        logger.error("\nTo create a template config file, run:")
        logger.error(
            f"python {os.path.basename(__file__)} --create-config --config={args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"ERROR: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)