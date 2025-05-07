#!/usr/bin/env python3
import xmlrpc.client
import logging
import sys
import os

# Script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    filename=os.path.join(script_dir, 'update_cron.log')
)
logger = logging.getLogger(__name__)

# Odoo connection parameters
URL = 'http://localhost:8069'
DB = 'test'
USERNAME = 'api_user'  # Use a dedicated user with appropriate permissions
PASSWORD = 'test'  # Never hardcode passwords in production!


try:
    # Test server connection
    logger.info(f"Testing connection to {URL}")
    common = xmlrpc.client.ServerProxy(f'{URL}/xmlrpc/2/common')

    try:
        version = common.version()
        logger.info(
            f"Connected to Odoo {version.get('server_version', 'unknown')}")
    except Exception as conn_error:
        logger.error(f"Could not connect to Odoo server: {str(conn_error)}")
        sys.exit(1)

    # Authenticate
    logger.info(
        f"Attempting authentication for user '{USERNAME}' on database '{DB}'")
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})

    if not uid:
        logger.error("Authentication failed - please check credentials")
        sys.exit(1)

    logger.info(f"Authentication successful with UID: {uid}")

    # Get models interface
    models = xmlrpc.client.ServerProxy(f'{URL}/xmlrpc/2/object')

    # Call the risk assessment method
    logger.info("Starting risk assessment process")
    result = models.execute_kw(
        DB, uid, PASSWORD,
        'res.partner',  # This is correct as your model inherits res.partner
        'cron_run_risk_assessment',
        []
    )

    logger.info(f"Risk assessment completed: {result}")

except xmlrpc.client.Fault as fault:
    logger.error(f"XML-RPC Fault: {fault.faultCode} - {fault.faultString}")
    sys.exit(1)
except ConnectionRefusedError:
    logger.error("Connection refused. Make sure Odoo server is running.")
    sys.exit(1)
except Exception as e:
    logger.error(f"Error: {str(e)}", exc_info=True)
    sys.exit(1)
