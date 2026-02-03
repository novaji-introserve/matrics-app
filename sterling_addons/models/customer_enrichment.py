# -*- coding: utf-8 -*-
"""
Customer Enrichment Model for Sterling Bank
============================================

This module extends the customer.enrichment model for Sterling Bank-specific
implementations. The structure mirrors Altbank's implementation and is ready
to be activated when Sterling Bank provides their PEP enrichment source table.

Design Rationale:
-----------------
This file is included in sterling_addons to allow bank-specific customizations
to the enrichment model if needed in the future. Currently, it inherits the
base implementation from altbank_addons/customer_enrichment.py.

When Sterling Bank Data is Available:
--------------------------------------
1. Create source table in Sterling's ETL database (similar to imal_altbank_pep_list_new)
2. Add table configuration to /data/odoo/ETL_script/etl_scripts/configs/customer_etl_config.json
3. Configure ETL mappings for:
   - customer_no → customer_id
   - account_name → corporate_name
   - name_of_the_pep → individual_name
   - status_position_designation → position
   - relationship_with_pep → relationship
   - source_of_fund → source_of_fund
   - source_of_wealth → source_of_wealth
   - nature_of_business_and_occupation → nature_of_business
   - branch_name → branch_name
   - bvn → bvn

4. Run ETL to spool enrichment data
5. The enrichment will automatically link to customers via customer_id

Author: Olumide Awodeji
Version: 1.0.0
"""

from odoo import api, fields, models
import logging

_logger = logging.getLogger(__name__)


class CustomerEnrichmentSterling(models.Model):
    """
    Sterling Bank Customer Enrichment Extension

    This model is ready to receive Sterling Bank-specific enrichment data
    when the source table becomes available. The base enrichment model
    provides all necessary fields and functionality.

    Sterling-specific customizations can be added here if needed.
    """

    _inherit = 'customer.enrichment'

    # -------------------------------------------------------------------------
    # Sterling Bank Specific Fields (if needed in future)
    # -------------------------------------------------------------------------
    # Add any Sterling-specific fields here when required
    # Example:
    # sterling_specific_field = fields.Char(string='Sterling Field')

    # -------------------------------------------------------------------------
    # Sterling Bank Specific Methods (if needed in future)
    # -------------------------------------------------------------------------
    # Add any Sterling-specific business logic here when required
