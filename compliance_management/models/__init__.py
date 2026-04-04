# -*- coding: utf-8 -*-
"""
Module for managing various aspects of customer data, risk assessment, and dashboard functionalities.

This module includes models that handle customer-related information, risk management,
and dashboard components necessary for the application.
"""


from . import utils
from . import education_level
from . import kyc_limit
from . import identification_types
from . import marital_status
from . import customer_sector
from . import region
from . import branch
from . import account_ledger
from . import account_product
from . import gender
from . import risk_level
from . import users
from . import customer
from . import customer_account_type
from . import customer_account
from . import customer_tier
from . import risk_assessment_type
from . import risk_category
from . import risk_subject
from . import risk_type
from . import risk_universe
from . import risk_assessment
from . import risk_assessment_line
from . import pep
from . import edd
from . import feplist
from . import blacklist
from . import watchlist
from . import greylist
from . import statistic
from . import resource_uri
from . import risk_assessment_plan
from . import settings
from . import transaction
from . import transaction_screening_rule
from . import transaction_type
from . import pep_customer
from . import adverse_media
from . import pep_source
from . import import_log
from . import open_sanctions_extension
from . import job_queue_handler
from . import web_scraping
from . import dynamic_charts
from . import risk_assessment_control
from . import risk_assesment_mitigation
from . import res_risk_implication
from . import fcra_score
from . import dashboard_cache
from . import aggregate_customer_score
from . import dashboard_chart_view_refresher
from . import res_materialized_views    
from . import peplist    
from . import sanction_list    
from . import digital_product    
from . import risk_profiling
from . import account_aggregate
from . import customer_screening
from . import transaction_screening_history
from . import change_data_capture
