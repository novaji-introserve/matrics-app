#!/usr/bin/env python3
"""
Script to generate Optimized partitioned tables for risk analysis with go engine.
"""

import os
import re
import time
import logging
import psycopg2
import configparser
import multiprocessing
from datetime import datetime
from multiprocessing import Pool, cpu_count
import sys


# Default database cnfiguration file path
# path to your .conf file eg odoo.conf
DEFAULT_CONFIG_FILE = "/home/novaji/odoo/icomply_odoo/risk_analysis/config.conf"

# DEFAULT_CONFIG_FILE = (
#     os.path.join(os.path.dirname(os.path.abspath(__file__)), 'risk_analysis/config.conf')
# )



def setup_logging():
    """Configure logging with rotation based on file size"""
    log_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_file = os.path.join(log_dir, 'RiskAnalysis.log')
    
    # Check if file exists and exceeds size limit (30MB)
    if os.path.exists(log_file) and os.path.getsize(log_file) >= 30 * 1024 * 1024:
        # Create a backup of old log
        backup_name = f"UpdateScript_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log.bak"
        os.rename(log_file, os.path.join(log_dir, backup_name))
    
    # Configure logging to file with proper format
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            # logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),

        ]
    )
    return logging.getLogger("__RISK_ASSESSMENT_MV__")

def get_db_connection(config_file=DEFAULT_CONFIG_FILE):
    """Create a database connection from config file"""
    config = configparser.ConfigParser()
    config.read(config_file)


    print(config.items())
    
    # Check for Odoo's [options] section first
    if 'options' in config and config['options']:
        db_config = config['options']
        conn = psycopg2.connect(
            host=db_config.get('db_host', 'localhost'),
            port=db_config.get('db_port', 5432),
            user=db_config.get('db_user'),
            password=db_config.get('db_password'),
            dbname=db_config.get('db_name')
        )
    # Fallback to a generic [database] section
    elif 'database' in config and config['database']:
        db_config = config['database']
        conn = psycopg2.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 5432),
            user=db_config.get('user'),
            password=db_config.get('password'),
            dbname=db_config.get('dbname')
        )
    else:
        raise configparser.Error(f"Config file '{config_file}' is missing a valid [options] or [database] section.")
    return conn

def slugify(text):
    """Creates a valid SQL identifier from text."""
    text = text.lower().strip()
    text = re.sub(r'[\s\.]+', '_', text)
    return re.sub(r'[^\w_]', '', text)

def extract_pattern_data(sql_query, plan_code, risk_score):
    """
    Extracts pattern data from SQL queries for set-based operations.
    """
    sql_lower = sql_query.lower().strip()
    
    # Pattern 1: res_partner_account with category
    match = re.search(
        r"from\s+res_partner_account\s+a.*?"
        r"where.*?(?:r|rp|a\.customer_id|rpa\.customer_id)\.id\s*=\s*%s.*?"
        r"and\s+(?:lower\s*\(\s*)?a\.(category(?:_description)?)\s*(?:\)\s*)?=\s*'([^']+)'",
        sql_lower, re.DOTALL
    )
    if match:
        field, value = match.groups()
        return {
            'type': 'account_category',
            'table': 'res_partner_account',
            'field': field,
            'value': value.lower(),
            'code': plan_code,
            'score': risk_score,
            'use_latest': True,
            'join_field': 'customer_id'
        }
    
    # Pattern 2: customer_industry_id subquery
    match = re.search(
        r"customer_industry_id\s+in\s*\(\s*select\s+id\s+from\s+customer_industry\s+"
        r"where\s+(?:lower\s*\(\s*name\s*\)|name)\s*=\s*'([^']+)'",
        sql_lower, re.DOTALL
    )
    if match:
        return {
            'type': 'industry',
            'table': 'customer_industry',
            'field': 'name',
            'value': match.group(1).lower(),
            'code': plan_code,
            'score': risk_score,
            'join_field': 'customer_industry_id',
            'partner_field': 'id'
        }
    
    # Pattern 3: res_partner_region join
    match = re.search(
        r"from\s+res_partner\s+(?:rp|r).*?"
        r"(?:join|inner join)\s+res_partner_region\s+(?:rpr|r)\s+on\s+(?:rp|r)\.region_id\s*=\s*(?:rpr|r)\.id.*?"
        r"where.*?(?:rp|r)\.id\s*=\s*%s.*?"
        r"and\s+(?:lower\s*\(\s*)?(?:rpr|r)\.name(?:\s*\))?\s*=\s*'([^']+)'",
        sql_lower, re.DOTALL
    )
    if match:
        return {
            'type': 'region',
            'table': 'res_partner_region',
            'field': 'name',
            'value': match.group(1).lower(),
            'code': plan_code,
            'score': risk_score,
            'join_field': 'region_id',
            'partner_field': 'id'
        }
    
    # Pattern 4: customer_channel_subscription
    match = re.search(
        r"from\s+customer_channel_subscription\s+ccs.*?"
        r"join\s+digital_delivery_channel\s+ddc.*?"
        r"where.*?ccs\.partner_id.*?=\s*%s.*?"
        r"and.*?ddc\.code\s*=\s*'([^']+)'",
        sql_lower, re.DOTALL
    )
    if match:
        value_condition = None
        if "ccs.value::bool = true" in sql_lower:
            value_condition = "value::bool = true"
        elif "lower(ccs.value) in ('yes', 'enrolled')" in sql_lower:
            value_condition = "lower(value) IN ('yes', 'enrolled')"
        
        return {
            'type': 'channel',
            'table': 'customer_channel_subscription',
            'channel_code': match.group(1),
            'value_condition': value_condition,
            'code': plan_code,
            'score': risk_score,
            'partner_field': 'partner_id'
        }
    
    # Pattern 5: branch region
    match = re.search(
        r"from\s+res_partner_account\s+(?:rpa|a).*?"
        r"(?:join|inner join)\s+res_branch\s+(?:rb|r|b)\s+on\s+(?:rpa|a)\.branch_id\s*=\s*(?:rb|r|b)\.id.*?"
        r"where.*?(?:rpa|a)\.customer_id\s*=\s*%s.*?"
        r"and\s+(?:lower\s*\(\s*)?(?:trim\s*\(\s*)?(?:rb|r|b)\.region(?:\s*\))?(?:\s*\))?\s*=\s*'([^']+)'",
        sql_lower, re.DOTALL
    )
    if match:
        return {
            'type': 'branch_region',
            'table': 'res_partner_account',
            'value': match.group(1).lower(),
            'code': plan_code,
            'score': risk_score,
            'use_latest': True,
            'join_field': 'customer_id'
        }
    
    return None

def extract_independent_pattern(sql_query, plan_code, risk_score):
    """
    Extracts risk pattern data from independent risk assessment SQL queries.
    """
    sql_lower = sql_query.lower().strip()
    
    # Pattern 1: Multiple accounts with same phone
    if "customer_phone in" in sql_lower and "group by customer_phone" in sql_lower:
        # Extract the threshold
        match = re.search(r"having\s+count\s*\(\s*\*\s*\)\s*>=\s*(\d+)", sql_lower)
        threshold = int(match.group(1)) if match else 3  # Default to 3 if not found
        
        return {
            'type': 'multiple_phone_accounts',
            'code': plan_code,
            'score': risk_score,
            'threshold': threshold
        }
    
    # Pattern 2: Invalid BVN
    if "bvn is null" in sql_lower or "bvn like" in sql_lower:
        return {
            'type': 'invalid_bvn',
            'code': plan_code,
            'score': risk_score
        }
    
    # Pattern 3: Invalid name
    if "trim(name)" in sql_lower and ("= ''" in sql_lower or "~" in sql_lower):
        return {
            'type': 'invalid_name',
            'code': plan_code,
            'score': risk_score
        }
    
    # Pattern 4: Missing contact information
    if "mobile is null" in sql_lower and "phone is null" in sql_lower and "customer_phone is null" in sql_lower:
        return {
            'type': 'missing_contact',
            'code': plan_code,
            'score': risk_score
        }
    
    # Pattern 5: Sanctions
    if "likely_sanction" in sql_lower:
        return {
            'type': 'sanction',
            'code': plan_code,
            'score': risk_score
        }
    
    # Pattern 6: PEP
    if "is_pep" in sql_lower:
        return {
            'type': 'pep',
            'code': plan_code,
            'score': risk_score
        }
    
    # Pattern 7: Watchlist
    if "is_watchlist" in sql_lower:
        return {
            'type': 'watchlist',
            'code': plan_code,
            'score': risk_score
        }
        
    # Pattern 8: Default Risk Rating
    if "is_default" in sql_lower and "risk_rating" in sql_lower:
        return {
            'type': 'default_risk',
            'code': plan_code,
            'score': risk_score
        }
    
    return None

def build_optimized_view(conn, universe_id, universe_code, universe_name):
    """Build SQL for a partitioned table structure."""
    logger = logging.getLogger("__RISK_ASSESSMENT_MV__")
    
    view_name = f"mv_risk_{slugify(universe_code)}"
    
    # Extract active risk assessment plans for this universe
    with conn.cursor() as cursor:
        # cursor.execute("""
        #     SELECT code, sql_query, COALESCE(risk_assessment, 0) 
        #     FROM res_compliance_risk_assessment_plan
        #     WHERE state = 'active' 
        #     AND sql_query IS NOT NULL
        #     AND universe_id = %s
        # """, (universe_id,))
        cursor.execute("""
    SELECT p.code, p.sql_query, 
        CASE 
            WHEN p.compute_score_from = 'risk_assessment' THEN COALESCE(ra.risk_rating, 0)
            WHEN p.compute_score_from = 'static' THEN COALESCE(p.risk_score, 0)
            ELSE 0.0
        END as risk_score
    FROM res_compliance_risk_assessment_plan p
    LEFT JOIN res_risk_assessment ra ON p.risk_assessment = ra.id
    WHERE p.state = 'active' 
    AND p.sql_query IS NOT NULL
    AND p.universe_id = %s
""",(universe_id,))
        
        plans_data = cursor.fetchall()
    
    # Extract all patterns
    patterns = {
        'account_category': {},
        'industry': {},
        'region': {},
        'channel': {},
        'branch_region': {},
    }
    
    unmatched = []
    
    for plan_code, sql_query, risk_score in plans_data:
        pattern = extract_pattern_data(sql_query, plan_code, risk_score)
        
        if not pattern:
            unmatched.append(plan_code)
            continue
        
        ptype = pattern['type']
        
        # Group patterns by their filter values for efficient CASE statements
        if ptype == 'account_category':
            key = pattern['field']
            if key not in patterns[ptype]:
                patterns[ptype][key] = []
            patterns[ptype][key].append(pattern)
        
        elif ptype in ['industry', 'region']:
            if 'items' not in patterns[ptype]:
                patterns[ptype]['items'] = []
            patterns[ptype]['items'].append(pattern)
        
        elif ptype == 'channel':
            key = (pattern['channel_code'], pattern.get('value_condition'))
            if key not in patterns[ptype]:
                patterns[ptype][key] = []
            patterns[ptype][key].append(pattern)
        
        elif ptype == 'branch_region':
            if 'items' not in patterns[ptype]:
                patterns[ptype]['items'] = []
            patterns[ptype]['items'].append(pattern)
    
    # Build UNION ALL branches
    union_branches = []
    
    # Branch 1: Account Categories (optimized with DISTINCT ON)
    for field, items in patterns['account_category'].items():
        if not items:
            continue
        
        # Build CASE for all values of this field
        case_whens = []
        values_list = []
        for item in items:
            case_whens.append(
                f"        WHEN lower(a.{field}) = '{item['value']}' THEN '{item['code']}'"
            )
            case_whens.append(
                f"        WHEN lower(a.{field}) = '{item['value']}' THEN {item['score']}"
            )
            values_list.append(f"'{item['value']}'")
        
        # Create two columns: one for risk_code, one for risk_score
        union_branches.append(f"""
        -- Account {field}
        SELECT 
            a.customer_id AS partner_id,
            CASE 
    {chr(10).join(case_whens[::2])}
            END AS risk_code,
            CASE 
    {chr(10).join(case_whens[1::2])}
            END AS risk_score
        FROM (
            SELECT DISTINCT ON (customer_id, {field})
                customer_id, {field}
            FROM res_partner_account
            WHERE lower({field}) IN ({', '.join(values_list)})
            ORDER BY customer_id, {field}, opening_date DESC
        ) a""")
    
    # Branch 2: Industries (simple join)
    if patterns['industry'].get('items'):
        case_whens_code = []
        case_whens_score = []
        values_list = []
        
        for item in patterns['industry']['items']:
            case_whens_code.append(
                f"        WHEN lower(ci.name) = '{item['value']}' THEN '{item['code']}'"
            )
            case_whens_score.append(
                f"        WHEN lower(ci.name) = '{item['value']}' THEN {item['score']}"
            )
            values_list.append(f"'{item['value']}'")
        
        union_branches.append(f"""
        -- Industries
        SELECT 
            rp.id AS partner_id,
            CASE 
    {chr(10).join(case_whens_code)}
            END AS risk_code,
            CASE 
    {chr(10).join(case_whens_score)}
            END AS risk_score
        FROM res_partner rp
        INNER JOIN customer_industry ci ON rp.customer_industry_id = ci.id
        WHERE lower(ci.name) IN ({', '.join(values_list)})""")
    
    # Branch 3: Regions (simple join)
    if patterns['region'].get('items'):
        case_whens_code = []
        case_whens_score = []
        values_list = []
        
        for item in patterns['region']['items']:
            case_whens_code.append(
                f"        WHEN lower(rpr.name) = '{item['value']}' THEN '{item['code']}'"
            )
            case_whens_score.append(
                f"        WHEN lower(rpr.name) = '{item['value']}' THEN {item['score']}"
            )
            values_list.append(f"'{item['value']}'")
        
        union_branches.append(f"""
        -- Regions
        SELECT 
            rp.id AS partner_id,
            CASE 
    {chr(10).join(case_whens_code)}
            END AS risk_code,
            CASE 
    {chr(10).join(case_whens_score)}
            END AS risk_score
        FROM res_partner rp
        INNER JOIN res_partner_region rpr ON rp.region_id = rpr.id
        WHERE lower(rpr.name) IN ({', '.join(values_list)})""")
    
    # Branch 4: Channel Subscriptions (grouped by channel)
    for (channel_code, value_cond), items in patterns['channel'].items():
        if not items:
            continue
        
        # Handle value_condition correctly 
        value_filter = ""
        if value_cond:
            if "lower(" in value_cond:
                value_filter = f"AND {value_cond.replace('value', 'ccs.value')}"
            else:
                value_filter = f"AND ccs.{value_cond}"
        
        # All items in this group have same channel, so we can return multiple risk codes
        for item in items:
            union_branches.append(f"""
        -- Channel: {channel_code}
        SELECT 
            ccs.partner_id::integer AS partner_id,
            '{item['code']}' AS risk_code,
            {item['score']} AS risk_score
        FROM customer_channel_subscription ccs
        INNER JOIN digital_delivery_channel ddc ON ccs.channel_id = ddc.id
        WHERE ddc.code = '{channel_code}'
        {value_filter}""")
    
    # Branch 5: Branch Regions (optimized with DISTINCT ON)
    if patterns['branch_region'].get('items'):
        case_whens_code = []
        case_whens_score = []
        values_list = []
        
        for item in patterns['branch_region']['items']:
            case_whens_code.append(
                f"        WHEN lower(trim(rpa_latest.region)) = '{item['value']}' THEN '{item['code']}'"
            )
            case_whens_score.append(
                f"        WHEN lower(trim(rpa_latest.region)) = '{item['value']}' THEN {item['score']}"
            )
            values_list.append(f"'{item['value']}'")

        union_branches.append(f"""
        -- Branch Regions
        SELECT 
            rpa_latest.customer_id AS partner_id,
            CASE 
        {chr(10).join(case_whens_code)}
            END AS risk_code,
            CASE 
        {chr(10).join(case_whens_score)}
            END AS risk_score
        FROM (
            SELECT DISTINCT ON (rpa.customer_id)
                rpa.customer_id, rb.region
            FROM res_partner_account rpa
            INNER JOIN res_branch rb ON rpa.branch_id = rb.id
            WHERE lower(trim(rb.region)) IN ({', '.join(values_list)})
            ORDER BY rpa.customer_id, rpa.opening_date DESC
        ) rpa_latest
        """)
    
    # Handle empty case
    if not union_branches:
        logger.warning(f"No patterns matched for universe {universe_name}. Unmatched: {unmatched}")
        return {
            'name': view_name,
            'code': "-- Empty view, no patterns matched",
            'universe': universe_name,
            'stats': f"No patterns matched. Unmatched: {len(unmatched)}",
            'is_partitioned': True
        }
    
    # Assemble the SQL for populating partitions
    all_flags_cte = ""
    if union_branches:
        all_flags_cte = union_branches[0]  # First branch without UNION ALL
        for branch in union_branches[1:]:
            all_flags_cte += f"\n    UNION ALL{branch}"
    
    # Create the SQL for populating partitioned tables with exclusive upper bound
    populate_sql = f"""
    -- SQL to populate partitioned table {view_name}
    INSERT INTO {view_name} (partner_id, partner_name, risk_data)
    WITH all_risk_flags AS (
    {all_flags_cte}
    )
    SELECT 
        rp.id AS partner_id,
        rp.name AS partner_name,
        COALESCE(
            (SELECT jsonb_object_agg(risk_code, risk_score)
            FROM all_risk_flags arf
            WHERE arf.partner_id = rp.id
            AND arf.risk_code IS NOT NULL
            AND arf.risk_score IS NOT NULL),
            '{{}}'::jsonb
        ) AS risk_data
    FROM res_partner rp
    WHERE rp.id >= %s AND rp.id < %s;  -- Note the < instead of <= for upper bound
    """
    
    logger.info(f"Built view SQL for {view_name} with {len(union_branches)} pattern branches")
    
    return {
        'name': view_name,
        'code': populate_sql,
        'universe': universe_name,
        'stats': f"Patterns matched: {len(union_branches)}, Unmatched: {len(unmatched)}",
        'is_partitioned': True
    }

def build_independent_risk_view(conn):
    """Build SQL for independent risk factors (not tied to universes)."""
    logger = logging.getLogger("__RISK_ASSESSMENT_MV__")
    
    view_name = "mv_risk_independent_factors"
    
    # Fetch active risk assessment plans without a universe
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT code, sql_query, COALESCE(risk_score, 0) 
            FROM res_compliance_risk_assessment_plan
            WHERE state = 'active' 
            AND sql_query IS NOT NULL
            AND universe_id IS NULL
        """)
        
        plans_data = cursor.fetchall()
    
    # Extract risk patterns
    risk_patterns = []
    unmatched = []
    
    for plan_code, sql_query, risk_score in plans_data:
        pattern = extract_independent_pattern(sql_query, plan_code, risk_score)
        if pattern:
            risk_patterns.append(pattern)
        else:
            unmatched.append(plan_code)
    
    # Build the union branches for risk patterns
    union_branches = []
    
    for pattern in risk_patterns:
        if pattern['type'] == 'invalid_bvn':
            union_branches.append(f"""
        -- Invalid or missing BVN
        SELECT 
            rp.id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner rp
        WHERE (rp.bvn IS NULL OR rp.bvn LIKE '%%[a-zA-Z]%%' OR rp.bvn LIKE 'NOBVN%%')""")
        
        elif pattern['type'] == 'invalid_name':
            union_branches.append(f"""
        -- Invalid name format
        SELECT 
            rp.id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner rp
        WHERE (trim(rp.name) = '' OR trim(rp.name) ~ '^[^a-zA-Z0-9]')""")
        
        elif pattern['type'] == 'missing_contact':
            union_branches.append(f"""
        -- Missing contact information
        SELECT 
            rp.id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner rp
        WHERE rp.mobile IS NULL AND rp.phone IS NULL AND rp.customer_phone IS NULL""")
        
        elif pattern['type'] == 'sanction':
            union_branches.append(f"""
        -- Sanctions list
        SELECT 
            rp.id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner rp
        WHERE rp.likely_sanction = TRUE""")
        
        elif pattern['type'] == 'pep':
            union_branches.append(f"""
        -- PEP status
        SELECT 
            rp.id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner rp
        WHERE rp.is_pep = TRUE""")
            
        elif pattern['type'] == 'default_risk':
            union_branches.append(f"""
            -- Default risk rating
            SELECT 
                rp.id AS partner_id,
                '{pattern['code']}' AS risk_code,
                {pattern['score']} AS risk_score
            FROM res_partner rp
            CROSS JOIN (
                SELECT risk_rating 
                FROM res_risk_assessment
                WHERE is_default = TRUE
                LIMIT 1
            ) default_rating""")
        
        elif pattern['type'] == 'watchlist':
            union_branches.append(f"""
        -- Watchlist status
        SELECT 
            rp.id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner rp
        WHERE rp.is_watchlist = TRUE""")
    
    # Handle empty case
    if not union_branches:
        logger.warning(f"No independent risk patterns matched. Unmatched: {unmatched}")
        return {
            'name': view_name,
            'code': "-- Empty view, no patterns matched",
            'universe': 'Independent Risk Factors',
            'stats': f"No patterns matched. Unmatched: {len(unmatched)}",
            'is_partitioned': True
        }
    
    # Assemble the SQL for populating partitions
    all_flags_cte = ""
    if union_branches:
        all_flags_cte = union_branches[0]  # First branch without UNION ALL
        for branch in union_branches[1:]:
            all_flags_cte += f"\n    UNION ALL{branch}"
    
    # Create the SQL for populating partitioned tables with exclusive upper bound
    populate_sql = f"""
    -- SQL to populate partitioned table {view_name}
    INSERT INTO {view_name} (partner_id, partner_name, risk_data)
    WITH all_risk_flags AS (
    {all_flags_cte}
    )
    SELECT 
        rp.id AS partner_id,
        rp.name AS partner_name,
        COALESCE(
            (SELECT jsonb_object_agg(risk_code, risk_score)
            FROM all_risk_flags arf
            WHERE arf.partner_id = rp.id
            AND arf.risk_code IS NOT NULL
            AND arf.risk_score IS NOT NULL),
            '{{}}'::jsonb
        ) AS risk_data
    FROM res_partner rp
    WHERE rp.id >= %s AND rp.id < %s;  -- Note the < instead of <= for upper bound
    """
    
    logger.info(f"Built view SQL for independent risks with {len(union_branches)} pattern branches")
    
    return {
        'name': view_name,
        'code': populate_sql,
        'universe': 'Independent Risk Factors',
        'stats': f"Patterns matched: {len(union_branches)}, Unmatched: {len(unmatched)}",
        'is_partitioned': True
    }

def setup_partitioned_view(conn, view_name):
    """Create a partitioned table structure."""
    logger = logging.getLogger("__RISK_ASSESSMENT_MV__")
    
    try:
        with conn.cursor() as cursor:
            # Increase timeout and work memory
            cursor.execute("SET statement_timeout = '3600000';")  # 1 hour
            cursor.execute("SET maintenance_work_mem = '1GB';")
            cursor.execute("SET work_mem = '256MB';")
            
            # Check if object exists and its type
            cursor.execute("""
                SELECT c.relkind 
                FROM pg_class c 
                JOIN pg_namespace n ON n.oid = c.relnamespace 
                WHERE c.relname = %s 
                AND n.nspname = current_schema()
            """, (view_name,))
            
            result = cursor.fetchone()
            
            # Drop existing object properly based on its type
            if result:
                object_type = result[0]
                if object_type == 'm':  # materialized view
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
                else:  # table or other object
                    cursor.execute(f"DROP TABLE IF EXISTS {view_name} CASCADE;")
            else:
                # Object doesn't exist, try both just to be safe
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
                cursor.execute(f"DROP TABLE IF EXISTS {view_name} CASCADE;")
            
            # Create parent table with partitioning
            cursor.execute(f"""
            CREATE TABLE {view_name} (
                partner_id INTEGER PRIMARY KEY,
                partner_name VARCHAR,
                risk_data JSONB
            ) PARTITION BY RANGE (partner_id);
            """)
            
            # Get min/max partner IDs to determine partition ranges
            cursor.execute("SELECT MIN(id), MAX(id) FROM res_partner;")
            min_id, max_id = cursor.fetchone()
            
            if not min_id or not max_id:
                logger.warning(f"No partners found to create partitions for {view_name}")
                conn.rollback()
                return False
            
            # Calculate partition size to create roughly 10 partitions
            partition_size = max(1, (max_id - min_id + 1) // 10)
            
            # Create partitions with non-overlapping boundaries
            current_id = min_id
            partition_num = 1
            
            while current_id < max_id:
                next_id = min(current_id + partition_size, max_id + 1)
                partition_name = f"{view_name}_p{partition_num}"
                
                # Use exclusive upper bound (< next_id) for non-overlapping ranges
                cursor.execute(f"""
                CREATE TABLE {partition_name} PARTITION OF {view_name}
                FOR VALUES FROM ({current_id}) TO ({next_id});
                """)
                
                current_id = next_id
                partition_num += 1
            
            conn.commit()
            logger.info(f"✓ Created partitioned table {view_name} with {partition_num-1} partitions")
            return True
            
    except Exception as e:
        logger.error(f"✗ Failed to create partitioned table {view_name}: {str(e)}")
        conn.rollback()
        return False

def create_partition_indexes(conn, view_name):
    """Create necessary indexes on each partition."""
    logger = logging.getLogger("__RISK_ASSESSMENT_MV__")
    
    try:
        with conn.cursor() as cursor:
            # Set higher memory for index creation
            cursor.execute("SET maintenance_work_mem = '1GB';")
            
            # Get all partitions for this view
            cursor.execute(f"""
            SELECT inhrelid::regclass AS partition_name
            FROM pg_inherits
            WHERE inhparent = '{view_name}'::regclass;
            """)
            
            partitions = [row[0] for row in cursor.fetchall()]
            
            for partition in partitions:
                # Create index name without schema parts
                partition_str = str(partition).split('.')[-1]
                index_name = f"idx_{partition_str}_risk_data_gin"
                has_risks_name = f"idx_{partition_str}_has_risks"
                
                # Create GIN index on risk_data
                cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {index_name} 
                ON {partition} USING GIN (risk_data);
                """)
                conn.commit()
                
                # Create index for has_risks condition
                cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {has_risks_name} 
                ON {partition} (partner_id) 
                WHERE risk_data != '{{}}'::jsonb;
                """)
                conn.commit()
                
            logger.info(f"✓ Created indexes for all partitions of {view_name}")
            return True
    except Exception as e:
        logger.error(f"✗ Failed to create indexes for {view_name}: {str(e)}")
        conn.rollback()
        return False

def populate_partition_chunk(args):
    """Worker function to populate a partition chunk (to be used with multiprocessing)"""
    config_file, view_name, sql_code, chunk_start, chunk_end = args

    logger = logging.getLogger(f"__RISK_WORKER_{chunk_start}_{chunk_end}__")
    conn = None
    try:
        conn = get_db_connection(config_file)
        conn.autocommit = False

        with conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = '600000';")
            cursor.execute("SET work_mem = '512MB';")
            cursor.execute("SET synchronous_commit = 'off';")

            logger.info(f"Processing partners {chunk_start} to {chunk_end-1}")
            cursor.execute(sql_code, (chunk_start, chunk_end))
            conn.commit()

            logger.info(f"✓ Completed chunk {chunk_start}-{chunk_end-1}")
            return True

    except Exception as e:
        logger.error(f"Error processing chunk {chunk_start}-{chunk_end-1}: {str(e)}")
        if conn and not conn.closed:
            conn.rollback()
        return False
    finally:
        if conn and not conn.closed:
            conn.close()


def upsert_partition_chunk(args):
    """Worker function for incremental upsert of changed partners only."""
    config_file, view_name, sql_code, partner_ids_chunk = args

    logger = logging.getLogger(f"__RISK_UPSERT_WORKER__")
    conn = None
    try:
        conn = get_db_connection(config_file)
        conn.autocommit = False

        with conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = '600000';")
            cursor.execute("SET work_mem = '512MB';")
            cursor.execute("SET synchronous_commit = 'off';")

            # sql_code uses %s placeholder for the partner_ids tuple
            cursor.execute(sql_code, (tuple(partner_ids_chunk),))
            conn.commit()

            logger.info(f"✓ Upserted {len(partner_ids_chunk)} partners into {view_name}")
            return True

    except Exception as e:
        logger.error(f"Error upserting into {view_name}: {str(e)}")
        if conn and not conn.closed:
            conn.rollback()
        return False
    finally:
        if conn and not conn.closed:
            conn.close()

def _get_num_workers():
    """Use all available CPUs, leaving 2 for the OS and PostgreSQL."""
    return max(2, multiprocessing.cpu_count() - 2)


def _get_partition_ranges(conn, view_name):
    """Return list of (start_id, end_id) for all partitions of view_name."""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT pg_get_expr(child.relpartbound, child.oid) AS partition_bound
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
            WHERE parent.relname = %s
            ORDER BY child.relname;
        """, (view_name,))
        return cursor.fetchall()


def populate_partitioned_view(config_file, view_name, populate_sql):
    """
    Full population of a partitioned table using all available CPU workers.
    All partitions are processed in parallel — not sequentially.
    """
    logger = logging.getLogger("__RISK_ASSESSMENT_MV__")

    try:
        conn = get_db_connection(config_file)
        partitions = _get_partition_ranges(conn, view_name)
        conn.close()

        # Build ALL chunk args across ALL partitions at once
        batch_size = 150000
        all_chunks = []
        for (bounds,) in partitions:
            match = re.search(r"FROM\s*\((\d+)\)\s*TO\s*\((\d+)\)", bounds)
            if not match:
                continue
            start_id, end_id = int(match.group(1)), int(match.group(2))
            for chunk_start in range(start_id, end_id, batch_size):
                chunk_end = min(chunk_start + batch_size, end_id)
                all_chunks.append((config_file, view_name, populate_sql, chunk_start, chunk_end))

        if not all_chunks:
            logger.warning(f"No partition chunks found for {view_name}")
            return False

        num_workers = _get_num_workers()
        logger.info(f"Populating {view_name}: {len(all_chunks)} chunks across {num_workers} workers")

        # Process all partitions in parallel simultaneously
        with Pool(processes=num_workers) as pool:
            results = pool.map(populate_partition_chunk, all_chunks)

        failed = results.count(False)
        if failed:
            logger.warning(f"{failed}/{len(all_chunks)} chunks failed for {view_name}")

        logger.info(f"✓ Populated {view_name} ({len(all_chunks) - failed}/{len(all_chunks)} chunks ok)")
        return failed == 0

    except Exception as e:
        logger.error(f"✗ Failed to populate {view_name}: {str(e)}")
        return False


def incremental_update_view(config_file, view_name, populate_sql, last_refresh):
    """
    Incremental upsert: only recompute partners whose source data changed
    since last_refresh. New partners (not yet in the MV) are also included.

    Uses INSERT ... ON CONFLICT DO UPDATE so the table is never dropped.
    """
    logger = logging.getLogger("__RISK_ASSESSMENT_MV__")

    try:
        conn = get_db_connection(config_file)

        with conn.cursor() as cursor:
            # Partners whose profile changed since last refresh
            cursor.execute("""
                SELECT DISTINCT rp.id
                FROM res_partner rp
                WHERE rp.write_date > %s
            """, (last_refresh,))
            changed_partner_ids = {row[0] for row in cursor.fetchall()}

            # Accounts changed since last refresh (category, branch)
            cursor.execute("""
                SELECT DISTINCT customer_id
                FROM res_partner_account
                WHERE write_date > %s AND customer_id IS NOT NULL
            """, (last_refresh,))
            changed_partner_ids.update(row[0] for row in cursor.fetchall())

            # Channel subscriptions changed since last refresh
            cursor.execute("""
                SELECT DISTINCT partner_id
                FROM customer_channel_subscription
                WHERE last_updated > %s AND partner_id IS NOT NULL
            """, (last_refresh,))
            changed_partner_ids.update(row[0] for row in cursor.fetchall())

            # Partners not yet in this MV at all (new since last run)
            cursor.execute(f"""
                SELECT id FROM res_partner
                WHERE id NOT IN (SELECT partner_id FROM {view_name})
            """)
            new_partner_ids = {row[0] for row in cursor.fetchall()}

        conn.close()

        all_ids = list(changed_partner_ids | new_partner_ids)
        if not all_ids:
            logger.info(f"No changes detected for {view_name}, skipping.")
            return True

        logger.info(
            f"{view_name}: {len(changed_partner_ids)} changed + "
            f"{len(new_partner_ids)} new = {len(all_ids)} partners to recompute"
        )

        # Build an upsert version of the populate_sql:
        # Replace the range-based INSERT with an IN-list upsert
        upsert_sql = _build_upsert_sql(view_name, populate_sql)

        # Chunk the partner IDs and process in parallel
        chunk_size = 5000
        chunks = [all_ids[i:i + chunk_size] for i in range(0, len(all_ids), chunk_size)]

        num_workers = _get_num_workers()
        logger.info(f"Upserting {len(all_ids)} partners in {len(chunks)} chunks, {num_workers} workers")

        chunk_args = [(config_file, view_name, upsert_sql, chunk) for chunk in chunks]

        with Pool(processes=num_workers) as pool:
            results = pool.map(upsert_partition_chunk, chunk_args)

        failed = results.count(False)
        if failed:
            logger.warning(f"{failed}/{len(chunks)} upsert chunks failed for {view_name}")

        logger.info(f"✓ Incremental update done for {view_name}")
        return failed == 0

    except Exception as e:
        logger.error(f"✗ Incremental update failed for {view_name}: {str(e)}")
        return False


def _build_upsert_sql(view_name, populate_sql):
    """
    Convert the range-based INSERT SQL into an IN-list upsert SQL.

    Original pattern:
        INSERT INTO {view_name} (partner_id, partner_name, risk_data)
        WITH all_risk_flags AS ( ... )
        SELECT ... FROM res_partner rp
        WHERE rp.id >= %s AND rp.id < %s;

    Becomes:
        INSERT INTO {view_name} (partner_id, partner_name, risk_data)
        WITH all_risk_flags AS ( ... )
        SELECT ... FROM res_partner rp
        WHERE rp.id = ANY(%s)
        ON CONFLICT (partner_id) DO UPDATE
            SET partner_name = EXCLUDED.partner_name,
                risk_data     = EXCLUDED.risk_data;
    """
    # Replace the range WHERE clause with an IN list placeholder
    upsert_sql = re.sub(
        r"WHERE\s+rp\.id\s*>=\s*%s\s*AND\s+rp\.id\s*<\s*%s",
        "WHERE rp.id = ANY(%s)",
        populate_sql,
        flags=re.IGNORECASE,
    )
    # Strip trailing semicolon so we can append ON CONFLICT
    upsert_sql = upsert_sql.rstrip().rstrip(';')
    upsert_sql += """
    ON CONFLICT (partner_id) DO UPDATE
        SET partner_name = EXCLUDED.partner_name,
            risk_data    = EXCLUDED.risk_data;
    """
    return upsert_sql

def record_view_metadata(conn, view_data):
    """Record metadata about the view in the risk.analysis table."""
    logger = logging.getLogger("__RISK_ASSESSMENT_MV__")
    
    try:
        with conn.cursor() as cursor:
            # Delete existing record if present
            cursor.execute("""
            DELETE FROM risk_analysis WHERE name = %s
            """, (view_data['name'],))
            
            # Insert new record
            cursor.execute("""
            INSERT INTO risk_analysis 
            (name, code, universe, pattern_stats, last_refresh, create_date, write_date, create_uid, write_uid) 
            VALUES (%s, %s, %s, %s, NOW(), NOW(), NOW(), 1, 1)
            """, (
                view_data['name'], 
                view_data['code'], 
                view_data['universe'], 
                view_data.get('stats', '')
            ))
            conn.commit()
            
        logger.info(f"✓ Recorded metadata for {view_data['name']}")
        return True
    except Exception as e:
        logger.error(f"✗ Failed to record metadata for {view_data['name']}: {str(e)}")
        conn.rollback()
        return False

def refresh_risk_views(config_file=DEFAULT_CONFIG_FILE):
    """Main function to refresh all risk analysis views."""
    logger = setup_logging()
    logger.info("Starting risk analysis materialized view refresh")
    
    try:
        # Initial connection for setup and metadata
        conn = get_db_connection(config_file)
        
        # Get all risk universes
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT id, code, name FROM res_risk_universe WHERE active = true
            """)
            universes = cursor.fetchall()
        
        # Clean up existing risk.analysis records
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM risk_analysis")
            conn.commit()
            logger.info("Cleared existing risk analysis records")
        
        # Process each universe
        for universe_id, universe_code, universe_name in universes:
            logger.info(f"Processing risk universe: {universe_name}")
            
            # 1. Build the view SQL
            view_data = build_optimized_view(conn, universe_id, universe_code, universe_name)
            view_name = view_data['name']
            
            # 2. Setup the partitioned table
            if not setup_partitioned_view(conn, view_name):
                logger.error(f"Failed to create table structure for {view_name}, skipping")
                continue
            
            # 3. Populate the view data (using multiprocessing)
            if not populate_partitioned_view(config_file, view_name, view_data['code']):
                logger.error(f"Failed to populate data for {view_name}, skipping")
                continue
            
            # 4. Create indexes on partitions
            new_conn = get_db_connection(config_file)  # Create fresh connection
            if not create_partition_indexes(new_conn, view_name):
                logger.error(f"Failed to create indexes for {view_name}")
            
            # 5. Record metadata
            if not record_view_metadata(new_conn, view_data):
                logger.error(f"Failed to record metadata for {view_name}")
            
            new_conn.close()
            logger.info(f"✓ Completed view creation for {view_name}")
        
        # Process independent risk factors
        logger.info("Processing independent risk factors")
        view_data = build_independent_risk_view(conn)
        view_name = view_data['name']
        
        # Create and populate independent risk view
        if setup_partitioned_view(conn, view_name):
            if populate_partitioned_view(config_file, view_name, view_data['code']):
                # Create a fresh connection
                new_conn = get_db_connection(config_file)
                create_partition_indexes(new_conn, view_name)
                record_view_metadata(new_conn, view_data)
                new_conn.close()
                logger.info(f"✓ Completed independent risk view: {view_name}")
        
        conn.close()
        logger.info("Completed risk analysis view refresh")
        
    except Exception as e:
        logger.error(f"Error during risk view refresh: {str(e)}")
        if 'conn' in locals() and conn and not conn.closed:
            conn.close()

def optimize_database(config_file=DEFAULT_CONFIG_FILE):
    """Apply PostgreSQL optimizations for large operations."""
    logger = logging.getLogger("__RISK_ASSESSMENT_MV__")
    
    try:
        conn = get_db_connection(config_file)
        with conn.cursor() as cursor:
            # Increase work_mem for complex sorts and joins
            cursor.execute("SET work_mem = '256MB';")
            
            # Increase maintenance_work_mem for index creation
            cursor.execute("SET maintenance_work_mem = '1GB';")
            
            # Increase statement timeout to 2 hours
            cursor.execute("SET statement_timeout = '7200000';")
            
            # Disable synchronous_commit temporarily for speed
            cursor.execute("SET synchronous_commit = 'off';")
            
            # Use parallel workers
            cursor.execute("SET max_parallel_workers_per_gather = 4;")
            
            
            conn.commit()
            logger.info("✓ Database optimized for large operations")
        conn.close()
        return True
    except Exception as e:
        logger.error(f"✗ Failed to optimize database settings: {e}")
        if 'conn' in locals() and conn and not conn.closed:
            conn.close()
        return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Risk Analysis Materialized View Generator')
    parser.add_argument('--config', default=DEFAULT_CONFIG_FILE, help='Path to the database configuration file')
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging()
    logger.info("Starting Risk Analysis MV Generator")
    
    try:
        # 1. Optimize database settings
        logger.info("Optimizing database settings...")
        optimize_database(args.config)
        
        # 2. Refresh all risk views
        refresh_risk_views(args.config)
        
        
        
        logger.info("Risk Analysis MV Generator completed successfully")
    except Exception as e:
        logger.error(f"Fatal error in Risk Analysis MV Generator: {str(e)}")
        # Try to reset database parameters even if an error occurred
        

if __name__ == "__main__":
    import argparse
    main()