from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError, UserError
import pytz
import csv
import io
import base64
import time
import uuid
import logging
import re
import hashlib
import json
from collections import defaultdict

# Smart SQL Analysis
try:
    from sql_metadata import get_query_tables, get_query_columns
    SQL_METADATA_AVAILABLE = True
except ImportError:
    SQL_METADATA_AVAILABLE = False
    logging.warning("sql-metadata package not installed. Falling back to pattern matching.")

_logger = logging.getLogger(__name__)


class SmartSQLAnalyzer:
    """
    Intelligent SQL Query Analyzer for Alert Rules
    Uses sql_metadata with pattern matching fallback
    """
    
    def __init__(self):
        self.date_column_patterns = [
            'date_created', 'created_at', 'transaction_date', 'posting_date', 
            'value_date', 'entry_date', 'process_date', 'tran_date'
        ]
        
        self.date_keywords = ['date', 'time', 'created', 'updated', 'posted']
    
    def analyze_query(self, sql_query):
        """
        Main analysis method - tries sql_metadata first, falls back to patterns
        """
        try:
            if SQL_METADATA_AVAILABLE:
                return self._sql_metadata_analysis(sql_query)
            else:
                return self._pattern_analysis(sql_query)
        except Exception as e:
            _logger.warning(f"SQL analysis failed, using pattern fallback: {str(e)}")
            return self._pattern_analysis(sql_query)
    
    def _sql_metadata_analysis(self, sql_query):
        """
        Advanced analysis using sql_metadata package
        """
        try:
            # Extract tables and columns
            tables = get_query_tables(sql_query)
            columns = get_query_columns(sql_query)
            
            # Detect table aliases
            aliases = self._extract_table_aliases(sql_query, tables)
            
            # Find potential date columns
            date_columns = self._detect_date_columns(columns, sql_query)
            
            # Check if query already has date filtering
            has_date_filter = self._has_existing_date_filter(sql_query)
            
            # Determine main table and its alias
            main_table_info = self._get_main_table_info(sql_query, tables, aliases)
            
            return {
                'success': True,
                'method': 'sql_metadata',
                'tables': tables,
                'columns': columns,
                'aliases': aliases,
                'date_columns': date_columns,
                'has_date_filter': has_date_filter,
                'main_table': main_table_info['table'],
                'main_alias': main_table_info['alias'],
                'confidence': 'high'
            }
            
        except Exception as e:
            _logger.warning(f"sql_metadata analysis failed: {str(e)}")
            raise
    
    def _pattern_analysis(self, sql_query):
        """
        Fallback pattern matching analysis
        """
        try:
            # Extract tables and aliases using regex
            tables, aliases = self._extract_tables_and_aliases_pattern(sql_query)
            
            # Find date columns in SELECT clause
            date_columns = self._detect_date_columns_pattern(sql_query)
            
            # Check for existing date filters
            has_date_filter = self._has_existing_date_filter(sql_query)
            
            # Determine main table
            main_table = tables[0] if tables else None
            main_alias = aliases.get(main_table) if main_table else None
            
            return {
                'success': True,
                'method': 'pattern_matching',
                'tables': tables,
                'columns': [],  # Hard to extract reliably with patterns
                'aliases': aliases,
                'date_columns': date_columns,
                'has_date_filter': has_date_filter,
                'main_table': main_table,
                'main_alias': main_alias,
                'confidence': 'medium'
            }
            
        except Exception as e:
            _logger.error(f"Pattern analysis failed: {str(e)}")
            return {
                'success': False,
                'method': 'failed',
                'confidence': 'none',
                'error': str(e)
            }
    
    def _extract_table_aliases(self, sql_query, tables):
        """
        Extract table to alias mapping
        """
        aliases = {}
        
        # Pattern to match: FROM table_name alias
        pattern = r'FROM\s+(\w+)\s+(\w+)(?:\s|,|$)'
        matches = re.findall(pattern, sql_query, re.IGNORECASE)
        
        for table, alias in matches:
            if table in tables:
                aliases[table] = alias
        
        # Pattern to match: JOIN table_name alias
        join_pattern = r'JOIN\s+(\w+)\s+(\w+)(?:\s|,|$)'
        join_matches = re.findall(join_pattern, sql_query, re.IGNORECASE)
        
        for table, alias in join_matches:
            if table in tables:
                aliases[table] = alias
        
        return aliases
    
    def _detect_date_columns(self, columns, sql_query):
        """
        Intelligent date column detection
        """
        detected_dates = []
        
        # Priority-based detection
        for priority_col in self.date_column_patterns:
            if priority_col in columns:
                detected_dates.append(priority_col)
        
        # Keyword-based detection for remaining columns
        for col in columns:
            if col not in detected_dates:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in self.date_keywords):
                    detected_dates.append(col)
        
        # If no date columns found, look in SQL for common patterns
        if not detected_dates:
            date_pattern = r'\b(\w*date\w*|\w*time\w*|\w*created\w*)\b'
            matches = re.findall(date_pattern, sql_query, re.IGNORECASE)
            detected_dates.extend(list(set(matches)))
        
        return detected_dates
    
    def _has_existing_date_filter(self, sql_query):
        """
        Check if query already has date filtering
        """
        date_filter_patterns = [
            r'\bdate_created\s*>=',
            r'\bcreated_at\s*>=',
            r'\btransaction_date\s*>=',
            r'\bposting_date\s*>=',
            r'\bvalue_date\s*>=',
            r'\bwhere\s+.*date.*\s*>=',
            r'CURRENT_TIMESTAMP',
            r'CURRENT_DATE',
            r'INTERVAL',
            r'>=\s*[\'\"]\d{4}-\d{2}-\d{2}',
        ]
        
        for pattern in date_filter_patterns:
            if re.search(pattern, sql_query, re.IGNORECASE):
                return True
        
        return False
    
    def _get_main_table_info(self, sql_query, tables, aliases):
        """
        Determine the main table and its alias
        """
        if not tables:
            return {'table': None, 'alias': None}
        
        # The first table in FROM clause is usually the main table
        main_table = tables[0]
        main_alias = aliases.get(main_table)
        
        return {'table': main_table, 'alias': main_alias}
    
    def _extract_tables_and_aliases_pattern(self, sql_query):
        """
        Pattern-based table and alias extraction
        """
        tables = []
        aliases = {}
        
        # Match FROM table alias pattern
        from_pattern = r'FROM\s+(\w+)(?:\s+(\w+))?'
        from_match = re.search(from_pattern, sql_query, re.IGNORECASE)
        
        if from_match:
            table = from_match.group(1)
            alias = from_match.group(2)
            tables.append(table)
            if alias and alias.upper() not in ['WHERE', 'ORDER', 'GROUP', 'HAVING']:
                aliases[table] = alias
        
        # Match JOIN table alias patterns
        join_pattern = r'JOIN\s+(\w+)(?:\s+(\w+))?'
        join_matches = re.findall(join_pattern, sql_query, re.IGNORECASE)
        
        for table, alias in join_matches:
            if table not in tables:
                tables.append(table)
            if alias and alias.upper() not in ['ON', 'WHERE', 'ORDER', 'GROUP']:
                aliases[table] = alias
        
        return tables, aliases
    
    def _detect_date_columns_pattern(self, sql_query):
        """
        Pattern-based date column detection from SELECT clause
        """
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return []
        
        select_clause = select_match.group(1)
        
        # Find date-related columns
        date_columns = []
        for pattern in self.date_column_patterns:
            if pattern in select_clause.lower():
                date_columns.append(pattern)
        
        # Find columns with date keywords
        words = re.findall(r'\b\w+\b', select_clause)
        for word in words:
            if any(keyword in word.lower() for keyword in self.date_keywords):
                if word not in date_columns:
                    date_columns.append(word)
        
        return date_columns


class SmartTimeFilterEngine:
    """
    Intelligent time filter application
    """
    
    def __init__(self, analyzer_result):
        self.analysis = analyzer_result
    
    def add_smart_time_filter(self, query, rule):
        """
        Add intelligent time filter based on analysis
        """
        if not self.analysis['success']:
            return self._fallback_filter(query, rule)
        
        # Skip if query already has date filtering
        if self.analysis['has_date_filter']:
            _logger.info(f"Rule {rule.id} - Query already has date filter, skipping")
            return query
        
        # Generate appropriate filter
        time_filter = self._generate_time_filter(rule)
        if not time_filter:
            return query
        
        # Apply filter intelligently
        return self._apply_filter_intelligently(query, time_filter)
    
    def _generate_time_filter(self, rule):
        """
        Generate time filter based on rule configuration - FIXED to use configurable hours
        """
        try:
            # Determine date column to use
            date_column = self._select_best_date_column(rule)
            if not date_column:
                return None
            
            # Generate filter based on initialization status
            if not rule.initialization_complete:
                return self._get_initialization_filter(rule, date_column)
            else:
                # FIXED: Use configurable hours instead of hardcoded 72
                hours = rule.processing_window_hours or 72
                return f"{date_column} >= (CURRENT_TIMESTAMP - INTERVAL '{hours} hours')"
        
        except Exception as e:
            _logger.error(f"Error generating time filter: {str(e)}")
            return None
    
    def _select_best_date_column(self, rule):
        """
        Select the best date column to use for filtering
        """
        # Priority 1: User configured column
        if hasattr(rule, 'query_date_column') and rule.query_date_column:
            configured_col = rule.query_date_column
            if configured_col != "a.date_created":  # Not the default
                return configured_col
        
        # Priority 2: Detected date columns
        date_columns = self.analysis.get('date_columns', [])
        if date_columns:
            # Use first priority date column
            for priority_col in ['date_created', 'created_at', 'transaction_date']:
                if priority_col in date_columns:
                    return self._format_column_with_alias(priority_col)
            
            # Use first detected date column
            return self._format_column_with_alias(date_columns[0])
        
        # Priority 3: Default with smart alias
        return self._format_column_with_alias('date_created')
    
    def _format_column_with_alias(self, column):
        """
        Format column with appropriate alias
        """
        main_alias = self.analysis.get('main_alias')
        if main_alias:
            return f"{main_alias}.{column}"
        else:
            return column
    
    def _get_initialization_filter(self, rule, date_column):
        """
        Get initialization filter based on strategy
        """
        try:
            if rule.initialization_strategy == 'current_year':
                return f"{date_column} >= '2025-01-01'"
            elif rule.initialization_strategy == 'full_history':
                return "1=1"
            elif rule.initialization_strategy == 'last_6_months':
                return f"{date_column} >= (CURRENT_DATE - INTERVAL '6 months')"
            elif rule.initialization_strategy == 'last_12_months':
                return f"{date_column} >= (CURRENT_DATE - INTERVAL '12 months')"
            elif rule.initialization_strategy == 'custom_period':
                months = rule.custom_initialization_months or 6
                return f"{date_column} >= (CURRENT_DATE - INTERVAL '{months} months')"
            else:
                return f"{date_column} >= '2025-01-01'"
        except Exception as e:
            _logger.error(f"Error getting initialization filter: {str(e)}")
            return f"{date_column} >= '2025-01-01'"
    
    def _apply_filter_intelligently(self, query, time_filter):
        """
        Apply filter to query intelligently
        """
        try:
            # Handle ORDER BY clause properly
            query_upper = query.upper()
            order_by_pos = query_upper.rfind('ORDER BY')
            
            if order_by_pos != -1:
                main_query = query[:order_by_pos].strip()
                order_by_clause = query[order_by_pos:].strip()
                
                if "WHERE" in main_query.upper():
                    main_query += f" AND {time_filter}"
                else:
                    main_query += f" WHERE {time_filter}"
                
                return f"{main_query} {order_by_clause}"
            else:
                if "WHERE" in query_upper:
                    return f"{query} AND {time_filter}"
                else:
                    return f"{query} WHERE {time_filter}"
        
        except Exception as e:
            _logger.error(f"Error applying filter: {str(e)}")
            return query
    
    def _fallback_filter(self, query, rule):
        """
        Fallback filter when analysis fails
        """
        # Use configurable hours instead of hardcoded 72
        hours = rule.processing_window_hours or 72
        time_filter = f"date_created >= (CURRENT_TIMESTAMP - INTERVAL '{hours} hours')"
        
        if "WHERE" in query.upper():
            return f"{query} AND {time_filter}"
        else:
            return f"{query} WHERE {time_filter}"


class AlertSignature(models.Model):
    """
    OPTIMIZED: Database table for storing alert signatures
    Designed for date-filtered queries (4-5 months of data)
    """
    _name = 'alert.signature'
    _description = 'Alert Rule Signatures for Duplicate Detection'
    _order = 'created_date desc'
    _rec_name = 'signature_hash'
    
    alert_rule_id = fields.Many2one(
        'alert.rules', 
        string='Alert Rule', 
        required=True, 
        ondelete='cascade',
        index=True
    )
    signature_hash = fields.Char(
        string='Signature Hash', 
        size=32, 
        required=True, 
        index=True,
        help="sha256 hash of the record data"
    )
    created_date = fields.Datetime(
        string='Created Date', 
        default=fields.Datetime.now, 
        required=True,
        index=True
    )
    last_seen_date = fields.Datetime(
        string='Last Seen Date',
        default=fields.Datetime.now,
        help="When this signature was last encountered"
    )
    
    #fields for date-filtered queries
    record_date = fields.Date(
        string='Record Date',
        help="Date from the actual record (for cleanup purposes)",
        index=True
    )
    
    _sql_constraints = [
        ('unique_signature_per_rule', 
         'unique(alert_rule_id, signature_hash)', 
         'Signature must be unique per alert rule')
    ]
    
    @api.model
    def cleanup_old_signatures_by_date(self, rule_id, older_than_months=6):
        """
        OPTIMIZED CLEANUP: Remove signatures for records older than X months
        Safe for date-filtered queries since old records won't appear again
        """
        cutoff_date = fields.Date.today() - relativedelta(months=older_than_months)
        
        old_signatures = self.search([
            ('alert_rule_id', '=', rule_id),
            ('record_date', '<', cutoff_date)
        ])
        
        return {
            'count': len(old_signatures),
            'cutoff_date': cutoff_date,
            'signatures': old_signatures
        }
    
    @api.model
    def get_signature_stats_optimized(self):
        """Optimized signature statistics"""
        query = """
            SELECT 
                ar.name,
                ar.id,
                COUNT(asig.id) as signature_count,
                MIN(asig.record_date) as oldest_record_date,
                MAX(asig.record_date) as newest_record_date,
                MIN(asig.created_date) as oldest_signature,
                MAX(asig.created_date) as newest_signature
            FROM alert_rules ar
            LEFT JOIN alert_signature asig ON ar.id = asig.alert_rule_id
            WHERE ar.status = '1'
            GROUP BY ar.id, ar.name
            ORDER BY signature_count DESC
        """
        
        self.env.cr.execute(query)
        results = self.env.cr.dictfetchall()
        
        stats = {}
        for row in results:
            stats[row['name']] = {
                'rule_id': row['id'],
                'signature_count': row['signature_count'] or 0,
                'oldest_record_date': row['oldest_record_date'],
                'newest_record_date': row['newest_record_date'],
                'oldest_signature': row['oldest_signature'],
                'newest_signature': row['newest_signature']
            }
        
        return stats


class AlertRules(models.Model):
    _name = 'alert.rules'
    _description = "Alert Rules - Smart Banking Initialization System"
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'id desc'
    
    name = fields.Char(string="Name", required=True, tracking=True)
    narration = fields.Html(string="Narration", required=True, tracking=True)
    sql_text = fields.Many2one("process.sql", string="SQL Query", required=True, tracking=True)
    frequency_id = fields.Many2one('exception.frequency', string="Frequency", required=True, tracking=True)
    
    status = fields.Selection([
        ("1", "Active"), 
        ("0", "Inactive")
    ], default="1", string="Alert Status", tracking=True)
    
    specific_email_recipients = fields.Many2many(
        'res.users', "alert_rules_email_rel", "alert_rules_id", "user_id", 
        string="Specific Recipients", required=True, tracking=True
    )
    
    alert_id = fields.Many2one("alert.group", string="Alert Group")
    first_owner = fields.Many2one("res.users", string="First Line Owner") 
    second_owner = fields.Many2one("res.users", string="Second Line Owner") 
    process_id = fields.Char(string="Process", tracking=True)
    
    risk_rating = fields.Selection([
        ("low", "Low"),
        ("medium", "Medium"), 
        ("high", "High")
    ], default="low", string="Risk Rating")
    
    date_created = fields.Datetime(string="Created Date", readonly=True)
    last_checked = fields.Datetime(string="Last Checked", readonly=True)
    
    # ========================================
    # TRACKING FIELDS
    # ========================================
    last_email_count = fields.Integer(string="Last Email Count", readonly=True, default=0)
    total_emails_sent = fields.Integer(string="Total Emails Sent", readonly=True, default=0)
    last_error = fields.Text(string="Last Error", readonly=True)
    is_processing = fields.Boolean(string="Is Processing", default=False, readonly=True)
    next_scheduled_run = fields.Datetime(string="Next Scheduled Run", readonly=True)
    processing_duration = fields.Float(string="Last Processing Duration (seconds)", readonly=True)
    
    # signature tracking
    last_record_count = fields.Integer(string="Last Record Count", readonly=True, default=0)
    last_new_record_count = fields.Integer(string="Last New Record Count", readonly=True, default=0)
    total_signatures_stored = fields.Integer(string="Total Signatures Stored", readonly=True, default=0)
    
    # Smart SQL fields
    query_date_column = fields.Char(
        string="Date Column in Query",
        help="Column name used for date filtering. Leave blank for auto-detection.",
        default=""
    )
    date_filter_months = fields.Integer(
        string="Date Filter (Months)",
        help="Number of months to look back for records",
        default=5
    )

    # ========================================
    # INITIALIZATION FIELDS
    # ========================================
    initialization_strategy = fields.Selection([
        ('current_year', 'Current Year Only (2025)'),
        ('full_history', 'Full Historical Data'),  
        ('last_6_months', 'Last 6 Months'),
        ('last_12_months', 'Last 12 Months'),
        ('custom_period', 'Custom Period')
    ], default='current_year', required=True, string="Initialization Strategy",
       help="How much historical data to process on first run")
    
    custom_initialization_months = fields.Integer(
        string="Custom Period (Months)",
        default=6,
        help="Only used if 'Custom Period' is selected"
    )
    
    processing_window_hours = fields.Integer(
        string="Processing Window (Hours)",
        default=72,
        help="Hours to look back for ongoing processing after initialization"
    )
    
    initialization_complete = fields.Boolean(
        string="Initialization Complete", 
        default=False, 
        readonly=True,
        help="Whether this rule has completed its first initialization run"
    )

    # ========================================
    # LIFECYCLE METHODS
    # ========================================
    @api.model
    def create(self, vals_list):
        """Enhanced create method with smart initialization"""
        current_time = fields.Datetime.now()
        vals_list.update({
            'last_checked': current_time,
            'date_created': vals_list.get('date_created', current_time),
            'total_emails_sent': 0,
            'last_email_count': 0,
            'is_processing': False,
            'last_record_count': 0,
            'last_new_record_count': 0,
            'total_signatures_stored': 0,
            'initialization_complete': False  # Always start with initialization
        })
        
        # Calculate initial next run time
        if vals_list.get('frequency_id'):
            frequency = self.env['exception.frequency'].browse(vals_list['frequency_id'])
            next_run = self._calculate_next_check_time(current_time, frequency.name, frequency.period)
            vals_list['next_scheduled_run'] = next_run
        
        return super(AlertRules, self).create(vals_list)

    @api.onchange('sql_text')
    def onchange_sql_text(self):
        """Improved validation to prevent duplicate SQL queries"""
        if self.sql_text:
            existing = self.search([
                ("sql_text", "=", self.sql_text.id),
                ("id", "!=", self.id if self.id else 0)
            ])
            if existing:
                raise ValidationError(
                    f"Alert rule for SQL query '{self.sql_text.name}' already exists!\n"
                    f"Existing rule: '{existing[0].name}' (ID: {existing[0].id})\n"
                    f"Please use a different SQL query or modify the existing rule."
                )

    # ========================================
    # MAIN PROCESSING METHODS
    # ========================================
    @api.model
    def process_alert_rules(self):
        """MAIN ENTRY POINT: Process all active alert rules with smart initialization"""
        start_time = time.time()
        _logger.info("Starting smart alert processing cycle")
        
        try:
            # Get all active rules that are not currently being processed
            alert_rules = self.search([
                ("status", "=", "1"),
                ("is_processing", "=", False)
            ])
            
            total_rules = len(alert_rules)
            
            if not alert_rules:
                _logger.info("No active alert rules found")
                return
            
            processed_count = 0
            sent_count = 0
            error_count = 0
            total_signatures_created = 0
            initialization_count = 0
            
            # Process each rule individually with full isolation
            for rule in alert_rules:
                try:
                    # Check if it's actually time to process this rule
                    if not self._is_time_to_process(rule):
                        continue
                    
                    # Track initialization vs normal processing
                    if not rule.initialization_complete:
                        initialization_count += 1
                    
                    # Process the rule safely
                    result = self._process_single_rule_safely(rule)
                    emails_sent = result['emails_sent']
                    signatures_created = result['signatures_created']
                    
                    processed_count += 1
                    sent_count += emails_sent
                    total_signatures_created += signatures_created
                    
                except Exception as e:
                    error_count += 1
                    _logger.error(f"Error processing rule {rule.id} '{rule.name}': {str(e)}")
                    
                    # Update rule with error info and unlock it
                    try:
                        rule.write({
                            'last_error': f"Error at {fields.Datetime.now()}: {str(e)}",
                            'is_processing': False
                        })
                    except:
                        _logger.error(f"Failed to update error status for rule {rule.id}")
                    continue
            
            duration = time.time() - start_time
            _logger.info(f"Smart alert cycle complete: {processed_count}/{total_rules} processed, {sent_count} emails, {error_count} errors in {duration:.1f}s")
            
        except Exception as e:
            _logger.error(f"CRITICAL ERROR in alert processing cycle: {str(e)}")
            raise

    def _is_time_to_process(self, rule):
        """Optimized timing logic"""
        current_time = fields.Datetime.now()
        
        if not rule.next_scheduled_run:
            if not rule.last_checked:
                return True
            else:
                next_run = self._calculate_next_check_time(
                    rule.last_checked, 
                    rule.frequency_id.name, 
                    rule.frequency_id.period
                )
                rule.write({'next_scheduled_run': next_run})
        
        return current_time >= rule.next_scheduled_run

    def _calculate_next_check_time(self, last_checked, unit, period):
        """Calculate next check time based on frequency"""
        try:
            if unit == 'minutes':
                return last_checked + timedelta(minutes=period)
            elif unit == 'hourly':
                return last_checked + timedelta(hours=period)
            elif unit == 'daily':
                return last_checked + timedelta(days=period)
            elif unit == 'weekly':
                return last_checked + timedelta(weeks=period)
            elif unit == 'monthly':
                return last_checked + relativedelta(months=period)
            elif unit == 'yearly':
                return last_checked + relativedelta(years=period)
            else:
                _logger.error(f"Unsupported frequency unit: {unit}")
                return last_checked + timedelta(hours=1)
        except Exception as e:
            _logger.error(f"Error calculating next check time: {str(e)}")
            return last_checked + timedelta(hours=1)

    def _process_single_rule_safely(self, rule):
        """Process a single rule with comprehensive safety measures"""
        start_time = time.time()
        
        try:
            # Lock the rule for processing
            rule.write({
                'is_processing': True,
                'last_error': False
            })
            
            # Execute the main alert logic
            result = self._execute_smart_alert_logic(rule)
            
            # Mark initialization as complete if this was first run
            if not rule.initialization_complete:
                rule.write({'initialization_complete': True})
            
            # Calculate next run time
            current_time = fields.Datetime.now()
            next_run = self._calculate_next_check_time(
                current_time, 
                rule.frequency_id.name, 
                rule.frequency_id.period
            )
            
            processing_duration = time.time() - start_time
            
            # Update all rule fields atomically
            rule.write({
                'last_checked': current_time,
                'next_scheduled_run': next_run,
                'is_processing': False,
                'last_email_count': result['emails_sent'],
                'total_emails_sent': rule.total_emails_sent + result['emails_sent'],
                'processing_duration': processing_duration,
                'last_error': False
            })
            
            return result
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} processing failed: {str(e)}")
            
            try:
                rule.write({
                    'is_processing': False,
                    'last_error': f"Error at {fields.Datetime.now()}: {str(e)}"
                })
            except:
                _logger.error(f"Failed to unlock rule {rule.id} after error")
            
            return {'emails_sent': 0, 'signatures_created': 0}

    def _execute_smart_alert_logic(self, rule):
        """SMART: Main alert execution logic with intelligent SQL analysis"""
        try:
            # Get the smart query with intelligent analysis
            query = self._get_smart_banking_query(rule)
            if not query:
                _logger.warning(f"Rule {rule.id} - Empty or invalid query")
                return {'emails_sent': 0, 'signatures_created': 0}
            
            # Execute query with proper error handling
            try:
                with self.env.cr.savepoint():
                    self.env.cr.execute(query)
                    rows = self.env.cr.fetchall()
                    columns = [desc[0] for desc in self.env.cr.description]
            except Exception as e:
                _logger.error(f"Rule {rule.id} SQL execution error: {str(e)}")
                return {'emails_sent': 0, 'signatures_created': 0}
            
            total_records = len(rows)
            
            # Update rule with record count
            rule.write({'last_record_count': total_records})
            
            # Early return if no records
            if not rows:
                return {'emails_sent': 0, 'signatures_created': 0}
            
            # Get only truly new records
            new_records_result = self._get_new_records_optimized(rule, rows, columns)
            new_records = new_records_result['new_records']
            signatures_created = new_records_result['signatures_created']
            
            new_record_count = len(new_records)
            
            # Update rule with new record count and signature count
            rule.write({
                'last_new_record_count': new_record_count,
                'total_signatures_stored': rule.total_signatures_stored + signatures_created
            })
            
            if not new_records:
                return {'emails_sent': 0, 'signatures_created': signatures_created}
            
            # Send alerts for new records with smart routing
            emails_sent = self._smart_route_alert_by_type(rule, new_records, columns)
            
            return {'emails_sent': emails_sent, 'signatures_created': signatures_created}
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} execution error: {str(e)}")
            return {'emails_sent': 0, 'signatures_created': 0}

    # ========================================
    # SMART QUERY METHODS
    # ========================================
    def _get_smart_banking_query(self, rule):
        """
        SMART BANKING QUERY: Intelligent SQL analysis and time filter application
        """
        try:
            if not rule.sql_text or not rule.sql_text.query:
                return ""
            
            query = rule.sql_text.query.strip()
            
            # Basic validation
            if not query.upper().startswith('SELECT'):
                _logger.warning(f"Rule {rule.id} - Query doesn't start with SELECT")
                return ""
            
            # Remove trailing semicolon if present
            if query.endswith(";"):
                query = query[:-1]
            
            # SMART ANALYSIS
            analyzer = SmartSQLAnalyzer()
            analysis_result = analyzer.analyze_query(query)
            
            _logger.info(f"Rule {rule.id} - SQL analysis: {analysis_result['method']} (confidence: {analysis_result['confidence']})")
            
            # SMART TIME FILTER APPLICATION
            filter_engine = SmartTimeFilterEngine(analysis_result)
            enhanced_query = filter_engine.add_smart_time_filter(query, rule)
            
            # Add ORDER BY if not present
            if "ORDER BY" not in enhanced_query.upper():
                try:
                    main_alias = analysis_result.get('main_alias')
                    if main_alias:
                        enhanced_query += f" ORDER BY {main_alias}.id DESC"
                    elif analysis_result.get('main_table'):
                        enhanced_query += f" ORDER BY id DESC"
                except Exception:
                    pass
            
            return enhanced_query
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} smart query generation error: {str(e)}")
            # Fallback to basic logic
            return self._fallback_query_logic(rule)
    
    def _fallback_query_logic(self, rule):
        """
        Fallback to original logic when smart analysis fails
        """
        try:
            query = rule.sql_text.query.strip()
            
            if query.endswith(";"):
                query = query[:-1]
            
            # Simple time filter with configurable hours
            if not rule.initialization_complete:
                time_filter = "date_created >= '2025-01-01'"
            else:
                hours = rule.processing_window_hours or 72
                time_filter = f"date_created >= (CURRENT_TIMESTAMP - INTERVAL '{hours} hours')"
            
            # Add filter
            if "WHERE" in query.upper():
                query += f" AND {time_filter}"
            else:
                query += f" WHERE {time_filter}"
            
            return query
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} fallback query error: {str(e)}")
            return ""

    # ========================================
    # SIGNATURE DETECTION METHODS (PRESERVED)
    # ========================================
    def _get_new_records_optimized(self, rule, current_rows, columns):
        """Get new records with enhanced signature management"""
        if not current_rows:
            return {'new_records': [], 'signatures_created': 0}
        
        try:
            # Generate signatures for current records
            current_signatures_data = self._generate_signatures_with_dates(current_rows, columns)
            current_signatures = set(sig['hash'] for sig in current_signatures_data)
            
            # Get existing signatures from database
            existing_signatures = self._get_existing_signatures_optimized(rule)
            
            # Find new signatures
            new_signatures = current_signatures - existing_signatures
            
            if not new_signatures:
                # Update last_seen_date for existing signatures
                self._update_existing_signatures_last_seen(rule, existing_signatures)
                return {'new_records': [], 'signatures_created': 0}
            
            # Get new records and their data
            new_records = []
            new_signature_data = []
            
            for i, row in enumerate(current_rows):
                sig_data = current_signatures_data[i]
                if sig_data['hash'] in new_signatures:
                    new_records.append(row)
                    new_signature_data.append(sig_data)
            
            # Store new signatures in database
            signatures_created = self._store_new_signatures_optimized(rule, new_signature_data)
            
            # Update existing signatures
            self._update_existing_signatures_last_seen(rule, existing_signatures)
            
            return {'new_records': new_records, 'signatures_created': signatures_created}
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} signature detection error: {str(e)}")
            return {'new_records': [], 'signatures_created': 0}

    def _generate_signatures_with_dates(self, rows, columns):
        """Generate signatures with associated record dates"""
        signatures_data = []
        
        # Try to find date column in results
        date_column_index = self._find_date_column_index(columns)
        
        for row in rows:
            try:
                # Generate signature
                signature_hash = self._generate_single_record_signature(row)
                if not signature_hash:
                    continue
                
                # Extract record date
                record_date = None
                if date_column_index is not None:
                    try:
                        date_value = row[date_column_index]
                        if isinstance(date_value, str):
                            record_date = datetime.strptime(date_value, '%Y-%m-%d').date()
                        elif isinstance(date_value, datetime):
                            record_date = date_value.date()
                        elif hasattr(date_value, 'date'):
                            record_date = date_value.date()
                    except:
                        record_date = fields.Date.today()
                else:
                    record_date = fields.Date.today()
                
                signatures_data.append({
                    'hash': signature_hash,
                    'record_date': record_date
                })
                
            except Exception:
                continue
        
        return signatures_data

    def _find_date_column_index(self, columns):
        """Find date column index in query results"""
        date_patterns = ['date_created', 'created_at', 'date', 'created']
        
        for i, column in enumerate(columns):
            if any(pattern in column.lower() for pattern in date_patterns):
                return i
        return None

    def _generate_single_record_signature(self, row):
        """Generate sha256 signature for a single record"""
        try:
            # Normalize the row data
            normalized_row = []
            for cell in row:
                if cell is None:
                    normalized_row.append("<NULL>")
                elif isinstance(cell, (str, int, float, bool)):
                    normalized_row.append(str(cell))
                elif isinstance(cell, datetime):
                    normalized_row.append(cell.isoformat())
                else:
                    normalized_row.append(str(cell))
            
            # Create JSON string and hash
            normalized_tuple = tuple(normalized_row)
            record_string = json.dumps(normalized_tuple, sort_keys=True, ensure_ascii=True)
            
            return hashlib.sha256(record_string.encode('utf-8')).hexdigest()
            
        except Exception:
            return None

    def _get_existing_signatures_optimized(self, rule):
        """Get existing signatures from database"""
        try:
            existing_signatures = self.env['alert.signature'].search([
                ('alert_rule_id', '=', rule.id)
            ])
            
            return set(sig.signature_hash for sig in existing_signatures)
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} - Error getting existing signatures: {str(e)}")
            return set()

    def _store_new_signatures_optimized(self, rule, signature_data):
        """Store new signatures with deduplication and error handling"""
        try:
            if not signature_data:
                return 0
            
            current_time = fields.Datetime.now()
            
            # Remove duplicates within the same batch
            unique_signatures = {}
            for sig_data in signature_data:
                # Use signature hash as key to automatically deduplicate
                unique_signatures[sig_data['hash']] = sig_data
            
            # Batch insert with duplicate handling
            batch_size = 1000  # Process in smaller batches
            total_created = 0
            
            unique_sig_list = list(unique_signatures.values())
            
            for i in range(0, len(unique_sig_list), batch_size):
                batch = unique_sig_list[i:i + batch_size]
                
                try:
                    # Prepare batch data
                    create_data = []
                    for sig_data in batch:
                        create_data.append({
                            'alert_rule_id': rule.id,
                            'signature_hash': sig_data['hash'],
                            'record_date': sig_data['record_date'],
                            'created_date': current_time,
                            'last_seen_date': current_time
                        })
                    
                    # Use individual inserts for safety with constraint handling
                    for record_data in create_data:
                        try:
                            self.env['alert.signature'].create(record_data)
                            total_created += 1
                        except Exception as e:
                            if 'duplicate key value violates unique constraint' in str(e):
                                # Signature already exists, skip it
                                continue
                            else:
                                # Some other error, log and continue
                                _logger.warning(f"Rule {rule.id} - Error creating signature: {str(e)}")
                                continue
                    
                except Exception as e:
                    _logger.error(f"Rule {rule.id} - Error in signature batch: {str(e)}")
                    continue
            
            return total_created
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} - Failed to store signatures: {str(e)}")
            return 0

    def _update_existing_signatures_last_seen(self, rule, existing_signatures):
        """Update last_seen_date for existing signatures"""
        try:
            if not existing_signatures:
                return
            
            # Find signature records to update
            signature_records = self.env['alert.signature'].search([
                ('alert_rule_id', '=', rule.id),
                ('signature_hash', 'in', list(existing_signatures))
            ])
            
            if signature_records:
                signature_records.write({
                    'last_seen_date': fields.Datetime.now()
                })
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} - Error updating last_seen_date: {str(e)}")

    # ========================================
    # EMAIL TEMPLATE CONFIGURATION METHODS
    # ========================================
    def _get_template_config_for_email(self):
        """Get active template configuration for email generation - FIXED VERSION"""
        try:
            template_config = self.env['email.template.config'].get_active_template()
            if template_config:
                # FIXED: Proper logo handling
                logo_data = None
                if template_config.effective_logo:
                    # Convert logo to proper format for email embedding
                    logo_data = template_config.effective_logo
                
                return {
                    'config': template_config,
                    'button_color': template_config.button_bg_color or '#28a745',
                    'button_text_color': template_config.button_text_color or '#ffffff',
                    'button_radius': template_config.button_border_radius or 8,
                    'primary_color': template_config.primary_brand_color or '#007046',
                    'font_family': dict(template_config._fields['font_family'].selection)[template_config.font_family] if template_config.font_family else 'Arial, sans-serif',
                    'email_width': template_config.email_width or 590,
                    'content_padding': template_config.content_padding or 16,
                    'logo_data': logo_data,
                    'logo_width': template_config.logo_width or 192,
                    'logo_height': template_config.logo_height or 192,
                    'show_footer': template_config.show_footer,
                    'footer_bg_color': template_config.footer_bg_color or '#ffffff',
                    'footer_text_color': template_config.footer_text_color or '#454748',
                    'company_name': template_config.effective_company_name,
                    'has_config': True
                }
            else:
                return {
                    'config': None,
                    'button_color': '#28a745',
                    'button_text_color': '#ffffff', 
                    'button_radius': 8,
                    'primary_color': '#007046',
                    'font_family': 'Arial, sans-serif',
                    'email_width': 590,
                    'content_padding': 16,
                    'logo_data': None,
                    'show_footer': True,
                    'has_config': False
                }
        except Exception as e:
            _logger.error(f"Error getting template config: {str(e)}")
            return {
                'config': None,
                'has_config': False,
                'email_width': 590,
            }

    
    def _generate_html_table_with_template_config(self, columns, rows):
        """Generate PROPERLY RESPONSIVE HTML table - FIXED VERSION"""
        try:
            # Get active template configuration
            template_config = self.env['email.template.config'].get_active_template()
            
            if template_config:
                # Use template config colors
                header_bg = template_config.table_header_bg_color or '#007046'
                header_text = template_config.table_header_text_color or '#ffffff'
                border_color = template_config.table_border_color or '#dddddd'
                even_row_color = template_config.table_row_even_color or '#f9f9f9'
                odd_row_color = template_config.table_row_odd_color or '#ffffff'
                font_family = dict(template_config._fields['font_family'].selection)[template_config.font_family] if template_config.font_family else 'Arial, sans-serif'
            else:
                # Fallback to default colors
                header_bg = '#007046'
                header_text = '#ffffff'
                border_color = '#dddddd'
                even_row_color = '#f9f9f9'
                odd_row_color = '#ffffff'
                font_family = 'Arial, sans-serif'
            
            # Filter out branch_id columns
            pattern = re.compile(r'\bbranch\s*_?\s*id\b', re.IGNORECASE)
            branch_id_indices = [i for i, col in enumerate(columns) if pattern.fullmatch(col)]
            
            # Generate headers
            header_html = ""
            for i, col in enumerate(columns):
                if i not in branch_id_indices:
                    header_html += f"""<th style='padding: 8px; background-color: {header_bg}; color: {header_text}; border: 1px solid {border_color}; font-family: {font_family}; font-size: 14px; text-align: left;'>{' '.join(col.split('_')).title()}</th>"""
            
            # Generate rows (limit for email)
            max_rows = 20
            rows_html = ""
            
            for row_index, row in enumerate(rows[:max_rows]):
                bg_color = even_row_color if row_index % 2 == 0 else odd_row_color
                rows_html += "<tr>"
                for i, cell in enumerate(row):
                    if i not in branch_id_indices:
                        formatted_cell = self._format_cell_for_html(cell)
                        rows_html += f"""<td style='padding: 8px; border: 1px solid {border_color}; background-color: {bg_color}; font-family: {font_family}; font-size: 14px;'>{formatted_cell}</td>"""
                rows_html += "</tr>"
            
            # Truncation message
            if len(rows) > max_rows:
                colspan = len([col for i, col in enumerate(columns) if i not in branch_id_indices])
                rows_html += f"""
                <tr>
                    <td colspan='{colspan}' style='padding: 8px; font-style: italic; text-align: center; background-color: #fffacd; border: 1px solid {border_color}; font-family: {font_family}; font-size: 12px;'>
                        ... and {len(rows) - max_rows} more record(s). See attached CSV for complete data.
                    </td>
                </tr>
                """
            
            # FIXED: PROPERLY RESPONSIVE TABLE
            # Takes full width first, only scrolls when actually needed
            return f"""
            <div style="width: 100%; margin: 10px 0; overflow-x: auto;">
                <table style="
                    border-collapse: collapse; 
                    font-family: {font_family}; 
                    width: 100%;
                    font-size: 14px;
                ">
                    <thead><tr>{header_html}</tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>
            """
            
        except Exception as e:
            _logger.error(f"Error generating HTML table with template config: {str(e)}")
            # Fallback to original method
            return self._generate_html_table_optimized(columns, rows)

    # ========================================
    # SMART ALERT ROUTING METHODS WITH TEMPLATE CONFIG
    # ========================================
    def _smart_route_alert_by_type(self, rule, new_records, columns):
        """SMART: Enhanced alert routing with template configuration support"""
        if not rule.alert_id:
            _logger.warning(f"Rule {rule.id} - No alert group configured")
            return 0
        
        try:
            if rule.alert_id.tag == "internal":
                return self._send_smart_internal_alert_with_config(rule, new_records, columns)
            else:
                return self._send_smart_external_alert_with_config(rule, new_records, columns)
        except Exception as e:
            _logger.error(f"Rule {rule.id} smart alert routing error: {str(e)}")
            return 0

    def _send_smart_internal_alert_with_config(self, rule, new_records, columns):
        """Send smart internal alert with template configuration"""
        # Build recipient lists (same as before)
        mailto = set()
        mailcc = set()
        
        if rule.first_owner:
            mailto.add(rule.first_owner.login)
        if rule.second_owner:
            mailcc.add(rule.second_owner.login)
        
        for user in rule.specific_email_recipients:
            mailto.add(user.login)
        
        if rule.alert_id:
            try:
                for user in rule.alert_id.email_cc:
                    mailcc.add(user.login)
            except AttributeError:
                pass
        
        # Remove duplicates
        mailcc = mailcc - mailto
        
        if not mailto and not mailcc:
            _logger.warning(f"Rule {rule.id} - No recipients configured")
            return 0
        
        # Create email content with template configuration
        encoded_content = self._create_csv_attachment_optimized(columns, new_records)
        table_html = self._generate_html_table_with_template_config(columns, new_records)
        
        # Send email
        return self._send_email_optimized(rule, table_html, encoded_content, mailto, mailcc)

    def _send_smart_external_alert_with_config(self, rule, new_records, columns):
        """Send smart external alert with template configuration"""
        # Check for branch-specific routing
        branch_column_index = self._find_branch_column_index(columns)
        
        if branch_column_index is not None:
            return self._handle_smart_branch_alerts_with_config(rule, new_records, columns, branch_column_index)
        else:
            return self._send_general_external_alert_with_config(rule, new_records, columns)

    def _handle_smart_branch_alerts_with_config(self, rule, new_records, columns, branch_column_index):
        """Handle branch alerts with template configuration"""
        # Separate records by branch vs NULL
        branch_records = {}
        null_branch_records = []
        
        for record in new_records:
            try:
                branch_id = record[branch_column_index]
                if branch_id and str(branch_id).strip():
                    if branch_id not in branch_records:
                        branch_records[branch_id] = []
                    branch_records[branch_id].append(record)
                else:
                    null_branch_records.append(record)
            except (IndexError, TypeError):
                null_branch_records.append(record)
        
        total_emails = 0
        
        # Process branch-specific records
        for branch_id, branch_recs in branch_records.items():
            try:
                emails_sent = self._send_general_external_alert_with_config(rule, branch_recs, columns)
                total_emails += emails_sent
                _logger.info(f"Rule {rule.id} - Branch {branch_id}: {emails_sent} emails sent")
            except Exception as e:
                _logger.error(f"Rule {rule.id} - Error processing branch {branch_id}: {str(e)}")
                continue
        
        # Handle NULL branch_id records with control officer
        if null_branch_records:
            try:
                emails_sent = self._send_to_control_officer_with_config(rule, null_branch_records, columns)
                total_emails += emails_sent
                _logger.info(f"Rule {rule.id} - NULL branch records sent to control officer: {emails_sent} emails")
            except Exception as e:
                _logger.error(f"Rule {rule.id} - Error sending to control officer: {str(e)}")
        
        return total_emails

    def _send_general_external_alert_with_config(self, rule, new_records, columns):
        """Send general external alert with template configuration"""
        mailto = set()
        mailcc = set()
        
        if rule.alert_id:
            try:
                for user in rule.alert_id.email:
                    mailto.add(user.login)
            except AttributeError:
                pass
            
            try:
                for user in rule.alert_id.email_cc:
                    mailcc.add(user.login)
            except AttributeError:
                pass
        
        for user in rule.specific_email_recipients:
            mailcc.add(user.login)
        
        if not mailto and not mailcc:
            _logger.warning(f"Rule {rule.id} - No external recipients configured")
            return 0
        
        encoded_content = self._create_csv_attachment_optimized(columns, new_records)
        table_html = self._generate_html_table_with_template_config(columns, new_records)
        
        return self._send_email_optimized(rule, table_html, encoded_content, mailto, mailcc)

    def _send_to_control_officer_with_config(self, rule, null_records, columns):
        """Send NULL branch_id records to control officer with template configuration"""
        try:
            # Find control officer for this alert group
            control_officer = self.env['control.officer'].search([
                ('alert_id', '=', rule.alert_id.id)
            ], limit=1)
            
            if not control_officer:
                _logger.warning(f"Rule {rule.id} - No control officer found for alert group {rule.alert_id.name}")
                return self._send_general_external_alert_with_config(rule, null_records, columns)
            
            # Build recipient list
            mailto = {control_officer.officer.login}
            mailcc = set()
            
            for user in rule.specific_email_recipients:
                mailcc.add(user.login)
            
            if rule.alert_id:
                try:
                    for user in rule.alert_id.email_cc:
                        mailcc.add(user.login)
                except AttributeError:
                    pass
            
            mailcc = mailcc - mailto
            
            # Create email content with template configuration
            encoded_content = self._create_csv_attachment_optimized(columns, null_records)
            table_html = self._generate_html_table_with_template_config(columns, null_records)
            
            # Get template config for styling the control officer note
            template_info = self._get_template_config_for_email()
            primary_color = template_info.get('primary_color', '#007046')
            
            # Add control officer note with dynamic styling
            control_note = f"""
            <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 10px; margin-bottom: 15px; border-radius: 4px; font-family: {template_info.get('font_family', 'Arial, sans-serif')};">
                <strong style="color: {primary_color};">Control Officer Alert:</strong> These records have NULL/missing branch_id and require your attention.
                <br><strong>Control Officer:</strong> {control_officer.officer.name}
                <br><strong>Branch:</strong> {control_officer.branch_id.name if control_officer.branch_id else 'N/A'}
            </div>
            """
            table_html = control_note + table_html
            
            return self._send_email_optimized(rule, table_html, encoded_content, mailto, mailcc)
            
        except Exception as e:
            _logger.error(f"Rule {rule.id} - Error in control officer routing: {str(e)}")
            return self._send_general_external_alert_with_config(rule, null_records, columns)

    def _find_branch_column_index(self, columns):
        """Find branch_id column index"""
        for i, column in enumerate(columns):
            if 'branch_id' in column.lower():
                return i
        return None

    # ========================================
    # EMAIL UTILITIES (UPDATED WITH TEMPLATE CONFIG SUPPORT)
    # ========================================
    
    def _send_email_optimized(self, rule, table_html, encoded_content, mailto, mailcc):
        """Send email with dynamic template integration"""
        _logger.info(f"Rule {rule.id} - Sending email to {len(mailto)} TO, {len(mailcc)} CC")
        
        try:
            # Get template (updated reference path)
            template = self.env.ref('alert_management.alert_rules_mail_template', raise_if_not_found=False)
            if not template:
                # Fallback search
                template = self.env['mail.template'].search([
                    ('name', '=', 'Alert Mailing System')
                ], limit=1)
            
            if not template:
                _logger.error(f"Rule {rule.id} - Mail template not found")
                return 0
            
            # Create attachment
            attachment_id = self._create_attachment_optimized(encoded_content)
            
            # Generate alert ID
            alert_id = f"Alert{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
            
            # Get template configuration for email context
            template_config = self.env['email.template.config'].get_active_template()
            
            # Create alert history with template context
            alert_history = self.env['alert.history'].create({
                "alert_id": alert_id,
                "attachment_data": attachment_id.id,
                "attachment_link": f"/web/content/{attachment_id.id}?download=true",
                "html_body": table_html,
                "ref_id": f"alert.rules,{rule.id}",
                "process_id": rule.process_id or "",
                "risk_rating": rule.risk_rating,
                "last_checked": rule.last_checked,
                "email": ",".join(list(mailto)) if mailto else "",
                "email_cc": ",".join(list(mailcc)) if mailcc else "",
                "narration": rule.narration or "",
                "name": rule.name,
                # "source": "alert_rules",
                "source": "alert rules",
                # Add template config to context
                "template_config_id": template_config.id if template_config else False
            })
            
            # Send email
            mail_id = template.send_mail(alert_history.id, force_send=True)
            
            # Check status
            mail_record = self.env['mail.mail'].browse(mail_id)
            if mail_record.state in ["exception", "cancel"]:
                _logger.error(f"Rule {rule.id} - Email failed: {mail_record.failure_reason}")
                return 0
            else:
                _logger.info(f"Rule {rule.id} - Email sent successfully")
                return 1
                
        except Exception as e:
            _logger.error(f"Rule {rule.id} - Email sending failed: {str(e)}")
            return 0
        
    def _create_enhanced_email_html(self, table_html, template_info, rule):
        """Create enhanced email HTML - FIXED VERSION (No 'Company Logo' text)"""
        try:
            if not template_info.get('has_config'):
                return table_html  # Return simple table if no config
            
            email_width = template_info.get('email_width', 590)
            content_padding = template_info.get('content_padding', 16)
            font_family = template_info.get('font_family', 'Arial, sans-serif')
            primary_color = template_info.get('primary_color', '#007046')
            
            # FIXED: Logo handling - NO PLACEHOLDER TEXT
            logo_section = ""
            if template_info.get('logo_data'):
                logo_width = template_info.get('logo_width', 192)
                logo_height = template_info.get('logo_height', 192)
                # Only show logo if we actually have logo data, no placeholder text
                logo_section = f"""
                <div style="text-align: center; margin-bottom: 20px; padding: 10px;">
                    <img src="data:image/png;base64,{template_info['logo_data']}" 
                        style="max-width: {logo_width}px; max-height: {logo_height}px; height: auto;" 
                        alt="" />
                </div>
                """
            # If no logo data, logo_section stays empty (no placeholder text)
            
            # FIXED: Proper footer (only if enabled, no logo duplication)
            footer_section = ""
            if template_info.get('show_footer', True):
                footer_bg = template_info.get('footer_bg_color', '#ffffff')
                footer_text_color = template_info.get('footer_text_color', '#454748')
                company_name = template_info.get('company_name', 'Company')
                
                footer_section = f"""
                <div style="
                    background-color: {footer_bg}; 
                    color: {footer_text_color}; 
                    padding: 15px; 
                    margin-top: 20px; 
                    border-radius: 4px; 
                    font-size: 12px; 
                    text-align: center;
                    font-family: {font_family};
                    border-top: 1px solid #eeeeee;
                ">
                    <p style="margin: 0; font-weight: bold;">{company_name}</p>
                    <p style="margin: 5px 0 0 0; font-size: 11px;">Automated Alert System</p>
                </div>
                """
            
            # FIXED: Email HTML with proper responsive table container
            enhanced_html = f"""
            <div style="
                font-family: {font_family}; 
                max-width: {email_width}px; 
                margin: 0 auto; 
                padding: {content_padding}px;
                background-color: #ffffff;
            ">
                {logo_section}
                
                <div style="margin-bottom: 20px;">
                    <h2 style="
                        color: {primary_color}; 
                        font-size: 20px; 
                        margin: 0 0 10px 0;
                        font-family: {font_family};
                    ">Alert: {rule.name}</h2>
                    <p style="
                        color: #666666; 
                        font-size: 14px; 
                        margin: 0;
                        font-family: {font_family};
                    ">Generated on {fields.Datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div style="width: 100%; overflow-x: auto;">
                    {table_html}
                </div>
                
                {footer_section}
            </div>
            """
            
            return enhanced_html
            
        except Exception as e:
            _logger.error(f"Error creating enhanced email HTML: {str(e)}")
            return table_html  # Fallback to simple table
    
    
    def _create_attachment_optimized(self, encoded_content):
        """Create optimized attachment - FIXED to prevent logo conflicts"""
        try:
            # Generate unique filename to avoid conflicts
            timestamp = int(time.time())
            attachment = {
                'name': f'alert_report_{timestamp}.csv',
                'mimetype': 'text/csv',
                'type': 'binary',
                'datas': encoded_content,
                'res_model': 'alert.history',  # Link to specific model
                'public': False,  # Don't make it public to avoid showing in email
            }
            return self.env['ir.attachment'].create(attachment)
        except Exception as e:
            _logger.error(f"Failed to create attachment: {str(e)}")
            raise

    def _create_csv_attachment_optimized(self, columns, rows):
        """Create optimized CSV attachment"""
        try:
            # Filter out branch_id columns
            pattern = re.compile(r'\bbranch\s*_?\s*id\b', re.IGNORECASE)
            branch_id_indices = [i for i, col in enumerate(columns) if pattern.fullmatch(col)]
            
            csv_buffer = io.StringIO()
            csv_writer = csv.writer(csv_buffer)
            
            # Write headers
            filtered_columns = [
                " ".join(col.split("_")).title() 
                for i, col in enumerate(columns) 
                if i not in branch_id_indices
            ]
            csv_writer.writerow(filtered_columns)
            
            # Write data rows
            for row in rows:
                filtered_row = []
                for i, cell in enumerate(row):
                    if i not in branch_id_indices:
                        formatted_cell = self._format_cell_for_csv(cell)
                        filtered_row.append(formatted_cell)
                csv_writer.writerow(filtered_row)
            
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()
            
            return base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
            
        except Exception as e:
            _logger.error(f"Error creating CSV: {str(e)}")
            raise

    def _format_cell_for_csv(self, cell):
        """Format cell for CSV"""
        if cell is None:
            return ""
        elif isinstance(cell, (int, float)):
            return str(int(cell)) if isinstance(cell, float) and cell == int(cell) else str(cell)
        else:
            return str(cell)

    def _generate_html_table_optimized(self, columns, rows):
        """Generate responsive HTML table with dynamic template configuration"""
        try:
            # Get template configuration
            template_config = self.env['email.template.config'].get_active_template()
            
            if template_config:
                # Use dynamic colors from template
                header_bg = template_config.table_header_bg_color or '#007046'
                header_text = template_config.table_header_text_color or '#fff'
                border_color = template_config.table_border_color or '#ddd'
                even_row_color = template_config.table_row_even_color or '#f9f9f9'
                odd_row_color = template_config.table_row_odd_color or '#ffffff'
                font_family = dict(template_config._fields['font_family'].selection)[template_config.font_family] if template_config.font_family else 'Arial, sans-serif'
            else:
                # Safe fallbacks if no template exists
                header_bg = '#007046'
                header_text = '#fff'
                border_color = '#ddd'
                even_row_color = '#f9f9f9'
                odd_row_color = '#ffffff'
                font_family = 'Arial, sans-serif'
            
            # Filter out branch_id columns (UNCHANGED from old code)
            pattern = re.compile(r'\bbranch\s*_?\s*id\b', re.IGNORECASE)
            branch_id_indices = [i for i, col in enumerate(columns) if pattern.fullmatch(col)]
            
            # Generate headers with dynamic styling
            header_html = ""
            for i, col in enumerate(columns):
                if i not in branch_id_indices:
                    header_html += f"<th style='padding: 8px; background-color: {header_bg}; color: {header_text}; border: 1px solid {border_color}; font-family: {font_family}; font-size: 14px;'>{' '.join(col.split('_')).title()}</th>"
            
            # Generate rows with dynamic styling (limit for email)
            max_rows = 20
            rows_html = ""
            
            for row_index, row in enumerate(rows[:max_rows]):
                bg_color = even_row_color if row_index % 2 == 0 else odd_row_color
                rows_html += "<tr>"
                for i, cell in enumerate(row):
                    if i not in branch_id_indices:
                        formatted_cell = self._format_cell_for_html(cell)
                        rows_html += f"<td style='padding: 8px; border: 1px solid {border_color}; background-color: {bg_color}; font-family: {font_family}; font-size: 14px;'>{formatted_cell}</td>"
                rows_html += "</tr>"
            
            # Truncation message
            if len(rows) > max_rows:
                colspan = len([col for i, col in enumerate(columns) if i not in branch_id_indices])
                rows_html += f"""
                <tr>
                    <td colspan='{colspan}' style='padding: 8px; font-style: italic; text-align: center; background-color: #fffacd; border: 1px solid {border_color}; font-family: {font_family}; font-size: 12px;'>
                        ... and {len(rows) - max_rows} more record(s). See attached CSV for complete data.
                    </td>
                </tr>
                """
            
            # KEEP EXACT OLD RESPONSIVE STRUCTURE - just with dynamic font
            return f"""
            <div style="overflow-x: auto; margin: 10px 0;">
                <table style="border-collapse: collapse; font-family: {font_family}; width: 100%;">
                    <thead><tr>{header_html}</tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>
            """
            
        except Exception as e:
            _logger.error(f"Error generating HTML table: {str(e)}")
            return f"<p>Error generating table: {str(e)}</p>"

    def _format_cell_for_html(self, cell):
        """Format cell for HTML"""
        if cell is None:
            return ""
        elif isinstance(cell, (int, float)):
            return f"{cell:,.2f}" if isinstance(cell, float) and abs(cell) > 0.01 else str(cell)
        elif isinstance(cell, str):
            return cell.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        else:
            return str(cell)

    # ========================================
    # MANAGEMENT METHODS WITH UI BUTTONS
    # ========================================
    def clear_all_signatures(self):
        """Clear all signatures for this rule"""
        self.ensure_one()
        
        signatures = self.env['alert.signature'].search([
            ('alert_rule_id', '=', self.id)
        ])
        
        count = len(signatures)
        signatures.unlink()
        
        self.write({
            'total_signatures_stored': 0,
            'initialization_complete': False
        })
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Signatures Cleared',
                'message': f'Cleared {count} signatures for rule "{self.name}". Initialization will run on next processing.',
                'type': 'success'
            }
        }

    def test_smart_rule(self):
        """Test rule with smart processing"""
        self.ensure_one()
        
        try:
            # Override timing
            original_next_run = self.next_scheduled_run
            self.write({'next_scheduled_run': fields.Datetime.now() - timedelta(minutes=1)})
            
            # Test processing
            result = self._process_single_rule_safely(self)
            
            # Restore
            self.write({'next_scheduled_run': original_next_run})
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Smart Test Complete',
                    'message': f'✅ Emails sent: {result["emails_sent"]}\n📊 Signatures created: {result["signatures_created"]}\n🔍 Records found: {self.last_record_count}\n📧 New records: {self.last_new_record_count}',
                    'type': 'success',
                    'sticky': True
                }
            }
            
        except Exception as e:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Test Failed',
                    'message': f'❌ Error: {str(e)}',
                    'type': 'danger',
                    'sticky': True
                }
            }

    def reset_initialization(self):
        """Reset rule to initialization state"""
        self.ensure_one()
        self.write({
            'initialization_complete': False,
            'total_signatures_stored': 0,
            'is_processing': False,
            'next_scheduled_run': False,
            'last_error': False,
            'last_record_count': 0,
            'last_new_record_count': 0,
            'last_email_count': 0
        })
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Initialization Reset',
                'message': f'✅ Rule "{self.name}" reset to initialization mode\n📅 Will use {self.initialization_strategy} strategy on next run\n⏰ Processing window: {self.processing_window_hours or 72} hours',
                'type': 'success',
                'sticky': True
            }
        }

    def seed_signatures_optimized(self):
        """Placeholder method for seed signatures button"""
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Seed Signatures',
                'message': 'Seed signatures functionality not implemented.',
                'type': 'info'
            }
        }

    @api.model
    def get_smart_system_statistics(self):
        """Get smart system statistics"""
        try:
            stats = self.env['alert.signature'].get_signature_stats_optimized()
            
            total_signatures = sum(data['signature_count'] for data in stats.values())
            active_rules = len([data for data in stats.values() if data['signature_count'] > 0])
            
            # Get initialization statistics
            all_rules = self.search([('status', '=', '1')])
            initialized_rules = self.search([('status', '=', '1'), ('initialization_complete', '=', True)])
            pending_init = len(all_rules) - len(initialized_rules)
            
            # Check smart analysis availability
            analysis_status = "Available" if SQL_METADATA_AVAILABLE else "Pattern Fallback Only"
            
            message = f"🎯 Smart Banking Alert System Statistics:\n\n"
            message += f"📊 SMART ANALYSIS:\n"
            message += f"• SQL Intelligence: {analysis_status}\n"
            message += f"• Control Officer Support: Active\n"
            message += f"• Email Template Config: Available\n\n"
            message += f"📊 RULES STATUS:\n"
            message += f"• Total active rules: {len(all_rules)}\n"
            message += f"• Initialized rules: {len(initialized_rules)}\n"
            message += f"• Pending initialization: {pending_init}\n\n"
            message += f"📧 SIGNATURES:\n"
            message += f"• Total signatures: {total_signatures:,}\n"
            message += f"• Rules with signatures: {active_rules}\n\n"
            
            message += "🏆 Top 5 rules by activity:\n"
            sorted_stats = sorted(stats.items(), key=lambda x: x[1]['signature_count'], reverse=True)
            for rule_name, data in sorted_stats[:5]:
                message += f"• {rule_name}: {data['signature_count']:,} signatures\n"
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Smart System Statistics',
                    'message': message,
                    'type': 'info',
                    'sticky': True
                }
            }
            
        except Exception as e:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Statistics Error',
                    'message': f'❌ Error getting statistics: {str(e)}',
                    'type': 'danger'
                }
            }