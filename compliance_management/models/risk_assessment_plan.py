# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.tools.safe_eval import safe_eval
from odoo.exceptions import UserError, ValidationError
import re
import logging

_logger = logging.getLogger(__name__)

class RiskAssessmentPlan(models.Model):
    _name = 'res.compliance.risk.assessment.plan'
    _description = 'Customer Risk Analysis'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _sql_constraints = [
        ('uniq_stats_code', 'unique(code)',
         "Plan code already exists. Value must be unique!"),
        ('uniq_stats_name', 'unique(name)',
         "Plan Name already exists. Value must be unique!")
    ]
    _order = 'priority asc'

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    sql_query = fields.Text(string='SQL Query',help="SQL query returning single value")
    priority = fields.Integer(
        string='Sequence', help="Order of priority in which plan will be evaluated", required=True, default=1)
    state = fields.Selection(string='State', selection=[('draft', 'Draft'), (
        'active', 'Active'), ('inactive', 'Inactive')], default='draft', index=True)
    narration = fields.Text(string='Narration')
    risk_score = fields.Integer(string='Risk Score', default=1)
    compute_score_from = fields.Selection(string='Compute Risk Score From', selection=[(
        'dynamic', 'SQL Query Return Single Value'), ('static', 'From Risk Rating'),('risk_assessment','Related Risk Assessment'),('python','Python Expression')], default='risk_assessment', index=True,required=True)
    risk_assessment = fields.Many2one(comodel_name='res.risk.assessment', string='Risk Assessment', index=True, required=False,
                                      help="Risk Assessment to which this plan is associated")
    risk_assessment_score = fields.Float(string='Risk Assessment Score',digits=(10, 2),related="risk_assessment.risk_rating")
    
    use_composite_calculation = fields.Boolean(string='Use Composite Calculation', default=False,
                                               help="If checked, composite risk calculation will be used")
    universe_id = fields.Many2one(comodel_name='res.risk.universe', string='Risk Universe',
                                  help="Risk Universe associated with this plan")
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    condition_python = fields.Text(string='Python Expression',help='Applied this rule for calculation if condition is true. You can specify condition like result = 10 if customer.is_pep == True else 1')
    
    
    
    @api.model
    def create(self, vals):
        condition_select  = vals.get('compute_score_from')
        if condition_select == 'dynamic':
            sql_query = vals.get('sql_query')
            if not sql_query:
                raise ValidationError(_("SQL Query is required for SQL condition type."))
        elif condition_select == 'python':
            python_code = vals.get('condition_python')
            if not python_code:
                raise ValidationError(_("Python Expression is required for Python condition type."))
        return super(RiskAssessmentPlan, self).create(vals)
    
    def write(self, vals):
        condition_select = vals.get('compute_score_from', self.compute_score_from)
        if condition_select == 'dynamic':
            sql_query = vals.get('sql_query', self.sql_query)
            if not sql_query:
                raise ValidationError(_("SQL Query is required for SQL condition type."))
        elif condition_select == 'python':
            python_code = vals.get('condition_python', self.condition_python)
            if not python_code:
                raise ValidationError(_("Python Expression is required for Python condition type."))
        return super(RiskAssessmentPlan, self).write(vals)
            
    @api.onchange('risk_assessment')
    def _onchange_risk_assessment(self):
        """Automatically set universe_id based on risk_assessment"""
        if self.risk_assessment and self.risk_assessment.universe_id:
            self.universe_id = self.risk_assessment.universe_id

    @api.onchange('compute_score_from')
    def _onchange_compute_score_from(self):
        """Reset composite calculation if compute_score_from is not risk_assessment"""
        if self.compute_score_from != 'risk_assessment':
            self.use_composite_calculation = False

    def action_activate(self):
        for e in self:
            e.write({'state': 'active'})

    def action_deactivate(self):
        for e in self:
            e.write({'state': 'inactive'})
            
    def compute_score_from_code(self, localdict):
        """
        This method is used to compute the rule based on the local dictionary.
        It evaluates the condition_python or executes the sql_query based on the condition_select.
        
        # Available variables in localdict:
        #----------------------
        # customer: object containing the customer
        # branch: object containing the branch
        # env: environment  object
        #----------------------
        # Note: returned value have to be set in the variable 'result'
        """
        self.ensure_one()
        try:
            safe_eval(self.condition_python, localdict, mode='exec', nocopy=True)
            if 'result' in localdict and localdict['result'] is not None:
                return localdict['result']
            return False
        except:
            raise UserError(_('Wrong python code defined for customer risk analysis rule %s (%s).') % (self.name, self.code))


class RiskAnalysis(models.Model):
    """
    Highly optimized Materialized Views using pure set-based operations.
    No correlated subqueries, no multiple joins, single aggregation pass.
    """
    _name = 'risk.analysis'
    _description = 'Risk Analysis Materialized View'
    _order = 'universe, name'

    name = fields.Char(string="View Name", required=True, readonly=True)
    code = fields.Text(string='Definition Script', required=True, readonly=True)
    universe = fields.Char(string="Risk Universe", required=True, readonly=True)
    last_refresh = fields.Datetime(string="Last Refreshed", readonly=True)
    pattern_stats = fields.Text(string="Pattern Statistics", readonly=True,
                                 help="Breakdown of matched patterns")

    def _slugify(self, text):
        """Creates a valid SQL identifier from text."""
        text = text.lower().strip()
        text = re.sub(r'[\s\.]+', '_', text)
        return re.sub(r'[^\w_]', '', text)

    def _extract_pattern_data(self, sql_query, plan_code, risk_score):
        """
        Extracts just the essential data needed for set-based operations.
        Returns: (pattern_type, filter_field, filter_value, join_table, join_condition)
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
                'use_latest': True,  # Uses opening_date DESC
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

    
    @api.model
    def _build_optimized_view(self, universe, plans):
        """Builds a partitioned table structure instead of materialized view."""
        view_name = f"mv_risk_{self._slugify(universe.code)}"
        
        # Extract all patterns (keep your existing code here)...
        # ... (all your pattern extraction code) ...
        # Extract all patterns
        patterns = {
            'account_category': {},
            'industry': {},
            'region': {},
            'channel': {},
            'branch_region': {},
        }
        
        unmatched = []
        
        for plan in plans:
            pattern = self._extract_pattern_data(
                plan.sql_query, 
                plan.code, 
                plan.risk_assessment_score or 0
            )
            
            if not pattern:
                unmatched.append(plan.code)
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
            
            # FIX: Handle value_condition correctly to avoid ccs.lower() syntax error
            value_filter = ""
            if value_cond:
                if "lower(" in value_cond:
                    # If it's already a function call, replace "value" with "ccs.value"
                    value_filter = f"AND {value_cond.replace('value', 'ccs.value')}"
                else:
                    # Otherwise, prefix the column with the table alias
                    value_filter = f"AND ccs.{value_cond}"
            
            # All items in this group have same channel, so we can return multiple risk codes
            for item in items:
                # FIX: Use actual risk score from item instead of hardcoded 0
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
                    f"        WHEN lower(trim(rb.region)) = '{item['value']}' THEN '{item['code']}'"
                )
                case_whens_score.append(
                    f"        WHEN lower(trim(rb.region)) = '{item['value']}' THEN {item['score']}"
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
            return {
                'name': view_name,
                'code': "-- Empty view, no patterns matched",
                'universe': universe.name,
                'stats': f"No patterns matched. Unmatched: {len(unmatched)}",
                'is_partitioned': True
            }
        
        # Assemble the SQL for populating partitions
        all_flags_cte = ""
        if union_branches:
            all_flags_cte = union_branches[0]  # First branch without UNION ALL
            for branch in union_branches[1:]:
                all_flags_cte += f"\n    UNION ALL{branch}"
        
        # Create the SQL for populating partitioned tables - NOT creating a materialized view 
        
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
        WHERE rp.id >= ? AND rp.id < ?;  -- Note the < instead of <= for upper bound
        """
        
        return {
            'name': view_name,
            'code': populate_sql,
            'universe': universe.name,
            'stats': f"Patterns matched: {len(union_branches)}, Unmatched: {len(unmatched)}",
            'is_partitioned': True
        }     
        
    
    @api.model
    def _build_independent_risk_view(self):
        """
        Creates a partitioned table for risk assessments that are not tied to a universe.
        Handles standalone risk queries like PEP, sanctions, watchlists, etc.
        """
        view_name = "mv_risk_independent_factors"
        
        # Fetch all active risk assessment plans that don't have a universe
        plans = self.env['res.compliance.risk.assessment.plan'].search([
            ('state', '=', 'active'),
            ('sql_query', '!=', False),
            ('universe_id', '=', False)
        ])
        
        # Extract risk patterns from the SQL queries
        risk_patterns = []
        unmatched = []
        
        for plan in plans:
            pattern = self._extract_independent_pattern(plan.sql_query, plan.code, plan.risk_score or 0)
            if pattern:
                risk_patterns.append(pattern)
            else:
                unmatched.append(plan.code)
        
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
        
        # Add union branches based on risk patterns
        # [Keep your existing code for building these branches]
        
        # Assemble the SQL for populating partitions
        all_flags_cte = ""
        if union_branches:
            all_flags_cte = union_branches[0]
            for branch in union_branches[1:]:
                all_flags_cte += f"\n    UNION ALL{branch}"
        
        # Create the SQL for populating partitioned tables
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
        WHERE rp.id BETWEEN ? AND ?;
        """
        
        return {
            'name': view_name,
            'code': populate_sql,
            'universe': 'Independent Risk Factors',
            'stats': f"Patterns matched: {len(union_branches)}, Unmatched: {len(unmatched)}",
            'is_partitioned': True
        }
    
    
    @api.model
    def _extract_independent_pattern(self, sql_query, plan_code, risk_score):
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

            
    @api.model
    def _cron_generate_views(self):
        """Main cron entry point to generate all partitioned views."""
        # Clear existing views
        self.search([]).unlink()
    
        # First, handle universe-based risk assessments
        universe_plans = {}
        plans = self.env['res.compliance.risk.assessment.plan'].search([
            ('state', '=', 'active'),
            ('sql_query', '!=', False),
            ('universe_id', '!=', False)
        ])
        
        for plan in plans:
            universe_plans.setdefault(plan.universe_id, []).append(plan)
        
        for universe, plans in universe_plans.items():
            view_data = self._build_optimized_view(universe, plans)
            view_name = view_data['name']
            
            # Create the table structure in its own transaction
            table_created = False
            try:
                with self.pool.cursor() as new_cr:
                    # Setup environment with new cursor
                    env = api.Environment(new_cr, self.env.uid, self.env.context)
                    table_created = env[self._name]._setup_partitioned_view(view_name)
                    new_cr.commit()
            except Exception as e:
                _logger.error(f"✗ Failed to create table structure for {view_name}: {e}")
                continue
                
            if not table_created:
                continue
            
            # Populate the data in its own transaction
            populated = False
            try:
                with self.pool.cursor() as new_cr:
                    env = api.Environment(new_cr, self.env.uid, self.env.context)
                    populated = env[self._name]._populate_partitioned_view(view_name, view_data['code'])
                    new_cr.commit()
            except Exception as e:
                _logger.error(f"✗ Failed to populate data for {view_name}: {e}")
                continue
                
            if not populated:
                continue
                
            # Create indexes in its own transaction
            indexes_created = False
            try:
                with self.pool.cursor() as new_cr:
                    env = api.Environment(new_cr, self.env.uid, self.env.context)
                    indexes_created = env[self._name]._create_partition_indexes(view_name)
                    new_cr.commit()
            except Exception as e:
                _logger.error(f"✗ Failed to create indexes for {view_name}: {e}")
            
            # Create the record in a separate transaction with retry
            record_created = False
            retry_count = 0
            max_retries = 3
            
            while not record_created and retry_count < max_retries:
                try:
                    with self.pool.cursor() as new_cr:
                        env = api.Environment(new_cr, self.env.uid, self.env.context)
                        env[self._name].create({
                            'name': view_name,
                            'code': view_data['code'],
                            'universe': view_data['universe'],
                            'pattern_stats': view_data.get('stats', ''),
                            'last_refresh': fields.Datetime.now()
                        })
                        new_cr.commit()
                        record_created = True
                        _logger.info(f"✓ Created risk analysis record for: {view_name}")
                except Exception as e:
                    retry_count += 1
                    _logger.error(f"✗ Failed to create record for {view_name} (attempt {retry_count}): {e}")
                    time.sleep(1)  # Wait before retry
            
            if record_created:
                _logger.info(f"✓ Completed view creation for {view_name}")
        
        # Process independent risk assessments with the same approach
        view_data = self._build_independent_risk_view()
        view_name = view_data['name']
        
        if self._setup_partitioned_view(view_name):
            if self._populate_partitioned_view(view_name, view_data['code']):
                self._create_partition_indexes(view_name)
                
                self.create({
                    'name': view_name,
                    'code': view_data['code'],
                    'universe': view_data['universe'],
                    'pattern_stats': view_data.get('stats', ''),
                    'last_refresh': fields.Datetime.now()
                })
                _logger.info(f"✓ Created partitioned table for independent risks: {view_name}")

    def _create_partition_indexes(self, view_name):
        """Create necessary indexes on each partition."""
        try:
            with self.pool.cursor() as cr:
                # Set higher memory for index creation
                cr.execute("SET maintenance_work_mem = '1GB';")
                
                # Get all partitions for this view
                cr.execute(f"""
                SELECT inhrelid::regclass AS partition_name
                FROM pg_inherits
                WHERE inhparent = '{view_name}'::regclass;
                """)
                
                partitions = [row[0] for row in cr.fetchall()]
                
                for partition in partitions:
                    # Create index name without schema parts
                    partition_str = str(partition).split('.')[-1]
                    index_name = f"idx_{partition_str}_risk_data_gin"
                    has_risks_name = f"idx_{partition_str}_has_risks"
                    
                    # Create GIN index on risk_data
                    cr.execute(f"""
                    CREATE INDEX IF NOT EXISTS {index_name} 
                    ON {partition} USING GIN (risk_data);
                    """)
                    cr.commit()
                    
                    # Create index for has_risks condition
                    cr.execute(f"""
                    CREATE INDEX IF NOT EXISTS {has_risks_name} 
                    ON {partition} (partner_id) 
                    WHERE risk_data != '{{}}'::jsonb;
                    """)
                    cr.commit()
                    
                _logger.info(f"✓ Created indexes for all partitions of {view_name}")
                return True
        except Exception as e:
            _logger.error(f"✗ Failed to create indexes for {view_name}: {e}")
            return False
        
    def _setup_partitioned_view(self, view_name):
        """Create a partitioned table structure to replace a materialized view."""
        try:
            with self.pool.cursor() as cr:
                # Increase timeout and work memory
                cr.execute("SET statement_timeout = '3600000';")  # 1 hour
                cr.execute("SET maintenance_work_mem = '1GB';")
                
                # Check if object exists and its type
                cr.execute("""
                    SELECT c.relkind 
                    FROM pg_class c 
                    JOIN pg_namespace n ON n.oid = c.relnamespace 
                    WHERE c.relname = %s 
                    AND n.nspname = current_schema()
                """, (view_name,))
                
                result = cr.fetchone()
                
                # Drop existing object properly based on its type
                if result:
                    object_type = result[0]
                    if object_type == 'm':  # materialized view
                        cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
                    else:  # table or other object
                        cr.execute(f"DROP TABLE IF EXISTS {view_name} CASCADE;")
                else:
                    # Object doesn't exist, try both just to be safe
                    cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
                    cr.execute(f"DROP TABLE IF EXISTS {view_name} CASCADE;")
                
                # Create parent table with partitioning
                cr.execute(f"""
                CREATE TABLE {view_name} (
                    partner_id INTEGER PRIMARY KEY,
                    partner_name VARCHAR,
                    risk_data JSONB
                ) PARTITION BY RANGE (partner_id);
                """)
                
                # Get min/max partner IDs to determine partition ranges
                cr.execute("SELECT MIN(id), MAX(id) FROM res_partner;")
                min_id, max_id = cr.fetchone()
                
                if not min_id or not max_id:
                    _logger.warning(f"No partners found to create partitions for {view_name}")
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
                    cr.execute(f"""
                    CREATE TABLE {partition_name} PARTITION OF {view_name}
                    FOR VALUES FROM ({current_id}) TO ({next_id});
                    """)
                    
                    current_id = next_id
                    partition_num += 1
                
                cr.commit()
                _logger.info(f"✓ Created partitioned table {view_name} with {partition_num-1} partitions")
                return True
                
        except Exception as e:
            _logger.error(f"✗ Failed to create partitioned table {view_name}: {e}")
            return False    
    
    def action_refresh_view(self):
        """Refresh partitioned views without overlapping ranges."""
        for record in self:
            try:
                # Clear the table but keep structure
                with self.pool.cursor() as cr:
                    # Verify the table exists
                    cr.execute(f"SELECT to_regclass('{record.name}');")
                    exists = cr.fetchone()[0]
                    
                    if not exists:
                        # Table doesn't exist, we need to recreate it
                        self._setup_partitioned_view(record.name)
                    else:
                        # Table exists, just truncate it
                        cr.execute(f"TRUNCATE TABLE {record.name};")
                    
                    cr.commit()
                
                # Re-populate with fresh data
                self._populate_partitioned_view(record.name, record.code)
                
                # Ensure indexes exist
                self._create_partition_indexes(record.name)
                
                # Update refresh timestamp
                record.last_refresh = fields.Datetime.now()
                _logger.info(f"✓ Refreshed partitioned table {record.name}")
                
            except Exception as e:
                _logger.error(f"✗ Failed to refresh {record.name}: {e}")
                
    def _populate_partitioned_view(self, view_name, populate_sql):
        """
        Populate the partitioned table with data, ensuring no batch overlap and using
        independent transactions for better resilience.
        """
        try:
            # Get partition ranges
            with self.pool.cursor() as cr:
                cr.execute(f"""
                SELECT 
                    child.relname AS child_table,
                    pg_get_expr(child.relpartbound, child.oid) AS partition_bound
                FROM pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
                JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
                WHERE parent.relname = '{view_name}'
                ORDER BY child.relname;
                """)
                partitions = cr.fetchall()
            
            # Process each partition with its own cursor/transaction
            for child_table, bounds in partitions:
                # Extract the range values from the partition bounds
                match = re.search(r"FROM\s*\((\d+)\)\s*TO\s*\((\d+)\)", bounds)
                if not match:
                    continue
                    
                start_id, end_id = int(match.group(1)), int(match.group(2))
                
                # Clear existing data for this partition
                with self.pool.cursor() as cr:
                    cr.execute(f"DELETE FROM {child_table};")
                    cr.commit()
                
                # Process this range in smaller chunks with independent transactions
                batch_size = 150000
                for chunk_start in range(start_id, end_id, batch_size):
                    chunk_end = min(chunk_start + batch_size, end_id)
                    
                    try:
                        with self.pool.cursor() as cr:
                            # Set optimizations for this chunk
                            cr.execute("SET statement_timeout = '600000';")  # 10 minutes per chunk
                            cr.execute("SET work_mem = '512MB';")
                            cr.execute("SET synchronous_commit = 'off';")
                            
                            # Modify SQL to use exclusive upper bound (< not <=)
                            modified_sql = populate_sql.replace("?", "%s")
                            
                            _logger.info(f"Processing partners {chunk_start} to {chunk_end-1}")
                            cr.execute(modified_sql, (chunk_start, chunk_end))
                            cr.commit()
                        
                    except Exception as e:
                        _logger.error(f"Error processing chunk {chunk_start}-{chunk_end-1}: {e}")
                        # Continue with next chunk even if this one failed
                        continue
                        
                _logger.info(f"✓ Completed partition {child_table}")
            
            return True
        except Exception as e:
            _logger.error(f"✗ Failed to populate partitioned view {view_name}: {e}")
            return False
        
        
        
        