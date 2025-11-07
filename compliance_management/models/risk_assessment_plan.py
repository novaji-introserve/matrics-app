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
    This model stores the generated PostgreSQL functions and contains the logic
    to automatically generate them via a cron job.
    """
    _name = 'risk.analysis'
    _description = 'Risk Analysis SQL Function'
    _order = 'universe, name'

    name = fields.Char(
        string="Function Name",
        required=True,
        readonly=True,
        help="The unique name of the generated PostgreSQL function (e.g., check_partner_industry)."
    )
    code = fields.Text(
        string='SQL Function Script',
        required=True,
        readonly=True,
        help="The complete 'CREATE OR REPLACE FUNCTION...' statement for this function."
    )
    sql_query = fields.Char(
        string='Sample Execution Query',
        readonly=True,
        help="A sample SQL query to execute the function (e.g., SELECT check_partner_industry(partner_id);)."
    )
    universe = fields.Char(
        string="Risk Universe",
        required=True,
        readonly=True,
        help="The name of the risk universe these checks belong to."
    )

    # --- HELPER & LOGIC METHODS ---

    def _slugify(self, text):
        """Helper to create a valid SQL function name from a string."""
        text = text.lower().strip()
        text = re.sub(r'[\s\.]+', '_', text)
        text = re.sub(r'[^\w_]', '', text)
        return text

    def _get_optimized_check_type(self, sql_query):
        """
        Analyzes a SQL query to determine the best optimization strategy.
        """
        sql_lower = sql_query.lower()

        if 'select risk_score' in sql_lower:
            return {'type': 'risk_score'}

        # Pattern 1: One-to-One (Partner Region) - More robust
        if 'res_partner_region' in sql_lower:
            match = re.search(
                r"where\s+(?:lower\s*\(\s*rpr.name\s*\)|rpr.name)\s*=\s*'([^']+)'", sql_lower)
            if match:
                return {
                    'type': 'one-to-one', 'group': 'partner_region', 'value': match.group(1).lower(),
                    'fetch_sql': "SELECT lower(rpr.name) FROM res_partner rp JOIN res_partner_region rpr ON rp.region_id = rpr.id WHERE rp.id = p_partner_id",
                    'variable_name': 'partner_region_name'
                }

        # Pattern 2: One-to-One (Customer Industry) - More robust
        if 'customer_industry' in sql_lower:
            # This regex handles both `lower(name) = '...'` and `name = '...'`
            match = re.search(
                r"where\s+(?:lower\s*\(\s*name\s*\)|name)\s*=\s*'([^']+)'", sql_lower)
            if match:
                return {
                    'type': 'one-to-one', 'group': 'customer_industry', 'value': match.group(1).lower(),
                    'fetch_sql': "SELECT lower(ci.name) FROM res_partner rp JOIN customer_industry ci ON rp.customer_industry_id = ci.id WHERE rp.id = p_partner_id",
                    'variable_name': 'partner_industry_name'
                }

        return {'type': 'one-to-many'}

    @api.model
    def _build_function_for_universe(self, universe, queries):
        """
        Builds a single, consolidated SQL function for a list of queries within a universe.
        """
        function_name = f"check_{self._slugify(universe.code)}"
        declarations = ["result_json jsonb := '{}'::jsonb;"]
        body_parts = []
        one_to_one_groups = {}

        for query_obj in queries:
            analysis = self._get_optimized_check_type(query_obj.sql_query)

            if analysis['type'] == 'one-to-one':
                group = analysis['group']
                if group not in one_to_one_groups:
                    one_to_one_groups[group] = {
                        'fetch_sql': analysis['fetch_sql'],
                        'variable_name': analysis['variable_name'],
                        'checks': []
                    }
                    if f"{analysis['variable_name']} text;" not in declarations:
                        declarations.append(
                            f"{analysis['variable_name']} text;")
                one_to_one_groups[group]['checks'].append({
                    'value': analysis['value'],
                    'plan_code': query_obj.code,
                    'score': query_obj.risk_assessment_score or 0
                })

            elif analysis['type'] == 'risk_score':
                sql = query_obj.sql_query.strip().rstrip(';').replace('%s', 'p_partner_id')
                temp_var = f"temp_score_{self._slugify(query_obj.code)}"
                declarations.append(f"{temp_var} integer;")
                body_parts.append(f"""
    {temp_var} := ({sql.replace('LIMIT 1', '')} LIMIT 1);
    IF {temp_var} IS NOT NULL THEN
        result_json := result_json || jsonb_build_object('{query_obj.code}', {temp_var});
    END IF;""")

            else:  # 'one-to-many'
                sql = query_obj.sql_query.strip().rstrip(';').replace('%s', 'p_partner_id')
                score = query_obj.risk_assessment_score or 0
                body_parts.append(f"""
    IF EXISTS ({sql}) THEN
        result_json := result_json || jsonb_build_object('{query_obj.code}', {score});
    END IF;""")

        # Process the grouped one-to-one checks at the start of the function body
        one_to_one_body = []
        for group_info in one_to_one_groups.values():
            one_to_one_body.append(
                f"{group_info['fetch_sql']} INTO {group_info['variable_name']};")
            for check in group_info['checks']:
                one_to_one_body.append(f"""
    IF COALESCE({group_info['variable_name']}, '') = '{check['value']}' THEN
        result_json := result_json || jsonb_build_object('{check['plan_code']}', {check['score']});
    END IF;""")

        body_str = "\n\n    ".join(one_to_one_body + body_parts)
        declaration_str = "\n    ".join(declarations)

        function_sql = f"""
DROP FUNCTION IF EXISTS {function_name}(integer);
CREATE OR REPLACE FUNCTION {function_name}(p_partner_id integer)
RETURNS jsonb AS $$
DECLARE
    {declaration_str}
BEGIN
    {body_str}

    RETURN result_json;
END;
$$ LANGUAGE plpgsql STABLE;
"""
        return {'name': function_name, 'code': function_sql, 'sql_query': f"SELECT {function_name}(partner_id);", 'universe': universe.name}

    @api.model
    def _build_function_for_single_query(self, plan):
        """
        Builds a dedicated SQL function for a single query plan that has no universe.
        """
        function_name = f"check_{self._slugify(plan.code)}"
        sql = plan.sql_query.strip().rstrip(';').replace('%s', 'p_partner_id')
        analysis = self._get_optimized_check_type(sql)

        function_sql = ''  # Initialize variable to ensure it's always available
        if analysis['type'] == 'risk_score':
            function_body = f"""
DECLARE
    risk_score_val integer;
BEGIN
    risk_score_val := ({sql.replace('LIMIT 1', '')} LIMIT 1);
    IF risk_score_val IS NOT NULL THEN
        RETURN jsonb_build_object('{plan.code}', risk_score_val);
    END IF;
    RETURN '{{}}'::jsonb;
END;
"""
            function_sql = f"""
DROP FUNCTION IF EXISTS {function_name}(integer);
CREATE OR REPLACE FUNCTION {function_name}(p_partner_id integer)
RETURNS jsonb AS $$
{function_body}
$$ LANGUAGE plpgsql STABLE;
"""
        else:  # boolean check
            score = plan.risk_assessment_score or 0
            function_body = f"""
BEGIN
    IF EXISTS({sql}) THEN
        RETURN jsonb_build_object('{plan.code}', {score});
    END IF;
    RETURN '{{}}'::jsonb;
END;
"""
            function_sql = f"""
DROP FUNCTION IF EXISTS {function_name}(integer);
CREATE OR REPLACE FUNCTION {function_name}(p_partner_id integer)
RETURNS jsonb AS $$
{function_body}
$$ LANGUAGE plpgsql STABLE;
"""

        return {'name': function_name, 'code': function_sql, 'sql_query': f"SELECT {function_name}(partner_id);", 'universe': 'Standalone'}

    @api.model
    def _cron_generate_sql_functions(self):
        """Main method to be called by the cron job."""

        plans = self.env['res.compliance.risk.assessment.plan'].search([
            ('state', '=', 'active'),
            ('sql_query', '!=', False)
        ])
        if not plans:
            _logger.warning(
                "CRON: No active compliance plans found. Skipping generation.")
            return

        queries_by_universe = {}
        orphan_queries = []

        for plan in plans:
            if plan.universe_id:
                key = plan.universe_id.id
                if key not in queries_by_universe:
                    queries_by_universe[key] = []
                queries_by_universe[key].append(plan)
            else:
                orphan_queries.append(plan)

        self.search([]).unlink()

        # Process queries grouped by a universe
        all_universes = {
            u.id: u for u in self.env['res.risk.universe'].search([])}
        
        for universe_id, queries in queries_by_universe.items():
            universe = all_universes.get(universe_id)
            if not universe:
                
                continue

            function_data = self._build_function_for_universe(
                universe, queries)
            try:
                self.env.cr.execute(function_data['code'])
                self.env.cr.commit()
                self.create(function_data)
            except Exception as e:
                _logger.error(
                    f"CRON: Failed to create function for universe '{universe.name}': {e}")
                self.env.cr.rollback()

        # Process queries with no universe, creating one function for each
        
        for plan in orphan_queries:
            function_data = self._build_function_for_single_query(plan)
            try:
                self.env.cr.execute(function_data['code'])
                self.env.cr.commit()
                self.create(function_data)
            except Exception as e:
                _logger.error(
                    f"CRON: Failed to create function for standalone plan '{plan.name}': {e}")
                self.env.cr.rollback()



class RiskAnalysisMat(models.Model):
    """
    Highly optimized Materialized Views using pure set-based operations.
    No correlated subqueries, no multiple joins, single aggregation pass.
    """
    _name = 'risk.analysis.mv'
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
            r"where.*?r\.id\s*=\s*%s.*?"
            r"and\s+lower\s*\(\s*a\.(category(?:_description)?)\s*\)\s*=\s*'([^']+)'",
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
            r"from\s+res_partner\s+rp.*?"
            r"join\s+res_partner_region\s+rpr\s+on\s+rp\.region_id\s*=\s*rpr\.id.*?"
            r"where.*?rp\.id\s*=\s*%s.*?"
            r"and\s+lower\s*\(\s*rpr\.name\s*\)\s*=\s*'([^']+)'",
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
            r"from\s+res_partner_account\s+rpa.*?"
            r"join\s+res_branch\s+rb\s+on\s+rpa\.branch_id\s*=\s*rb\.id.*?"
            r"where.*?rpa\.customer_id\s*=\s*%s.*?"
            r"and\s+lower\s*\(\s*trim\s*\(\s*rb\.region\s*\)\s*\)\s*=\s*'([^']+)'",
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
        """
        Builds a single UNION ALL query with one final aggregation.
        Pure set-based operations, no correlated subqueries.
        """
        view_name = f"mv_risk_{self._slugify(universe.code)}"
        
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
            rpa.customer_id AS partner_id,
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
        INNER JOIN res_branch rb ON lower(trim(rb.region)) IN ({', '.join(values_list)})""")
        
        # Handle empty case
        if not union_branches:
            view_sql = f"""
    DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;
    CREATE MATERIALIZED VIEW {view_name} AS
    SELECT
        id AS partner_id,
        name AS partner_name,
        '{{}}'::jsonb AS risk_data
    FROM res_partner
    WHERE FALSE;

    CREATE UNIQUE INDEX idx_{view_name}_partner_id ON {view_name} (partner_id);
    """
            return {
                'name': view_name,
                'code': view_sql,
                'universe': universe.name,
                'stats': f"No patterns matched. Unmatched: {len(unmatched)}"
            }
        
        # Assemble final query with single aggregation
        # Properly join branches with UNION ALL between them
        all_flags_cte = ""
        if union_branches:
            all_flags_cte = union_branches[0]  # First branch without UNION ALL
            for branch in union_branches[1:]:
                all_flags_cte += f"\n    UNION ALL{branch}"  # Add UNION ALL between branches
        
        view_sql = f"""
    DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;

    CREATE MATERIALIZED VIEW {view_name} AS
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
    FROM res_partner rp;

    -- Indexes for performance
    CREATE UNIQUE INDEX idx_{view_name}_partner_id ON {view_name} (partner_id);
    CREATE INDEX idx_{view_name}_risk_data_gin ON {view_name} USING GIN (risk_data);
    CREATE INDEX idx_{view_name}_has_risks ON {view_name} (partner_id) 
        WHERE risk_data != '{{}}'::jsonb;
    """
        
        return {
            'name': view_name,
            'code': view_sql,
            'universe': universe.name,
            'stats': f"Patterns matched: {len(union_branches)}, Unmatched: {len(unmatched)}"
        }
    
    
    @api.model
    def _cron_generate_views(self):
        """Main cron entry point."""
        plans = self.env['res.compliance.risk.assessment.plan'].search([
            ('state', '=', 'active'),
            ('sql_query', '!=', False),
            ('universe_id', '!=', False)
        ])

        queries_by_universe = {}
        for plan in plans:
            queries_by_universe.setdefault(plan.universe_id, []).append(plan)

        self.search([]).unlink()

        for universe, universe_plans in queries_by_universe.items():
            view_data = self._build_optimized_view(universe, universe_plans)
            try:
                with self.pool.cursor() as new_cr:
                    new_cr.execute(view_data['code'])
                    new_cr.commit()
                
                self.create({
                    'name': view_data['name'],
                    'code': view_data['code'],
                    'universe': view_data['universe'],
                    'pattern_stats': view_data.get('stats', '')
                })
                _logger.info(f"✓ Created optimized MV: {view_data['name']}")
                _logger.info(f"  {view_data.get('stats', '')}")
            except Exception as e:
                _logger.error(f"✗ Failed to create view for {universe.name}: {e}")

    def action_refresh_view(self):
        """Refresh view concurrently (non-blocking)."""
        for record in self:
            try:
                with self.pool.cursor() as new_cr:
                    new_cr.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {record.name}")
                    new_cr.commit()
                record.last_refresh = fields.Datetime.now()
                _logger.info(f"✓ Refreshed {record.name}")
            except Exception as e:
                raise models.ValidationError(_("Refresh failed: %s") % str(e))           
            


class RiskAnalysisMat2(models.Model):
    """
    Highly optimized Materialized Views using pure set-based operations.
    No correlated subqueries, no multiple joins, single aggregation pass.
    """
    _name = 'risk.analysis.mv2'
    _description = 'Risk Analysis Materialized View'
    _order = 'universe, name'

    name = fields.Char(string="View Name", required=True, readonly=True)
    code = fields.Text(string='Definition Script', required=True, readonly=True)
    universe = fields.Char(string="Risk Universe", required=True, readonly=True)
    last_refresh = fields.Datetime(string="Last Refreshed", readonly=True)
    pattern_stats = fields.Text(string="Pattern Statistics", readonly=True,
                                help="Breakdown of matched patterns")
    is_universal = fields.Boolean(string="Universe Independent", default=False, readonly=True,
                                 help="Whether this risk applies independently of any universe")

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
            r"where.*?r\.id\s*=\s*%s.*?"
            r"and\s+lower\s*\(\s*a\.(category(?:_description)?)\s*\)\s*=\s*'([^']+)'",
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
            r"from\s+res_partner\s+rp.*?"
            r"join\s+res_partner_region\s+rpr\s+on\s+rp\.region_id\s*=\s*rpr\.id.*?"
            r"where.*?rp\.id\s*=\s*%s.*?"
            r"and\s+lower\s*\(\s*rpr\.name\s*\)\s*=\s*'([^']+)'",
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
                value_condition = "lower(value) in ('yes', 'enrolled')"
            
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
            r"from\s+res_partner_account\s+rpa.*?"
            r"join\s+res_branch\s+rb\s+on\s+rpa\.branch_id\s*=\s*rb\.id.*?"
            r"where.*?rpa\.customer_id\s*=\s*%s.*?"
            r"and\s+lower\s*\(\s*trim\s*\(\s*rb\.region\s*\)\s*\)\s*=\s*'([^']+)'",
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

    def _extract_universal_risk_pattern(self, sql_query, code, risk_score):
        """
        Extract patterns from universe-independent risk queries
        Returns a dictionary with pattern information or None if unrecognized
        """
        sql_lower = sql_query.lower().strip()
        
        # Pattern 1: Multiple occurrences of same phone number
        if "customer_phone in" in sql_lower and "group by customer_phone" in sql_lower and "having count(*) >=" in sql_lower:
            match = re.search(r'having\s+count\(\*\)\s*>=\s*(\d+)', sql_lower)
            threshold = int(match.group(1)) if match else 3
            
            return {
                'type': 'duplicate_phone',
                'code': code,
                'score': risk_score,
                'threshold': threshold
            }
        
        # Pattern 2: Missing or invalid BVN
        if ("bvn is null" in sql_lower or "bvn like" in sql_lower) and "or" in sql_lower:
            return {
                'type': 'invalid_bvn',
                'code': code,
                'score': risk_score
            }
        
        # Pattern 3: Invalid name (empty or starting with non-alphanumeric)
        if ("trim(name) = ''" in sql_lower or "trim(name) ~" in sql_lower) and "or" in sql_lower:
            return {
                'type': 'invalid_name',
                'code': code,
                'score': risk_score
            }
        
        # Pattern 4: No contact information
        if "mobile is null" in sql_lower and "phone is null" in sql_lower and "customer_phone is null" in sql_lower:
            return {
                'type': 'no_contact',
                'code': code,
                'score': risk_score
            }
        
        # Pattern 5: On sanctions list
        if "likely_sanction=true" in sql_lower:
            return {
                'type': 'sanctions',
                'code': code,
                'score': risk_score
            }
        
        # Pattern 6: PEP
        if "is_pep = true" in sql_lower:
            return {
                'type': 'pep',
                'code': code,
                'score': risk_score
            }
        
        # Pattern 7: On watchlist
        if "is_watchlist=true" in sql_lower:
            return {
                'type': 'watchlist',
                'code': code,
                'score': risk_score
            }
        
        # Pattern 8: Default risk rating (if no other patterns match)
        if "risk_rating from res_risk_assessment where is_default=true" in sql_lower:
            return {
                'type': 'default_risk',
                'code': code,
                'score': risk_score
            }
        
        # General pattern for new simple conditions on res_partner
        match = re.search(r"WHERE\s+id\s*=\s*%s\s+AND\s+(.*)", sql_query, re.DOTALL | re.IGNORECASE)
        if match:
            condition = match.group(1).strip()
            # Fix escaped percent signs for LIKE operators
            condition = condition.replace('%%', '%')
            return {
                'type': 'simple_condition',
                'code': code,
                'score': risk_score,
                'condition': condition
            }
        
        return None
    
    def _build_universal_risk_view(self, risk_queries):
        """
        Build materialized view for universe-independent risk queries
        """
        view_name = "mv_risk_universal"
        
        patterns = {
            'duplicate_phone': [],
            'invalid_bvn': [],
            'invalid_name': [],
            'no_contact': [],
            'sanctions': [],
            'pep': [],
            'watchlist': [],
            'default_risk': [],
            'simple_condition': []
        }
        
        unmatched = []
        
        # Parse all the queries and categorize them
        for query_info in risk_queries:
            query, code, score = query_info['query'], query_info['code'], query_info['score']
            pattern = self._extract_universal_risk_pattern(query, code, score)
            
            if pattern:
                patterns[pattern['type']].append(pattern)
            else:
                unmatched.append(code)
        
        # Build the CTE parts
        cte_parts = []
        
        # Duplicate phone numbers
        if patterns['duplicate_phone']:
            for pattern in patterns['duplicate_phone']:
                threshold = pattern.get('threshold', 3)
                cte_parts.append(f"""
        -- Duplicate phone numbers ({threshold}+ occurrences)
        SELECT 
            rp.id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner rp
        JOIN (
            SELECT customer_phone
            FROM res_partner
            WHERE customer_phone IS NOT NULL AND customer_phone != ''
            GROUP BY customer_phone
            HAVING COUNT(*) >= {threshold}
        ) dupes ON rp.customer_phone = dupes.customer_phone
        WHERE rp.customer_phone IS NOT NULL AND rp.customer_phone != ''""")
        
        # Invalid BVN
        if patterns['invalid_bvn']:
            for pattern in patterns['invalid_bvn']:
                cte_parts.append(f"""
        -- Invalid or missing BVN
        SELECT 
            id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner
        WHERE (bvn IS NULL OR bvn LIKE '%[a-zA-Z]%' OR bvn LIKE 'NOBVN%')""")
        
        # Invalid name
        if patterns['invalid_name']:
            for pattern in patterns['invalid_name']:
                cte_parts.append(f"""
        -- Invalid name
        SELECT 
            id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner
        WHERE (trim(name) = '' OR trim(name) ~ '^[^a-zA-Z0-9]')""")
        
        # No contact info
        if patterns['no_contact']:
            for pattern in patterns['no_contact']:
                cte_parts.append(f"""
        -- No contact information
        SELECT 
            id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner
        WHERE mobile IS NULL AND phone IS NULL AND customer_phone IS NULL""")
        
        # Sanctions list
        if patterns['sanctions']:
            for pattern in patterns['sanctions']:
                cte_parts.append(f"""
        -- On sanctions list
        SELECT 
            id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner
        WHERE likely_sanction = TRUE""")
        
        # PEP
        if patterns['pep']:
            for pattern in patterns['pep']:
                cte_parts.append(f"""
        -- Politically exposed person
        SELECT 
            id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner
        WHERE is_pep = TRUE""")
        
        # Watchlist
        if patterns['watchlist']:
            for pattern in patterns['watchlist']:
                cte_parts.append(f"""
        -- On watchlist
        SELECT 
            id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner
        WHERE is_watchlist = TRUE""")
        
        # Simple conditions (for new patterns)
        if patterns['simple_condition']:
            for pattern in patterns['simple_condition']:
                cte_parts.append(f"""
        -- Simple condition: {pattern['code']}
        SELECT 
            id AS partner_id,
            '{pattern['code']}' AS risk_code,
            {pattern['score']} AS risk_score
        FROM res_partner
        WHERE {pattern['condition']}""")
        
        # Default risk (handled specially - applies to all partners who aren't caught by any other criteria)
        default_risk = None
        if patterns['default_risk']:
            # Just take the first one if multiple are defined
            default_risk = patterns['default_risk'][0]
        
        # Handle empty case or only default risk
        if not cte_parts and not default_risk:
            view_sql = f"""
    DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;
    CREATE MATERIALIZED VIEW {view_name} AS
    SELECT
        id AS partner_id,
        name AS partner_name,
        '{{}}'::jsonb AS risk_data
    FROM res_partner
    WHERE FALSE;

    CREATE UNIQUE INDEX idx_{view_name}_partner_id ON {view_name} (partner_id);
    """
            return {
                'name': view_name,
                'code': view_sql,
                'universe': 'Universal Risks',
                'stats': f"No patterns matched. Unmatched: {len(unmatched)}"
            }
        
        # Assemble the final query
        all_flags_cte = ""
        if cte_parts:
            all_flags_cte = cte_parts[0]  # First part without UNION ALL
            for part in cte_parts[1:]:
                all_flags_cte += f"\n    UNION ALL{part}"
        
        # Final query construction
        # If we have a default risk, we need to apply it to partners not caught by other criteria
        if default_risk:
            view_sql = f"""
    DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;
    
    CREATE MATERIALIZED VIEW {view_name} AS
    WITH all_risk_flags AS (
    {all_flags_cte}
    ),
    flagged_partners AS (
        SELECT DISTINCT partner_id 
        FROM all_risk_flags
    ),
    default_risk AS (
        -- Apply default risk to partners not caught by specific criteria
        SELECT
            rp.id AS partner_id,
            '{default_risk['code']}' AS risk_code,
            {default_risk['score']} AS risk_score
        FROM res_partner rp
        WHERE NOT EXISTS (
            SELECT 1 FROM flagged_partners fp
            WHERE fp.partner_id = rp.id
        )
    ),
    combined_risks AS (
        SELECT * FROM all_risk_flags
        UNION ALL
        SELECT * FROM default_risk
    )
    SELECT 
        rp.id AS partner_id,
        rp.name AS partner_name,
        COALESCE(
            (SELECT jsonb_object_agg(risk_code, risk_score)
            FROM combined_risks cr
            WHERE cr.partner_id = rp.id),
            '{{}}'::jsonb
        ) AS risk_data
    FROM res_partner rp;
    
    -- Indexes for performance
    CREATE UNIQUE INDEX idx_{view_name}_partner_id ON {view_name} (partner_id);
    CREATE INDEX idx_{view_name}_risk_data_gin ON {view_name} USING GIN (risk_data);
    CREATE INDEX idx_{view_name}_has_risks ON {view_name} (partner_id) 
        WHERE risk_data != '{{}}'::jsonb;
    """
        else:
            # If no default risk, just use the specific criteria
            view_sql = f"""
    DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;
    
    CREATE MATERIALIZED VIEW {view_name} AS
    WITH all_risk_flags AS (
    {all_flags_cte}
    )
    SELECT 
        rp.id AS partner_id,
        rp.name AS partner_name,
        COALESCE(
            (SELECT jsonb_object_agg(risk_code, risk_score)
            FROM all_risk_flags arf
            WHERE arf.partner_id = rp.id),
            '{{}}'::jsonb
        ) AS risk_data
    FROM res_partner rp;
    
    -- Indexes for performance
    CREATE UNIQUE INDEX idx_{view_name}_partner_id ON {view_name} (partner_id);
    CREATE INDEX idx_{view_name}_risk_data_gin ON {view_name} USING GIN (risk_data);
    CREATE INDEX idx_{view_name}_has_risks ON {view_name} (partner_id) 
        WHERE risk_data != '{{}}'::jsonb;
    """
        
        return {
            'name': view_name,
            'code': view_sql,
            'universe': 'Universal Risks',
            'stats': f"Patterns matched: {len(cte_parts)}, Default risk: {'Yes' if default_risk else 'No'}, Unmatched: {len(unmatched)}"
        }
    
    @api.model
    def _collect_universal_risk_queries(self):
        """
        Collect all universe-independent risk queries from the database
        """
        
        # Here we'd typically do something like:
        no_universe_plans = self.env['res.compliance.risk.assessment.plan'].search([
            ('state', '=', 'active'),
            ('sql_query', '!=', False),
            ('universe_id', '=', False)
        ])
        #
        risk_queries = []
        for plan in no_universe_plans:
            risk_queries.append({
                'query': plan.sql_query,
                'code': plan.code,
                'score': plan.risk_assessment_score or 0
            })
        
        return risk_queries
        
        
    
    @api.model
    def _build_optimized_view(self, universe, plans):
        """
        Builds a single UNION ALL query with one final aggregation.
        Pure set-based operations, no correlated subqueries.
        """
        view_name = f"mv_risk_{self._slugify(universe.code)}"
        
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
            
            # Handle value_condition correctly to avoid syntax errors
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
                    f"        WHEN lower(trim(region)) = '{item['value']}' THEN '{item['code']}'"
                )
                case_whens_score.append(
                    f"        WHEN lower(trim(region)) = '{item['value']}' THEN {item['score']}"
                )
                values_list.append(f"'{item['value']}'")
            
            union_branches.append(f"""
        -- Branch Regions
        SELECT 
            customer_id AS partner_id,
            CASE 
    {chr(10).join(case_whens_code)}
            END AS risk_code,
            CASE 
    {chr(10).join(case_whens_score)}
            END AS risk_score
        FROM (
            SELECT DISTINCT ON (rpa.customer_id)
                rpa.customer_id, 
                rb.region
            FROM res_partner_account rpa
            INNER JOIN res_branch rb ON rpa.branch_id = rb.id
            WHERE lower(trim(rb.region)) IN ({', '.join(values_list)})
            ORDER BY rpa.customer_id, rpa.opening_date DESC
        ) rpa_latest""")
        
        # Handle empty case
        if not union_branches:
            view_sql = f"""
    DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;
    CREATE MATERIALIZED VIEW {view_name} AS
    SELECT
        id AS partner_id,
        name AS partner_name,
        '{{}}'::jsonb AS risk_data
    FROM res_partner
    WHERE FALSE;

    CREATE UNIQUE INDEX idx_{view_name}_partner_id ON {view_name} (partner_id);
    """
            return {
                'name': view_name,
                'code': view_sql,
                'universe': universe.name,
                'stats': f"No patterns matched. Unmatched: {len(unmatched)}"
            }
        
        # Assemble final query with single aggregation
        # Properly join branches with UNION ALL between them
        all_flags_cte = ""
        if union_branches:
            all_flags_cte = union_branches[0]  # First branch without UNION ALL
            for branch in union_branches[1:]:
                all_flags_cte += f"\n    UNION ALL{branch}"  # Add UNION ALL between branches
        
        view_sql = f"""
    DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;

    CREATE MATERIALIZED VIEW {view_name} AS
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
    FROM res_partner rp;

    -- Indexes for performance
    CREATE UNIQUE INDEX idx_{view_name}_partner_id ON {view_name} (partner_id);
    CREATE INDEX idx_{view_name}_risk_data_gin ON {view_name} USING GIN (risk_data);
    CREATE INDEX idx_{view_name}_has_risks ON {view_name} (partner_id) 
        WHERE risk_data != '{{}}'::jsonb;
    """
        
        return {
            'name': view_name,
            'code': view_sql,
            'universe': universe.name,
            'stats': f"Patterns matched: {len(union_branches)}, Unmatched: {len(unmatched)}"
        }
    
    @api.model
    def _cron_generate_views(self):
        """Main cron entry point that generates both universe-specific and universal risk views."""
        # Handle universe-specific risk views
        plans = self.env['res.compliance.risk.assessment.plan'].search([
            ('state', '=', 'active'),
            ('sql_query', '!=', False),
            ('universe_id', '!=', False)
        ])

        queries_by_universe = {}
        for plan in plans:
            queries_by_universe.setdefault(plan.universe_id, []).append(plan)

        # Clear all existing views
        self.search([]).unlink()

        # Generate universe-specific views
        for universe, universe_plans in queries_by_universe.items():
            view_data = self._build_optimized_view(universe, universe_plans)
            try:
                with self.pool.cursor() as new_cr:
                    new_cr.execute(view_data['code'])
                    new_cr.commit()
                
                self.create({
                    'name': view_data['name'],
                    'code': view_data['code'],
                    'universe': view_data['universe'],
                    'pattern_stats': view_data.get('stats', ''),
                    'is_universal': False
                })
                _logger.info(f"✓ Created optimized MV: {view_data['name']}")
                _logger.info(f"  {view_data.get('stats', '')}")
            except Exception as e:
                _logger.error(f"✗ Failed to create view for {universe.name}: {e}")

        # Handle universal (universe-independent) risks
        self._generate_universal_risk_view()
    
    @api.model
    def _generate_universal_risk_view(self):
        """Generate materialized view for universe-independent risk factors."""
        # Get universal risk queries
        universal_risk_queries = self._collect_universal_risk_queries()
        
        # Build the view
        view_data = self._build_universal_risk_view(universal_risk_queries)
        
        try:
            with self.pool.cursor() as new_cr:
                new_cr.execute(view_data['code'])
                new_cr.commit()
            
            self.create({
                'name': view_data['name'],
                'code': view_data['code'],
                'universe': view_data['universe'],
                'pattern_stats': view_data.get('stats', ''),
                'is_universal': True
            })
            _logger.info(f"✓ Created universal risk MV: {view_data['name']}")
            _logger.info(f"  {view_data.get('stats', '')}")
        except Exception as e:
            _logger.error(f"✗ Failed to create universal risk view: {e}")

    def action_refresh_view(self):
        """Refresh view concurrently (non-blocking)."""
        for record in self:
            try:
                with self.pool.cursor() as new_cr:
                    new_cr.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {record.name}")
                    new_cr.commit()
                record.last_refresh = fields.Datetime.now()
                _logger.info(f"✓ Refreshed {record.name}")
            except Exception as e:
                raise ValidationError(_("Refresh failed: %s") % str(e))