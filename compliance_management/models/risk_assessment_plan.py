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
                    'plan_code': query_obj.code
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
                body_parts.append(f"""
    IF EXISTS ({sql}) THEN
        result_json := result_json || jsonb_build_object('{query_obj.code}', true);
    END IF;""")

        # Process the grouped one-to-one checks at the start of the function body
        one_to_one_body = []
        for group_info in one_to_one_groups.values():
            one_to_one_body.append(
                f"{group_info['fetch_sql']} INTO {group_info['variable_name']};")
            for check in group_info['checks']:
                one_to_one_body.append(f"""
    IF COALESCE({group_info['variable_name']}, '') = '{check['value']}' THEN
        result_json := result_json || jsonb_build_object('{check['plan_code']}', true);
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
            function_body = f"""
BEGIN
    IF EXISTS({sql}) THEN
        RETURN jsonb_build_object('{plan.code}', true);
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
        _logger.info(
            "CRON: Starting generation of risk analysis SQL functions.")

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
                _logger.warning(
                    f"CRON: Skipping queries for non-existent universe ID {universe_id}")
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
        _logger.info(
            f"CRON: Found {len(orphan_queries)} standalone queries to process.")
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

