from odoo import _, api, fields, models, tools
import logging
import uuid

_logger = logging.getLogger(__name__)


class PepCustomer(models.Model):
    _name = 'res.customer.pep'
    _auto = False
    _description = 'Customers matching PEP'
    _sql_constraints = [
        ('uniq_unique_id', 'unique(unique_id)',
         "PEP already exists. PEP must be unique!"),
    ]

    customer_id = fields.Many2one('res.partner', string='Customer',tracking=True)
    branch_id = fields.Many2one('res.branch', string='Branch', tracking=True)
    firstname = fields.Char(string='Firstname', tracking=True)
    lastname = fields.Char(string='Lastname', tracking=True)
    internal_category = fields.Char(string='Internal Category', tracking=True)
    name = fields.Char(string='Name', tracking=True)
    pep_id = fields.Integer(string='PEP ID')
    is_pep = fields.Boolean(string="Is PEP")
    unique_id = fields.Char(string='Unique Identifier', tracking=True, default=lambda self: str(uuid.uuid4()), readonly=True,  copy=False)
    position = fields.Text(string='Position', tracking=True)

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute(f"""
            CREATE OR REPLACE VIEW {self._table} AS (
                SELECT 
                    c.id AS id,
                    c.id AS customer_id,
                    c.branch_id,
                    c.firstname,
                    c.lastname,
                    c.internal_category,
                    c.name,
                    c.global_pep_id AS pep_id,
                    c.is_pep
                FROM res_partner c
                WHERE c.is_pep = TRUE
            )
        """)

    def action_view_customer_pep(self):
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = []
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id])  ]
        return {
            'name': _('Customers In Pep List'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.pep',
            'view_mode': 'tree,form',
            'domain': domain,
            # 'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id])],
            'context': {'search_default_group_branch': 1}
        }
    
    # def action_mark_pep(self):
    #     for e in self:
    #         customers = self.env['res.partner'].search([('id','=',e.id)])
    #         for c in customers:
    #             try:
    #                 c.write({'is_pep':True,'global_pep':True,'global_pep_id':e.pep_id})
    #                 c.action_compute_risk_score_with_plan()
    #             except:
    #                 pass
    