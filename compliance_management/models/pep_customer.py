from odoo import _, api, fields, models, tools


class PepCustomer(models.Model):
    _name = 'res.customer.pep'
    _auto = False
    _description = 'Customers matching PEP'

    customer_id = fields.Char(string='Customer ID')
    branch_id = fields.Many2one('res.branch', string='Branch')
    firstname = fields.Char(string='Firstname')
    lastname = fields.Char(string='Lastname')
    unique_identifier = fields.Char(string='Unique Identifier')
    internal_category = fields.Char(string='Internal Category')
    name = fields.Char(string='Name')
    pep_id = fields.Integer(string='Pep ID')


    def init(self):
        tools.drop_view_if_exists(self._cr, 'res_customer_pep')
        self._cr.execute("""
        create or replace view res_customer_pep as (
            select c.id as id, c.customer_id, c.branch_id, c.firstname, c.lastname,
            c.internal_category, p.unique_identifier, c.name, p.id pep_id
            from res_partner c 
            join res_pep p
            on lower(c.firstname) = lower(p.first_name) and lower(c.lastname) = lower(p.surname)
            where c.global_pep_id is null and c.is_pep = True
        )""")

    def action_view_customer_pep(self):
        # domain = [
        #     ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
        #     ('internal_category', '=', 'customer')
        # ]

        return {
            'name': _('Customers matching PEP'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree',
            'domain': [('is_pep', '=', True)],
            # 'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id])],
            'context': {'search_default_group_branch': 1}
        }
    
    def action_mark_pep(self):
        for e in self:
            customers = self.env['res.partner'].search([('id','=',e.id)])
            for c in customers:
                try:
                    c.write({'is_pep':True,'global_pep':True,'global_pep_id':e.pep_id})
                    c.action_compute_risk_score_with_plan()
                except:
                    pass
            
        
