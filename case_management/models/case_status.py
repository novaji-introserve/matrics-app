from odoo import models, fields, api

class CaseStatus(models.Model):
    _name = 'case.status'
    _description = 'Case Status'

    name = fields.Selection([
        ('open', 'Open'),
        ('closed', 'Closed'),
        ('overdue', 'Overdue')
    ], string='Status', required=True)

    slug = fields.Char(string='Slug', compute='_compute_slug', store=True)
    created_at = fields.Char(string='Created At', size=4000, required=False)
    updated_at = fields.Char(string='Updated At', size=4000, required=False)
    description = fields.Char(string='Description', size=4000, required=False)

    @api.depends('name')
    def _compute_slug(self):
        for record in self:
            record.slug = record.name if record.name else ''










# from odoo import models, fields, api

# class CaseStatus(models.Model):
#     _name = 'case.status'
#     _description = 'Case Status'

#     #name = fields.Char(string='Name', required=False, readonly=True)
#     name = fields.Selection([
#         ('open', 'Open'),
#         ('closed', 'Closed'),
#         ('overdue', 'Overdue')
#     ], string='Status', compute='_compute_selection_name', store=True, readonly=True)
    
#     # selection_name = fields.Selection([
#     #     ('open', 'Open'),
#     #     ('closed', 'Closed'),
#     #     ('overdue', 'Overdue')
#     # ], string='Status', compute='_compute_selection_name', store=True, readonly=True)
    
#     #treated_status=fields.Boolean(default=False)

#     slug = fields.Char(string='Slug', compute='_compute_slug', store=True)
#     created_at = fields.Char(string='Created At', size=4000, required=False)
#     updated_at = fields.Char(string='Updated At', size=4000, required=False)
#     description = fields.Char(string='Description', size=4000, required=False)

#     # @api.depends('selection_name')
#     # def _compute_slug(self):
#     #     for record in self:
#     #         record.slug = record.selection_name if record.selection_name else ''

#     # @api.depends('name')
#     # def _compute_selection_name(self):
#     #     # This will populate the selection_name based on the stored name
#     #     name_map = dict(self._fields['selection_name'].selection)
#     #     for record in self:
#     #         record.selection_name = record.name if record.name in name_map else False













# # from odoo import models, fields, api

# # class CaseStatus(models.Model):
# #     _name = 'case.status'
# #     _description = 'Case Status'

# #     name = fields.Selection([
# #         ('open', 'Open'),
# #         ('closed', 'Closed'),
# #         ('overdue', 'Overdue')
# #     ], string='Name', required=False)
    
# #     slug = fields.Char(string='Slug', compute='_compute_slug', store=True)
# #     created_at = fields.Char(string='Created At', size=4000, required=False)
# #     updated_at = fields.Char(string='Updated At', size=4000, required=False)
# #     description = fields.Char(string='Description', size=4000, required=False)
    
# #     @api.depends('name')
# #     def _compute_slug(self):
# #         for record in self:
# #             record.slug = record.name if record.name else ''








# # # from odoo import models, fields

# # # class CaseStatus(models.Model):
# # #     _name = 'case.status'
# # #     _description = 'Case Status'

# # #     name = fields.Char(string='Name', size=4000, required=False)
# # #     slug = fields.Char(string='Slug', size=4000, required=False)
# # #     created_at = fields.Char(string='Created At', size=4000, required=False)
# # #     updated_at = fields.Char(string='Updated At', size=4000, required=False)
# # #     description = fields.Char(string='Description', size=4000, required=False)
