# -*- coding: utf-8 -*-

# from odoo import models, fields, api


# class global_pep_list_web_scrapper(models.Model):
#     _name = 'global_pep_list_web_scrapper.global_pep_list_web_scrapper'
#     _description = 'global_pep_list_web_scrapper.global_pep_list_web_scrapper'

#     name = fields.Char()
#     value = fields.Integer()
#     value2 = fields.Float(compute="_value_pc", store=True)
#     description = fields.Text()
#
#     @api.depends('value')
#     def _value_pc(self):
#         for record in self:
#             record.value2 = float(record.value) / 100
