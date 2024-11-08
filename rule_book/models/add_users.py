# models/import_extension.py
from odoo import models, fields, api, _
from odoo.exceptions import UserError
import logging
_logger = logging.getLogger(__name__)


class CustomImport(models.TransientModel):
    _inherit = 'base_import.import'

    def do(self, fields, columns, options, dryrun=False):
        if self.res_model == 'res.users':
            try:
                # Get the imported data
                import_data = self.env['base_import.import'].load_rows(
                    self.file, self.file_name)
                
                # Find the mapped field indices
                field_mapping = dict(zip(fields, columns))
                name_field = 'name'  # Map to 'OFFICER RESPONSIBLE'
                login_field = 'login'  # Map to 'OFFICER EMAIL'
                
                # Get indices from the mapping
                name_idx = columns.index(field_mapping[name_field])
                login_idx = columns.index(field_mapping[login_field])
                
                for row in import_data:
                    if len(row) > max(name_idx, login_idx):
                        officer_names = row[name_idx].split(',') if row[name_idx] else []
                        officer_emails = row[login_idx].split(',') if row[login_idx] else []
                        
                        if len(officer_names) == len(officer_emails):
                            for username, email in zip(officer_names, officer_emails):
                                username = username.strip()
                                email = email.strip()
                                
                                if username and email and not self.env['res.users'].search([('login', '=', email)]):
                                    self.env['res.users'].sudo().create({
                                        'name': username,
                                        'login': email,
                                        'email': email,
                                        'password': 'temp123',
                                        'groups_id': [(6, 0, [self.env.ref('base.group_user').id])],
                                    })
                            
            except Exception as e:
                _logger.error(f"Import error: {str(e)}")
                raise UserError(_(f"Import failed: {str(e)}"))

        return super(CustomImport, self).do(fields, columns, options, dryrun)

# class CustomImport(models.TransientModel):
#     _inherit = 'base_import.import'

#     def parse_preview(self, options, count=10):
#         res = super(CustomImport, self).parse_preview(options, count)
#         return res

#     def do(self, fields, columns, options, dryrun=False):
#         if self.res_model == 'res.users':
#             # Get the preview data first
#             preview_data = super(CustomImport, self).parse_preview(options)
#             if preview_data and 'headers' in preview_data:
#                 try:
#                     # Find the column indices
#                     officer_idx = preview_data['headers'].index('OFFICER RESPONSIBLE')
#                     email_idx = preview_data['headers'].index('OFFICER EMAIL')
                    
#                     # Get the actual data
#                     import_data = self.env['base_import.import'].load_rows(
#                         self.file, self.file_name)
                    
#                     for row in import_data:
#                         if len(row) > max(officer_idx, email_idx):
#                             officer_names = row[officer_idx].split(',') if row[officer_idx] else []
#                             officer_emails = row[email_idx].split(',') if row[email_idx] else []
                            
#                             if len(officer_names) == len(officer_emails):
#                                 for username, email in zip(officer_names, officer_emails):
#                                     username = username.strip()
#                                     email = email.strip()
                                    
#                                     if username and email and not self.env['res.users'].search([('login', '=', email)]):
#                                         self.env['res.users'].sudo().create({
#                                             'name': username,
#                                             'login': email,
#                                             'email': email,
#                                             'password': 'temp123',
#                                             'groups_id': [(6, 0, [self.env.ref('base.group_user').id])],
#                                         })
#                 except Exception as e:
#                     _logger.error(f"Import error: {str(e)}")
#                     raise UserError(_(f"Import failed: {str(e)}"))

#         return super(CustomImport, self).do(fields, columns, options, dryrun)
