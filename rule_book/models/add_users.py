import logging
import re
from odoo import models, _ 
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

class CustomImport(models.TransientModel):
    _inherit = 'base_import.import'

    DEFAULT_PASSWORD = 'temp123'

    def do(self, fields, columns, options, dryrun=False):
        _logger.info("CustomImport.do method started")
        _logger.info("code is available here")
        if self.res_model == 'res.users':

            created_users = []
            skipped_users = []
            _logger.info("code is available here")

            return

            try:
                # Get the imported data
                import_data = self.env['base_import.import'].load_rows(self.file, self.file_name)

                # Find the mapped field indices
                field_mapping = dict(zip(fields, columns))
                name_field = 'officer responsible'  # Column for officer names
                email_field = 'officer email'  # Column for officer emails

                name_idx = columns.index(field_mapping[name_field])
                email_idx = columns.index(field_mapping[email_field])

                for row_index, row in enumerate(import_data, start=1):
                    # Extract names and emails
                    officer_names = row[name_idx].split(',') if row[name_idx] else []
                    officer_emails = row[email_idx].split(',') if row[email_idx] else []

                    # Strip whitespace from names and emails
                    officer_names = [name.strip() for name in officer_names]
                    officer_emails = [email.strip() for email in officer_emails]

                    # Ensure the lengths match
                    if len(officer_names) != len(officer_emails):
                        skipped_users.append({
                            'row': row_index,
                            'reason': 'Number of names does not match number of emails'
                        })
                        continue

                    # Create users based on the paired names and emails
                    for username, email in zip(officer_names, officer_emails):
                        if not username or not email:
                            continue

                        # Validate email format
                        email_regex = r'^[\w\.-]+@[\w\.-]+\.\w+$'
                        if not re.match(email_regex, email):
                            skipped_users.append({
                                'row': row_index,
                                'username': username,
                                'email': email,
                                'reason': 'Invalid email format'
                            })
                            continue

                        # Check if the user already exists
                        existing_user = self.env['res.users'].sudo().search([
                            '|',
                            ('login', '=', email),
                            ('email', '=', email)
                        ])

                        if existing_user:
                            skipped_users.append({
                                'row': row_index,
                                'username': username,
                                'email': email,
                                'reason': 'User already exists'
                            })
                            continue

                        try:
                            # Create user
                            new_user = self.env['res.users'].sudo().create({
                                'name': username,
                                'login': email,
                                'email': email,
                                'password': self.DEFAULT_PASSWORD,
                                'groups_id': [(6, 0, [self.env.ref('base.group_user').id])],
                            })
                            created_users.append({
                                'row': row_index,
                                'username': username,
                                'email': email
                            })
                        except Exception as user_error:
                            skipped_users.append({
                                'row': row_index,
                                'username': username,
                                'email': email,
                                'reason': str(user_error)
                            })

                # Prepare result message
                message = []
                message.append(f"Successfully created {len(created_users)} users")
                if skipped_users:
                    message.append("\nSkipped users:")
                    for skip in skipped_users:
                        message.append(
                            f"\nRow {skip['row']}: {skip['username']} ({skip['email']}) - {skip['reason']}"
                        )

                # Log the results
                _logger.info("\n".join(message))

                # Show user-friendly message
                if not dryrun:
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'title': _('Import Complete'),
                            'message': "\n".join(message),
                            'type': 'info',
                            'sticky': True,
                        }
                    }

            except Exception as e:
                _logger.error(f"Import error: {str(e)}")
                raise UserError(_(f"Import failed: {str(e)}"))
            
            
# class CustomImport(models.TransientModel):
#     _inherit = 'base_import.import'

#     def do(self, fields, columns, options, dryrun=False):
#         if self.res_model == 'res.users':
#             try:
#                 # Get the imported data
#                 import_data = self.env['base_import.import'].load_rows(
#                     self.file, self.file_name)
                
#                 # Find the mapped field indices
#                 field_mapping = dict(zip(fields, columns))
#                 name_field = 'name'  # Map to 'OFFICER RESPONSIBLE'
#                 login_field = 'login'  # Map to 'OFFICER EMAIL'
                
#                 # Get indices from the mapping
#                 name_idx = columns.index(field_mapping[name_field])
#                 login_idx = columns.index(field_mapping[login_field])
                
#                 for row in import_data:
#                     if len(row) > max(name_idx, login_idx):
#                         officer_names = row[name_idx].split(',') if row[name_idx] else []
#                         officer_emails = row[login_idx].split(',') if row[login_idx] else []
                        
#                         if len(officer_names) == len(officer_emails):
#                             for username, email in zip(officer_names, officer_emails):
#                                 username = username.strip()
#                                 email = email.strip()
                                
#                                 if username and email and not self.env['res.users'].search([('login', '=', email)]):
#                                     self.env['res.users'].sudo().create({
#                                         'name': username,
#                                         'login': email,
#                                         'email': email,
#                                         'password': 'temp123',
#                                         'groups_id': [(6, 0, [self.env.ref('base.group_user').id])],
#                                     })
                            
#             except Exception as e:
#                 _logger.error(f"Import error: {str(e)}")
#                 raise UserError(_(f"Import failed: {str(e)}"))

#         return super(CustomImport, self).do(fields, columns, options, dryrun)

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
