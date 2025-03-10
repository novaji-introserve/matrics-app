from odoo import models, fields, api
from odoo.exceptions import ValidationError

class UserImport(models.Model):
    _name = 'user.import'
    _description = 'User Import Model'
    
    # Comma-separated fields for names and emails
    names = fields.Text(string="Usernames", help="Comma-separated list of usernames")
    emails = fields.Text(string="Emails", help="Comma-separated list of emails")
    
    @api.model
    def create_or_update_users(self):
        """ This method will create or update users from the names and emails """
        # Fetch the comma-separated lists from the model's fields
        names = self.names or ''
        emails = self.emails or ''
        
        # Convert the strings to lists by splitting at commas
        name_list = [name.strip() for name in names.split(',')]
        email_list = [email.strip() for email in emails.split(',')]
        
        if len(name_list) != len(email_list):
            raise ValidationError("The number of usernames must match the number of emails.")
        
        created_users = []
        
        # Create or update users
        for name, email in zip(name_list, email_list):
            # Search for an existing user by email
            user = self.env['res.users'].search([('email', '=', email)], limit=1)
            
            if not user:
                # If the user doesn't exist, create a new user
                user = self.env['res.users'].create({
                    'name': name,
                    'login': email,  # Set the email as the login
                    'email': email,
                    'groups_id': self.env.ref('base.group_user').id,  # Assign user to the basic user group (adjust if necessary)
                })
                created_users.append(user)
            else:
                # If user exists, update user name (optional)
                user.write({
                    'name': name,
                })
                created_users.append(user)
        
        # Return the created or updated users (for logging or further actions)
        return created_users
