from odoo import api, fields, models


class AlertHistory(models.Model):
    _inherit = 'alert.history'

    status = fields.Selection(
        selection_add=[
            ('draft', 'Draft'),
            ('open', 'Open'),
            ('closed', 'Closed'),
            ('overdue', 'Overdue'),
            ('archived', 'Archived'),
        ],
        ondelete={
            'draft': 'set default',
            'open': 'set default',
            'closed': 'set default',
            'overdue': 'set default',
            'archived': 'set default',
        }
    )

    user_in_emails_ = fields.Boolean(
        string="User In Emails",
        compute="_compute_user_in_emails_",
        search="_search_user_in_emails_",
        store=False
    )

    @api.depends('email', 'email_cc')
    def _compute_user_in_emails_(self):
        current_user = self.env.user
        current_email = current_user.email and current_user.email.lower().strip()

        for record in self:
            # First check if this is a Case Manager alert
            is_case_alert = record.source == self.env['case.manager']._description

            # Skip further checks if not a case alert
            if not is_case_alert:
                record.user_in_emails = False
                continue

            in_to_field = False
            in_cc_field = False

            # Check primary email field
            if record.email and current_email:
                emails_list = [email.lower().strip()
                               for email in record.email.split(',')]
                in_to_field = current_email in emails_list

            # Check CC email field
            if record.email_cc and current_email:
                cc_emails_list = [email.lower().strip()
                                  for email in record.email_cc.split(',')]
                in_cc_field = current_email in cc_emails_list

            # Only mark as True if it's a case alert AND user's email is found
            record.user_in_emails_ = is_case_alert and (
                in_to_field or in_cc_field)

    def _search_user_in_emails_(self, operator, value):
        if operator not in ('=', '!=') or not isinstance(value, bool):
            return []

        current_user = self.env.user
        current_email = current_user.email

        if not current_email:
            return [('id', '=', False)]  # No matches if user has no email

        # Email pattern for matching
        email_pattern = f"%{current_email}%"
        
        source_desc = self.env['case.manager']._description

        # For positive case (show records where user is in emails AND source is Case Manager)
        if (operator == '=' and value) or (operator == '!=' and not value):
            domain = [
                ('source', '=', source_desc),
                '|',
                ('email', 'ilike', email_pattern),
                ('email_cc', 'ilike', email_pattern)
            ]
        # For negative case (show records where user is NOT in emails OR source is NOT Case Manager)
        else:
            domain = [
                '|',
                ('source', '!=', source_desc),
                '&',
                ('email', 'not ilike', email_pattern),
                ('email_cc', 'not ilike', email_pattern)
            ]

        return domain
