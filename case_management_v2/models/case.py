import re
from odoo import models, fields, api, _
from datetime import timedelta, datetime, time
from dateutil.relativedelta import relativedelta
from odoo.exceptions import UserError
import logging
from odoo.tools import format_date
from odoo.exceptions import AccessError
from odoo.http import request
import pytz
import os
import json
import traceback

from dotenv import load_dotenv

load_dotenv()
_logger = logging.getLogger(__name__)


class CaseManager(models.Model):
    _name = 'case.manager'
    _description = 'Case Manager'
    _order = 'id desc'
    _rec_name = "case_ref"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    case_ref = fields.Char(string="Case Reference",
                           required=True, index=True, default=lambda self: 'New', copy=False)

    active = fields.Boolean(string='Active', default=True, tracking=True)

    document = fields.Binary(
        string="Attached Document",
        help="Any document (e.g., image, doc, xlsx) attached to the reply.",
        tracking=True,
        store=True,
        attachment=True,
        copy=False,
        index=True
    )

    document_filename = fields.Char(
        string="Document Filename", tracking=True, store=True,  copy=False)

    document_url = fields.Char(
        string="Document URL", compute="_compute_document_url", store=False)

    case_status = fields.Selection(
        [
            ("draft", "Draft"),
            ("open", "Open"),
            ("closed", "Closed"),
            ("overdue", "OverDue"),
            ("archived", "Archived"),
        ],
        string="Case Status",
        help="The current status of the related case.",
        tracking=True,
        copy=False,
        index=True,
        required=True,
        default='draft',
    )

    case_rating = fields.Selection(
        [
            ("low", "Low"),
            ("medium", "Medium"),
            ("high", "High"),
        ],
        string="Case Rating",
        help="case rating.",
        tracking=True,
        index=True,
        required=True
    )

    case_score = fields.Float(
        string='Risk Score',
        digits=(10, 2), )

    supervisors = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        string="Supervisors(s)",
        tracking=True,
        help="Select supervisors to copy for this case.",
        required=True
    )

    officer_responsible = fields.Many2one(
        'res.users', string='Officer Responsible', required=True)

    transaction_id = fields.Many2one(
        comodel_name='res.customer.transaction',
        string='Transaction Reference',
        index=True,
        ondelete='cascade'
    )
    narration = fields.Text(string='Narration', required=False, tracking=True)

    description = fields.Text(string='Further Description', tracking=True)

    customer_id = fields.Many2one(
        'res.partner', string='Customer', required=False, domain="[('origin', 'in', ['demo', 'test', 'prod'])]")

    response_ids = fields.One2many(
        'case.response.', 'case_id', string='Responses', tracking=True)

    event_date = fields.Datetime(
        string='Event Date', required=True, tracking=True, default=datetime.now().replace(microsecond=0))

    process_category = fields.Many2one(
        'exception.category.', string='Exception Process Type', required=True, index=True)
    process = fields.Many2one(
        'exception.process.', string='Exception Process', required=True, index=True)

    cases_action = fields.Text(
        string='Required Action', required=True, tracking=True)

    department_id = fields.Many2one(
        "hr.department",
        string="Department",
        default=lambda self: self.env.user.department_id.id,
        help="The user department",
        tracking=True,
        store=True,
    )
    is_responsible = fields.Boolean(
        string='Is Responsible',
        compute='_compute_is_responsible',
        store=False
    )
    new_response = fields.Text(
        string='Add Response',
        tracking=False,
        store=False,  # Not stored in database
        copy=False,
        help="Add a new response to this case. Will be cleared after saving."
    )
    can_close_case = fields.Boolean(
        string='Can Close Case',
        compute='_compute_can_close_case',
        store=False
    )

    close_remarks = fields.Text(
        string='Closure Remarks',
        tracking=True,
        copy=False,
        help="Provide remarks about why this case is being closed."
    )

    show_closure_fields = fields.Boolean(
        string='Show Closure Fields',
        compute='_compute_show_closure_fields',
        store=False
    )

    overdue_alert_message = fields.Html(
        string="Overdue Alert",
        compute="_compute_overdue_alert_message",
        store=False
    )

    @api.depends('document', 'write_date')  # Add write_date dependency
    def _compute_document_url(self):
        base_url = self.env['ir.config_parameter'].sudo(
        ).get_param('web.base.url')
        for record in self:
            if record.document:
                attachment = self.env['ir.attachment'].search([
                    ('res_model', '=', self._name),
                    ('res_id', '=', record.id),
                    ('res_field', '=', 'document')
                ], limit=1, order='create_date desc')  # Add order to get the latest
                if attachment:
                    # Add timestamp to prevent caching
                    timestamp = fields.Datetime.now().strftime('%Y%m%d%H%M%S')
                    record.document_url = f'{base_url}/web/content/{attachment.id}?download=false&t={timestamp}'
                else:
                    record.document_url = False
            else:
                record.document_url = False

    @api.model_create_multi
    def create(self, vals_list):
        """Create a new case record"""
        for vals in vals_list:            
            if vals.get('case_ref', 'New') == 'New':
                vals['case_ref'] = self.env['ir.sequence'].next_by_code(
                    'case.manager')
                if not vals['case_ref']:
                    raise ValueError(
                        "Sequence 'case.manager' is not configured correctly.")

        result = super().create(vals_list)

        return result

    
    def write(self, vals):
        # Check if new_response has content and user is responsible
        if 'new_response' in vals and vals.get('new_response') and self.is_responsible and self.case_status != 'closed':
            response_content = vals.get('new_response')

            # Create a response
            self.env['case.response.'].create({
                'case_id': self.id,
                'response': response_content,
            })
            # Clear the new_response field after saving
            vals['new_response'] = False
            self._send_case_response_alert(response_content)

        # Track officer_responsible changes before write
        officer_changed_records = []
        if 'officer_responsible' in vals:
            for record in self:
                if record.officer_responsible.id != vals['officer_responsible']:
                    officer_changed_records.append(record)

        result = super(CaseManager, self).write(vals)

        # Send creation alert to newly assigned officer responsible
        for record in officer_changed_records:
            if record.case_status not in ('draft', 'archived'):
                record._send_case_creation_alert()

        return result
    
    def action_view_document(self):
        self.ensure_one()
        if not self.document:
            return

        attachment = self.env['ir.attachment'].search([
            ('res_model', '=', self._name),
            ('res_id', '=', self.id),
            ('res_field', '=', 'document')
        ], limit=1, order='create_date desc')

        if not attachment:
            return

        # Add timestamp to prevent caching
        timestamp = fields.Datetime.now().strftime('%Y%m%d%H%M%S')
        return {
            'type': 'ir.actions.act_url',
            'url': f'/web/content/{attachment.id}?download=false&t={timestamp}',
            'target': 'new',
        }

    @api.depends('officer_responsible')
    def _compute_is_responsible(self):
        for record in self:
            record.is_responsible = record.officer_responsible.id == self.env.user.id

    def action_save_and_stay(self):
        # This will save the record and return a special action to reload the same form
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'case.manager',
            'res_id': self.id,
            'view_mode': 'form',
            'view_type': 'form',
            'views': [(False, 'form')],
            'target': 'new',  # Keep it as a dialog
            # Set to readonly since this is now a saved record
            'flags': {'mode': 'readonly'},
            'context': self.env.context,
        }

    @api.onchange('process_category')
    def _onchange_process_category(self):
        if self.process_category:
            domain = [('type_id', '=', self.process_category.id)]
            return {'domain': {'process': domain}}
        return {'domain': {'process': []}}

    def action_close_case(self):
        """Close the case - only available when responses exist"""
        self.ensure_one()
        if not self.response_ids:
            raise UserError("Cannot close case without any responses.")

        if self.create_uid.id == self.env.user.id:
            if not self.close_remarks or not self.close_remarks.strip():
                raise UserError(
                    "Reason For Closure is required when closing a case.")

        self.case_status = 'closed'
        self._send_case_closure_alert()

        return True

    def action_open_case(self):
        """Open a the case - only available when case is in draft"""
        self.ensure_one()

        self.case_status = 'open'
        self._send_case_creation_alert()

        return True

    def action_archive_case(self):
        """Archive case - available to anyone if case has been closed"""
        self.ensure_one()
        if self.case_status != 'closed':
            raise UserError("Cannot archive a case that has not been closed.")

        self.case_status = 'archived'
        self.active = False
        return True

    @api.depends('create_uid', 'response_ids', 'case_status')
    def _compute_can_close_case(self):
        """Check if current user can close the case"""
        for record in self:
            record.can_close_case = (
                record.create_uid.id == self.env.user.id and  # User is creator
                bool(record.response_ids) and  # Has responses
                record.case_status != 'closed' and  # Not already closed
                record.case_status != 'archived'  # Not already closed
            )

    def _send_case_creation_alert(self):
        """Send an alert email for case creation"""
        self.ensure_one()

        # Get email template
        template = self.env.ref(
            'case_management_v2.case_creation_alert_template')
        if not template:
            return

        from_email = self.env['ir.config_parameter'].sudo(
        ).get_param('mail.default.from')

        _logger.info(
            f"Retrieved mail.default.from in ir.config value: '{from_email}'")

        if not from_email:
            from_email = "developer@novajii.com"
            _logger.info(f"Using hardcoded fallback email: '{from_email}'")

        base_url = self.env['ir.config_parameter'].sudo(
        ).get_param('web.base.url')

        try:
            action = self.env.ref(
                'case_management_v2.action_open_cases')
            record_url = f"{base_url}/web#id={self.id}&action={action.id}&model=case.manager&view_type=form"
        except Exception as e:
            _logger.warning(f"Could not get action reference: {e}")
            # record_url = f"{base_url}/web#id={self.id}&model=res.partner.screening.result&view_type=form"

        # Prepare email values
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        ctx = {
            'event_date': self.event_date,
            'case_ref': self.case_ref,
            'alert_id': self.case_ref,
            'department_id': self.department_id.name if self.department_id else '',
            'case_rating': self.case_rating,
            'case_status': self.case_status,
            'case_score': self.case_score,
            'exception_category': self.process_category.name if self.process_category else '',
            'exception_process': self.process.name if self.process else '',
            'case_action': self.cases_action,
            'description': self.description or '',
            'company_logo': self.get_company_logo(base_url),
            'datetime': current_time,
            'record_url': record_url,

        }

        # Prepare mail values - to officer responsible, CC creator & supervisors
        if not self.officer_responsible or not self.officer_responsible.email:
            _logger.warning(
                f"Officer responsible {self.officer_responsible.name} has no email address")

            raise UserError(
                _("Officer responsible {self.officer_responsible.name} has no email address"))

        # Prepare mail values - to officer responsible, CC creator & supervisors
        mail_values = {
            'email_to': self.officer_responsible.email,
            'email_cc': '',
            'email_from': from_email,
            'attachment_ids': []
        }

        # Add creator to CC if not the officer responsible and has email
        cc_emails = []
        if (self.create_uid and
            self.create_uid.id != self.officer_responsible.id and
                self.create_uid.email):  # Check all conditions before append
            cc_emails.append(self.create_uid.email)

        # Add supervisors to CC
        for supervisor in self.supervisors:
            if (supervisor.id != self.officer_responsible.id and
                supervisor.email and  # Check email exists and is not False
                    supervisor.email not in cc_emails):
                cc_emails.append(supervisor.email)

        # Filter out any False/None values just in case
        cc_emails = [email for email in cc_emails if email]

        if cc_emails:
            mail_values['email_cc'] = ','.join(cc_emails)

        # Add document attachment if present
        if self.document and self.document_filename:
            attachment_data = {
                'name': self.document_filename,
                'datas': self.document,
                'res_model': self._name,
                'res_id': self.id,
            }
            attachment = self.env['ir.attachment'].create(attachment_data)
            mail_values['attachment_ids'] = [(4, attachment.id)]

        # Render the email content
        template_id = template.with_context(**ctx)
        rendered_html = template_id._render_template(
            template_id.body_html,
            template_id.model,
            [self.id],
            engine='qweb',
            add_context=ctx
        )[self.id]

        # Send the email
        email_result = template_id.send_mail(
            self.id,
            force_send=True,
            email_values=mail_values
        )

        # Save to alert history
        self._save_alert_to_history(
            email_result,
            rendered_html,
            'Case Creation Alert',
            mail_values
        )

    def _send_case_response_alert(self, response_content):
        """Send an alert email for case responses"""
        self.ensure_one()

        # Get email template
        template = self.env.ref(
            'case_management_v2.case_response_alert_template')
        if not template:
            return

        # Get the creator, responder information
        creator = self.create_uid
        responder = self.env.user

        from_email = self.env['ir.config_parameter'].sudo(
        ).get_param('mail.default.from')

        _logger.info(
            f"Retrieved mail.default.from in ir.config value: '{from_email}'")

        if not from_email:
            from_email = "developer@novajii.com"
            _logger.info(f"Using hardcoded fallback email: '{from_email}'")

        base_url = self.env['ir.config_parameter'].sudo(
        ).get_param('web.base.url')

        try:
            action = self.env.ref(
                'case_management_v2.action_open_cases')
            record_url = f"{base_url}/web#id={self.id}&action={action.id}&model=case.manager&view_type=form"
        except Exception as e:
            _logger.warning(f"Could not get action reference: {e}")
            record_url = f"{base_url}/web#id={self.id}&model=case.manager&view_type=form"

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Prepare email values
        ctx = {
            'event_date': fields.Datetime.now(),
            'case_ref': self.case_ref,
            'department_id': self.department_id.name if self.department_id else '',
            'case_rating': self.case_rating,
            'case_tatus': self.case_status,
            'process_category': self.process_category.name if self.process_category else '',
            'process': self.process.name if self.process else '',
            'case_action': self.cases_action,
            'response': response_content,
            'case_score': self.case_score,
            'creator_name': creator.name,
            'creator_email': creator.email,
            'responder_name': responder.name,
            'responder_email': responder.email,
            'record_url': record_url,
            'datetime': current_time,
            'company_logo': self.get_company_logo(base_url),

        }

        # Prepare mail values - to creator, CC supervisors
        mail_values = {
            'email_to': creator.email,
            'email_cc': '',
            'email_from': from_email,

        }

        # Add creator to CC if not the officer responsible and has email
        cc_emails = []
        if (self.create_uid and
            self.create_uid.id != self.officer_responsible.id and
                self.create_uid.email):  # Check all conditions before append
            cc_emails.append(self.create_uid.email)

        # Add supervisors to CC
        for supervisor in self.supervisors:
            if (supervisor.id != self.officer_responsible.id and
                supervisor.email and  # Check email exists and is not False
                    supervisor.email not in cc_emails):
                cc_emails.append(supervisor.email)

        # Filter out any False/None values just in case
        cc_emails = [email for email in cc_emails if email]

        if cc_emails:
            mail_values['email_cc'] = ','.join(cc_emails)

        # Render the email content
        template_id = template.with_context(**ctx)
        rendered_html = template_id._render_template(
            template_id.body_html,
            template_id.model,
            [self.id],
            engine='qweb',
            add_context=ctx
        )[self.id]

        # Send the email
        email_result = template_id.send_mail(
            self.id,
            force_send=True,
            email_values=mail_values
        )

        # Save to alert history
        self._save_alert_to_history(
            email_result,
            rendered_html,
            'Case Response Alert',
            mail_values
        )

    def _send_case_closure_alert(self):
        """Send an alert email for case closure"""
        self.ensure_one()

        # Get email template
        template = self.env.ref(
            'case_management_v2.case_closure_alert_template')
        if not template:
            return

        # Get the creator, closer information
        creator = self.create_uid
        closer = self.env.user
        # Base URL for the link
        from_email = self.env['ir.config_parameter'].sudo(
        ).get_param('mail.default.from')

        _logger.info(
            f"Retrieved mail.default.from in ir.config value: '{from_email}'")

        if not from_email:
            from_email = "developer@novajii.com"
            _logger.info(f"Using hardcoded fallback email: '{from_email}'")

        base_url = self.env['ir.config_parameter'].sudo(
        ).get_param('web.base.url')

        try:
            action = self.env.ref(
                'case_management_v2.action_closed_cases')
            record_url = f"{base_url}/web#id={self.id}&action={action.id}&model=case.manager&view_type=form"
        except Exception as e:
            _logger.warning(f"Could not get action reference: {e}")
            record_url = f"{base_url}/web#id={self.id}&model=case.manager&view_type=form"

        # Prepare email values
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Prepare email values
        ctx = {
            'event_date': fields.Datetime.now(),
            'case_ref': self.case_ref,
            'department_id': self.department_id.name if self.department_id else '',
            'case_rating': self.case_rating,
            'case_status': self.case_status,
            'process_category': self.process_category.name if self.process_category else '',
            'process': self.process.name if self.process else '',
            'creator_name': creator.name,
            'creator_email': creator.email,
            'case_score': self.case_score,
            'user_name': closer.name,
            'user_email': closer.email,
            'close_remarks': self.close_remarks,
            'record_url': record_url,
            'datetime': current_time,
            # Safe logo getter
            'company_logo': self.get_company_logo(base_url),

        }

        # Prepare mail values - to officer responsible, CC creator and supervisors
        mail_values = {
            'email_to': self.officer_responsible.email,
            'email_cc': '',
            'email_from': from_email,

        }

        # Add creator to CC if not the officer responsible and has email
        cc_emails = []
        if (creator and
            creator.id != self.officer_responsible.id and
                creator.email):  # Check all conditions before append
            cc_emails.append(creator.email)

        # Add supervisors to CC
        for supervisor in self.supervisors:
            if (supervisor.id != self.officer_responsible.id and
                supervisor.email and  # Check email exists and is not False
                    supervisor.email not in cc_emails):
                cc_emails.append(supervisor.email)

        # Filter out any False/None values just in case
        cc_emails = [email for email in cc_emails if email]

        if cc_emails:
            mail_values['email_cc'] = ','.join(cc_emails)
            # ---------

        # Render the email content
        template_id = template.with_context(**ctx)
        rendered_html = template_id._render_template(
            template_id.body_html,
            template_id.model,
            [self.id],
            engine='qweb',
            add_context=ctx
        )[self.id]

        # Send the email
        email_result = template_id.send_mail(
            self.id,
            force_send=True,
            email_values=mail_values
        )

        # Save to alert history
        self._save_alert_to_history(
            email_result,
            rendered_html,
            'Case Closure Alert',
            mail_values
        )

    def _save_alert_to_history(self, email_result, rendered_html, title, mail_values):
        """Save the alert to alert history"""
        if email_result:
            mail = self.env['mail.mail'].browse(email_result)
            if mail.state == 'sent':
                model_description = self._description

                # Prepare process_id value
                process_id = self.process.name if self.process else None

                # Create alert history record
                self.env['alert.history'].sudo().create({
                    'html_body': rendered_html,
                    'ref_id': f"{self._name},{self.id}",
                    'risk_rating': self.case_rating if self.case_rating else "Low",
                    'process_id': process_id,
                    'date_created': fields.Datetime.now(),
                    'narration': title,
                    'email': mail_values.get('email_to', ''),
                    'email_cc': mail_values.get('email_cc', ''),
                    'source': model_description,
                    'last_checked': fields.Datetime.now()
                })

    def get_company_logo(self, base_url):
        company = self.env.user.company_id
        logo_url = f"{base_url}/web/image/res.company/{company.id}/logo_web"
        return logo_url

    @api.depends('response_ids', 'create_uid', 'case_status')
    def _compute_show_closure_fields(self):
        """Determine if closure fields should be visible"""
        for record in self:
            record.show_closure_fields = (
                record.create_uid.id == self.env.user.id and  # User is creator
                bool(record.response_ids) and  # Has responses
                # Not already closed or archived
                record.case_status not in ['closed', 'archived']
            )

    @api.model
    def _check_overdue_cases(self):
        """
        Cron job to automatically set open cases to overdue
        based on case settings
        """
        # Get overdue period setting
        setting = self.env['case.settings'].search(
            [('code', '=', 'case_overdue_period')], limit=1)
        if not setting or not setting.date_val or not setting.date_unit:
            _logger.error(
                "Case overdue period setting not found or incomplete using default")
            setting.date_val = 48
            setting.date_unit = 'hours'

        # Calculate the cutoff date
        time_delta = self._calculate_time_delta(
            setting.date_val, setting.date_unit)
        cutoff_date = fields.Datetime.now() - time_delta
        _logger.info(f"Cutoff date! {cutoff_date}")

        # Find open cases without response older than the cutoff date
        open_cases = self.search([
            ('case_status', '=', 'open'),
            ('response_ids', '=', False),
            ('create_date', '<', cutoff_date)
        ])

        # Update their status to overdue
        if open_cases:
            _logger.info(f"Setting {len(open_cases)} cases to overdue status")
            open_cases.write({'case_status': 'overdue'})

        return True

    @api.model
    def _archive_old_closed_cases(self):
        """
        Cron job to automatically archive closed cases 
        based on case settings
        """
        # Get archive period setting
        setting = self.env['case.settings'].search(
            [('code', '=', 'case_archive_period')], limit=1)
        if not setting or not setting.date_val or not setting.date_unit:
            _logger.error(
                "Case archive period setting not found using default")
            setting.date_val = 180
            setting.date_unit = 'days'

        # Calculate the cutoff date
        time_delta = self._calculate_time_delta(
            setting.date_val, setting.date_unit)
        cutoff_date = fields.Datetime.now() - time_delta

        # Find cases that have been closed for longer than the cutoff period
        closed_cases = self.search([
            ('case_status', '=', 'closed'),
            ('write_date', '<', cutoff_date)
        ])

        # Update their status to archived
        if closed_cases:
            _logger.info(f"Archiving {len(closed_cases)} old closed cases")
            for case in closed_cases:
                case.write({
                    'case_status': 'archived',
                    'active': False
                })

        return True

    def _calculate_time_delta(self, value, unit):
        """
        Helper method to convert a time value and unit to a timedelta
        """
        if unit == 'seconds':
            return relativedelta(seconds=value)
        elif unit == 'minutes':
            return relativedelta(minutes=value)
        elif unit == 'hours':
            return relativedelta(hours=value)
        elif unit == 'days':
            return relativedelta(days=value)
        elif unit == 'weeks':
            return relativedelta(weeks=value)
        elif unit == 'months':
            return relativedelta(months=value)
        elif unit == 'years':
            return relativedelta(years=value)
        else:
            return relativedelta(days=0)

    # @api.model
    @api.depends('event_date')
    def _compute_overdue_alert_message(self):
        setting = self.env['case.settings'].search(
            [('code', '=', 'case_overdue_period')], limit=1)

        if setting and setting.date_val and setting.date_unit:
            # Format the period text (e.g., "48 hours", "3 days")
            unit_label = setting.date_unit
            if setting.date_val == 1:  # Handle singular case
                unit_label = unit_label[:-1]  # Remove 's' from the end

            period_text = f"{setting.date_val} {unit_label}"
            message = f"""
                <div class="alert alert-info" role="alert">
                    All opened cases not responded to after <strong>{period_text}</strong> will become overdue!
                </div>
            """
        else:
            # Fallback message if setting not found
            message = """
                <div class="alert alert-info" role="alert">
                    All opened cases not responded to after 48 hours will become overdue!
                </div>
            """

        for record in self:
            record.overdue_alert_message = message

    
    @api.onchange('case_rating')
    def _onchange_case_rating(self):
        """Update case_score when case_rating changes in the UI"""
        _logger.info(
            f"Onchange fired: case_rating={self.case_rating}, case_score={self.case_score}")

        # Check if this case was created from customer form
        from_customer = self.env.context.get('from_customer_form', False)

        if from_customer and self.case_score:
            _logger.info(
                "Skipping onchange - case created from customer with existing score")
            return

        if not from_customer and self.case_score and self._origin.id:
            _logger.info("Allowing onchange for existing record not from customer")

        if not self.case_rating:
            self.case_score = 0.0  # Explicitly set to 0 when no rating
            return

        # Update score based on rating
        settings = self.env['case.settings']
        try:
            thresholds = {
                'low': float(settings.get_setting('low_risk_threshold') or 3.9),
                'medium': float(settings.get_setting('medium_risk_threshold') or 6.9),
                'high': float(settings.get_setting('high_risk_threshold') or 9.0)
            }

            new_score = thresholds.get(self.case_rating, 0.0)
            self.case_score = new_score

            _logger.info(
                f"Set case_score to {self.case_score} based on rating {self.case_rating}")
            
            _logger.info(f"returned case score {new_score} ")

            
        except Exception as e:
            _logger.error(f"Error setting default case score: {e}")
            

           
    
    
   
class CaseResponse(models.Model):
    _name = 'case.response.'
    _description = 'Case Responses'
    _order = 'id desc'
    _rec_name = 'create_date'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    case_id = fields.Many2one('case.manager', string='Case',
                              required=True, ondelete='cascade')
    response = fields.Text(string='Response', required=True, index=True)
    create_date = fields.Datetime(
        string='Response Date', default=datetime.now().replace(microsecond=0), readonly=True)
    create_uid = fields.Many2one(
        'res.users', string='Responder', readonly=True)


class CaseSettings(models.Model):
    _name = 'case.settings'
    _description = 'Case Settings'
    _sql_constraints = [
        ('uniq_case_settings_code', 'unique(code)',
         "Code already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True, index=True)
    narration = fields.Text(string='Narration')
    val = fields.Char(string='Value')
    date_val = fields.Integer(string='Date Value')
    date_unit = fields.Selection(
        [
            ("seconds", "Seconds"),
            ("minutes", "Minutes"),
            ("hours", "Hours"),
            ("days", "Days"),
            ("weeks", "Weeks"),
            ("months", "Months"),
            ("years", "Years"),
        ],
        string="Date Unit",
        # required=True,
        help="Select the unit for the date.",
        store=True,
    )

    @api.model
    def get_setting(self, code):
        """Get a setting value by code"""
        setting = self.search([('code', '=', code)], limit=1)
        if setting:
            return setting.val
        return None
