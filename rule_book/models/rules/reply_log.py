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
from ...controllers.rule_book.rule_book import *

from dotenv import load_dotenv

load_dotenv()
_logger = logging.getLogger(__name__)


class ReplyLog(models.Model):
    _name = "reply.log"
    _description = "Rulebook Logs"
    _rec_name = "create_date"
    _order = "id desc"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    active = fields.Boolean(string='Active', default=True)

    rulebook_id = fields.Many2one(
        'rulebook', required=True, ondelete='cascade', tracking=True, string="Rulebook", help="Reference to the related Rulebook",)

    department_id = fields.Many2one(
        "hr.department",
        string="Department",
        default=lambda self: self.env.user.department_id.id,
        help="The department this reply log belongs to",
        tracking=True,
        store=True,

    )

    rulebook_name = fields.Char(
        string="Type Of Return",
        # compute="_compute_rulebook_name_stripped",
        store=True,  # Not stored in the database
        tracking=True,
        related="rulebook_id.type_of_return"

    )

    # The date the reply was submitted, auto-set to the current date, and readonly
    reply_date = fields.Datetime(
        string="Reply Date",
        # default=fields.Datetime.now(),
        default=datetime.now().replace(microsecond=0),
        readonly=True,
        tracking=True,
        help="The date when the reply was submitted. It is automatically set to the current date.",
        store=True,
        copy=False,
        index= True
    )

    # The textual content of the reply submitted by the reporter
    reply_content = fields.Text(
        string="Reply Note", help="The content of the reply provided by the inputer.",
        tracking=True,
        store=True,
        copy=False,
        index= True
    )

    # The name of the person who submitted the reply (inputter)
    reporter = fields.Many2one(
        "res.users",
        related='rulebook_id.officer_responsible',  # Pointing to the rulebook's field
        tracking=True,
        string="Inputer",
        required=True,
        help="The primary user responsible for this rulebook reply.",
        # store=True,
    )

    # Attached document for the reply (binary file)
    document = fields.Binary(
        string="Attached Document",
        help="Any document (e.g., image, doc, xlsx) attached to the reply.",
        tracking=True,
        store=True,
        attachment=True,
        copy=False,
        index=True
    )

    document_filename = fields.Char(string="Document Filename", tracking=True, store=True,  copy=False

                                    )

    document_url = fields.Char(
        string="Document URL", compute="_compute_document_url", store=False)

    # The regulatory date as computed from the rulebook (related field)
    rulebook_compute_date = fields.Datetime(
        string="Internal Due Date",
        store=True,
        tracking=True,
        help="The regulatory date calculated from the related rulebook.",
        # related='rulebook_id.rulebook_compute_date',
    )

    last_escalation_sent = fields.Datetime(
        string="Last Escalation Sent",
        store=True,
        tracking=True,
        help="The last escalation date calculated from the related rulebook.",
        copy=False

        # related='rulebook_id.last_escalation_sent',
    )

    next_compute_date = fields.Datetime(
        string="Next internal Date",
        store=True,
        tracking=True,
        help="Next due date.",
        # related='rulebook_id.next_compute_date'
    )

    # The status of the rulebook (pending, submitted, reviewed, completed)
    rulebook_status = fields.Selection(
        [
            ("pending", "Pending"),
            ("submitted", "Submitted"),
            ("reviewed", "Reviewed"),
            ("completed", "Completed"),
        ],
        string="Rulebook Status",
        default="pending",
        help="The current status of the related rulebook.",
        tracking=True,
        copy=False,
        index= True

    )

    # Field to track the timing of submission compared to the regulatory date (early, on time, late)
    submission_timing = fields.Selection(
        [
            ("early", "Early Submission"),
            ("on_time", "Right on Time"),
            ("after_internal", "Submitted After Internal Due Date"),
            ("pending", "Pending Not Yet Due"),
            ("late", "Late Submission"),
            ("not_responded", "Over Due/ Not Responded "),
        ],
        string="Submission Timing",
        compute="_compute_submission_timing",
        store=True,
        help="Indicates whether the reply was submitted early, on time, or late based on the regulatory date.",
        tracking=True,
        copy=False,
        index= True

    )

    formatted_reply_date = fields.Char(
        string="Formatted Reply Date",
        compute="_compute_formatted_reply_date",
        # store=True
    )

    formatted_rulebook_date = fields.Char(
        string="Formatted Rulebook Date",
        compute="_compute_formatted_rulebook_date",
        # store=True,
        index=True

    )

    formatted_regulatory_date = fields.Char(
        string="Formatted Regulatory Date",
        compute="_compute_formatted_regulatory_date",
        # store=True,
        index= True
    )

    formatted_reminder_date = fields.Char(
        string="Formatted Reminder Date",
        compute="_compute_formatted_reminder_date",
        # store=True
    )

    formatted_escalation_date = fields.Char(
        string="Formatted Escalation Date",
        compute="_compute_formatted_escalation_date",
        # store=True
    )

    frequency_type = fields.Selection(
        related='rulebook_id.frequency_type',
        string='Frequency Type',
        readonly=True,
        help="Frequency type from the associated rulebook",
        store=True,
    )

    escalation_date_value = fields.Integer(
        string="Escalation Date Value",
        # required=True,
        help="Enter the value for the escalation date.",
        store=True,
    )

    escalation_date_unit = fields.Selection(
        [
            ("seconds", "Seconds"),
            ("minutes", "Minutes"),
            ("hours", "Hours"),
            ("days", "Days"),
            ("weeks", "Weeks"),
            ("months", "Months"),
            ("years", "Years"),
        ],
        string="Escalation Date Unit",
        # required=True,
        help="Select the unit for the escalation date.",
        store=True,

    )

    escalation_date = fields.Datetime(
        string="Escalation Date",
        # compute="_compute_escalation_date",
        store=True,
        tracking=True,
        help="The calculated escalation date based on the provided internal due date values.",
    )

    first_line_escalation = fields.Many2many(
        "res.users",
        related='rulebook_id.first_line_escalation',  # Pointing to the rulebook's field
        string="First Line Escalation",
        # required=True,
        tracking=True,
        help="Select the user responsible for the first line escalation.",
        # store=True,

    )

    second_line_escalation = fields.Many2one(
        "res.users",
        string="Second Line Escalation",
        related='rulebook_id.second_line_escalation',  # Pointing to the rulebook's field
        # required=True,
        tracking=True,
        help="Select the user responsible for the second line escalation.",
        # store=True,

    )

    officer_cc = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        related='rulebook_id.officer_cc',  # Pointing to the rulebook's field
        string="Officers To Copy",
        tracking=True,
        help="Select the person(s) to copy for this rulebook.",
        # store=True,

    )

    reg_due_date_value = fields.Integer(
        string="Due Date Value",
        # required=True,
        default=1,
        help="Enter the value for the due date.",
        store=True,

    )

    reg_due_date_unit = fields.Selection(
        [
            ("seconds", "Seconds"),
            ("minutes", "Minutes"),
            ("hours", "Hours"),
            ("days", "Days"),
            ("weeks", "Weeks"),
            ("months", "Months"),
            ("years", "Years"),
        ],
        string="Due Date Unit",
        default="days",
        help="Select the unit for the due date.",
        store=True,

    )

    last_reg_due_date_sent = fields.Datetime(
        string="Last Regulatory Alert sent",
        # required=True,
        tracking=True,
        store=True,

    )

    last_internal_due_date_sent = fields.Datetime(
        string="Last Internal Alert sent",
        # required=True,
        tracking=True,
        store=True,

    )

    last_reminder_due_date_sent = fields.Datetime(
        string="Last Reminder Alert sent",
        # required=True,
        tracking=True,
        store=True,

    )

    reg_due_date = fields.Datetime(
        string="Regulatory Date",
        store=True,
        tracking=True,
        help="due date.",
        index= True
    )

    reminder_due_date_value = fields.Integer(
        string="Reminder due date value",
        # required=True,
        store=True,
        help="Enter the value for the reminder due date.",
    )
    reminder_due_date_unit = fields.Selection(
        [
            ("seconds", "Seconds"),
            ("minutes", "Minutes"),
            ("hours", "Hours"),
            ("days", "Days"),
            ("weeks", "Weeks"),
            ("months", "Months"),
            ("years", "Years"),
        ],
        string="Reminder due date unit",
        # required=True,
        tracking=True,
        help="Select the unit for the reminder due date.",
        store=True,

    )

    reminder_due_date = fields.Datetime(
        string="Reminder Due Date",
        compute="_compute_reminder_due_date",
        store=True,
        help="The calculated reminder date based on the provided internal due date values.",
    )

    status = fields.Selection(
        [
            ("active", "Active"),
            ("inactive", "Inactive"),
            ("deleted", "Deleted"),
        ],
        string="Status",
        default="active",
        tracking=True,
        help="Indicate the current status of the rulebook.",
                store=True,

    )

    quarter_day = fields.Integer(string='Day of Quarter', default=7,   store=True,
                                 )

    last_escalation_sent = fields.Datetime(string="Last Escalation Sent", copy=False
                                           )

    semi_annual_month1 = fields.Integer(
        string='First Month', default=1)  # January
    semi_annual_month2 = fields.Integer(
        string='Second Month', default=8)  # August
    semi_annual_day1 = fields.Integer(
        string='First Month Day', default=28)  # 28th
    semi_annual_day2 = fields.Integer(
        string='Second Month Day', default=6)  # 6th

    bi_monthly_day1 = fields.Integer(string='First Day of Month', default=1)
    bi_monthly_day2 = fields.Integer(string='Second Day of Month', default=15)

    month_value = fields.Selection(
        [
            ("01", "January"),
            ("02", "February"),
            ("03", "March"),
            ("04", "April"),
            ("05", "May"),
            ("06", "June"),
            ("07", "July"),
            ("08", "August"),
            ("09", "September"),
            ("10", "October"),
            ("11", "November"),
            ("12", "December"),
        ],
        string="Month",
        help="Select the month for the regulatory action.",
    )

    day_of_week = fields.Selection([
        ('0', 'Monday'),
        ('1', 'Tuesday'),
        ('2', 'Wednesday'),
        ('3', 'Thursday'),
        ('4', 'Friday'),
        ('5', 'Saturday'),
        ('6', 'Sunday')
    ], string='Day of Week')

    risk_category = fields.Many2one(
        related="rulebook_id.risk_category",
        string="Risk Category",
        # required=True,
        tracking=True,
        help="Select the risk category for this rulebook.",
        default="Compliance Risk"
    )

    next_due_date_computed = fields.Boolean(
        string='Internal Due Date Processed',
        default=False,
        help='Flag to ensure next internal due date is computed only once on the exact due date'
    )

    reg_due_date_processed = fields.Boolean(
        "Reg Due Date Processed", default=False)

    # can_archive = fields.Boolean(store=False)

    def toggle_active(self):
        # Check if user has compliance manager rights
        if not self.env.user.has_group('rule_book.group_chief_compliance_officer_'):
            return False
        return super(ReplyLog, self).toggle_active()

    @api.depends("rulebook_compute_date")
    def _compute_formatted_rulebook_date(self):
        for record in self:
            if record.rulebook_compute_date:

                record.formatted_rulebook_date = self._compute_formatted_date(
                    record.rulebook_compute_date)
            else:
                record.formatted_rulebook_date = "N/A"

    @api.depends("reg_due_date")
    def _compute_formatted_regulatory_date(self):
        for record in self:
            if record.reg_due_date:

                record.formatted_regulatory_date = self._compute_formatted_date(
                    record.reg_due_date)
            else:
                record.formatted_regulatory_date = "N/A"

    @api.depends("reminder_due_date")
    def _compute_formatted_reminder_date(self):
        for record in self:
            if record.reminder_due_date:

                record.formatted_reminder_date = self._compute_formatted_date(
                    record.reminder_due_date)
            else:
                record.formatted_reminder_date = "N/A"

    @api.depends("escalation_date")
    def _compute_formatted_escalation_date(self):
        for record in self:
            if record.escalation_date:

                record.formatted_escalation_date = self._compute_formatted_date(
                    record.escalation_date)
            else:
                record.formatted_escalation_date = "N/A"

    @api.depends("reply_date")
    def _compute_formatted_reply_date(self):
        for record in self:
            if record.reply_date:
                # Format the date as desired

                record.formatted_reply_date = self._compute_formatted_date(
                    record.reply_date)

            else:
                record.formatted_reply_date = "N/A"

    @api.model
    def open_reply_log(self):
        # Check if user belongs to compliance or COO groups
        compliance_group = self.env.ref('rule_book.group_compliance_manager_')
        coo_group = self.env.ref('rule_book.group_chief_compliance_officer_')

        if compliance_group in self.env.user.groups_id or coo_group in self.env.user.groups_id:
            # No domain restrictions for these groups
            domain = []
        else:
            # Restrict to user's department for other groups
            if not self.env.user.department_id:
                raise AccessError(
                    "You must be assigned to a department to view rulebook logs.")
            domain = [('department_id', '=', self.env.user.department_id.id)]

        return {
            'name': ('Rulebook Logs'),
            'type': 'ir.actions.act_window',
            'res_model': 'reply.log',  # This is your target model
            'view_mode': 'tree,form,kanban',
            'domain': domain,
            'context': {
                'search_default_not_deleted': 1,
                'default_department_id': self.env.user.department_id.id if self.env.user.department_id else False,
            }
        }

    def write(self, vals):
        """Overrides the default write method to enforce rules."""
        vals = self._validate_update_conditions(vals)

        # Perform the write operation
        result = super(ReplyLog, self).write(vals)

        # Iterate through the records being updated
        for record in self:
            rulebook = self.env["rulebook"].sudo().browse(
                int(record.rulebook_id))
            url = self.env["rulebook"]._record_link(
                record.id, model_name='reply.log')

            now = datetime.now()
            now_without_microseconds = now.replace(microsecond=0)

            global global_data

            # "email_to": ", ".join(["asanda@boi.ng", "iabubakar@boi.ng"]),
            # "email_cc": ", ".join(["MAkeju@boi.ng",  "MMuntaka@boi.ng",
            #                        "himam@boi.ng", "fakindele@boi.ng", "oabiola@boi.ng",
            #                        "ooyewunmi@boi.ng", "MAderibigbe@boi.ng", "uchukwuneke@boi.ng",
            #                        "aalhassan@boi.ng", "rezeani@boi.ng", "cdavid@boi.ng",
            #                        "makhator@boi.ng"]),
            global_data = {
                "email_from":  os.getenv("EMAIL_FROM"),
                "email_cc": rulebook.second_line_escalation.email or "",
                "email_to": ", ".join(record.first_line_escalation.mapped('email')) or "",

                "type_of_return": re.sub(r'(<[^>]+>|&\w+;)', '', rulebook.type_of_return),
                "rulebook_source":  rulebook.name.name,
                "content": record.reply_content,
                "url_link": url,
                "current_year": datetime.now().year,
                "first_line_escalation_name": ", ".join(record.first_line_escalation.mapped('name')) or "",
                "datetime": self._compute_formatted_date(now_without_microseconds)
                # "first_line_escalation_name": rulebook.first_line_escalation.name,
            }

            self.set_global_data(global_data)

            submitted_report = (record.document and len(record.document) > 0) or (
                record.reply_content and record.reply_content.strip() != "")

            if record.rulebook_status == 'submitted' and record.first_line_escalation and submitted_report:
                self.trigger_escalation_alert(record)

        return result

    def trigger_escalation_alert(self, report):
        # Logic for sending email to escalation officers
        template = self.env.ref(
            "rule_book.email_template_rulebook_log_notification_")
        if template:
            template.sudo().send_mail(report.id, force_send=True)
            _logger.critical(
                f"Report notification sent to escalation officer! ")
        else:
            _logger.critical(
                "Email template 'rule_book.email_template_rulebook_log_notification_' not found.")

    @api.model
    def get_awaiting_replies(self):
        _logger.info("Fetching awaiting replies...")

        try:
            # Fetch completed replies
            completed_replies = self.search(
                [("rulebook_status", "=", "completed")])
            completed_ids = {
                reply.rulebook_id.id for reply in completed_replies if reply.rulebook_id}

            _logger.info(f"Completed rulebook IDs: {completed_ids}")

            # Search for awaiting replies
            awaiting_replies = self.search([
                ("rulebook_status", "not in", [
                 "completed", "reviewed", "pending"]),
                ("rulebook_id", "not in", list(completed_ids))
            ])

            _logger.info(f"Awaiting replies found: {awaiting_replies.ids}")

            # Prepare the result
            result = []
            for reply in awaiting_replies:
                formatted_date = fields.Datetime.to_string(
                    reply.reply_date) if reply.reply_date else "No date"

                _logger.debug(f"Reply status: {reply.rulebook_status.title()}")

                result.append({
                    "id": reply.id,
                    "rulebook_name": reply.rulebook_name if hasattr(reply, 'rulebook_name') else reply.rulebook_id.name,
                    "status": reply.rulebook_status.title(),
                    "reply_date": formatted_date,
                    "form_link": f"/web#id={reply.id}&model=reply.log&view_type=form",
                })

            return result

        except Exception as e:
            _logger.critical(f"Error fetching awaiting replies: {e}")
            return {"CRictical_error": str(e)}

    global_data = {}

    # to send the data in the global variable to the template
    def data(self):
        global global_data
        # send the global value to the email template
        return global_data

    def set_global_data(self, data):
        global global_data
        global_data = data

    def create(self, vals):

        # Create rulebook record
        record = super(ReplyLog, self).create(vals)
        # self._update_submission_timing()

        return record

    @api.depends("reply_date", "rulebook_compute_date", "reply_content", "document")
    def _compute_submission_timing(self):
        """Compute the submission timing based on the full datetime of reply_date and rulebook_compute_date."""
        for record in self:
            # today = fields.Datetime.now()
            today = datetime.now().replace(microsecond=0)

            try:

                reply_datetime = record.reply_date
                internal_due_date = record.rulebook_compute_date
                reg_due_date = record.reg_due_date

                # Log values for debugging
                _logger.critical(
                    f"internal date: {internal_due_date}  .... reply date: {reply_datetime}")

                if reply_datetime:
                    if reg_due_date and reply_datetime > reg_due_date:
                        record.submission_timing = "late"
                    elif internal_due_date and reply_datetime < internal_due_date:
                        record.submission_timing = "early"
                    elif internal_due_date and reply_datetime > internal_due_date:
                        record.submission_timing = "after_internal"
                    else:
                        record.submission_timing = "pending"
                else:
                    # Handle case where reply_datetime is None
                    record.submission_timing = "pending"
            except Exception as e:
                _logger.critical(
                    f"CRITICAL Error computing submission timing for record {record.id}: {e}")

    @api.model
    def _update_submission_timing(self):
        today = datetime.now().replace(microsecond=0)
        rulebook_logs = self.env['reply.log'].search(
            [('reply_date', '=', False)])

        _logger.critical(
            f"Cron job to compute the submission timing for all rulebook logs started NOW {today} rulebook logs found {len(rulebook_logs)}")

        for record in rulebook_logs:
            try:
                # Check if record still exists
                if not record.exists():
                    _logger.warning(
                        f"Record {record.id} no longer exists, skipping.")
                    continue

                if not record.reply_date:
                    is_overdue = False
                    if not record.reg_due_date and record.rulebook_compute_date:
                        is_overdue = record.rulebook_compute_date < today
                    elif record.reg_due_date:
                        is_overdue = record.reg_due_date < today

                    if is_overdue and record.submission_timing != "not_responded":
                        record.submission_timing = "not_responded"
                        # Using sudo to bypass access rights
                        record.sudo().write({
                            'submission_timing': record.submission_timing,
                        })

                        _logger.critical(
                            f"submission timing updated to {record.submission_timing}: internal due date {record.rulebook_compute_date} todayls date {today} id: {record.id}")

                    continue

            except Exception as e:
                _logger.critical(
                    f"CRITICAL Error computing submission timing for record {record.id}: {str(e)}")

    def _compute_next_due_date(self):
        _logger.critical("Updating next due date...")

        """Compute the next due date for the rulebook when the status is 'completed'."""

        for record in self:

            try:
                next_compute_date = None

                if record.frequency_type == "monthly":
                    next_compute_date = record.rulebook_compute_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "quarterly":
                    next_compute_date = record.rulebook_compute_date + \
                        relativedelta(months=3)

                elif record.frequency_type == "yearly":
                    next_compute_date = record.rulebook_compute_date + \
                        relativedelta(years=1)

                elif record.frequency_type == "daily":
                    next_compute_date = record.rulebook_compute_date + \
                        relativedelta(days=1)

                elif record.frequency_type == "weekly":
                    next_compute_date = record.rulebook_compute_date + \
                        relativedelta(weeks=1)

                elif record.frequency_type == "day_of_month":
                    next_compute_date = record.rulebook_compute_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "day_every_month":
                    next_compute_date = record.rulebook_compute_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "bi_monthly":
                    # Get the current day of the computed date
                    current_day = record.rulebook_compute_date.day

                    # Determine if it's the first or second date of the month
                    if current_day == record.bi_monthly_day1:
                        # If current is first day, next is second day of same month
                        next_compute_date = record.rulebook_compute_date.replace(
                            day=record.bi_monthly_day2)
                    else:
                        # If current is second day, next is first day of next month
                        next_compute_date = record.rulebook_compute_date + \
                            relativedelta(months=1)
                        next_compute_date = next_compute_date.replace(
                            day=record.bi_monthly_day1)

                elif record.frequency_type == "semi_annually":
                    # Get current date components
                    current_month = record.rulebook_compute_date.month
                    current_day = record.rulebook_compute_date.day

                    if current_month == record.semi_annual_month1:
                        # Move to second date of the year
                        next_compute_date = record.rulebook_compute_date.replace(
                            month=record.semi_annual_month2,
                            day=record.semi_annual_day2
                        )
                    else:
                        # Move to first date of next year
                        next_compute_date = record.rulebook_compute_date.replace(
                            year=record.rulebook_compute_date.year + 1,
                            month=record.semi_annual_month1,
                            day=record.semi_annual_day1
                        )
                elif record.frequency_type == "three_yearly":
                    next_compute_date = record.rulebook_compute_date + \
                        relativedelta(years=3)

                elif record.frequency_type == "date":
                    next_compute_date = record.rulebook_compute_date

                elif record.frequency_type == "immediate":
                    next_compute_date = record.rulebook_compute_date

                else:
                    next_compute_date = record.rulebook_compute_date

                if record.rulebook_compute_date:
                    # Your code to compute next date
                    if not next_compute_date:
                        _logger.critical(
                            f"Invalid frequency type '{record.frequency_type}' for record {record.id}.")
                        next_compute_date = record.rulebook_compute_date

                _logger.critical(
                    f"Copying data here is the next reminder date {next_compute_date}.")

                existing_record = self.env['reply.log'].search([
                    ('rulebook_compute_date', '=', next_compute_date),
                    ('rulebook_name', '=', record.rulebook_name)
                ],
                    order='create_date desc',
                    limit=1)

                _logger.critical(
                    f"existing_record  {existing_record}.")

                if not existing_record:
                    new_record = record.copy({
                        'reply_date': None,
                        'next_compute_date': next_compute_date,
                        'rulebook_compute_date': next_compute_date,
                        'submission_timing': "pending",
                        'rulebook_status': "pending",
                        'reply_content': None,
                        'document': None,
                        'document_filename': None
                    })

                # Optionally, compute escalation date and due date for the new record
                    new_record._compute_escalation_date()
                    new_record._compute_reg_due_date()
                    new_record._compute_reminder_due_date()

                    _logger.critical(
                        f"Record {new_record}: Computed next due date as {next_compute_date}.")

                    _logger.critical(
                        f"rulebook_compute_date date found for record {record.id}... date is {record.rulebook_compute_date}")
                else:
                    _logger.critical(
                        f"rulebook_compute_date is not set for record {record.id}")

            except Exception as e:
                _logger.critical(
                    f"CRITICAL Error computing next due date for record {record.id}: {str(e)}"
                )
                # record.next_compute_date = None

    def _compute_escalation_date(self):
        for record in self:
            if record.rulebook_compute_date and record.escalation_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                delta_args = {
                    record.escalation_date_unit: record.escalation_date_value
                }
                record.escalation_date = record.rulebook_compute_date + relativedelta(
                    **delta_args
                )
                _logger.critical(
                    f"writing NEW escalation date Here .. {record.escalation_date}")

                record.sudo().write({
                    'escalation_date': record.escalation_date,
                })
            else:
                # If rulebook_compute_date is not available, set reg_due_date to False or handle accordingly
                record.escalation_date = None

    def _compute_reg_due_date(self):
        for record in self:
            if record.rulebook_compute_date and record.reg_due_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                if record.reg_due_date_value:
                    delta_args = {
                        record.reg_due_date_unit: record.reg_due_date_value
                    }
                    record.reg_due_date = record.rulebook_compute_date + relativedelta(
                        **delta_args
                    )
                    _logger.critical(
                        f"writing NEW Regulatory Due date HERE.. {record.next_compute_date} value is {record.reg_due_date_value}")
                    record.sudo().write({
                        'reg_due_date': record.reg_due_date,
                    })
            else:
                # If rulebook_compute_date is not available, set due_date to False or handle accordingly
                record.reg_due_date = None

    def _compute_reminder_due_date(self):
        for record in self:
            if record.rulebook_compute_date and record.reminder_due_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                if record.reminder_due_date_value:
                    delta_args = {
                        record.reminder_due_date_unit: -record.reminder_due_date_value
                    }
                    record.reminder_due_date = record.rulebook_compute_date + relativedelta(
                        **delta_args
                    )
                    _logger.critical(
                        f"writing NEW Reminder Due date HERE ..{record.reminder_due_date}")
                    record.sudo().write({
                        'reminder_due_date': record.reminder_due_date,
                    })

            else:
                # If computed_date is not available, set reg_due_date to False or handle accordingly
                record.reminder_due_date = None

    @api.model
    def check_rulebook_and_update_due_date(self):
        today = datetime.now().replace(microsecond=0)

        rulebooks = self.env["reply.log"].search([
            ("rulebook_id.is_recurring", "=", True),
            "|",
            "&",
            ("reg_due_date", "!=", False),
            ("reg_due_date", "<=", today),
            "&",
            ("reg_due_date", "=", False),
            ("rulebook_compute_date", "<=", today)
        ])

        _logger.critical(
            f"Rulebooks to update {rulebooks}.., today's datetime {today}..")

        records_to_update = []

        for record in rulebooks:
            try:
                if record.reg_due_date and not record.reg_due_date_processed and record.reg_due_date <= today:
                    # Add context here
                    record.with_context(
                        allow_auto_update=True)._compute_next_due_date()
                    records_to_update.append((record.id, {
                        'next_due_date_computed': True,
                        'reg_due_date_processed': True
                    }))
                    _logger.critical(
                        f"Record updated: {record.rulebook_id} - Reg Due Date")

                elif not record.reg_due_date and record.rulebook_compute_date and not record.next_due_date_computed and record.rulebook_compute_date <= today:
                    # Add context here
                    record.with_context(
                        allow_auto_update=True)._compute_next_due_date()
                    records_to_update.append((record.id, {
                        'next_due_date_computed': True,
                    }))
                    _logger.critical(
                        f"Record updated: {record.rulebook_id} - Compute Date")
            except Exception as e:
                _logger.critical(f"Failed to update rulebook {record.id}: {e}")

          # Correct write operation
        if records_to_update:
            for record_id, vals in records_to_update:
                self.env['reply.log'].browse(record_id).with_context(
                    allow_auto_update=True).sudo().write(vals)

    @api.model
    def send_reminder_email(self):
        """Send reminder and escalation emails for due or escalated rulebooks."""

        today = datetime.now().replace(microsecond=0)

        _logger.critical(
            f"Send reminder and escalation emails cron job has started ")

        # Get all rulebook IDs in the current recordset
        rulebook_ = self.env["rulebook"].search([])
        rulebook_ids = rulebook_.ids

        incomplete_rulebook_logs = self.env['reply.log'].search([
            ('rulebook_status', '!=', 'completed'),
            ('status', '=', 'active')
        ])

        for rulebook in incomplete_rulebook_logs:

            try:

                # Reminder Email: Check if reg_due_date matches today
                computed_date = rulebook.rulebook_compute_date
                time_diff = abs((computed_date - today).total_seconds())
                # not_submitted = not rulebook.document_filename and not rulebook.reply_content
                not_submitted = not rulebook.document or len(
                    rulebook.document) == 0 or not rulebook.reply_content or rulebook.reply_content.strip() == ""

                # Internal Email: Check if Internal_due_date matches today

                if computed_date and today.date() == computed_date.date():
                    _logger.critical(
                        f" Checking Internal date for rulebook {rulebook.id}: internal_due_date {computed_date}, today {today}")
                    if today.time() >= computed_date.time():  # Check if current time is at or after computed time
                        if not_submitted and (not rulebook.last_internal_due_date_sent or rulebook.last_internal_due_date_sent.date() != today.date()):

                            if_success = rulebook._send_internal_due_date_email()

                            if if_success:
                                rulebook.sudo().write({
                                    'last_internal_due_date_sent': today
                                })
                                _logger.critical(
                                    f" Interanal Due date email sent!  {rulebook} .. Updated last_internal_due_date_sent for rulebook log {rulebook.ids}")

                # Escalation Email: Check if escalation_date matches today

                # if rulebook.escalation_date and today.date() == rulebook.escalation_date.date():
                if (rulebook.escalation_date and today.date() == rulebook.escalation_date.date()) or (rulebook.reg_due_date and today.date() == rulebook.reg_due_date.date()):
                    _logger.critical(
                        f" Checking escalation for rulebook {rulebook.id}: escalation_date {rulebook.escalation_date}, reg_due_date {rulebook.reg_due_date}, today {today}")

                    # Use time comparison for escalation check:
                    if rulebook.escalation_date:
                        escalation_time = rulebook.escalation_date.time()
                    else:
                        # If there's no escalation_date, use the time from reg_due_date if available, otherwise default to 4 PM
                        escalation_time = rulebook.reg_due_date.time() if rulebook.reg_due_date else time(16, 0)  # Default to 4 PM

                    if today.time() >= escalation_time:
                        if not_submitted and (not rulebook.last_escalation_sent or rulebook.last_escalation_sent.date() != today.date()):
                            if_success = rulebook._send_escalation_due_date_email()

                            if if_success:
                                rulebook.sudo().write({
                                    'last_escalation_sent': today
                                })
                                _logger.critical(
                                    f" Escalation email sent!  {rulebook} .. Updated last_escalation_sent for rulebook log {rulebook.ids}")

                # Check if reminder_due_date is due today

                if rulebook.reminder_due_date:
                    _logger.critical(
                        f" Checking reminder for rulebook {rulebook.id}: reminder_due_date {rulebook.reminder_due_date}, today {today}")

                    comparison_date = rulebook.reg_due_date or rulebook.rulebook_compute_date

                    # Check if we're between reminder_due_date and the due date
                    if (rulebook.reminder_due_date.date() <= today.date() and
                        comparison_date and
                            today.date() <= comparison_date.date()):

                        due_time_today = rulebook.reminder_due_date.time()

                        _logger.critical(
                            f"Document: {bool(rulebook.document)}, Reply Content: {bool(rulebook.reply_content)}")

                        # Check if the current time is at or later than the reminder due time
                        if today.time() >= due_time_today:

                            if not_submitted and (not rulebook.last_reminder_due_date_sent or rulebook.last_reminder_due_date_sent.date() != today.date()):
                                if_success = rulebook._send_reminder_email()

                                if if_success:
                                    rulebook.sudo().write({
                                        'last_reminder_due_date_sent': today
                                    })

            except Exception as e:
                _logger.critical(
                    f"Failed to process Rulebook {rulebook.id}: {e}")

    def _prepare_email_data(self):
        """Prepare email data dictionary"""
        try:
            # Ensure self.rulebook_id is properly accessed
            rulebook_id = self.rulebook_id.id if isinstance(
                self.rulebook_id, models.BaseModel) else self.rulebook_id

            # Search for the rulebook based on its ID
            rulebook = self.env['rulebook'].search(
                [('id', '=', rulebook_id)],
                order='create_date desc',
                limit=1
            )

            if rulebook and not rulebook.officer_responsible:
                _logger.critical(
                    f"No officer responsible for record {self.id}")
                return {}
            now = datetime.now()
            now_without_microseconds = now.replace(microsecond=0)

            url = self.env["rulebook"]._record_link(
                rulebook.id, model_name='reply.log')

            global global_data
            global_data = {
                "officer_responsible": rulebook.officer_responsible.name or "N/A",
                "responsible_id": rulebook.responsible_id.name or "N/A",
                "rulebook_name": rulebook.name.name or "N/A",
                "reg_due_date": self._compute_formatted_date(self.reg_due_date) or "N/A",
                "record_link": self._record_link(self.id) or "N/A",
                # "upload_link": self._compute_upload_link(self.id) or "N/A",
                "current_year": fields.Date.today().year,
                "rulebook_return": re.sub(r'(<[^>]+>|&\w+;)', '', self.rulebook_name) or "N/A",
                "regulatory_name": rulebook.regulatory_agency_id.name or "N/A",
                "risk_category": rulebook.risk_category.name if rulebook.risk_category else "N/A",
                "email_to": rulebook.officer_responsible.email or "N/A",
                "email_cc": ", ".join(rulebook.officer_cc.mapped('email')) or "",
                # "first_line_escalation": rulebook.first_line_escalation.email or "",
                # "email_to": ", ".join(["asanda@boi.ng", "iabubakar@boi.ng"]),
                # "email_cc": ", ".join(["MAkeju@boi.ng",  "MMuntaka@boi.ng",
                #                        "himam@boi.ng", "fakindele@boi.ng", "oabiola@boi.ng",
                #                        "ooyewunmi@boi.ng", "MAderibigbe@boi.ng", "uchukwuneke@boi.ng",
                #                        "aalhassan@boi.ng", "rezeani@boi.ng", "cdavid@boi.ng",
                #                        "makhator@boi.ng"]),
                "first_line_escalation":  ", ".join(rulebook.first_line_escalation.mapped('email')) or "",
                "email_from": os.getenv("EMAIL_FROM"),
                "first_line_name":  ", ".join(rulebook.first_line_escalation.mapped('name')) or "",
                "second_line_escalation": rulebook.second_line_escalation.email or "",
                "computed_date": self._compute_formatted_date(self.rulebook_compute_date) or "N/A",
                "escalation_date": self._compute_formatted_date(self.escalation_date) or "N/A",
                "reg_due_date": self._compute_formatted_date(self.reg_due_date) or "N/A",
                "datetime": self._compute_formatted_date(now_without_microseconds),
                "url_link": url
            }

            # Optional CC handling

            return global_data

        except Exception as e:
            _logger.critical(f"Critical Error preparing email data: {str(e)}")
            _logger.critical(traceback.format_exc())
            return {}

    def _compute_formatted_date(self, dt):
        """
        Format datetime to format like 'November 28, 2024 07:44 AM'
        Args:
            dt: datetime object
        Returns:
            str: Formatted date string
        """
        if not dt:
            return ""

        try:
            # Check if datetime has timezone info, if not, set to UTC

            # Format the date and time as "November 28, 2024 07:44 AM"
            formatted_time = dt.strftime("%B %d, %Y %I:%M %p")

            return formatted_time

        except Exception as e:
            _logger.critical(f"Date formatting CRITICAL error: {e}")
            return str(dt)

    def _compute_upload_link(self, id):
        base_url = self.env["ir.config_parameter"].sudo(
        ).get_param("web.base.url")
        appendedValue = encrypt_id(id)
        _logger.critical(f"{base_url}/report_submission/{appendedValue}")
        return f"{base_url}/report_submission/{appendedValue}"

    def _record_link(self, record_id, model_name=None):
        """
        Generate a dynamic URL to access a record's form view.

        :param record_id: ID of the record to link
        :param model_name: Optional model name (defaults to current model)
        :return: Constructed URL or False if URL cannot be generated
        """
        try:
            # Use current model if no model name provided
            if model_name is None:
                model_name = self._name

            # Get the base URL from system parameters
            base_url = self.env["ir.config_parameter"].sudo(
            ).get_param("web.base.url")

            # Find the default action for the model
            action = self.env["ir.actions.act_window"].search(
                [("res_model", "=", model_name)], limit=1
            )

            # Find the corresponding menu (if exists)
            menu = False
            if action:
                menu = self.env["ir.ui.menu"].search(
                    [("action", "=", f"ir.actions.act_window,{action.id}")], limit=1
                )

            # Construct the URL dynamically
            url_parts = [
                f"{base_url}/web#id={record_id}",
                f"model={model_name}",
                "view_type=form"
            ]

            # Conditionally add menu and action
            if menu:
                url_parts.append(f"menu_id={menu.id}")
            if action:
                url_parts.append(f"action={action.id}")

            return "&".join(url_parts)

        except Exception as e:
            # Log the error and return False
            self.env['ir.logging'].sudo().create({
                'name': 'Record Link Generation',
                'type': 'server',
                'dbname': self.env.cr.dbname,
                'level': 'ERROR',
                'message': f'Failed to generate record link: {str(e)}'
            })
            return False

    def _send_escalation_due_date_email(self):
        """Send escalation_due_date notification email"""
        try:
            # Ensure the record exists and is valid
            if not self:
                _logger.critical("No record found to send email")
                return False

            # Verify email data is not empty
            email_data = self._prepare_email_data()
            if not email_data:
                _logger.critical("No email data prepared for sending")
                return False

            # Find the email template
            try:
                template = self.env.ref(
                    "rule_book.email_template_first_line_escalation_")
            except ValueError:
                _logger.critical(
                    "Escalation due date Email template not found")
                return False

            # Detailed logging for debugging
            _logger.critical(
                f"Attempting to send Escalation due date email for record {self.id}")
            _logger.critical(f"Email data: {email_data}")

            # Send the email with more detailed error handling
            try:
                email_result = template.send_mail(
                    self.id, force_send=True, raise_exception=True)
                if email_result:
                    # Check if the email was actually sent (in Odoo 14+, this will return the mail.mail id)
                    mail = self.env['mail.mail'].browse(email_result)
                    if mail.state == 'sent':
                        _logger.critical(
                            f"Escalation due date email sent successfully. Mail ID: {email_result}, State: {mail.state}")
                        return True
                    else:
                        _logger.critical(
                            f"Escalation due date email was not sent immediately, it's in state: {mail.state}. Mail ID: {email_result}")
                        return False
                else:
                    _logger.critical("Email sending returned no result.")
                    return False
            except Exception as e:
                _logger.critical(
                    f"Failed to send Escalation due date email: {str(e)}")
                # Check if this is an SMTP authentication issue or network issue
                if "SMTP Authentication Error" in str(e) or "Connection refused" in str(e):
                    raise UserError(
                        _("Escalation due date Email sending failed due to SMTP or network issues: %s", str(e)))
                return False

        except Exception as e:
            _logger.critical(f"Comprehensive email send failure: {str(e)}")
            # Log the full traceback for more detailed debugging
            _logger.critical(traceback.format_exc())
            return False

    def _send_internal_due_date_email(self):
        """Send internal_due_date notification email"""
        try:
            # Ensure the record exists and is valid
            if not self:
                _logger.critical("No record found to send email")
                return False

            # Verify email data is not empty
            email_data = self._prepare_email_data()
            if not email_data:
                _logger.critical("No email data prepared for sending")
                return False

            # Find the email template
            try:
                template = self.env.ref(
                    "rule_book.email_template_internal_due_date_")
            except ValueError:
                _logger.critical(
                    " internal due date Email template not found")
                return False

            # Detailed logging for debugging
            _logger.critical(
                f"Attempting to send Internal due date email for record {self.id}")
            _logger.critical(f"Email data: {email_data}")

            # Send the email with more detailed error handling
            try:
                email_result = template.send_mail(
                    self.id, force_send=True, raise_exception=True)
                if email_result:
                    # Check if the email was actually sent (in Odoo 14+, this will return the mail.mail id)
                    mail = self.env['mail.mail'].browse(email_result)
                    if mail.state == 'sent':
                        _logger.critical(
                            f"internal due date email sent successfully. Mail ID: {email_result}, State: {mail.state}")
                        return True
                    else:
                        _logger.critical(
                            f"Internal due date email was not sent immediately, it's in state: {mail.state}. Mail ID: {email_result}")
                        return False
                else:
                    _logger.critical("Email sending returned no result.")
                    return False
            except Exception as e:
                _logger.critical(
                    f"Failed to send Internal due date email: {str(e)}")
                # Check if this is an SMTP authentication issue or network issue
                if "SMTP Authentication Error" in str(e) or "Connection refused" in str(e):
                    raise UserError(
                        _("Internal due date Email sending failed due to SMTP or network issues: %s", str(e)))
                return False

        except Exception as e:
            _logger.critical(f"Comprehensive email send failure: {str(e)}")
            # Log the full traceback for more detailed debugging
            _logger.critical(traceback.format_exc())
            return False

    def _send_reminder_email(self):
        """Send regulatory_due_date notification email, returning True only if email was sent successfully."""
        try:
            # Ensure the record exists and is valid
            if not self:
                _logger.critical("No record found to send email")
                return False

            # Verify email data is not empty
            email_data = self._prepare_email_data()
            if not email_data:
                _logger.critical("No email data prepared for sending")
                return False

            # Find the email template
            try:
                template = self.env.ref(
                    "rule_book.reminder_email_template_")
            except ValueError:
                _logger.critical(
                    " reminder_email_template_ Email template not found")
                return False

            # Detailed logging for debugging
            _logger.critical(
                f"Attempting to send reminder email for record {self.id}")
            _logger.critical(f"Email data: {email_data}")

            # Send the email with more detailed error handling
            try:
                email_result = template.send_mail(
                    self.id, force_send=True, raise_exception=True)
                if email_result:
                    # Check if the email was actually sent (in Odoo 14+, this will return the mail.mail id)
                    mail = self.env['mail.mail'].browse(email_result)
                    if mail.state == 'sent':
                        _logger.critical(
                            f"Reminder email sent successfully. Mail ID: {email_result}, State: {mail.state}")
                        return True
                    else:
                        _logger.critical(
                            f"Reminder email was not sent immediately, it's in state: {mail.state}. Mail ID: {email_result}")
                        return False
                else:
                    _logger.critical("Email sending returned no result.")
                    return False
            except Exception as e:
                _logger.critical(f"Failed to send Reminder email: {str(e)}")
                # Check if this is an SMTP authentication issue or network issue
                if "SMTP Authentication Error" in str(e) or "Connection refused" in str(e):
                    raise UserError(
                        _("Reminder Email sending failed due to SMTP or network issues: %s", str(e)))
                return False

        except Exception as e:
            _logger.critical(f"Comprehensive email send failure: {str(e)}")
            # Log the full traceback for more detailed debugging
            _logger.critical(traceback.format_exc())
            return False

    def _validate_update_conditions(self, vals):
        """
        Validate update conditions for reply content and document.
        - Ensures both fields are uploaded together on first submission
        - Prevents both fields from being empty after initial submission
        - Validates date conditions before allowing updates to reply or document
        """
        # Check if user is attempting to update either reply content or document
        updating_reply_content = "reply_content" in vals
        updating_document = "document" in vals or "document_filename" in vals
        # one_month_from_today = datetime.today().date() + relativedelta(months=1)
        one_month_from_today = datetime.today().date() - relativedelta(months=1)

        for record in self:
            # Date validation checks only when updating reply content or document
            if updating_reply_content or updating_document:
                if (record.reg_due_date and record.reg_due_date.date() < one_month_from_today) or \
                        (record.rulebook_compute_date and record.rulebook_compute_date.date() < one_month_from_today):
                    raise AccessError(
                        _("You cannot modify the reply or upload documents as the Due Date is older than a month.")
                    )
                if not self._check_access(record):
                    raise AccessError(
                        _('You cannot submit because you are neither responsible or copied in this return.'))

            # Check if the record has never been submitted before
            is_first_submission = not record.document and not record.reply_content

            # First-time submission validation
            if is_first_submission:
                # Prevent status change if reply content or document is not updated

                allowed_columns = ['submission_timing', 'next_due_date_computed',
                                   'reminder_due_date', 'reg_due_date', 'escalation_date', 'last_internal_due_date_sent',
                                   'last_escalation_sent', 'last_reminder_due_date_sent', 'reg_due_date_processed']

                if not self.env.user.has_group('rule_book.group_chief_compliance_officer_') and \
                        not self.env.context.get('allow_auto_update', False) and \
                        not any(key in vals for key in allowed_columns) and \
                        not (updating_reply_content or updating_document):
                    raise AccessError('Action not allowed.')

                # For first submission, both reply content and document must be provided together
                if updating_reply_content and not updating_document:
                    raise AccessError(
                        _("You must upload a document along with the reply note on first submission."))

                if updating_document and not updating_reply_content:
                    raise AccessError(
                        _("You must provide a reply note along with the document on first submission."))

                # Validate that both fields have content when first submitting
                if updating_reply_content:
                    if not vals.get("reply_content"):
                        raise AccessError(
                            _("Reply note cannot be empty."))

                if updating_document:
                    if "document_filename" in vals and not vals.get("document_filename"):
                        raise AccessError(
                            _("File upload is required."))

                    if "document" in vals and not vals.get("document"):
                        raise AccessError(
                            _("File upload is required."))

                # If both fields are valid on first submission, update status
                if updating_reply_content and updating_document:
                    vals["rulebook_status"] = "submitted"
                    vals["reply_date"] = fields.Datetime.now()

            # Validation for subsequent updates
            else:
                # Check if attempting to empty both reply content and document
                is_emptying_reply_content = (
                    updating_reply_content and
                    (vals.get("reply_content") ==
                     '' or vals.get("reply_content") is False)
                )

                is_emptying_document = (
                    updating_document and
                    (
                        (vals.get("document") is False) and
                        (vals.get("document_filename") ==
                         '' or vals.get("document_filename") is False or vals.get("document") == '')
                    )
                )

                # If both fields are being emptied, raise an exception
                if is_emptying_reply_content or is_emptying_document:
                    raise AccessError(_(
                        "Your reply note and the document cannot be empty. "
                    ))

                if not self._can_update_rulebook_log_status(record, vals):
                    raise AccessError(
                        _('You do not have permission to update the status of this return since you are either responsible or copied in the return.'))

        return vals

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

    def _clean_old_attachments(self, record_id):
        """Clean up old attachments when updating the document"""
        domain = [
            ('res_model', '=', self._name),
            ('res_id', '=', record_id),
            ('res_field', '=', 'document')
        ]
        old_attachments = self.env['ir.attachment'].search(domain)
        if old_attachments:
            old_attachments.unlink()

    @api.model
    def clear_last_internal_due_date_sent(self):
        # Get today's date at midnight (start of today)
        today_start = datetime.combine(datetime.today(), datetime.min.time())

        # Search records where last_internal_due_date_sent is today
        records = self.search([
            ('last_internal_due_date_sent', '>=', today_start),
            ('last_internal_due_date_sent', '<', today_start + timedelta(days=1))
        ])

        # Set 'last_internal_due_date_sent' to None for all matching records
        records.write({
            'last_internal_due_date_sent': None
        })

        # Optionally, log the number of records updated
        _logger.info(
            f"Cleared 'last_internal_due_date_sent' (set to NULL) for {len(records)} records where the date is today.")

    def _check_access(self, record):
        user_id = self.env.user.id
        coo_group = self.env.ref('rule_book.group_chief_compliance_officer_')
        # Check if user is in CCO group
        if coo_group in self.env.user.groups_id:
            return True  # CCO can edit any record
        # If not CCO, check if user is either the reporter or in officer_cc
        if user_id != record.reporter.id and user_id not in record.officer_cc.ids:
            return False

        return True

    def _can_update_rulebook_log_status(self, record, vals):
        user = self.env.user
        coo_group = self.env.ref('rule_book.group_chief_compliance_officer_')

        # Check if status is being updated
        if 'rulebook_status' in vals:
            if vals['rulebook_status'] in ['completed', 'reviewed']:
                # Allow if user is in CCO group
                if coo_group in user.groups_id:
                    return True

                # allow mr sanda to be able to move rulebook status
                if user.login == 'asanda@boi.ng':
                    return True

                # Allow if user is not the reporter or in officer_cc
                if user.id != record.reporter.id and user.id not in record.officer_cc.ids:
                    return True

                # Deny if the user is either the reporter or in officer_cc
                return False

        # If status isn't being updated, allow the update
        return True
