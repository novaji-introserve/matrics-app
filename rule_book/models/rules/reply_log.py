import re
from odoo import models, fields, api, _
from datetime import timedelta, datetime
import pytz
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
    _description = "Reply Log Model"
    _rec_name = "create_date"
    _order = "id desc"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    rulebook_id = fields.Many2one(
        'rulebook', required=True, ondelete='cascade', tracking=True, string="Rulebook", help="Reference to the related Rulebook",)

    department_id = fields.Many2one(
        "hr.department",
        string="Department",
        default=lambda self: self.env.user.department_id.id,
        help="The department this reply log belongs to",
        tracking=True,
    )

    rulebook_name = fields.Char(
        string="Type Of Return",
        compute="_compute_rulebook_name_stripped",
        store=False,  # Not stored in the database
        tracking=True,

    )

    # The date the reply was submitted, auto-set to the current date, and readonly
    reply_date = fields.Datetime(
        string="Reply Date",
        # default=fields.Date.today,
        default=fields.Datetime.now(),
        # .astimezone( pytz.timezone('Africa/Lagos')).replace(tzinfo=None),
        # default=fields.Datetime.now,
        readonly=True,
        tracking=True,
        help="The date when the reply was submitted. It is automatically set to the current date.",
    )

    # The textual content of the reply submitted by the reporter
    reply_content = fields.Text(
        string="Reply Note", help="The content of the reply provided by the inputer.",
        tracking=True,

    )

    # The name of the person who submitted the reply (inputter)
    reporter = fields.Many2one(
        "res.users",
        tracking=True,
        string="Inputer",
        required=True,
        help="The user responsible for this rulebook reply.",
    )

    # Attached document for the reply (binary file)
    document = fields.Binary(
        string="Attached Document",
        help="Any document (e.g., image, doc, xlsx) attached to the reply.",
        tracking=True,
    )

    document_filename = fields.Char(string="Document Filename", tracking=True,)

    # The regulatory date as computed from the rulebook (related field)
    rulebook_compute_date = fields.Datetime(
        string="Internal Due Date",
        store=True,
        tracking=True,
        help="The regulatory date calculated from the related rulebook.",
        # related='rulebook_id.rulebook_computed_date',
    )

    last_escalation_sent = fields.Datetime(
        string="Last Escalation Sent",
        store=True,
        tracking=True,
        help="The last escalation date calculated from the related rulebook.",
        # related='rulebook_id.last_escalation_sent',
    )

    next_due_date = fields.Datetime(
        string="Next Regulatory Date",
        store=True,
        tracking=True,
        help="Next due date.",
        # related='rulebook_id.next_due_date'
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

    )

    # Field to track the timing of submission compared to the regulatory date (early, on time, late)
    submission_timing = fields.Selection(
        [
            ("early", "Early Submission"),
            ("on_time", "Right on Time"),
            ("pending", "Pending"),
            ("late", "Late Submission"),
            ("not_responded", "Over Due/ Not Responded "),
        ],
        string="Submission Timing",
        compute="_compute_submission_timing",
        store=True,
        help="Indicates whether the reply was submitted early, on time, or late based on the regulatory date.",
        tracking=True,

    )

    formatted_reply_date = fields.Char(
        string="Formatted Reply Date",
        compute="_compute_formatted_reply_date",
    )

    formatted_rulebook_date = fields.Char(
        string="Formatted Rulebook Date",
        compute="_compute_formatted_rulebook_date",
    )

    frequency_type = fields.Selection(
        related='rulebook_id.frequency_type',
        string='Frequency Type',
        readonly=True,
        help="Frequency type from the associated rulebook"
    )

    due_date_value = fields.Integer(
        string="Due Date Value",
        # required=True,
        tracking=True,
        help="Enter the value for the due date.",
    )

    due_date_unit = fields.Selection(
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
        # required=True,
        tracking=True,
        help="Select the unit for the due date.",
    )

    escalation_date_value = fields.Integer(
        string="Escalation Date Value",
        # required=True,
        tracking=True,
        help="Enter the value for the escalation date.",
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
        tracking=True,
        help="Select the unit for the escalation date.",
    )

    escalation_date = fields.Datetime(
        string="Escalation Date",
        # compute="_compute_escalation_date",
        store=True,
        help="The calculated escalation date based on the provided internal due date values.",
    )

    officer_cc = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        string="Officers To Copy",
        tracking=True,
        help="Select the person(s) to copy for this rulebook.",
    )

    last_reg_due_date_sent = fields.Datetime(
        string="Last Regulatory Alert sent Date Value",
        # required=True,
        tracking=True,

    )

    last_internal_due_date_sent = fields.Datetime(
        string="Last Internal Alert sent Date Value",
        # required=True,
        tracking=True,
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
    )

    quarter_day = fields.Integer(string='Day of Quarter', default=7)

    due_date = fields.Datetime(string="Regulatory Date",
                               store=True,
                               tracking=True,
                               help="due date.",
                               )
    last_escalation_sent = fields.Datetime(string="Last Escalation Sent")

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

    first_line_escalation = fields.Many2one(
        "res.users",
        string="First Line Escalation",
        # required=True,
        tracking=True,
        help="Select the user responsible for the first line escalation.",
    )

    second_line_escalation = fields.Many2one(
        "res.users",
        string="Second Line Escalation",
        # required=True,
        tracking=True,
        help="Select the user responsible for the second line escalation.",
    )

    risk_category = fields.Many2one(
        related="rulebook_id.risk_category",
        string="Risk Category",
        # required=True,
        tracking=True,
        help="Select the risk category for this rulebook.",
        default="Compliance Risk"
    )

    @api.depends("rulebook_compute_date")
    def _compute_formatted_rulebook_date(self):
        for record in self:
            if record.rulebook_compute_date:
                # Format the date as desired
                tz = pytz.timezone("Africa/Lagos")
                local_dt = pytz.utc.localize(
                    record.rulebook_compute_date).astimezone(tz)
                record.formatted_rulebook_date = local_dt.strftime(
                    "%B %d, %Y %H:%M")
            else:
                record.formatted_rulebook_date = "N/A"

    @api.depends("reply_date")
    def _compute_formatted_reply_date(self):
        for record in self:
            if record.reply_date:
                # Format the date as desired
                tz = pytz.timezone("Africa/Lagos")
                local_dt = pytz.utc.localize(record.reply_date).astimezone(tz)
                record.formatted_reply_date = local_dt.strftime(
                    "%B %d, %Y %H:%M")
            else:
                record.formatted_reply_date = ""

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
            'name': ('Reply Logs'),
            'type': 'ir.actions.act_window',
            'res_model': 'reply.log',  # This is your target model
            'view_mode': 'tree,form,kanban',
            'domain': domain,
            'context': {
                'search_default_not_deleted': 1,
                'default_department_id': self.env.user.department_id.id if self.env.user.department_id else False,
            }
        }

    # def write(self, vals):
    #     """
    #     Overrides the default write method to enforce rules for reply logs.

    #     Provides flexible validation and logging for different update scenarios.
    #     """
    #     # Comprehensive logging for debugging
    #     _logger.critical(f"Write method called")
    #     _logger.critical(f"Current environment context: {self.env.context}")
    #     _logger.critical(f"Current user: {self.env.user.name}")
    #     _logger.critical(f"Write vals: {vals}")

    #     # Fields that trigger detailed validation
    #     reply_submission_fields = ['reply_content', 'document']

    #     # Determine if this is a reply submission context
    #     is_reply_submission = any(field in vals for field in reply_submission_fields)

    #     # Perform specific validations for reply submission
    #     if is_reply_submission:
    #         for record in self:
    #             # Validate reply content
    #             if 'reply_content' in vals:
    #                 reply_content = vals.get('reply_content', '')
    #                 if not reply_content:
    #                     raise AccessError(_("You must provide a substantive reply note before updating."))

    #             # Validate document
    #             if 'document' in vals:
    #                 if not vals.get('document', False):
    #                     raise AccessError(_("You must upload a valid document before updating."))

    #             # Update status and timestamp for reply submissions
    #             vals['rulebook_status'] = 'submitted'
    #             vals['reply_date'] = fields.Datetime.now()

    #     try:
    #         # Perform the write operation
    #         result = super(ReplyLog, self).write(vals)

    #         # Optional post-write processing

    #         return result

    #     except Exception as e:
    #         _logger.error(f"Error during write operation: {str(e)}")
    #         raise

    def write(self, vals):
        """Overrides the default write method to enforce rules."""
        for record in self:
            # Check if reply_content or document is being updated
            if "reply_content" in vals or "document" in vals:
                # Ensure both fields are provided
                if "reply_content" in vals and not vals["reply_content"]:
                    raise AccessError(
                        _("You must provide a reply note before updating."))

                if "document" in vals and not vals["document"]:
                    raise AccessError(
                        _("You must upload a file before updating."))

                # Change status to submitted if both fields are set
                vals["rulebook_status"] = "submitted"
                vals["reply_date"] = fields.Datetime.now()

        # Perform the write operation
        result = super(ReplyLog, self).write(vals)

        rulebook = request.env["rulebook"].sudo().browse(
            int(record.rulebook_id))

        url = request.env["rulebook"]._record_link(
            record.id, model_name='reply.log')

        global_data = {
            "email_from":  os.getenv("EMAIL_FROM"),
            "email_to": rulebook.first_line_escalation.email,
            "name":  rulebook.type_of_return,
            "title":  rulebook.name.name,
            "content": record.reply_content,
            "url_link": url,
            "current_year": datetime.now().year,
        }
        self.set_global_data(global_data)

        for record in self:
            if record.rulebook_status == 'submitted' and rulebook.first_line_escalation:
                self.trigger_escalation_alert(record)

        return result

    def trigger_escalation_alert(self, report):
        # Logic for sending email to escalation officers
        template = request.env.ref(
            "rule_book.email_template_rulebook_log_notification_")
        if template:
            template.sudo().send_mail(report.id, force_send=True)
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
            _logger.error(f"Error fetching awaiting replies: {e}")
            return {"error": str(e)}

    def _compute_rulebook_name_stripped(self):
        for record in self:
            if record.rulebook_id:
                rulebook_name = record.rulebook_id.type_of_return or ""

                # Check if the string contains HTML tags
                if re.search(r"<[^>]+>", rulebook_name):
                    # Strip HTML tags
                    record.rulebook_name = re.sub(
                        r"<[^>]+>", "", rulebook_name)
                else:
                    # No HTML tags; use the name as is
                    record.rulebook_name = rulebook_name
            else:
                record.rulebook_name = ""

    global_data = {}

    # to send the data in the global variable to the template
    def data(self):
        global global_data
        # send the global value to the email template
        return global_data

    def set_global_data(self, data):
        global global_data
        global_data = data

    @api.depends("reply_date", "rulebook_compute_date")
    def _compute_submission_timing(self):
        """Compute the submission timing based on the full datetime of reply_date and rulebook_compute_date."""
        for record in self:
            if not record.reply_date:
                # If there's no reply date and the due date has passed, mark as not responded
                if (
                    record.rulebook_compute_date
                    and record.rulebook_compute_date < fields.Datetime.now()
                ):
                    record.submission_timing = "not_responded"
                continue

            try:
                # Convert reply_date to a datetime object
                reply_datetime = fields.Datetime.from_string(record.reply_date)
                # Convert rulebook_compute_date to a datetime object
                compute_datetime = fields.Datetime.from_string(
                    record.rulebook_compute_date)

                # Compare the reply datetime with the computed rulebook datetime
                if reply_datetime > compute_datetime:
                    record.submission_timing = "late"
                elif reply_datetime < compute_datetime:
                    record.submission_timing = "early"
                else:
                    record.submission_timing = "pending"
            except Exception as e:
                _logger.error(
                    f"Error computing submission timing for record {record.id}: {e}")
                record.submission_timing = "error"

    @api.model
    def update_reply_log_due_dates(self):
        """Update the next due date for all reply logs based on rulebook recurrence."""
        reply_logs = self.env['reply.log'].search(
            [('rulebook_id.is_recurring', '=', True), ('rulebook_status', '=', 'completed')])

        for reply_log in reply_logs:
            rulebook = reply_log.rulebook_id
            if rulebook:
                # Update the next due date based on rulebook's next_due_date
                reply_log.next_due_date = rulebook.next_due_date
                _logger.info(
                    f"Updated next due date for reply log {reply_log.id} based on rulebook {rulebook.name}")

    @api.constrains("rulebook_status")
    def _check_status_change(self):
        print(" updating check status change")
        """Check when status changes from 'completed' to any other status and adjust rulebook."""
        for record in self:
            rulebook = record.rulebook_id
            print("testing")
            print(record.next_due_date)
            if (
                rulebook
                and rulebook.is_recurring
                and record.rulebook_status != "completed"
                and record.next_due_date != False
            ):
                # Calculate the previous due date based on the frequency type
                previous_due_date = rulebook.computed_date - relativedelta(
                    days=rulebook.frequency_type == "daily" and 1 or 0,
                    weeks=rulebook.frequency_type == "weekly" and 1 or 0,
                    months=(rulebook.frequency_type == "monthly" and 1)
                    or (rulebook.frequency_type == "quarterly" and 3)
                    or 0,
                    years=rulebook.frequency_type == "yearly" and 1 or 0,
                )

                # Handle special cases for day_of_month, day_every_month, and month_of_year
                if rulebook.frequency_type == "day_of_month":
                    previous_due_date = rulebook.computed_date - \
                        relativedelta(months=1)
                elif rulebook.frequency_type == "day_every_month":
                    previous_due_date = rulebook.computed_date - \
                        relativedelta(months=1)
                elif rulebook.frequency_type == "month_of_year":
                    previous_due_date = rulebook.computed_date.replace(
                        year=record.rulebook_compute_date.year - 1
                    )
                print(previous_due_date)
                print("===========================")
                print(rulebook.computed_date)
                # If the rulebook's computed date is currently set to the new status
                if record.rulebook_compute_date == previous_due_date:
                    print("i got here")
                    # Move rulebook computed date back to the previous due date
                    rulebook.computed_date = previous_due_date

    def _compute_next_due_date(self):
        _logger.critical("Updating next due date...")

        """Compute the next due date for the rulebook when the status is 'completed'."""

        for record in self:

            try:
                next_due_date = None

                if record.frequency_type == "monthly":
                    next_due_date = record.rulebook_computed_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "quarterly":
                    next_due_date = record.rulebook_computed_date + \
                        relativedelta(months=3)

                elif record.frequency_type == "yearly":
                    next_due_date = record.rulebook_computed_date + \
                        relativedelta(years=1)

                elif record.frequency_type == "daily":
                    next_due_date = record.rulebook_computed_date + \
                        relativedelta(days=1)

                elif record.frequency_type == "weekly":
                    next_due_date = record.rulebook_computed_date + \
                        relativedelta(weeks=1)

                elif record.frequency_type == "day_of_month":
                    next_due_date = record.rulebook_computed_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "day_every_month":
                    next_due_date = record.rulebook_computed_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "bi_monthly":
                    # Get the current day of the computed date
                    current_day = record.rulebook_computed_date.day

                    # Determine if it's the first or second date of the month
                    if current_day == record.bi_monthly_day1:
                        # If current is first day, next is second day of same month
                        next_due_date = record.rulebook_computed_date.replace(
                            day=record.bi_monthly_day2)
                    else:
                        # If current is second day, next is first day of next month
                        next_due_date = record.rulebook_computed_date + \
                            relativedelta(months=1)
                        next_due_date = next_due_date.replace(
                            day=record.bi_monthly_day1)

                elif record.frequency_type == "semi_annually":
                    # Get current date components
                    current_month = record.rulebook_computed_date.month
                    current_day = record.rulebook_computed_date.day

                    if current_month == record.semi_annual_month1:
                        # Move to second date of the year
                        next_due_date = record.rulebook_computed_date.replace(
                            month=record.semi_annual_month2,
                            day=record.semi_annual_day2
                        )
                    else:
                        # Move to first date of next year
                        next_due_date = record.rulebook_computed_date.replace(
                            year=record.rulebook_computed_date.year + 1,
                            month=record.semi_annual_month1,
                            day=record.semi_annual_day1
                        )
                elif record.frequency_type == "three_yearly":
                    next_due_date = record.rulebook_computed_date + \
                        relativedelta(years=3)

                elif record.frequency_type == "date":
                    next_due_date = record.rulebook_computed_date

                elif record.frequency_type == "immediate":
                    next_due_date = record.rulebook_computed_date

                else:
                    next_due_date = record.rulebook_computed_date

                if not next_due_date:
                    _logger.critical(
                        f"Invalid frequency type '{record.frequency_type}' for record {record.id}.")
                    next_due_date = record.rulebook_computed_date

                record.next_due_date = next_due_date
                record.rulebook_computed_date = next_due_date

                record.sudo().write({
                    'next_due_date': next_due_date,
                    'rulebook_computed_date': next_due_date
                })

                record._compute_escalation_date_for_cron()
                record._compute_due_date_for_cron()

                _logger.critical(
                    f"Record {record.id}: Computed next due date as {next_due_date}.")

            except Exception as e:
                _logger.error(
                    f"Error computing next due date for record {record.id}: {str(e)}"
                )
                record.next_due_date = None

    def _compute_escalation_date_for_cron(self):
        for record in self:
            if record.rulebook_computed_date and record.escalation_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                delta_args = {
                    record.escalation_date_unit: -record.escalation_date_value
                }
                record.escalation_date = record.rulebook_computed_date + relativedelta(
                    **delta_args
                )
                _logger.critical(
                    f"writing escalation date {record.escalation_date}")

                record.sudo().write({
                    'escalation_date': record.escalation_date,
                })
            else:
                # If rulebook_computed_date is not available, set due_date to False or handle accordingly
                record.escalation_date = None

    def _compute_due_date_for_cron(self):
        for record in self:
            if record.rulebook_computed_date and record.due_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                if record.due_date_value:
                    delta_args = {
                        record.due_date_unit: -record.due_date_value
                    }
                    record.due_date = record.rulebook_computed_date + relativedelta(
                        **delta_args
                    )
                    _logger.critical(
                        f"writing escalation date {record.next_due_date}")
                    record.sudo().write({
                        'due_date': record.due_date,
                    })
            else:
                # If rulebook_computed_date is not available, set due_date to False or handle accordingly
                record.due_date = None

    @api.model
    def check_rulebook_and_update_due_date(self):
        """Check rulebooks with today's regulatory date and update next due date."""

        today = datetime.now().date()

        _logger.critical(
            f"rulebooks with today's regulatory date  {today}, plus one day {today + timedelta(days=1)}")

        today = fields.Datetime.now().astimezone(
            pytz.timezone('Africa/Lagos')).replace(tzinfo=None)

        # Adjust to local midnight without timezone conversion
        day_start = today.replace(hour=0, minute=0, second=0)
        day_end = today.replace(hour=23, minute=59, second=59)
        time_in_15_minutes = today + timedelta(minutes=15)

        day_start = day_start.replace(
            tzinfo=None, microsecond=0)  # Remove timezone info
        day_end = day_end.replace(tzinfo=None, microsecond=0)

        time_in_15_minutes = time_in_15_minutes.replace(
            tzinfo=None, microsecond=0)

        # Perform the search
        rulebooks = self.env["reply.log"].search(
            [
                ("rulebook_compute_date", ">=", day_start),
                ("rulebook_compute_date", "<", day_end),
                ("rulebook_id.is_recurring", "=", True),
            ]
        )

        for record in rulebooks:
            try:
                _logger.critical(
                    f"To Update rulebook {record.id}:,  type of return : {re.sub(r'<[^>]+>', '', record.type_of_return)} frequency : {record.frequency_type}")

                rulebook_computed_date = (
                    record.rulebook_rulebook_computed_date)
                time_in_5_minutes = rulebook_computed_date - \
                    timedelta(minutes=5)

                if today >= time_in_5_minutes and rulebook_computed_date:
                    record._compute_next_due_date()

            except Exception as e:
                _logger.critical(f"Failed to update rulebook {record.id}: {e}")

    @api.model
    def send_reminder_email(self):
        """Send reminder and escalation emails for due or escalated rulebooks."""
        today = fields.Datetime.now().astimezone(
            pytz.timezone('Africa/Lagos')).replace(tzinfo=None)

        rulebooks_to_update_internal_alert = self.env["reply.log"]
        rulebooks_to_update_escalation = self.env["reply.log"]
        rulebooks_to_update_regulatory_alert = self.env["reply.log"]

        time_in_20_minutes = today + timedelta(minutes=20)
        _logger.critical(
            f"Send reminder and escalation emails cron job has started ")

        # Get all rulebook IDs in the current recordset
        rulebook_ = self.env["rulebook"].search([])
        rulebook_ids = rulebook_.ids
        _logger.critical(f"Rulebook ids {rulebook_ids}")

        # Fetch completed reply logs for these rulebooks in a single query
        incomplete_rulebook_logs = self.env['reply.log'].search([
            ('rulebook_status', '!=', 'completed'),
        ]).mapped('rulebook_id')

        _logger.critical(
            f"INcomplete Rulebooks IDS {incomplete_rulebook_logs}")

        for rulebook in incomplete_rulebook_logs:
            try:
                needs_update_internal_alert = False
                needs_update_regulatory_alert = False
                needs_update_escalation = False

                rulebook_model = rulebook.rulebook_id

                _logger.critical(
                    f"Incomplete Rulebook logs to process type of return : {re.sub(r'<[^>]+>', '', rulebook_model.type_of_return)} ID {rulebook.id}")

                # Reminder Email: Check if due_date matches today
                computed_date = rulebook.rulebook_computed_date

                if computed_date and today.date() == computed_date.date():
                    if abs((computed_date - today).total_seconds()) <= 600:
                        if not rulebook.last_internal_due_date_sent or rulebook.last_internal_due_date_sent.date() != today.date():
                            rulebook._send_internal_due_date_email()
                            needs_update_internal_alert = True

                # Escalation Email: Check if escalation_date matches today
                if rulebook.escalation_date and today.date() == rulebook.escalation_date.date():
                    if abs((computed_date - today).total_seconds()) <= 600:
                        if not rulebook.last_escalation_sent or rulebook.last_escalation_sent.date() != today.date():
                            rulebook._send_escalation_due_date_email()
                            needs_update_escalation = True

                # Check if due_date is within 5 minutes of today
                if rulebook.due_date and rulebook.due_date < computed_date:
                    due_time = rulebook.due_date.time()
                    due_time_today = today.replace(
                        hour=due_time.hour, minute=due_time.minute, second=due_time.second)
                    due_time_before_15_minutes = due_time_today - \
                        timedelta(minutes=15)

                    _logger.critical(
                        f" due_time_before_15_minutes {due_time_before_15_minutes}  , due date today  {due_time_today}  , due time {due_time}")
                    if due_time_before_15_minutes <= today <= due_time_today:
                        if computed_date > today and (not rulebook.last_reg_due_date_sent or rulebook.last_reg_due_date_sent.date() != today.date()):
                            rulebook._send_regulatory_due_date_email()
                            needs_update_regulatory_alert = True

                # Conditionally add to rulebooks_to_update for alert or escalation
                if needs_update_internal_alert:
                    rulebooks_to_update_internal_alert |= rulebook
                if needs_update_escalation:
                    rulebooks_to_update_escalation |= rulebook
                if needs_update_regulatory_alert:
                    rulebooks_to_update_regulatory_alert |= rulebook

            except Exception as e:
                _logger.critical(
                    f"Failed to process Rulebook {rulebook.id}: {e}")

        # Update last_reg_due_date_sent for rulebooks with reminder emails
        if rulebooks_to_update_internal_alert:
            try:
                # Direct update using ORM
                for rulebooktoday in rulebooks_to_update_internal_alert:
                    rulebooktoday.sudo().write({
                        'last_reg_due_date_sent': today
                    })

                _logger.critical(
                    f"Updated last_reg_due_date_sent for rulebooks {rulebooks_to_update_internal_alert.ids}")
            except Exception as e:
                _logger.critical(
                    f"Failed to update last_reg_due_date_sent for rulebooks: {e}")

        # Update last_escalation_sent for rulebooks with escalation emails
        if rulebooks_to_update_escalation:
            try:
                for rulebooktoday in rulebooks_to_update_escalation:
                    rulebooktoday.sudo().write({
                        'last_escalation_sent': today
                    })

                _logger.critical(
                    f"Updated last_escalation_sent for rulebooks {rulebooks_to_update_escalation.ids}")
            except Exception as e:
                _logger.critical(
                    f"Failed to update last_escalation_sent for rulebooks: {e}")

        # Update last_internal_due_date_sent for rulebooks with internal emails
        if rulebooks_to_update_regulatory_alert:
            try:
                for rulebooktoday in rulebooks_to_update_regulatory_alert:
                    rulebooktoday.sudo().write({
                        'last_internal_due_date_sent': today
                    })

                _logger.critical(
                    f"Updated last_escalation_sent for rulebooks {rulebooks_to_update_regulatory_alert.ids}")
            except Exception as e:
                _logger.critical(
                    f"Failed to update last_escalation_sent for rulebooks: {e}")

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
                    " escalation_due_date Email template not found")
                return False

            # Detailed logging for debugging
            _logger.critical(
                f"Attempting to send escalation_due_date email for record {self.id}")
            _logger.critical(f"Email data: {email_data}")

            # Send the email
            email_result = template.send_mail(self.id, force_send=True)

            _logger.critical(
                f"escalation_due_date Email sent successfully. Result: {email_result}")
            return True
        except Exception as e:
            _logger.critical(f"Comprehensive email send failure: {str(e)}")
            # Log the full traceback for more detailed debugging
            _logger.critical(traceback.format_exc())
            return False

    def _prepare_email_data(self):
        """Prepare email data dictionary"""
        try:
            # Detailed checks for each required field
            rulebook = self.env['rulebook'].search(
                [('rulebook_id', '=', self.id)],
                order='create_date desc',
                limit=1
            )
            
            if rulebook and not rulebook[0].officer_responsible:
                _logger.critical(
                    f"No officer responsible for record {self.id}")
                return {}

            global global_data
            global_data = {
                "officer_responsible": rulebook[0].officer_responsible.name or "N/A",
                "responsible_id": rulebook[0].responsible_id.name or "N/A",
                "rulebook_name": rulebook[0].name.name or "N/A",
                "due_date": self._compute_formatted_date(self.due_date) or "N/A",
                "record_link": self._record_link(self.id) or "N/A",
                "upload_link": self._compute_upload_link(self.id) or "N/A",
                "current_year": fields.Date.today().year,
                "rulebook_return": re.sub(r'<[^>]+>', '', rulebook[0].type_of_return) or "N/A",
                # "regulatory_name": self.regulatory_name or "N/A",
                "regulatory_name": rulebook[0].regulatory_agency_id.name or "N/A",
                "risk_category": rulebook[0].risk_category.name if rulebook[0].risk_category else "N/A",
                "email_to": rulebook[0].officer_responsible.email or "N/A",
                "email_from": os.getenv("EMAIL_FROM"),
                "email_cc": ", ".join(rulebook[0].officer_cc.mapped('email')) or "",
                "first_line_escalation": self.first_line_escalation.email or "",
                "first_line_name": self.first_line_escalation.name or "",
                "second_line_escalation": self.second_line_escalation.email or "",
                "computed_date": self._compute_formatted_date(self.rulebook_computed_date) or "N/A",
                "escalation_date": self.escalation_date or "N/A",
                "due_date": self._compute_formatted_date(self.due_date) or "N/A",

            }

            # Optional CC handling

            # Extensive logging of prepared email data
            _logger.critical(f"Prepared email data: {global_data}")

            return global_data

        except Exception as e:
            _logger.critical(f"Error preparing email data: {str(e)}")
            _logger.critical(traceback.format_exc())
            return {}

    def _compute_formatted_date(self, dt):
        """
        Format datetime to format like '21st of November, 2024 by 2pm'
        Args:
            dt: datetime object
        Returns:
            str: Formatted date string
        """
        if not dt:
            return ""

        try:
            # Ordinal suffixes lookup
            if dt.tzinfo is None:

                dt = dt.replace(tzinfo=pytz.utc)

        # Convert to the desired timezone (e.g., Africa/Lagos)
            lagos_tz = pytz.timezone("Africa/Lagos")
            dt = dt.astimezone(lagos_tz)

            SUFFIXES = {
                1: "st", 2: "nd", 3: "rd",
                21: "st", 22: "nd", 23: "rd",
                31: "st"
            }

            # Get day suffix
            day_suffix = SUFFIXES.get(dt.day, "th")

            # Format date
            formatted_time = dt.strftime(
                f"%-d{day_suffix} of %B, %Y by %-I%p").lower()

            _logger.critical(f"formatted time %s: {formatted_time}")

            return formatted_time

        except Exception as e:
            _logger.error(f"Date formatting error: {e}")
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

