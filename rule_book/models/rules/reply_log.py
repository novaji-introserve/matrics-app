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
        string="Regulatory Date",
        store=True,
        tracking=True,
        help="The regulatory date calculated from the related rulebook.",
        related='rulebook_id.computed_date',
    )

    last_escalation_sent = fields.Datetime(
        string="Last Escalation Sent",
        store=True,
        tracking=True,
        help="The last escalation date calculated from the related rulebook.",
        related='rulebook_id.last_escalation_sent',
    )

    next_due_date = fields.Datetime(
        string="Next Due Date",
        store=True,
        tracking=True,
        help="Next due date.",
        related='rulebook_id.next_due_date'
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
