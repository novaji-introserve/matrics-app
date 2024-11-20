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


_logger = logging.getLogger(__name__)


class ReplyLog(models.Model):
    _name = "reply.log"
    _description = "Reply Log Model"
    _rec_name = "create_date"
    _order = "id desc"
    _inherit = ['mail.thread', 'mail.activity.mixin']

    # Link to the rulebook this reply relates to
    rulebook_id = fields.Many2one(
        "rulebook", string="Rulebook", help="Reference to the related Rulebook",
        tracking=True,

    )

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
        default=lambda self: fields.Datetime.now().astimezone(
            pytz.timezone('Africa/Lagos')).replace(tzinfo=None),
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

    document_filename = fields.Char(string="Document Filename",tracking=True,  )

    # The regulatory date as computed from the rulebook (related field)
    rulebook_compute_date = fields.Datetime(
        string="Regulatory Date",
        store=True,
        tracking=True,
        help="The regulatory date calculated from the related rulebook.",
    )
    next_due_date = fields.Datetime(
        string="Next Due Date",
        store=True,
        tracking=True,
        help="Next due date.",
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
            ("late", "Late Submission"),
            ("not_responded", "Not Responded"),
        ],
        string="Submission Timing",
        compute="_compute_submission_timing",
        store=True,
        help="Indicates whether the reply was submitted early, on time, or late based on the regulatory date.",
        tracking=True,

    )

    @api.model
    def open_reply_log(self):
        # Check if the user has a department
        restricted_group = self.env.ref('rule_book.group_department_user_')

        # Check if the user belongs to the restricted group
        if restricted_group in self.env.user.groups_id:
            # Check if the user has a department
            if not self.env.user.department_id:
                raise AccessError(
                    "You must be assigned to a department to view Reply Logs.")

            # Apply restriction to the domain
            domain = [('department_id', '=', self.env.user.department_id.id)]
        else:
            # No restrictions for other users
            domain = []
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
                ("rulebook_status", "not in", ["completed", "reviewed"]),
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
                # Strip HTML tags
                record.rulebook_name = re.sub(
                    r"<[^>]+>", "", record.rulebook_id.type_of_return
                )
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
                    record.submission_timing = "on_time"
            except Exception as e:
                _logger.error(
                    f"Error computing submission timing for record {record.id}: {e}")
                record.submission_timing = "error"

   
    @api.constrains("rulebook_status")
    def _compute_next_due_date(self):
        print(" updating next due date")
        """Compute the next due date for the rulebook when the status is 'completed'."""
        for record in self:
            rulebook = record.rulebook_id
            if (
                rulebook
                and rulebook.is_recurring
                and record.rulebook_status == "completed"
            ):
                # Check if regulatory date matches computed date
                if record.rulebook_compute_date == rulebook.computed_date:
                    if rulebook.frequency_type == "monthly":
                        next_due_date = record.rulebook_compute_date + relativedelta(
                            months=1
                        )
                    elif rulebook.frequency_type == "quarterly":
                        next_due_date = record.rulebook_compute_date + relativedelta(
                            months=3
                        )
                    elif rulebook.frequency_type == "yearly":
                        next_due_date = record.rulebook_compute_date + relativedelta(
                            years=1
                        )
                    elif rulebook.frequency_type == "daily":
                        next_due_date = record.rulebook_compute_date + relativedelta(
                            days=1
                        )
                    elif rulebook.frequency_type == "weekly":
                        next_due_date = record.rulebook_compute_date + relativedelta(
                            weeks=1
                        )
                    elif rulebook.frequency_type == "day_of_month":
                        # Move to the same day in the next month
                        next_due_date = record.rulebook_compute_date + relativedelta(
                            months=1
                        )
                    elif rulebook.frequency_type == "day_every_month":
                        # Move to the same day of the next month
                        next_due_date = record.rulebook_compute_date + relativedelta(
                            months=1
                        )
                    elif rulebook.frequency_type == "month_of_year":
                        # Move to the same month of the next year
                        next_due_date = record.rulebook_compute_date.replace(
                            year=record.rulebook_compute_date.year + 1
                        )
                    elif rulebook.frequency_type == "immediate":
                        # Immediate means no specific future due date
                        next_due_date = record.rulebook_compute_date
                    else:
                        next_due_date = record.rulebook_compute_date
                    print(next_due_date)
                    record.next_due_date = next_due_date
                    # Update the rulebook computed date and reset status for next cycle
                    rulebook.computed_date = next_due_date
                    print(next_due_date)
                else:
                    # Do nothing if regulatory date doesn't match computed date
                    continue
                

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
