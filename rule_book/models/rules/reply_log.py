import re
from odoo import models, fields, api,_
from datetime import timedelta, datetime
import pytz
from dateutil.relativedelta import relativedelta
from odoo.exceptions import UserError
import logging
from odoo.tools import format_date

_logger = logging.getLogger(__name__)

class ReplyLog(models.Model):
    _name = "reply.log"
    _description = "Reply Log Model"
    _rec_name = "create_date"
    _order = "id desc"

    # Link to the rulebook this reply relates to
    rulebook_id = fields.Many2one(
        "rulebook", string="Rulebook", help="Reference to the related Rulebook"
    )

    rulebook_name = fields.Char(
        string="Rulebook Name",
        compute='_compute_rulebook_name_stripped',
        store=False  # Not stored in the database
    )

    @api.model
    def get_awaiting_replies(self):
        _logger.info("Fetching awaiting replies...")

        # Fetch completed replies
        completed_replies = self.search([('rulebook_status', '=', 'completed')])
        completed_grouped = {}

        for reply in completed_replies:
            key = (reply.rulebook_id.id, reply.next_due_date)
            if key not in completed_grouped:
                completed_grouped[key] = reply.id

        _logger.info(f"Completed replies grouped: {completed_grouped}")

        # Search for awaiting replies
        awaiting_replies = self.search([
            ('rulebook_status', '!=', 'completed'),
            ('rulebook_status', '!=', 'reviewed'),
            ('next_due_date', 'not in', list(completed_grouped.values())),
            ('rulebook_id', 'not in', [k[0] for k in completed_grouped.keys()]),
        ])

        _logger.info(f"Awaiting replies found: {awaiting_replies.ids}")

        # Prepare the result with required fields
        result = []
        for reply in awaiting_replies:
            formatted_date = format_date(self.env, reply.reply_date) if reply.reply_date else 'No date'
            print(reply.rulebook_status.title())
            print("testing")
            result.append({
                'id': reply.id,
                'rulebook_name': reply.rulebook_name,  # Assuming rulebook_id has a 'name' field
                'status': reply.rulebook_status.title(),
                'reply_date': formatted_date,
                'form_link': f"/web#id={reply.id}&model=reply.log&view_type=form"  # Link to the form view
            })

        return result

    def _compute_rulebook_name_stripped(self):
        for record in self:
            if record.rulebook_id:
                # Strip HTML tags
                record.rulebook_name = re.sub(r'<[^>]+>', '', record.rulebook_id.type_of_return )
            else:
                record.rulebook_name = ''

    # The date the reply was submitted, auto-set to the current date, and readonly
    reply_date = fields.Date(
        string="Reply Date",
        default=fields.Date.today,
        readonly=True,
        help="The date when the reply was submitted. It is automatically set to the current date.",
    )

    # The textual content of the reply submitted by the reporter
    reply_content = fields.Text(
        string="Reply Content", help="The content of the reply provided by the inputer."
    )

    # The name of the person who submitted the reply (inputter)
    reporter = fields.Char(
        string="Inputer",
        required=True,
        help="The person who provided or inputted the reply.",
    )

    # Attached document for the reply (binary file)
    document = fields.Binary(
        string="Attached Document",
        help="Any document (e.g., image, doc, xlsx) attached to the reply.",
    )

    # The regulatory date as computed from the rulebook (related field)
    rulebook_compute_date = fields.Datetime(
        string="Regulatory Date",
        store=True,
        help="The regulatory date calculated from the related rulebook.",
    )
    next_due_date = fields.Datetime(
        string="Next Due Date",
        store=True,
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
    )

    # Field to track the timing of submission compared to the regulatory date (early, on time, late)
    submission_timing = fields.Selection(
        [
            ("early", "Early Submission"),
            ("on_time", "Right on Time"),
            ("late", "Late Submission"),
        ],
        string="Submission Timing",
        compute="_compute_submission_timing",
        store=True,
        help="Indicates whether the reply was submitted early, on time, or late based on the regulatory date.",
    )

    global_data = {}
    # to send the data in the global variable to the template
    def data(self):
        global global_data
        # send the global value to the email template
        return global_data

    def set_global_data(self,data):
        global global_data
        global_data=data

    @api.depends("reply_date", "rulebook_compute_date")
    def _compute_submission_timing(self):
        """Compute the submission timing based on the reply date and the regulatory date."""
        for record in self:
            # Convert reply_date to a datetime.date object
            reply_date_obj = datetime.strptime(
                str(record.reply_date), "%Y-%m-%d"
            ).date()
            # Extract the date part from rulebook_compute_date
            compute_date_obj = record.rulebook_compute_date.date()

            # Compare the reply date with the computed rulebook date
            if reply_date_obj > compute_date_obj:
                record.submission_timing = "late"
            elif reply_date_obj < compute_date_obj:
                record.submission_timing = "early"
            else:
                record.submission_timing = "on_time"

    @api.constrains("rulebook_status")
    def _compute_next_due_date(self):
        print(' updating next due date')
        """Compute the next due date for the rulebook when the status is 'completed'."""
        for record in self:
            rulebook = record.rulebook_id
            if rulebook and rulebook.is_recurring and record.rulebook_status == "completed":
                # Check if regulatory date matches computed date
                if record.rulebook_compute_date == rulebook.computed_date:
                    if rulebook.frequency_type == 'monthly':
                        next_due_date = record.rulebook_compute_date + relativedelta(months=1)
                    elif rulebook.frequency_type == 'quarterly':
                        next_due_date = record.rulebook_compute_date + relativedelta(months=3)
                    elif rulebook.frequency_type == 'yearly':
                        next_due_date = record.rulebook_compute_date + relativedelta(years=1)
                    elif rulebook.frequency_type == 'daily':
                        next_due_date = record.rulebook_compute_date + relativedelta(days=1)
                    elif rulebook.frequency_type == 'weekly':
                        next_due_date = record.rulebook_compute_date + relativedelta(weeks=1)
                    elif rulebook.frequency_type == 'day_of_month':
                        # Move to the same day in the next month
                        next_due_date = record.rulebook_compute_date + relativedelta(months=1)
                    elif rulebook.frequency_type == 'day_every_month':
                        # Move to the same day of the next month
                        next_due_date = record.rulebook_compute_date + relativedelta(months=1)
                    elif rulebook.frequency_type == 'month_of_year':
                        # Move to the same month of the next year
                        next_due_date = record.rulebook_compute_date.replace(year=record.rulebook_compute_date.year + 1)
                    elif rulebook.frequency_type == 'immediate':
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
        print(' updating check status change')
        """Check when status changes from 'completed' to any other status and adjust rulebook."""
        for record in self:
            rulebook = record.rulebook_id
            print('testing')
            print(record.next_due_date)
            if rulebook and rulebook.is_recurring and record.rulebook_status != "completed" and record.next_due_date!=False:
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
                    previous_due_date = rulebook.computed_date - relativedelta(months=1)
                elif rulebook.frequency_type == "day_every_month":
                    previous_due_date = rulebook.computed_date - relativedelta(months=1)
                elif rulebook.frequency_type == "month_of_year":
                    previous_due_date = rulebook.computed_date.replace(
                        year=record.rulebook_compute_date.year - 1
                    )
                print(previous_due_date)
                print('===========================')
                print(rulebook.computed_date)
                # If the rulebook's computed date is currently set to the new status
                if record.rulebook_compute_date == previous_due_date:
                    print('i got here')
                    # Move rulebook computed date back to the previous due date
                    rulebook.computed_date = previous_due_date
