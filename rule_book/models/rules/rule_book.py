from odoo import models, fields, api
from datetime import timedelta, datetime
import pytz
from dateutil.relativedelta import relativedelta
from ...controllers.rule_book.rule_book import *
import logging
from odoo.exceptions import AccessError

_logger = logging.getLogger(__name__)

from dotenv import load_dotenv

load_dotenv()


# Rulebook Title Model
class Rulebook(models.Model):
    _name = "rulebook"
    _description = "Rulebook"
    _rec_name = "name"
    _order = "id desc"
    _inherit = ["mail.thread"]

    name = fields.Many2one(
        "rulebook.title",
        string="RuleBook Title",
        required=True,
        tracking=True,
        help="Enter the title of the rulebook.",
    )

    theme_id = fields.Many2one(
        "rulebook.theme",
        string="Rulebook Theme",
        required=True,
        tracking=True,
        help="Select the theme associated with this rulebook.",
    )

    risk_rating = fields.Selection(
        [("low", "Low"), ("medium", "Medium"), ("critical", "Critical")],
        string="Risk Rating",
        required=True,
        tracking=True,
        help="Specify the risk rating of the rulebook.",
        compute="_compute_risk_rating",
        store=True,
    )

    risk_category = fields.Many2one(
        "rulebook.risk_category",
        string="Risk Category",
        required=True,
        tracking=True,
        help="Select the risk category for this rulebook.",
    )

    first_line_escalation = fields.Many2one(
        "res.users",
        string="First Line Escalation",
        required=True,
        tracking=True,
        help="Select the user responsible for the first line escalation.",
    )

    second_line_escalation = fields.Many2one(
        "res.users",
        string="Second Line Escalation",
        required=True,
        tracking=True,
        help="Select the user responsible for the second line escalation.",
    )

    type_of_return = fields.Char(
        string="Type of Return",
        required=True,
        tracking=True,
        help="Enter the type of return for this rulebook.",
    )

    regulatory_agency_id = fields.Many2one(
        "rulebook.sources",
        string="Regulatory Agency",
        required=True,
        tracking=True,
        help="Select the regulatory agency associated with this rulebook.",
    )

    responsible_id = fields.Many2one(
        "rulebook.responsible",
        string="Responsible",
        required=True,
        tracking=True,
        help="Select the person responsible for this rulebook.",
    )

    description = fields.Html(
        string="Description",
        tracking=True,
        help="Provide a detailed description of the rulebook.",
    )

    section = fields.Char(
        string="Section",
        help="Add relevant section in the rulebook title for reference purposes.",
    )

    sanction = fields.Html(
        string="Sanction",
        tracking=True,
        help="List sanctions here, using headings, styling, and other formatting options for clarity.",
    )

    internal_due_date_value = fields.Integer(
        string="Internal Due Date Value",
        required=True,
        tracking=True,
        help="Enter the value for the internal due date.",
    )

    internal_due_date_unit = fields.Selection(
        [
            ("seconds", "Seconds"),
            ("minutes", "Minutes"),
            ("hours", "Hours"),
            ("days", "Days"),
            ("weeks", "Weeks"),
            ("months", "Months"),
            ("years", "Years"),
        ],
        string="Internal Due Date Unit",
        required=True,
        tracking=True,
        help="Select the unit for the internal due date.",
    )

    escalation_date_value = fields.Integer(
        string="Escalation Date Value",
        required=True,
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
        required=True,
        tracking=True,
        help="Select the unit for the escalation date.",
    )

    internal_due_date = fields.Datetime(
        string="Internal Due Date",
        compute="_compute_internal_due_date",
        store=True,
        help="The calculated internal due date based on the provided values.",
    )

    escalation_date = fields.Datetime(
        string="Escalation Date",
        compute="_compute_escalation_date",
        store=True,
        help="The calculated escalation date based on the provided values.",
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

    frequency_type = fields.Selection(
        [
            ("date", "Date"),
            ("day_of_month", "Day of a Month"),
            ("day_every_month", "Day of Every Month"),
            ("daily", "Daily"),
            ("weekly", "Weekly"),
            ("monthly", "Monthly"),
            ("quarterly", "Quarterly"),
            ("yearly", "Yearly"),
            ("month_of_year", "Month of the Year"),
            ("immediate", "Immediate"),
        ],
        string="Frequency Type",
        required=True,
        help="Select the frequency type for the regulatory action.",
    )

    is_recurring = fields.Boolean(
        string="Recurring",
        default=False,
        help="Indicate if this rulebook entry should be recurring.",
    )

    date_value = fields.Datetime(
        string="Date", help="Specify the exact date for the regulatory action."
    )

    day_value = fields.Integer(
        string="Day", help="Specify the day of the month for the regulatory action."
    )

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

    computed_date = fields.Datetime(
        string="Next Regulatory Due Date",
        compute="_compute_date",
        store=True,
        help="The next regulatory due date based on the frequency type.",
    )

    global_data = {}

    # to send the data in the global variable to the template
    def data(self):
        # send the global value to the email template
        return global_data

    next_due_date = fields.Datetime(string="Next Due Date")
    last_escalation_sent = fields.Datetime(string="Last Escalation Sent")

    # * functinos

    # to open up vie resolution button
    def open_reply_log(self):
        try:
            # Define your action here
            action = self.env.ref("rule_book.action_reply_log").sudo().read()[0]

            # Set the default domain to show tickets with matching issue
            id = self.id
            action["domain"] = [("rulebook_id", "=", id)]

            return action
        except AccessError:
            # If the user lacks permissions, raise a friendly message
            raise AccessError("You do not have the necessary permissions to view the Reply Log.")    

    # @api.depends("id")
    def _compute_upload_link(self, id):
        base_url = self.env["ir.config_parameter"].sudo().get_param("web.base.url")
        appendedValue = encrypt_id(id)
        print(f"{base_url}/report_submission/{appendedValue}")
        return f"{base_url}/report_submission/{appendedValue}"

    def _record_link(self, id):
        print(id)
        base_url = self.env["ir.config_parameter"].sudo().get_param("web.base.url")
        #
        return f"{base_url}/web#id={id}&cids=1&menu_id=108&action=306&model=rulebook&view_type=form"

    @api.depends("risk_category")
    def _compute_risk_rating(self):
        """Compute the risk rating based on the risk priority of the risk category."""
        for record in self:
            if record.risk_category and record.risk_category.risk_priority:
                record.risk_rating = record.risk_category.risk_priority
            else:
                record.risk_rating = "low"  # Set a default if no category is selected

    @api.depends("frequency_type", "date_value", "day_value", "month_value")
    def _compute_date(self):
        today = fields.Datetime.now()
        default_time = datetime.combine(today, datetime.min.time()).replace(
            hour=8, minute=0, second=0
        )
        for record in self:
            if record.frequency_type == "date":
                record.computed_date = record.date_value or default_time
                record.is_recurring = False

            elif record.frequency_type == "day_of_month":
                if record.day_value and record.month_value:
                    # Get today's date
                    today = fields.Date.today()

                    # Ensure day_value and month_value are integers
                    try:
                        day_value = int(record.day_value)
                        month_value = int(record.month_value)

                        # Try to assign the computed date based on the chosen day and month in the current year
                        record.computed_date = fields.Datetime.to_datetime(
                            f"{today.year}-{month_value:02d}-{day_value:02d} 08:00:00"
                        )
                    except ValueError:
                        # If the combination is invalid (e.g., February 30), assign it to the next month
                        next_month = (month_value % 12) + 1
                        record.computed_date = fields.Datetime.to_datetime(
                            f"{today.year}-{next_month:02d}-{day_value:02d} 08:00:00"
                        )

                    # Ensure it's marked as recurring
                    record.is_recurring = True
            elif record.frequency_type == "day_every_month":
                if record.day_value:
                    next_month = today.month + 1 if today.month < 12 else 1
                    year = today.year if today.month < 12 else today.year + 1
                    record.computed_date = fields.Datetime.to_datetime(
                        f"{year}-{next_month:02d}-{record.day_value:02d} 08:00:00"
                    )
                    record.is_recurring = True

            elif record.frequency_type == "daily":
                record.computed_date = default_time + timedelta(days=1)
                record.is_recurring = True

            elif record.frequency_type == "weekly":
                record.computed_date = default_time + timedelta(weeks=1)
                record.is_recurring = True

            elif record.frequency_type == "monthly":
                next_month = today.month + 1 if today.month < 12 else 1
                year = today.year if today.month < 12 else today.year + 1
                default_time = datetime(year, next_month, 1, 7, 0)
                # Assign the new date to 'record.computed_date'
                record.computed_date = default_time
                record.is_recurring = True

            elif record.frequency_type == "quarterly":
                next_quarter_month = ((today.month - 1) // 3 + 1) * 3 + 1
                year = today.year if next_quarter_month <= 12 else today.year + 1
                next_quarter_month = (
                    next_quarter_month
                    if next_quarter_month <= 12
                    else next_quarter_month - 12
                )
                record.computed_date = default_time.replace(
                    year=year, month=next_quarter_month
                )
                record.is_recurring = True

            elif record.frequency_type == "yearly":
                record.computed_date = default_time.replace(year=today.year + 1)
                record.is_recurring = True

            elif record.frequency_type == "month_of_year":
                if record.month_value:
                    record.computed_date = default_time.replace(
                        month=int(record.month_value)
                    )
                    record.is_recurring = True

            elif record.frequency_type == "immediate":
                record.computed_date = fields.Datetime.now()
                record.is_recurring = False

    @api.depends("internal_due_date_value", "internal_due_date_unit", "escalation_date")
    def _compute_internal_due_date(self):
        for record in self:
            if record.escalation_date and record.internal_due_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                delta_args = {
                    record.internal_due_date_unit: -record.internal_due_date_value
                }
                record.internal_due_date = record.escalation_date + relativedelta(
                    **delta_args
                )
            else:
                # If computed_date is not available, set internal_due_date to False or handle accordingly
                record.internal_due_date = False

    @api.depends(
        "escalation_date_value",
        "escalation_date_unit",
        "computed_date",
    )
    def _compute_escalation_date(self):
        for record in self:
            if record.computed_date and record.escalation_date_unit in [
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
                record.escalation_date = record.computed_date + relativedelta(
                    **delta_args
                )
            else:
                # If computed_date is not available, set internal_due_date to False or handle accordingly
                record.escalation_date = False

    # creating the record
    @api.model
    def create(self, vals):
        print(vals)
        if "risk_category" in vals:
            category = self.env["rulebook.risk_category"].browse(vals["risk_category"])
            print(category)
            if category and category.risk_priority:
                vals["risk_rating"] = category.risk_priority
        print(vals)
        record = super(Rulebook, self).create(vals)
        if record.internal_due_date and record.escalation_date:
            record._schedule_due_dates()

            # send out email if the record frequency is immediate
        if record.frequency_type == "immediate":
            current_year = datetime.now().year
            global global_data
            global_data = {
                "email_to": record.responsible_id.email,
                "name": record.first_line_escalation.name,
                "title": record.name.name,
                "upload_link": self._compute_upload_link(record.id),
                "email_from": os.getenv("EMAIL_FROM"),
                "email_cc": record.responsible_id.cc,
                "due_date": self._compute_formatted_date(record.internal_due_date),
                "current_year": current_year,
            }

            print(record)

            # Ensure rulebook_id is active and frequency_type is not "immediate"

            template_id = self.env.ref("rule_book.email_template_internal_due_date_").id
            template = self.env["mail.template"].browse(template_id)

            if template.exists():
                # Send the email immediately
                print("i was called here ")
                template.send_mail(record.id, force_send=True)
            else:
                print(f"Mail template with ID {template_id} not found.")

        return record

    # editing the record
    def write(self, vals):
        # Log the record ID for debugging
        print(f"Record ID: {self.id}")

        # Check if 'risk_category' is being updated
        if "risk_category" in vals:
            category = self.env["rulebook.risk_category"].browse(vals["risk_category"])
            print(f"Category: {category}")

            # Update risk_rating based on risk_category
            if category and category.risk_priority:
                vals["risk_rating"] = category.risk_priority

        # Log the updated values for debugging
        print(f"Values to write: {vals}")

        # Call the super method and capture the result (typically a boolean)
        result = super(Rulebook, self).write(vals)

        # Check for due dates and schedule if needed
        if "internal_due_date" in vals and "escalation_date" in vals:
            if hasattr(self, "_schedule_due_dates"):
                self._schedule_due_dates()
        if "frequency_type" in vals and vals["frequency_type"] == "immediate":
            print("it came here")
            record = self.env["rulebook"].browse(self.id)
            current_year = datetime.now().year
            global global_data
            global_data = {
                "email_to": record.responsible_id.email,
                "name": record.first_line_escalation.name,
                "title": record.name.name,
                "upload_link": self._compute_upload_link(record.id),
                "email_from": os.getenv("EMAIL_FROM"),
                "email_cc": record.responsible_id.cc,
                "due_date": self._compute_formatted_date(record.internal_due_date),
                "current_year": current_year,
            }

            print(record.responsible_id.cc)

            # Ensure rulebook_id is active and frequency_type is not "immediate"

            template_id = self.env.ref("rule_book.email_template_internal_due_date_").id
            print(template_id)
            template = self.env["mail.template"].browse(template_id)
            print(template)

            if template.exists():
                # Send the email immediately
                print("i was called here ")
                return template.send_mail(record.id, force_send=True)
            else:
                print(f"Mail template with ID {template_id} not found.")
        # Return the result of the write operation
        if (
            "internal_due_date_value" in vals
            or "internal_due_date_unit" in vals
            or "escalation_date_value" in vals
            or "escalation_date_unit" in vals
            or "computed_date" in vals
            or "frequency_type" in vals
        ):
            self._schedule_due_dates()

        return result

    def _compute_formatted_date(self, dt):
        # Extract day, month, year, and time components
        day = dt.day
        month = dt.strftime("%B")
        year = dt.year
        hour = dt.strftime("%-I")  # Remove leading zero from the hour
        minute = dt.strftime("%M")
        am_pm = dt.strftime("%p").lower()

        # Determine the correct ordinal suffix for the day
        if day % 10 == 1 and day != 11:
            day_suffix = "st"
        elif day % 10 == 2 and day != 12:
            day_suffix = "nd"
        elif day % 10 == 3 and day != 13:
            day_suffix = "rd"
        else:
            day_suffix = "th"

        # Format the final string
        formatted_date = f"{day}{day_suffix} of {month}, {year} by {hour}{am_pm}"

        return formatted_date if formatted_date else dt

    def _schedule_due_dates(self):
        for record in self:
            # Ensure any existing events are removed
            events = self.env["calendar.event"].search(
                [("name", "ilike", f"Regulatory Due Date for {record.id}")]
            )
            events.unlink()
            events = self.env["calendar.event"].search(
                [("name", "ilike", f"Internal Due Date for {record.id}")]
            )
            events.unlink()
            events = self.env["calendar.event"].search(
                [("name", "ilike", f"Escalation Due Date for {record.id}")]
            )
            events.unlink()

            if record.computed_date:
                self.env["calendar.event"].create(
                    {
                        "name": f"Regulatory Due Date for {record.id}",
                        "start": record.computed_date,
                        "stop": record.computed_date
                        + timedelta(hours=1),  # Set duration of 1 hour
                        "allday": False,  # Event is not all day; includes specific time
                    }
                )

            if record.internal_due_date:
                self.env["calendar.event"].create(
                    {
                        "name": f"Internal Due Date for {record.id}",
                        "start": record.internal_due_date,
                        "stop": record.internal_due_date
                        + timedelta(hours=1),  # Set duration of 1 hour
                        "allday": False,  # Event is not all day; includes specific time
                    }
                )

            if record.escalation_date:
                self.env["calendar.event"].create(
                    {
                        "name": f"Escalation Due Date for {record.id}",
                        "start": record.escalation_date,
                        "stop": record.escalation_date
                        + timedelta(hours=1),  # Set duration of 1 hour
                        "allday": False,  # Event is not all day; includes specific time
                    }
                )

    # * function that gos to the calendar module and try to check if an event is due
    def send_due_date_emails(self):
        # Get the current time
        global global_data
        now = datetime.now(pytz.timezone("Africa/Lagos"))
        start_window = now - timedelta(minutes=29)
        end_window = now + timedelta(minutes=29)

        # Convert to Odoo Datetime format
        start_window_str = fields.Datetime.to_string(start_window)
        end_window_str = fields.Datetime.to_string(end_window)
        current_year = datetime.now().year

        # regulatory due date
        # Find events related to rulebooks that are within the 29-minute window
        events = self.env["calendar.event"].search(
            [
                ("name", "ilike", "Regulatory Due Date for"),
                ("start", "<=", end_window_str),
                ("start", ">=", start_window_str),
            ]
        )
        for event in events:

            global_data = {
                "name": self.get_record_name(event.name.split(" ")[-1]),
            }
            rulebook_id = self.env["rulebook"].search(
                [("id", "=", event.name.split(" ")[-1])]
            )
            print(event.name.split(" ")[-1])
            print(rulebook_id)
            global_data = {
                "email_to": rulebook_id.first_line_escalation.email, 
                "first_line_escalation": rulebook_id.first_line_escalation.name,
                "rulebook_name": rulebook_id.name.name,
                "upload_link": self._compute_upload_link(rulebook_id.id),
                "email_from":  os.getenv("EMAIL_FROM"),
                "email_cc": rulebook_id.second_line_escalation.email,
                "regulatory_name":rulebook_id.regulatory_agency_id.name,
                "risk_category": rulebook_id.risk_category.name,
                "record_link": self._record_link(rulebook_id.id),
                "current_year": current_year,
            }
            if (
                rulebook_id
                and rulebook_id.status == "active"
                and rulebook_id.frequency_type != "immediate"
            ):
                template_id = self.env.ref(
                    "rule_book.rulebook_due_date_notification_template"
                ).id
                self.env["mail.template"].browse(template_id).send_mail(
                    rulebook_id.id, force_send=True
                )
        # escalation due date
        # Find events related to rulebooks that are within the 29-minute window
        events = self.env["calendar.event"].search(
            [
                ("name", "ilike", "Escalation Due Date for"),
                ("start", "<=", end_window_str),
                ("start", ">=", start_window_str),
            ]
        )
        for event in events:

            # print(self.get_record_name(event.name.split(" ")[-1]))

            rulebook_id = self.env["rulebook"].search(
                [("id", "=", event.name.split(" ")[-1])]
            )

            global_data = {
                "email_to": rulebook_id.first_line_escalation.email,
                "name": rulebook_id.first_line_escalation.name,
                "title": rulebook_id.name.name,
                "upload_link": self._compute_upload_link(rulebook_id.id),
                "email_from":  os.getenv("EMAIL_FROM"),
                "email_cc": rulebook_id.second_line_escalation.email,
                "due_date": self._compute_formatted_date(rulebook_id.escalation_date),
                "record_link": self._record_link(rulebook_id.id),
                "current_year": current_year,
            }
            if (
                rulebook_id
                and rulebook_id.status == "active"
                and rulebook_id.frequency_type != "immediate"
            ):
                template_id = self.env.ref(
                    "rule_book.email_template_first_line_escalation"
                ).id
                self.env["mail.template"].browse(template_id).send_mail(
                    rulebook_id.id, force_send=True
                )

        # internal due date
        # Find events related to rulebooks that are within the 29-minute window
        events = self.env["calendar.event"].search(
            [
                ("name", "ilike", "Internal Due Date for"),
                ("start", "<=", end_window_str),
                ("start", ">=", start_window_str),
            ]
        )
        for event in events:

            print(self.get_record_name(event.name.split(" ")[-1]))

            rulebook_id = self.env["rulebook"].search(
                [("id", "=", event.name.split(" ")[-1])]
            )
            global_data = {
                "email_to": rulebook_id.responsible_id.email,
                "name": rulebook_id.first_line_escalation.name,
                "title": rulebook_id.name.name,
                "upload_link": self._compute_upload_link(rulebook_id.id),
                "email_from":  os.getenv("EMAIL_FROM"),
                "email_cc": rulebook_id.responsible_id.cc,
                "due_date": self._compute_formatted_date(rulebook_id.internal_due_date),
                "current_year": current_year,
            }
            print(rulebook_id)
            if (
                rulebook_id
                and rulebook_id.status == "active"
                and rulebook_id.frequency_type != "immediate"
            ):
                template_id = self.env.ref(
                    "rule_book.email_template_internal_due_date_"
                ).id
                template = self.env["mail.template"].browse(template_id)
                print(template.exists())
                if template.exists():
                    print(f"Mail template with ID {template_id} not found.")
                    return template.send_mail(rulebook_id.id, force_send=True)
                # self.env['mail.template'].browse(template_id).send_mail(rulebook_id, force_send=True)

    def get_model_name_and_id_from_string(self, record_string):
        # Split the string to extract model name and ID
        parts = record_string.strip(",)").split("(")
        model_name = parts[0]
        record_id = int(parts[1]) if len(parts) > 1 else None
        return model_name, record_id

    def get_record_name(self, record_string):
        model_name, record_id = self.get_model_name_and_id_from_string(record_string)

        if model_name and record_id:
            # Fetch the model using self.env
            Model = self.env[model_name]
            # Fetch the record
            record = Model.browse(record_id)
            # Access the 'name' field
            return record.name
        return None

    @api.model
    def check_rulebook_and_update_due_date(self):
        """Check rulebooks with today's regulatory date and update next due date."""
        current_date = fields.Datetime.now().date()  # Today's date

        rulebooks = self.env["reply.log"].search(
            [
                ("rulebook_compute_date", "=", current_date),
                ("rulebook_id.is_recurring", "=", True),
            ]
        )

        for record in rulebooks:
            record._compute_next_due_date()

    def _compute_next_due_date(self):
        print("Updating next due date...")
        """Compute the next due date for the rulebook when the status is 'completed'."""
        for record in self:
            # Check if regulatory date matches computed date
            if record.frequency_type == "monthly":
                next_due_date = record.computed_date + relativedelta(months=1)
            elif record.frequency_type == "quarterly":
                next_due_date = record.computed_date + relativedelta(months=3)
            elif record.frequency_type == "yearly":
                next_due_date = record.computed_date + relativedelta(years=1)
            elif record.frequency_type == "daily":
                next_due_date = record.computed_date + relativedelta(days=1)
            elif record.frequency_type == "weekly":
                next_due_date = record.computed_date + relativedelta(weeks=1)
            elif record.frequency_type == "day_of_month":
                # Move to the same day in the next month
                next_due_date = record.computed_date + relativedelta(months=1)
            elif record.frequency_type == "day_every_month":
                # Move to the same day of the next month
                next_due_date = record.computed_date + relativedelta(months=1)
            elif record.frequency_type == "month_of_year":
                # Move to the same month of the next year
                next_due_date = record.computed_date.replace(
                    year=record.computed_date.year + 1
                )
            elif record.frequency_type == "immediate":
                # Immediate means no specific future due date
                next_due_date = record.computed_date
            else:
                next_due_date = record.computed_date
            record.next_due_date = next_due_date
            # Update the rulebook computed date and reset status for next cycle
            record.computed_date = next_due_date
            print(next_due_date)

    def send_reminder_email(self):
        """Send a reminder email to the responsible party if the due date is approaching."""
        for rulebook in self:
            if (
                rulebook.report_status != "completed"
                and rulebook.internal_due_date <= fields.Datetime.now()
            ):
                template_id = self.env.ref("module_name.reminder_email_template").id
                self.env["mail.template"].browse(template_id).send_mail(rulebook.id)
                rulebook.last_escalation_sent = fields.Datetime.now()

            # Notify first and second line escalation officers
            if (
                rulebook.escalation_date <= fields.Datetime.now()
                and rulebook.report_status != "completed"
            ):
                template_id = self.env.ref("module_name.escalation_email_template").id
                self.env["mail.template"].browse(template_id).send_mail(rulebook.id)
                rulebook.last_escalation_sent = fields.Datetime.now()

    def _notify_user(self):
        # Create a client action to show the dialog
        action = {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Record Updated",
                "message": "The record has been updated and the next due date has been updated.",
                "sticky": False,  # Set to True if you want the notification to be sticky
            },
        }
        return action

    @api.model
    def check_and_update_rulebooks(self):
        """Scheduler to check rulebooks with today's date as the regulatory date and update next due date."""
        print("this is running")
        # Get today's date without time
        today_start = fields.Datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        today_end = fields.Datetime.now().replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

        # Search for rulebooks where computed_date is today and is_recurring is True
        rulebooks = self.search(
            [
                ("computed_date", ">=", today_start),
                ("computed_date", "<=", today_end),
                ("is_recurring", "=", True),
            ]
        )

        for rulebook in rulebooks:
            rulebook._compute_next_due_date()

    def _compute_next_due_date(self):
        """Compute the next due date based on the rulebook's frequency type."""
        for record in self:
            if record.is_recurring:
                # Calculate the next due date based on the frequency type
                if record.frequency_type == "monthly":
                    next_due_date = record.computed_date + relativedelta(months=1)
                elif record.frequency_type == "quarterly":
                    next_due_date = record.computed_date + relativedelta(months=3)
                elif record.frequency_type == "yearly":
                    next_due_date = record.computed_date + relativedelta(years=1)
                elif record.frequency_type == "daily":
                    next_due_date = record.computed_date + relativedelta(days=1)
                elif record.frequency_type == "weekly":
                    next_due_date = record.computed_date + relativedelta(weeks=1)
                elif record.frequency_type == "day_of_month":
                    next_due_date = record.computed_date + relativedelta(months=1)
                elif record.frequency_type == "day_every_month":
                    next_due_date = record.computed_date + relativedelta(months=1)
                elif record.frequency_type == "month_of_year":
                    next_due_date = record.computed_date.replace(
                        year=record.computed_date.year + 1
                    )
                elif record.frequency_type == "immediate":
                    next_due_date = record.computed_date
                else:
                    next_due_date = (
                        record.computed_date
                    )  # Default to current date if none specified

                # Log the next due date for debugging
                print(f"Next due date for rulebook {record.id}: {next_due_date}")

                # Update the computed_date with the next due date
                record.computed_date = next_due_date


class RulebookReport(models.Model):
    _name = "rulebook.report"
    _description = "Rulebook Report"
    _inherit = ["mail.thread", "mail.activity.mixin"]

    rulebook_id = fields.Many2one("rulebook", string="Rulebook", required=True)
    report_description = fields.Html(string="Report Description")
    submission_date = fields.Datetime(string="Submission Date")
    attachment = fields.Binary(string="Report Document")
    status = fields.Selection(
        [
            ("draft", "Draft"),
            ("submitted", "Submitted"),
            ("approved", "Approved"),
        ],
        string="Status",
        default="draft",
    )

    def submit_report(self):
        """Submit the report and notify responsible parties."""
        self.status = "submitted"
        self.rulebook_id.report_status = "submitted"
        self.rulebook_id.message_post(
            body="The report has been submitted for {}.".format(self.rulebook_id.name),
            partner_ids=[self.rulebook_id.responsible_id.partner_id.id],
        )
        # Add more actions like sending submission emails


# the sty
