from dotenv import load_dotenv
import re
from odoo import models, fields, api
from datetime import timedelta, datetime
import pytz
from dateutil.relativedelta import relativedelta
from ...controllers.rule_book.rule_book import *
import logging
from odoo.exceptions import AccessError
from odoo.exceptions import ValidationError
import calendar
import logging
from datetime import date
from babel.dates import format_datetime
import traceback
import json


_logger = logging.getLogger(__name__)


load_dotenv()


# Rulebook Title Model
class Rulebook(models.Model):
    _name = "rulebook"
    _description = "Rulebook"
    _rec_name = "name"
    _order = "id desc"
    # _inherit = ["mail.thread"]
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Many2one(
        "rulebook.title",
        string="RuleBook Source",
        required=True,
        tracking=True,
        help="Rulebook Source document.",
        # default=""
    )
    
    computed_date_adjusted = fields.Boolean(
        'Computed Date Adjusted', default=False)
    
    formatted_rulebook_date = fields.Char(
        string="Formatted Rulebook Date",
        compute="_compute_formatted_rulebook_date",        # store=True

    )
    
    formatted_regulatory_date = fields.Char(
        string="Formatted Regulatory Date",
        compute="_compute_formatted_regulatory_date",
        # store=True
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


    risk_rating = fields.Selection(
        [("low", "Low"), ("medium", "Medium"), ("critical", "Critical")],
        string="Risk Rating",
        required=True,
        tracking=True,
        help="Risk rating of the rulebook.",
        compute="_compute_risk_rating",
        store=True,
        # default="critical"
    )

    risk_category = fields.Many2one(
        "rulebook.risk_category",
        string="Risk Category",
        required=True,
        tracking=True,
        help="Risk category for this rulebook.",
        default="Compliance Risk"
    )

    first_line_escalation = fields.Many2one(
        "res.users",
        string="First Line Escalation",
        # required=True,
        tracking=True,
        help="First line escalation officer.",
    )

    second_line_escalation = fields.Many2one(
        "res.users",
        string="Second Line Escalation",
        # required=True,
        tracking=True,
        help="Second line escalation officer.",
    )

    type_of_return = fields.Char(
        string="Type of Return",
        required=True,
        tracking=True,
        help="Type of return for this rulebook.",
    )

    regulatory_agency_id = fields.Many2one(
        "rulebook.sources",
        string="Regulatory Agency",
        required=True,
        tracking=True,
        help="Regulatory agency associated with this rulebook.",
    )

    responsible_id = fields.Many2one(
        "hr.department",
        string="Department Responsible",
        required=True,
        tracking=True,
        help="Department responsible for this rulebook.",
        default=lambda self: self.env.user.department_id.id
    )

    officer_responsible = fields.Many2one(
        'res.users',  # Assuming you are linking to the res.users model
        string="Officer Responsible",
        required=True,
        tracking=True,
        help="Primary person responsible for this rulebook.",
        # default=''
    )

    officer_cc = fields.Many2many(
        'res.users',  # Assuming you are linking to the res.users model
        string="Officers To Copy",
        tracking=True,
        help="Select the person(s) to copy for this rulebook.",
    )

    description = fields.Text(
        string="Description",
        tracking=True,
        help="Detailed description of the rulebook.",
    )

    section = fields.Char(
        string="Section",
        help="Relevant section in the rulebook title for reference purposes.",
    )

    sanction = fields.Text(
        string="Sanction",
        tracking=True,
        help="List sanctions here.",
    )

    last_reg_due_date_sent = fields.Datetime(
        string="Last Regulatory Alert sent",
        # required=True,
        tracking=True,

    )

    last_internal_due_date_sent = fields.Datetime(
        string="Last Internal Alert sent Date Value",
        # required=True,
        tracking=True,
    )
    
    computed_date = fields.Datetime(
        string="Internal due date",
        compute="_compute_date",
        store=True,
        help="Internal due date for this rulebook.",
    )



    escalation_date_value = fields.Integer(
        string="Escalation date value",
        default=1,
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
        string="Escalation date unit",
        default='days',
        tracking=True,
        help="Select the unit for the escalation date.",
    )
    
    escalation_date = fields.Datetime(
        string="Escalation Date",
        compute="_compute_escalation_date",
        store=True,
        help="The calculated escalation date based on the provided internal due date values.",
    )
    
    reg_due_date_value = fields.Integer(
        string="Regulatory date value",
        # default=1,
        tracking=True,
        help="Enter the value for the regulatory due date.",
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
        string="Regulatory date unit",
        # required=True,
        # default='days',
        tracking=True,
        help="Select the unit for the regulatory due date.",
    )

    reg_due_date = fields.Datetime(
        string="Regulatory due date",
        compute="_compute_reg_due_date",
        store=True,
        help="The calculated due date based on the provided internal due date values.",
    )
    
    reminder_due_date_value = fields.Integer(
        string="Reminder date value",
        # default=True,
        tracking=True,
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
        string="Reminder date unit",
        # required=True,
        tracking=True,
        help="Select the unit for the reminder due date.",
    )

    reminder_due_date = fields.Datetime(
        string="Reminder due date",
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
    )

    frequency_type = fields.Selection(
        [
            ("date", "Date"),
            ("daily", "Daily"),
            ("weekly", "Weekly"),
            ("monthly", "Monthly"),
            ("day_of_month", "Day of a Month"),
            ("day_every_month", "Day of Every Month"),
            ("bi_monthly", "Bi-Monthly"),
            ("quarterly", "Quarterly"),
            ('semi_annually', 'Semi-Annually / Bi-Annually'),
            ("yearly", "Yearly / Annually"),
            ("three_yearly", "Every 3 Years"),
            # ("month_of_year", "Month of the Year"),
            ("immediate", "Immediate"),
        ],
        string="Frequency Type",
        required=True,
        help="Select the frequency type for the regulatory action.",
    )

    is_recurring = fields.Boolean(
        string="Recurring",
        default=True,
        help="Indicate if this rulebook entry should be recurring.",
    )

    date_value = fields.Datetime(
        string="Date", help="Specify the exact date for the regulatory action."
    )
    
    time_value = fields.Float(
        string="Time Value", help="Specify the exact time for the regulatory action."
    )

    day_value = fields.Integer(
        string="Day", help="Specify the day of the month for the regulatory action.",
        default=1
    )

    quarter_day = fields.Integer(string='Day of Quarter', default=7)

    next_compute_date = fields.Datetime(string="Next Due Date")
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

    year_month_value = fields.Integer(string='Month of the Year', default=1)

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
    
    active = fields.Boolean(string='Active', default=True)

    theme_id = fields.Many2one(
        "rulebook.theme",
        string="Rulebook Theme",
        # required=False,
        # tracking=True,
        help="Select the theme associated with this rulebook.",
    )

    global_data = {}

    # to send the data in the global variable to the template

    def your_method(self):

        africa_timezone = pytz.timezone("Africa/Lagos")
        # Get the current date and time in the Africa/Lagos timezone
        africa_now = datetime.now(africa_timezone)

        # Get today's start and end times (naive) and localize them
        today_start = africa_timezone.localize(africa_now.replace(
            hour=0, minute=0, second=0, microsecond=0).astimezone(pytz.UTC).replace(tzinfo=None))
        today_end = africa_timezone.localize(africa_now.replace(
            hour=23, minute=59, second=59, microsecond=0).astimezone(pytz.UTC).replace(tzinfo=None))

        # Format the date and time to the desired output
        formatted_today_start = today_start.strftime('%Y-%m-%d %H:%M:%S')
        formatted_today_end = today_end.strftime('%Y-%m-%d %H:%M:%S')

        # Logging the results with the formatted datetime
        _logger.critical("today start %s", formatted_today_start)
        _logger.critical("today end %s", formatted_today_end)

        # Get current date and time (without timezone info for today_now)
        current_date = fields.Datetime.now()  # Current date and time
        today_now = fields.Datetime.now().astimezone(
            pytz.timezone('Africa/Lagos')).replace(tzinfo=None)

        formatted_today_now = today_now.strftime('%Y-%m-%d %H:%M:%S')

        _logger.critical("Current Date and Time: %s", formatted_today_now)
        utc_now = datetime.now(pytz.UTC)
        _logger.critical("utc %s", utc_now.strftime('%Y-%m-%d %H:%M:%S'))
        _logger.critical("todays date odoo  %s",
                         current_date.strftime('%Y-%m-%d %H:%M:%S'))

    @api.model
    def open_rulebooks(self):
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
                    "You must be assigned to a department to view rulebooks.")
            domain = [('responsible_id', '=', self.env.user.department_id.id)]

        return {
            'name': 'RuleBooks',
            'type': 'ir.actions.act_window',
            'res_model': 'rulebook',
            'view_mode': 'tree,form,kanban',
            'domain': domain,
            'context': {
                'search_default_not_deleted': 1,
                'default_department_id': self.env.user.department_id.id if self.env.user.department_id else False,
            }
        }
        
        
    def data(self):
        # send the global value to the email template
        return global_data

    # to open up vie resolution button

    def open_reply_log(self):
        try:
            # Define your action here
            action = self.env.ref(
                "rule_book.action_reply_log").sudo().read()[0]

            # Set the default domain to show tickets with matching issue
            id = self.id
            action["domain"] = [("rulebook_id", "=", id)]

            return action
        except AccessError:
            # If the user lacks permissions, raise a friendly message
            raise AccessError(
                "You do not have the necessary permissions to view the Reply Log.")

    @api.depends('risk_category', 'risk_category.risk_priority')
    def _compute_risk_rating(self):
        """Compute risk rating based on the selected risk category's priority"""

        for record in self:
            if record.risk_category and record.risk_category.risk_priority:
                # Directly use the risk priority from the category
                record.risk_rating = record.risk_category.risk_priority
            else:
                record.risk_rating = False
                
    @api.onchange('semi_annual_month1', 'semi_annual_month2')
    def _onchange_semi_annual_months(self):
        if self.semi_annual_month1 == self.semi_annual_month2:
            return {
                'warning': {
                    'title': 'Invalid Month Selection',
                    'message': 'Please select different months for your semi-annual periods.'
                }
            }
            # raise AccessError("Semi-annual months must be different")

    # @api.depends("id")
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

   
    @api.depends("frequency_type", "date_value","time_value", "day_value", "month_value", "day_of_week",
                 "quarter_day", "bi_monthly_day1", "bi_monthly_day2", "semi_annual_month1",
                 "semi_annual_month2", "semi_annual_day1", "semi_annual_day2", "year_month_value")
    def _compute_date(self):
       
        today =  datetime.now()

        # Get the weekday (0 = Monday, 6 = Sunday)
        current_weekday = today.weekday()

        # Combine the date and set the time to 10:00 AM (Lagos time)
        default_time = datetime.combine(today.date(), datetime.min.time()).replace(
            hour=16, minute=0, second=0
        )


        for record in self:
            # Initialize is_recurring to False
            # record.is_recurring = False

            if record.frequency_type == "date":
                record.computed_date = record.date_value or default_time

                record.is_recurring = record.is_recurring or False

                _logger.critical("is reoccuring got here ",
                                 record.is_recurring)
                # Missing boolean assignment for is_recurring

            elif record.frequency_type == "day_of_month":
                if record.day_value and record.month_value:
                    try:
                        day_value = int(record.day_value)
                        month_value = int(record.month_value)
                        _logger.critical(
                            f"Day Value {day_value} , month value {month_value}")

                        # Calculate correct year
                        target_year = today.year
                        if month_value < today.month or (month_value == today.month and day_value < today.day):
                            target_year += 1

                        record.computed_date = fields.Datetime.to_datetime(
                            f"{target_year}-{month_value:02d}-{day_value:02d} 16:00:00"
                        )
                        # record.computed_date = datetime(
                        #     target_year, month_value, day_value, 8, 0, 0)

                        _logger.critical(
                            f"computed Date {record.computed_date}")
                    except ValueError:
                        next_month = (month_value % 12) + 1
                        record.computed_date = fields.Datetime.to_datetime(
                            f"{target_year}-{next_month:02d}-{day_value:02d} 16:00:00"
                        )
                        

                    # record.is_recurring = True
                    record.is_recurring = record.is_recurring or False

            elif record.frequency_type == "day_every_month":
                if record.day_value:
                    next_month = today.month + 1 if today.month < 12 else 1
                    year = today.year if today.month < 12 else today.year + 1
                    try:
                        record.computed_date = fields.Datetime.to_datetime(
                            f"{year}-{next_month:02d}-{int(record.day_value):02d} 16:00:00"
                        )

                        record.is_recurring = True
                    except ValueError:
                        # Handle invalid dates (e.g., February 31)
                        record.computed_date = default_time + \
                            timedelta(months=1)

            elif record.frequency_type == "daily":
                record.computed_date = default_time + timedelta(days=1)
                # record.is_recurring = True
                record.is_recurring = record.is_recurring or False

            elif record.frequency_type == "weekly":
                if record.day_of_week:
                    try:
                        selected_weekday = int(record.day_of_week)
                        days_ahead = selected_weekday - current_weekday
                        if days_ahead <= 0:
                            days_ahead += 7

                        next_date = today.date() + timedelta(days=days_ahead)
                        record.computed_date = datetime.combine(
                            next_date,
                            datetime.min.time()
                        ).replace(hour=16, minute=0, second=0)
                        

                    except (ValueError, TypeError):
                        record.computed_date = default_time + \
                            timedelta(weeks=1)
                else:
                    record.computed_date = default_time + timedelta(weeks=1)
                # record.is_recurring = True
                    record.is_recurring = record.is_recurring or False

            elif record.frequency_type == "monthly":
                next_month = today.month + 1 if today.month < 12 else 1
                year = today.year if today.month < 12 else today.year + 1
                record.computed_date = default_time.replace(
                    year=year, month=next_month, day=1)
                # record.is_recurring = True
                record.is_recurring = record.is_recurring or False

            elif record.frequency_type == "bi_monthly":
                # Ensure days are valid
                first_day = min(record.bi_monthly_day1 or 1, 28)
                second_day = min(record.bi_monthly_day2 or 15, 28)

                # Sort days to ensure first_day < second_day
                if first_day > second_day:
                    first_day, second_day = second_day, first_day

                current_month = today.month
                current_year = today.year

                # Create datetime objects for both days in current month
                first_date = default_time.replace(
                    year=current_year, month=current_month, day=first_day)
                second_date = default_time.replace(
                    year=current_year, month=current_month, day=second_day)

                # If both dates have passed in current month, move to next month
                if today.day > second_day:
                    if current_month == 12:
                        first_date = first_date.replace(
                            year=current_year + 1, month=1)
                        second_date = second_date.replace(
                            year=current_year + 1, month=1)
                    else:
                        first_date = first_date.replace(
                            month=current_month + 1)
                        second_date = second_date.replace(
                            month=current_month + 1)
                # If first date has passed but second hasn't
                elif today.day > first_day:
                    next_date = second_date
                    record.computed_date = next_date
                    # record.is_recurring = True
                    record.is_recurring = record.is_recurring or False

                    continue

                # Set next date to the first occurrence
                record.computed_date = first_date
                record.is_recurring = True
                record.is_recurring = record.is_recurring or False

            elif record.frequency_type == "quarterly":
                # today = fields.Date.today()
                current_quarter = (today.month - 1) // 3
                next_quarter = (current_quarter + 1) % 4
                next_quarter_month = (next_quarter * 3) + 1

                # If we're rolling over to next year
                year = today.year
                if next_quarter_month < today.month:
                    year += 1

                # Default to 7th if quarter_day is not set or invalid
                # Limit to 28 to avoid month overflow
                day = min(record.quarter_day or 7, 28)

                try:
                    record.computed_date = datetime(
                        year,
                        next_quarter_month,
                        day,
                        16, 0, 0  # 4 pM
                    )
                except ValueError:
                    # If date is invalid, default to first day of next quarter
                    record.computed_date = datetime(
                        year,
                        next_quarter_month,
                        1,
                        16, 0, 0
                    )
                # record.is_recurring = True
                record.is_recurring = record.is_recurring or False

            elif record.frequency_type == "semi_annually":
                today = fields.Date.today()
                year = today.year

                # Default values if not set
                month1 = record.semi_annual_month1 or 1
                month2 = record.semi_annual_month2 or 7
                # Limit to 28 to avoid month issues
                day1 = min(record.semi_annual_day1 or 28, 28)
                day2 = min(record.semi_annual_day2 or 6, 28)

                # Create datetime objects for comparison
                date1 = datetime(year, month1, day1, 16, 0, 0) 
                date2 = datetime(year, month2, day2, 16, 0,
                                 0) 

                # Sort dates chronologically
                if date1 > date2:
                    date1, date2 = date2, date1

                now = datetime.now()
                # now = datetime.now(pytz.timezone(
                #     "Africa/Lagos")).replace(tzinfo=None)
                # now = datetime.now(pytz.timezone("Africa/Lagos"))

                # Determine next occurrence
                if now < date1:
                    next_date = date1
                elif now < date2:
                    next_date = date2
                else:
                    # Both dates have passed, move to next year
                    next_date = date1.replace(year=year + 1)

                try:
                    record.computed_date = next_date
                except ValueError:
                    # Handle invalid dates (e.g., February 29 in non-leap year)
                    if next_date.month == 2:
                        record.computed_date = next_date.replace(day=28)
                    else:
                        # Use last day of the month
                        last_day = calendar.monthrange(
                            next_date.year, next_date.month)[1]
                        record.computed_date = next_date.replace(
                            day=min(next_date.day, last_day))

                # record.is_recurring = True
                record.is_recurring = record.is_recurring or False

            elif record.frequency_type == "yearly":

                if not record.year_month_value:
                    continue

                year = today.year
                month_value = int(record.month_value)

                # Validate month value
                if not (1 <= month_value <= 12):
                    _logger.warning(
                        f"Invalid month value: {month_value}. Setting to default month 12.")
                    month_value = 12

                # Get the maximum days for the selected month
                max_days = calendar.monthrange(year, month_value)[1]

                # Ensure day is valid for the selected month
                day = min(record.day_value or 1, max_days)

                # Create the yearly datetime
                try:
                    # Setting time to 7 AM
                    yearly_date = datetime(year, month_value, day, 16, 0)

                    # If the date has already passed this year, move to next year
                    if yearly_date < today:
                        yearly_date = yearly_date.replace(year=year + 1)

                    record.computed_date = yearly_date
                    # record.is_recurring = True
                    record.is_recurring = record.is_recurring or False

                except ValueError as e:
                    _logger.error(
                        f"Error creating date: year={year}, month={month_value}, day={day}. Error: {e}")
                    continue

            elif record.frequency_type == "three_yearly":
                if record.month_value and record.day_value:
                    try:
                        month_value = int(record.month_value)
                        # Limit to 28 to avoid month issues
                        day_value = min(record.day_value or 1, 28)

                        # Calculate the target year
                        current_year = today.year
                        target_year = current_year

                        # Create the initial date
                        target_date = datetime(
                            target_year,
                            month_value,
                            day_value,
                            16, 0, 0  # 4 pM
                        )

                        # If the date has passed this year, calculate next occurrence
                        if target_date < today:
                            # Find the next three-year cycle
                            years_to_add = (
                                3 - ((today.year - target_year) % 3))
                            target_date = target_date.replace(
                                year=today.year + years_to_add)

                        record.computed_date = target_date
                        # record.is_recurring = True
                        record.is_recurring = record.is_recurring or False

                    except ValueError as e:
                        _logger.error(f"Error creating three-yearly date: {e}")
                        record.computed_date = default_time

            elif record.frequency_type == "immediate" :
                record.computed_date = today
                # record.computed_date = today + timedelta(hours=record.time_value)                
                record.is_recurring = False
                

    @api.onchange('bi_monthly_day1', 'bi_monthly_day2')
    def _onchange_days(self):
        """Validate and adjust days if they exceed 28"""
        if self.bi_monthly_day1 and self.bi_monthly_day1 > 28:
            self.bi_monthly_day1 = 28
        if self.bi_monthly_day2 and self.bi_monthly_day2 > 28:
            self.bi_monthly_day2 = 28
            
    

    @api.onchange('month_value', 'day_value')
    def _onchange_yearly_date(self):
        """Validate and adjust yearly date values"""
        # today_date=self.get_lagos_date()
        if self.month_value:
            try:
                month = int(self.month_value)
            except ValueError:
                month = None

            # Validate month value
            if month is None or not (1 <= month <= 12):
                self.month = '12'
                return {
                    'warning': {
                        'title': 'Invalid Month',
                        'message': 'Month must be between 1 and 12. Setting to December (12).'
                    }
                }

            # Validate day value against the selected month
            if self.day_value:
                max_days = calendar.monthrange(
                    fields.Date.today().year, month)[1]
                if self.day_value > max_days:
                    self.day_value = max_days
                    return {
                        'warning': {
                            'title': 'Day Adjusted',
                            'message': f'Day value was adjusted to {max_days} to match the maximum days in the selected month.'
                        }
                    }

    @api.constrains('semi_annual_month1', 'semi_annual_month2', 'semi_annual_day1', 'semi_annual_day2')
    def _check_semi_annual_values(self):
        for record in self:
            if record.frequency_type == 'semi_annually':
                # Check months
                if record.semi_annual_month1 and not 1 <= record.semi_annual_month1 <= 12:
                    raise AccessError("First month must be between 1 and 12")
                if record.semi_annual_month2 and not 1 <= record.semi_annual_month2 <= 12:
                    raise AccessError("Second month must be between 1 and 12")
                if record.semi_annual_month1 == record.semi_annual_month2:
                    raise AccessError("Semi-annual months must be different")

                # Check days
                if record.semi_annual_day1 and not 1 <= record.semi_annual_day1 <= 31:
                    raise AccessError("First day must be between 1 and 31")
                if record.semi_annual_day2 and not 1 <= record.semi_annual_day2 <= 31:
                    raise AccessError("Second day must be between 1 and 31")

                # Validate specific month-day combinations
                for month, day in [(record.semi_annual_month1, record.semi_annual_day1),
                                   (record.semi_annual_month2, record.semi_annual_day2)]:
                    if month and day:
                        try:
                            # Try to create a date to validate the day is valid for the month
                            current_year = datetime.now().year
                            # Use leap year to allow Feb 29
                            datetime(current_year, month, day)
                        except ValueError:
                            raise AccessError(
                                f"Invalid day {day} for month {month}")

    @api.onchange('frequency_type')
    def _onchange_frequency_type(self):
        if self.frequency_type == 'semi_annually':
            self.semi_annual_month1 = 1  # January
            self.semi_annual_month2 = 8  # August
            self.semi_annual_day1 = 28   # 28th
            self.semi_annual_day2 = 6    # 6th

    @api.constrains('day_value')
    def _check_three_year_day(self):
        for record in self:
            if record.frequency_type == 'three_yearly' and record.day_value:
                if not 1 <= record.day_value <= 28:
                    raise ValidationError(
                        ("Please select a day between 1 and 28 for three-yearly frequency."))
                    

    @api.depends("reg_due_date_value", "reg_due_date_unit", "computed_date")
    def _compute_reg_due_date(self):
        for record in self:
            if record.computed_date and record.reg_due_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                delta_args = {
                    record.reg_due_date_unit: record.reg_due_date_value
                }
                record.reg_due_date = record.computed_date + relativedelta(
                    **delta_args
                )
               
            else:
                # If computed_date is not available, set reg_due_date to False or handle accordingly
                record.reg_due_date = None

   
    def _compute_reg_due_date_for_cron_job(self):
        for record in self:
            if record.computed_date and record.reg_due_date_unit in [
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
                    record.reg_due_date = record.computed_date + relativedelta(
                        **delta_args
                    )
                    _logger.critical(
                        f"writing escalation date {record.reg_due_date}")
                    record.sudo().write({
                        'reg_due_date': record.reg_due_date,
                    })
            else:
                # If computed_date is not available, set reg_due_date to False or handle accordingly
                record.reg_due_date = None
                

    def _compute_escalation_date_for_cron_job(self):
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
                    record.escalation_date_unit: record.escalation_date_value
                }
                record.escalation_date = record.computed_date + relativedelta(
                    **delta_args
                )
                _logger.critical(
                    f"writing escalation date {record.escalation_date}")

                record.sudo().write({
                    'escalation_date': record.escalation_date,
                })
            else:
                # If computed_date is not available, set reg_due_date to False or handle accordingly
                record.escalation_date = None

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
                    record.escalation_date_unit: record.escalation_date_value  # Remove negative sign
                }
                record.escalation_date = record.computed_date + relativedelta(**delta_args)
            else:
                # If computed_date is not available, set escalation_date to None or handle accordingly
                record.escalation_date = None
                
     
    @api.depends("reminder_due_date_value", "reminder_due_date_unit", "computed_date")
    def _compute_reminder_due_date(self):
        for record in self:
            if record.computed_date and record.reminder_due_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                delta_args = {
                    record.reminder_due_date_unit: -record.reminder_due_date_value
                }
                record.reminder_due_date = record.computed_date + relativedelta(
                    **delta_args
                )
               
            else:
                # If computed_date is not available, set reg_due_date to False or handle accordingly
                record.reminder_due_date = None

    @api.depends("computed_date")
    def _compute_formatted_rulebook_date(self):
        for record in self:
            if record.computed_date:

                record.formatted_rulebook_date = self._compute_formatted_date(
                    (record.computed_date))
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
     
    def _prepare_email_data(self):
        """Prepare email data dictionary"""
        try:
            # Detailed checks for each required field
            if not self.officer_responsible:
                _logger.critical(
                    f"No officer responsible for record {self.id}")
                return {}


            now = datetime.now()
            now_without_microseconds = now.replace(microsecond=0)
            global global_data
            global_data = {
                "officer_responsible": self.officer_responsible.name or "N/A",
                "responsible_id": self.responsible_id.name or "N/A",
                "rulebook_name": self.name.name or "N/A",
                "record_link": self._record_link(self.id) or "N/A",
                "upload_link": self._compute_upload_link(self.id) or "N/A",
                "current_year": fields.Date.today().year,
                "rulebook_return": re.sub(r'<[^>]+>', '', self.type_of_return) or "N/A",
                # "regulatory_name": self.regulatory_name or "N/A",
                "regulatory_name": self.regulatory_agency_id.name or "N/A",
                "risk_category": self.risk_category.name if self.risk_category else "N/A",
                "email_to": self.officer_responsible.email or "N/A",
                "email_from": os.getenv("EMAIL_FROM"),
                "email_cc": ", ".join(self.officer_cc.mapped('email')) or "",
                "first_line_escalation": self.first_line_escalation.email or "",
                "first_line_name": self.first_line_escalation.name or "",
                "second_line_escalation": self.second_line_escalation.email or "",
                "computed_date": self._compute_formatted_date(self.computed_date) or "N/A",
                "escalation_date": self.escalation_date or "N/A",
                "reg_due_date": self._compute_formatted_date(self.reg_due_date) or "N/A",
                "datetime": self._compute_formatted_date(now_without_microseconds)

            }

            # Optional CC handling

            # Extensive logging of prepared email data
            _logger.critical(f"Prepared email data: {global_data}")

            return global_data

        except Exception as e:
            _logger.critical(f"Error preparing email data: {str(e)}")
            _logger.critical(traceback.format_exc())
            return {}

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
                    "rule_book.email_template_internal_due_date_immediate_")
            except ValueError:
                _logger.critical("internal_due_date Email template not found")
                return False

            # Detailed logging for debugging
            _logger.critical(
                f"Attempting to send internal_due_date email for record {self.id}")
            _logger.critical(f"Email data from Rulebool model: {email_data}")

            # Send the email
            email_result = template.send_mail(self.id, force_send=True)

            _logger.critical(
                f"internal_due_date Email sent successfully. Result: {email_result}")
            return True
        except Exception as e:
            _logger.critical(f"Comprehensive email send failure: {str(e)}")
            # Log the full traceback for more detailed debugging
            _logger.critical(traceback.format_exc())
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

    def _send_reminder_due_date_email(self):
        """Send Reminder notification email"""
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
                    " reminder Email template not found")
                return False

            # Detailed logging for debugging
            _logger.critical(
                f"Attempting to send Reminder email for record {self.id}")
            _logger.critical(f"Email data: {email_data}")

            # Send the email
            email_result = template.send_mail(self.id, force_send=True)

            _logger.critical(
                f"Reminder Email sent successfully. Result: {email_result}")
            return True
        except Exception as e:
            _logger.critical(f"Comprehensive email send failure: {str(e)}")
            # Log the full traceback for more detailed debugging
            _logger.critical(traceback.format_exc())
            return False

    def _prepare_reply_log_vals(self, submission_status, submission_time):
        """Prepare values for reply log creation"""

        return {
            'rulebook_id': self.id,
            'reporter': self.officer_responsible.id if self.officer_responsible else False,
            'rulebook_status': submission_status,
            'submission_timing': submission_time,
            'reply_date': None,
            'reply_content': None,
            'department_id': self.responsible_id.id if self.responsible_id else False,
            'rulebook_compute_date': self.computed_date,
            'next_compute_date': self.reg_due_date,
            'day_of_week': self.day_of_week,
            'month_value': self.month_value,
            'bi_monthly_day2': self.bi_monthly_day2,
            'bi_monthly_day1': self.bi_monthly_day1,
            'semi_annual_day2': self.semi_annual_day2,
            'semi_annual_day1': self.semi_annual_day1,
            'semi_annual_month2': self.semi_annual_month2,
            'semi_annual_month1': self.semi_annual_month1,
            'quarter_day': self.quarter_day,
            'status': self.status,
            # 'officer_cc': self.officer_cc.id if self.officer_cc else False,
            'officer_cc': [(6, 0, self.officer_cc.ids)] if self.officer_cc else False,
            'escalation_date': self.escalation_date,
            'escalation_date_unit': self.escalation_date_unit,
            'escalation_date_value': self.escalation_date_value,
            'reg_due_date_unit': self.reg_due_date_unit,
            'reg_due_date_value': self.reg_due_date_value,
            'first_line_escalation': self.first_line_escalation.id if self.first_line_escalation else False,
            'second_line_escalation': self.second_line_escalation.id if self.second_line_escalation else False,
            'reg_due_date':self.reg_due_date,
            'rulebook_name': self.type_of_return,
            'reminder_due_date': self.reminder_due_date,
            'reminder_due_date_unit':self.reminder_due_date_unit,
            'reminder_due_date_value': self.reminder_due_date_value

        }

    def _prepare_reply_log_vals_for_update(self):
        """
        Prepare values for reply log creation with only specific fields
        """
        return {
            'reporter': self.officer_responsible.id if self.officer_responsible else None,
            'department_id': self.responsible_id.id if self.responsible_id else None,
            'officer_cc': self.officer_cc,
            'status': self.status,
            'first_line_escalation': self.first_line_escalation,
            'second_line_escalation': self.second_line_escalation
        }

    @api.model
    def create(self, vals):
        try:
            # Handle risk category
            self._update_risk_rating(vals)

            # Create rulebook record
            record = super(Rulebook, self).create(vals)

            # Add 1 hour to computed_date if it exists
            # if record.computed_date:
            #     _logger.critical(f"added one hour to rulebook")
            #     record.computed_date += timedelta(hours=1)
            if record.computed_date and not record.computed_date_adjusted:
                _logger.critical(f"Adding one hour to computed_date for Rulebook ID {record.id}")
                record.computed_date += timedelta(hours=1)
                record.computed_date_adjusted = True

            # Initialize submission status
            submission_status = 'pending'
            submission_time = 'pending'

            # Handle immediate frequency
            if record.frequency_type == "immediate":
                submission_status
                submission_time
                record._send_internal_due_date_email()
                record._copy_rulebook_chatter_to_reply_log()
            else:
                reply_log_vals = record._prepare_reply_log_vals(
                    submission_status,
                    submission_time
                )
                reply = self.env['reply.log'].create(reply_log_vals)
                _logger.critical(
                    f"insert rulebook record {reply}   computed date from rulebook {record.computed_date}")

                if reply and reply.next_compute_date or reply.escalation_date:
                    # reply._schedule_due_dates()
                    None

            return record

        except Exception as e:
            # Optional: add error handling
            _logger.critical(f"Error in Rulebook create method: {e}")
            raise

    
    def write(self, vals):
        # Log the record ID for debugging
        self._update_risk_rating(vals)
        # self.your_method()

        _logger.critical(f"Write values for rulebook email data: {vals}")
        
        updating_computed_date = "computed_date" in vals or "frequency_type" in vals

        # If 'computed_date' exists and is being updated, subtract one hour
        if updating_computed_date and "computed_date" in vals:
            _logger.critical(f"computed date was herrere")
            # Ensure computed_date is not None before performing arithmetic
            if vals["computed_date"]:
                _logger.critical(f"computed date got updated")

                vals["computed_date"] = vals["computed_date"] - timedelta(hours=1)

        # Call the super method and capture the result (typically a boolean)
        result = super(Rulebook, self).write(vals)
        # result = super(Rulebook, self.with_env(self.env).sudo()).write(vals)

        if vals.get("frequency_type") == "immediate":
            try:
                # Prepare email data
                email_data = self._prepare_email_data()

                # Enhanced logging
                _logger.info(
                    f"Frequency type set to immediate for record {self.id}")
                _logger.info(f"Prepared email data: {email_data}")

                # Send immediate notification
                if email_data:
                    send_result = self._send_internal_due_date_email()
                    if not send_result:
                        _logger.error(
                            f"Failed to send email for record {self.id}")
                else:
                    _logger.warning(
                        f"No email data prepared for record {self.id}")
                
                self._copy_rulebook_chatter_to_reply_log()


            except Exception as e:
                _logger.error(f"Error in email sending process: {str(e)}")

                self._schedule_due_dates()


        return result

    def _copy_rulebook_chatter_to_reply_log(self):
        # Search for the most recent reply log for this rulebook
        reply_logs = self.env['reply.log'].search(
            [('rulebook_id', '=', self.id)],
            order='create_date desc',
            limit=1
        )

        # Prepare values for the new reply log
        reply_log_vals = self._prepare_reply_log_vals(
            submission_status='pending',
            submission_time='pending'
        )

        reply_log_vals['rulebook_id'] = self.id

        _logger.critical(f"Reply log vals {reply_log_vals} rulebook_id {self.id}")

        # If no reply log exists and frequency type is "immediate", create a new reply log
        if not reply_logs and self.frequency_type == "immediate":
            _logger.critical(
                f"frequency is immediate and reply_logs is {reply_logs} logsid {reply_logs.id}  self id{self.id}")
            try :
                reply_log = self.env['reply.log'].create(reply_log_vals)
            except Exception as e:
                   _logger.critical(f"Error Insert Reply LOg: {e}")
            
            if reply_log :
                _logger.critical(
                    f"Inserted Success fully {reply_logs}  ID {self.id} , Valuses {reply_log_vals}")
            _logger.critical(
                f"Inserted record is {reply_log}")
        else:
            # If reply log exists or frequency type is not "immediate", use the existing reply log
            reply_log = reply_logs[0] if reply_logs else None

        # Ensure that reply_log is not None
        if not reply_log:
            _logger.critical(
                f"Failed to find or create a reply log for rulebook {self.id}")
            return None  # Return None or handle the error appropriately

        # Copy all messages from rulebook to reply log
        messages = self.env['mail.message'].search([
            ('res_id', '=', self.id),
            ('model', '=', 'rulebook')
        ], order='create_date asc')

        for message in messages:
            # Copy the message to the reply log
            message.copy({
                'model': 'reply.log',
                'res_id': reply_log.id
            })

        # Copy email templates (if any)
        if self.message_main_attachment_id:
            # Link the main attachment to the reply log
            self.env['ir.attachment'].browse(self.message_main_attachment_id.id).copy({
                'res_model': 'reply.log',
                'res_id': reply_log.id
            })

        return reply_log


    
    def _schedule_due_dates(self):
        for record in self:
            # Ensure any existing events are removed
            events = self.env["calendar.event"].search(
                [("name", "ilike",
                  f"Regulatory Due Date for {re.sub(r'<[^>]+>', '', record.rulebook_name)} (ID: {record.rulebook_id})")]
            )
            events.unlink()

            events = self.env["calendar.event"].search(
                [("name", "ilike",
                  f"Internal Due Date for {re.sub(r'<[^>]+>', '', record.rulebook_name)} (ID: {record.rulebook_id})")]
            )
            events.unlink()

            events = self.env["calendar.event"].search(
                [("name", "ilike",
                  f"Escalation Due Date for {re.sub(r'<[^>]+>', '', record.rulebook_name)} (ID: {record.rulebook_id})")]
            )
            events.unlink()

            if record.rulebook_compute_date:
                self.env["calendar.event"].create(
                    {
                        "name": f"Internal Due Date for {re.sub(r'<[^>]+>', '', record.rulebook_name)} (ID: {record.rulebook_id})",
                        "start": record.rulebook_compute_date,
                        "stop": record.rulebook_compute_date
                        + timedelta(hours=1),  # Set duration of 1 hour
                        "allday": False,  # Event is not all day; includes specific time
                    }
                )

            if record.reg_due_date:
                self.env["calendar.event"].create(
                    {
                        "name": f"Regulatory Due Date for {re.sub(r'<[^>]+>', '', record.rulebook_name)} (ID: {record.rulebook_id})",
                        "start": record.reg_due_date,
                        "stop": record.reg_due_date
                        + timedelta(hours=1),  # Set duration of 1 hour
                        "allday": False,  # Event is not all day; includes specific time
                    }
                )

            if record.escalation_date:
                self.env["calendar.event"].create(
                    {
                        "name": f"Escalation Due Date for {re.sub(r'<[^>]+>', '', record.rulebook_name)} (ID: {record.rulebook_id})",
                        "start": record.escalation_date,
                        "stop": record.escalation_date
                        + timedelta(hours=1),  # Set duration of 1 hour
                        "allday": False,  # Event is not all day; includes specific time
                    }
                )

    # editing the record
    def _update_risk_rating(self, vals):
        """
        Update risk rating based on risk category.

        Args:
            vals (dict): Dictionary of values being written
        """
        try:
            if "risk_category" in vals:
                category = self.env["rulebook.risk_category"].browse(
                    vals["risk_category"])

                if category and category.risk_priority:
                    vals["risk_rating"] = category.risk_priority

                _logger.info(f"Risk category updated: {category}")

        except Exception as e:
            _logger.error(f"Error updating risk rating: {e}")

    

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

            # # Log formatted time for debugging
            # _logger.critical(f"formatted time: {formatted_time}")

            return formatted_time

        except Exception as e:
            _logger.critical(f"Date formatting CRITICAL error: {e}")
            return str(dt)


    
    def send_due_date_emails(self):
        # Get the current time and calculate the start and end window for the email notifications
        now = fields.Datetime.now()
        start_window = now - timedelta(minutes=29)
        end_window = now + timedelta(minutes=29)

        start_window_str = fields.Datetime.to_string(start_window)
        end_window_str = fields.Datetime.to_string(end_window)

        # Load EVENT_TYPES from the environment variable and parse it into a dictionary
        event_types_str = os.getenv("EVENT_TYPES")
        if event_types_str:
            EVENT_TYPES = json.loads(event_types_str)
        else:
            _logger.critical("EVENT_TYPES not found in environment variables.")
            return

        # Iterate through event types and process each one
        for event_type, config in EVENT_TYPES.items():
            # Search for calendar events matching the criteria
            events = self.env["calendar.event"].search([
                ("name", "ilike", config["name_filter"]),
                ("start", "<=", end_window_str),
                ("start", ">=", start_window_str)
            ])

            # Process each event
            global global_data

            for event in events:
                # Extract the rulebook ID from the event's name (assuming ID is at the end)
                try:
                    rulebook_id = self._get_rulebook_from_event(event)

                    global_data = {

                        "first_line_escalation": rulebook_id.first_line_escalation.email or "",
                        "first_line_name": rulebook_id.first_line_escalation.name or "",
                        "second_line_escalation": rulebook_id.second_line_escalation.email or "",
                        "officer_responsible": rulebook_id.officer_responsible.name or "N/A",
                        "responsible_id": rulebook_id.responsible_id.name or "N/A",
                        "rulebook_name": rulebook_id.name.name or "N/A",
                        "computed_date": self._compute_formatted_date(rulebook_id.computed_date) or "N/A",
                        "escalation_date": self._compute_formatted_date(rulebook_id.escalation_date) or "N/A",
                        "reg_due_date": self._compute_formatted_date(rulebook_id.reg_due_date) or "N/A",
                        "record_link": self._record_link(self.id) or "N/A",
                        "upload_link": self._compute_upload_link(self.id) or "N/A",
                        "current_year": fields.Date.today().year,
                        # "rulebook_return": rulebook_id.type_of_return or "N/A",
                        "rulebook_return": re.sub(r'<[^>]+>', '', rulebook_id.type_of_return or '') or "N/A",
                        "regulatory_name": rulebook_id.regulatory_agency_id.name or "N/A",
                        "risk_category": rulebook_id.risk_category.name if self.risk_category else "N/A",
                        "email_to": rulebook_id.officer_responsible.email or "N/A",
                        "email_from": os.getenv("EMAIL_FROM"),
                        "email_cc": ", ".join(rulebook_id.officer_cc.mapped('email')) or ""
                    }

                    if rulebook_id and rulebook_id.status == "active" and rulebook_id.frequency_type != "immediate":
                        # Send the email using the appropriate template
                        template_ref = config["template_ref"]
                        self.env.ref(template_ref).send_mail(
                            rulebook_id.id, force_send=True)
                except ValueError:
                    # Handle cases where the rulebook ID is not valid or cannot be extracted
                    _logger.warning(
                        f"Invalid rulebook ID in event: {event.name}")

   
    def _get_rulebook_from_event(self, event):
        """
        Helper method to extract the rulebook ID from the event's name.
        Assumes the rulebook ID is in the format '(ID: <rulebook_id>)' in the event name.
        """
        try:
            # Use regex to find the ID number in the format (ID: <number>)
            match = re.search(r"\(ID:\s*(\d+)\)", event.name)
            if match:
                rulebook_id_str = match.group(1)  # Extract the matched ID
                return self.env["rulebook"].browse(int(rulebook_id_str))
            else:
                raise ValueError(
                    f"Rulebook ID not found in event name: {event.name}")
        except (ValueError, IndexError) as e:
            raise ValueError(
                f"Error extracting rulebook ID from event: {event.name}. Error: {str(e)}")

    @api.model
    def check_rulebook_and_update_due_date(self):
        """Check rulebooks with today's regulatory date and update next due date."""

        today = datetime.now().date()

        _logger.critical(
            f"rulebooks with today's regulatory date  {today}, plus one day {today + timedelta(days=1)}")

        today = fields.Datetime.now()
        # .astimezone(
        #     pytz.timezone('Africa/Lagos')).replace(tzinfo=None)

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
        rulebooks = self.env["rulebook"].search(
            [
                ("computed_date", ">=", day_start),
                ("computed_date", "<", day_end),
                ("is_recurring", "=", True),
            ]
        )

        for record in rulebooks:
            try:
                _logger.critical(
                    f"To Update rulebook {record.id}:,  type of return : {re.sub(r'<[^>]+>', '', record.type_of_return)} frequency : {record.frequency_type}")

                computed_date = (record.computed_date)
                time_in_5_minutes = computed_date - timedelta(minutes=5)

                if today >= time_in_5_minutes and computed_date:
                    record._compute_next_due_date()

            except Exception as e:
                _logger.critical(f"Failed to update rulebook {record.id}: {e}")
                
                

    def _compute_next_due_date(self):
        _logger.critical("Updating next due date...")

        """Compute the next internal due date for the rulebook when the status is 'completed'."""

        for record in self:

            try:
                next_compute_date = None

                if record.frequency_type == "monthly":
                    next_compute_date = record.computed_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "quarterly":
                    next_compute_date = record.computed_date + \
                        relativedelta(months=3)

                elif record.frequency_type == "yearly":
                    next_compute_date = record.computed_date + \
                        relativedelta(years=1)

                elif record.frequency_type == "daily":
                    next_compute_date = record.computed_date + \
                        relativedelta(days=1)

                elif record.frequency_type == "weekly":
                    next_compute_date = record.computed_date + \
                        relativedelta(weeks=1)

                elif record.frequency_type == "day_of_month":
                    next_compute_date = record.computed_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "day_every_month":
                    next_compute_date = record.computed_date + \
                        relativedelta(months=1)

                elif record.frequency_type == "bi_monthly":
                    # Get the current day of the computed date
                    current_day = record.computed_date.day

                    # Determine if it's the first or second date of the month
                    if current_day == record.bi_monthly_day1:
                        # If current is first day, next is second day of same month
                        next_compute_date = record.computed_date.replace(
                            day=record.bi_monthly_day2)
                    else:
                        # If current is second day, next is first day of next month
                        next_compute_date = record.computed_date + \
                            relativedelta(months=1)
                        next_compute_date = next_compute_date.replace(
                            day=record.bi_monthly_day1)

                elif record.frequency_type == "semi_annually":
                    # Get current date components
                    current_month = record.computed_date.month
                    current_day = record.computed_date.day

                    if current_month == record.semi_annual_month1:
                        # Move to second date of the year
                        next_compute_date = record.computed_date.replace(
                            month=record.semi_annual_month2,
                            day=record.semi_annual_day2
                        )
                    else:
                        # Move to first date of next year
                        next_compute_date = record.computed_date.replace(
                            year=record.computed_date.year + 1,
                            month=record.semi_annual_month1,
                            day=record.semi_annual_day1
                        )
                elif record.frequency_type == "three_yearly":
                    next_compute_date = record.computed_date + \
                        relativedelta(years=3)

                elif record.frequency_type == "date":
                    next_compute_date = record.computed_date

                elif record.frequency_type == "immediate":
                    next_compute_date = record.computed_date

                else:
                    next_compute_date = record.computed_date

                if not next_compute_date:
                    _logger.critical(
                        f"Invalid frequency type '{record.frequency_type}' for record {record.id}.")
                    next_compute_date = record.computed_date

                record.next_compute_date = next_compute_date
                record.computed_date = next_compute_date

                record.sudo().write({
                    'next_compute_date': next_compute_date,
                    'computed_date': next_compute_date
                })

                record._compute_escalation_date_for_cron_job()
                record._compute_reg_due_date_for_cron_job()

                _logger.critical(
                    f"Record {record.id}: Computed next due date as {next_compute_date}.")

            except Exception as e:
                _logger.error(
                    f"Error computing next due date for record {record.id}: {str(e)}"
                )
                record.next_compute_date = None

    @api.model
    def _create_rulebook_logs_entry(self):
        """create rulebook logs entry for rulebook."""
        # today = fields.Datetime.now().astimezone(
        #     pytz.timezone('Africa/Lagos')).replace(tzinfo=None)

        _logger.critical(
            f"create rulebook logs entry for rulebook cron job has started ")

        # Get all rulebook IDs in the current recordset
        rulebook_ = self.env["rulebook"].search([])
        rulebook_ids = rulebook_.ids
        _logger.critical(f"Rulebook ids {rulebook_ids}")

         
        rulebooks_with_no_reply_log = self.env['rulebook'].search([
            ('id', 'not in', self.env['reply.log'].search([]).mapped(
                'rulebook_id').ids),  # Rulebooks not in reply.log
            ('status', '=', 'active'),  # Status should be 'active'
            # Frequency should not be 'immediate'
            ('frequency_type', '!=', 'immediate'),
            ('computed_date', '!=', False)  # is_reoccuring should be True
        ])


        _logger.critical(f" Rulebooks IDS NOT IN Rulebook LOGS{rulebooks_with_no_reply_log}")

        for rulebook in rulebooks_with_no_reply_log:
            reply_log_vals = rulebook._prepare_reply_log_vals(
                submission_status='pending',
                submission_time='pending'

            )
            _logger.critical(
                f"REPLY LOGS VALUES FOR INSERT {reply_log_vals} ")
            try:
                
                reply_log_vals = rulebook._prepare_reply_log_vals(
                    submission_status ='pending',
                    submission_time='pending' ,                   
                )
                if rulebook.computed_date and not rulebook.computed_date_adjusted:
                    adjusted_computed_date = rulebook.computed_date + \
                        timedelta(hours=1)
                    
                else:
                    adjusted_computed_date = rulebook.computed_date
                    
                _logger.critical(
                    f" adjusted_computed_date {adjusted_computed_date},.... computed_date { rulebook.computed_date}")

                reply_log_vals['rulebook_compute_date'] = adjusted_computed_date
                
                          
                reply = self.env['reply.log'].create(reply_log_vals)

                _logger.critical(
                    f"Created reply.log for Rulebook ID {rulebook.id}  Reply Log ID {reply.id}")


            except Exception as e:
                _logger.critical(
                    f"Failed to process Rulebook {rulebook.id}: {e}")
                continue
    
             

   
    @api.model
    def check_and_update_rulebooks(self):
        """Scheduler to check rulebooks with today's date as the regulatory date and update next due date."""
        _logger.critical(
            "Scheduler to check rulebooks with today's date as the regulatory date and update next due date.")

        # Specify the Africa timezone (Lagos)

        timezone_str = "Africa/Lagos"
        timezone = pytz.timezone(timezone_str)
        # today = datetime.now(timezone)
        today = datetime.now()
        today_start = today.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = today.replace(
            hour=23, minute=59, second=59, microsecond=999999)

        # Format the date and time to the desired output
        formatted_today_start = today_start.strftime('%Y-%m-%d %H:%M:%S')
        formatted_today_end = today_end.strftime('%Y-%m-%d %H:%M:%S')

        time_in_15_minutes = today + timedelta(minutes=10)
        time_in_15_minutes = time_in_15_minutes.strftime('%Y-%m-%d %H:%M:%S')

        _logger.critical("today start : ", today_start,
                         " today end", today_end)

        # Search for rulebooks where computed_date is today and is_recurring is True
        rulebooks = self.search(
            [
                ("computed_date", ">=", formatted_today_start),
                ("computed_date", "<=", formatted_today_end),
                ("is_recurring", "=", True),
            ]
        )

        for rulebook in rulebooks:
            computed_date = rulebook.computed_date

            # Localize the naive datetime to Africa/Lagos timezone
            if computed_date.tzinfo is None:
                try:
                    # Attempt to localize the naive datetime
                    localized_computed_date = timezone.localize(computed_date)
                except Exception as e:
                    # Log any localization errors
                    _logger.error(f"Error localizing date: {e}")
                    continue
            else:
                # If already timezone-aware, convert to Africa/Lagos
                localized_computed_date = computed_date.astimezone(timezone)

            # Compare with current time in Africa/Lagos
            if today <= localized_computed_date < time_in_15_minutes:
                rulebook._compute_next_due_date()

    def get_lagos_date(self):
        # Get the current UTC time
        utc_now = datetime.now(pytz.UTC)

        # Convert to Lagos timezone
        lagos_tz = pytz.timezone('Africa/Lagos')
        lagos_now = utc_now.astimezone(lagos_tz)

        # Get just the date
        lagos_date = lagos_now.date()

        return lagos_date

    def clean_html_in_type_of_return(self):
        """
        Clean HTML tags from type_of_return field and capitalize the first letter of each word.
        """
        # Fetch all rulebook records (you can apply filters if needed)
        rulebooks = self.search([('type_of_return', '!=', False)])

        for rulebook in rulebooks:
            # Clean the 'type_of_return' field by removing HTML tags and entities
            clean_value = re.sub(r'(<[^>]+>|&\w+;)', '', rulebook.type_of_return)
            clean_value = re.sub(r'\s+', ' ', clean_value).strip()

            # Capitalize the first letter of each word
            clean_value = ' '.join(word.capitalize()
                                for word in clean_value.split())

            # Update the record with the cleaned and capitalized value
            if clean_value != rulebook.type_of_return:
                rulebook.sudo().write({'type_of_return': clean_value})
                _logger.critical(
                    f"Updated rulebook {rulebook.id}: Cleaned and capitalized type_of_return field.")

    def update_time_zone_for_all_users(self):
        users = self.env['res.users'].search([])  # Get all users
        for user in users:
            user.sudo().write({'tz': 'Africa/Lagos'})
            _logger.critical(
                F"timezone update successfull {user}  users found ..{users}")
        return True
    
    @api.model
    def clear_employee_mobile_phone(self):
        # Explicitly reference the hr.employee model
        employee_model = self.env['hr.employee']        
        # Fetch all employees explicitly
        employees = employee_model.search([])  # Get all employees
        # Loop over each employee and clear their mobile_phone
        for employee in employees:
            employee.sudo().write({'work_phone': False})  # Set mobile_phone to False (NULL in Odoo)

        return True
    
