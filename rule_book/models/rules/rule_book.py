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


_logger = logging.getLogger(__name__)

from dotenv import load_dotenv

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
        string="RuleBook Title",
        required=True,
        tracking=True,
        help="Enter the title of the rulebook.",
        # default=""
    )

    theme_id = fields.Many2one(
        "rulebook.theme",
        string="Rulebook Theme",
        # required=False,
        # tracking=True,
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
        # default="critical"
    )

    risk_category = fields.Many2one(
        "rulebook.risk_category",
        string="Risk Category",
        required=True,
        tracking=True,
        help="Select the risk category for this rulebook.",
        default="Compliance Risk"
    )

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
        "hr.department",
        string="Department Responsible",
        required=True,
        tracking=True,
        help="Select the department responsible for this rulebook.",
        default=lambda self: self.env.user.department_id.id
    )
    
    officer_responsible = fields.Many2one(
        'res.users',  # Assuming you are linking to the hr.employee model
        string="Officer Responsible",
        required=True,
        tracking=True,
        help="Select the primary person responsible for this rulebook.",
        # default=''
    )
    

    officer_cc = fields.Many2many(
        'res.users',  # Assuming you are linking to the hr.employee model
        string="Officers To Copy",
        tracking=True,
        help="Select the person(s) to copy for this rulebook.",
    )

    description = fields.Text(
        string="Description",
        tracking=True,
        help="Provide a detailed description of the rulebook.",
    )

    section = fields.Char(
        string="Section",
        help="Add relevant section in the rulebook title for reference purposes.",
    )

    sanction = fields.Text(
        string="Sanction",
        tracking=True,
        help="List sanctions here, using headings, styling, and other formatting options for clarity.",
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

    due_date = fields.Datetime(
        string="Due Date",
        compute="_compute_due_date",
        store=True,
        help="The calculated due date based on the provided internal due date values.",
    )

    escalation_date = fields.Datetime(
        string="Escalation Date",
        compute="_compute_escalation_date",
        store=True,
        help="The calculated escalation date based on the provided internal due date values.",
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

    day_value = fields.Integer(
        string="Day", help="Specify the day of the month for the regulatory action.",
        default=1
    )
    
    quarter_day = fields.Integer(string='Day of Quarter',default=7)
        

    
    semi_annual_month1 = fields.Integer(string='First Month', default=1)  # January
    semi_annual_month2 = fields.Integer(string='Second Month', default=8)  # August
    semi_annual_day1 = fields.Integer(string='First Month Day', default=28)  # 28th
    semi_annual_day2 = fields.Integer(string='Second Month Day', default=6)  # 6th
    
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

    computed_date = fields.Datetime(
        string="Next Regulatory Due Date",
        compute="_compute_date",
        store=True,
        help="The next regulatory due date based on the frequency type.",
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

    global_data = {}

    # to send the data in the global variable to the template
    
    
    @api.model
    def open_rulebooks(self):
        # Define the restricted group
        restricted_group = self.env.ref('rule_book.group_department_user_')

        # Check if the user belongs to the restricted group
        if restricted_group in self.env.user.groups_id:
            # Check if the user has a department
            if not self.env.user.department_id:
                raise AccessError(
                    "You must be assigned to a department to view rulebooks.")

            # Apply restriction to the domain
            domain = [('responsible_id', '=', self.env.user.department_id.id)]
        else:
            # No restrictions for other users
            domain = []

        # Return the action to open rulebook records
        return {
            'name': 'RuleBooks',
            'type': 'ir.actions.act_window',
            'res_model': 'rulebook',  # This is your target model
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

    # def open_reply_log(self):
    #     action = self.env.ref("rule_book.action_reply_log").sudo().read()[0]
    #     return action
        
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
        base_url = self.env["ir.config_parameter"].sudo().get_param("web.base.url")
        appendedValue = encrypt_id(id)
        print(f"{base_url}/report_submission/{appendedValue}")
        return f"{base_url}/report_submission/{appendedValue}"

    def _record_link(self, id):
        print(id)
        base_url = self.env["ir.config_parameter"].sudo().get_param("web.base.url")
        #
        return f"{base_url}/web#id={id}&cids=1&menu_id=277&action=423&model=rulebook&view_type=form"
    
    @api.depends("frequency_type", "date_value", "day_value", "month_value", "day_of_week",
                 "quarter_day","bi_monthly_day1","bi_monthly_day2","semi_annual_month1",
                 "semi_annual_month2","semi_annual_day1","semi_annual_day2","year_month_value")  
         
    def _compute_date(self):
        today = fields.Datetime.now()
        
        # today=fields.Datetime.now().astimezone(
        #     pytz.timezone('Africa/Lagos')).replace(tzinfo=None)
        
        current_weekday = today.weekday()
        
        default_time = datetime.combine(today.date(), datetime.min.time()).replace(
            hour=7, minute=0, second=0
        )

        for record in self:
            # Initialize is_recurring to False
            record.is_recurring = False
            
            if record.frequency_type == "date":
                record.computed_date = record.date_value or default_time
                # Missing boolean assignment for is_recurring

            elif record.frequency_type == "day_of_month":
                if record.day_value and record.month_value:
                    try:
                        day_value = int(record.day_value)
                        month_value = int(record.month_value)
                        
                        # Calculate correct year
                        target_year = today.year
                        if month_value < today.month or (month_value == today.month and day_value < today.day):
                            target_year += 1

                        record.computed_date = fields.Datetime.to_datetime(
                            f"{target_year}-{month_value:02d}-{day_value:02d} 07:00:00"
                        )
                    except ValueError:
                        next_month = (month_value % 12) + 1
                        record.computed_date = fields.Datetime.to_datetime(
                            f"{target_year}-{next_month:02d}-{day_value:02d} 07:00:00"
                        )
                    record.is_recurring = True

            elif record.frequency_type == "day_every_month":
                if record.day_value:
                    next_month = today.month + 1 if today.month < 12 else 1
                    year = today.year if today.month < 12 else today.year + 1
                    try:
                        record.computed_date = fields.Datetime.to_datetime(
                            f"{year}-{next_month:02d}-{int(record.day_value):02d} 07:00:00"
                        )
                        record.is_recurring = True
                    except ValueError:
                        # Handle invalid dates (e.g., February 31)
                        record.computed_date = default_time + timedelta(months=1)

            elif record.frequency_type == "daily":
                record.computed_date = default_time + timedelta(days=1)
                record.is_recurring = True
                
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
                        ).replace(hour=7, minute=0, second=0)
                    except (ValueError, TypeError):
                        record.computed_date = default_time + timedelta(weeks=1)
                else:
                    record.computed_date = default_time + timedelta(weeks=1)
                record.is_recurring = True

            elif record.frequency_type == "monthly":
                next_month = today.month + 1 if today.month < 12 else 1
                year = today.year if today.month < 12 else today.year + 1
                record.computed_date = default_time.replace(year=year, month=next_month, day=1)
                record.is_recurring = True
                
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
                first_date = default_time.replace(year=current_year, month=current_month, day=first_day)
                second_date = default_time.replace(year=current_year, month=current_month, day=second_day)
                
                # If both dates have passed in current month, move to next month
                if today.day > second_day:
                    if current_month == 12:
                        first_date = first_date.replace(year=current_year + 1, month=1)
                        second_date = second_date.replace(year=current_year + 1, month=1)
                    else:
                        first_date = first_date.replace(month=current_month + 1)
                        second_date = second_date.replace(month=current_month + 1)
                # If first date has passed but second hasn't
                elif today.day > first_day:
                    next_date = second_date
                    record.computed_date = next_date
                    record.is_recurring = True
                    continue
                
                # Set next date to the first occurrence
                record.computed_date = first_date
                record.is_recurring = True

          
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
                day = min(record.quarter_day or 7, 28)  # Limit to 28 to avoid month overflow
                
                try:
                    record.computed_date = datetime(
                        year, 
                        next_quarter_month, 
                        day, 
                        7, 0, 0  # 8 AM
                    )
                except ValueError:
                    # If date is invalid, default to first day of next quarter
                    record.computed_date = datetime(
                        year, 
                        next_quarter_month, 
                        1, 
                        7, 0, 0
                    )
                record.is_recurring = True
                
            elif record.frequency_type == "semi_annually":
                today = fields.Date.today()
                year = today.year

                # Default values if not set
                month1 = record.semi_annual_month1 or 1
                month2 = record.semi_annual_month2 or 7
                day1 = min(record.semi_annual_day1 or 28, 28)  # Limit to 28 to avoid month issues
                day2 = min(record.semi_annual_day2 or 6, 28)

                # Create datetime objects for comparison
                date1 = datetime(year, month1, day1, 7, 0, 0)
                date2 = datetime(year, month2, day2, 7, 0, 0)
                
                # Sort dates chronologically
                if date1 > date2:
                    date1, date2 = date2, date1

                # now = datetime.now()
                now = datetime.now(pytz.timezone(
                    "Africa/Lagos")).replace(tzinfo=None)
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
                        last_day = calendar.monthrange(next_date.year, next_date.month)[1]
                        record.computed_date = next_date.replace(day=min(next_date.day, last_day))
                
                record.is_recurring = True
                
           
            elif record.frequency_type == "yearly":
                
                if not record.year_month_value:
                    continue
                        
                year = today.year
                month_value = int(record.month_value)
                
                # Validate month value
                if not (1 <= month_value <= 12):
                    _logger.warning(f"Invalid month value: {month_value}. Setting to default month 12.")
                    month_value = 12
                
                # Get the maximum days for the selected month
                max_days = calendar.monthrange(year, month_value)[1]
                
                # Ensure day is valid for the selected month
                day = min(record.day_value or 1, max_days)
                
                # Create the yearly datetime
                try:
                    yearly_date = datetime(year, month_value, day, 7, 0)  # Setting time to 7 AM
                    
                    # If the date has already passed this year, move to next year
                    if yearly_date < today:
                        yearly_date = yearly_date.replace(year=year + 1)
                    
                    record.computed_date = yearly_date
                    record.is_recurring = True
                    
                except ValueError as e:
                    _logger.error(f"Error creating date: year={year}, month={month_value}, day={day}. Error: {e}")
                    continue
                
                
            elif record.frequency_type == "three_yearly":
                if record.month_value and record.day_value:
                    try:
                        month_value = int(record.month_value)
                        day_value = min(record.day_value or 1, 28)  # Limit to 28 to avoid month issues
                        
                        # Calculate the target year
                        current_year = today.year
                        target_year = current_year
                        
                        # Create the initial date
                        target_date = datetime(
                            target_year,
                            month_value,
                            day_value,
                            7, 0, 0  # 8 AM
                        )
                        
                        # If the date has passed this year, calculate next occurrence
                        if target_date < today:
                            # Find the next three-year cycle
                            years_to_add = (3 - ((today.year - target_year) % 3))
                            target_date = target_date.replace(year=today.year + years_to_add)
                        
                        record.computed_date = target_date
                        record.is_recurring = True
                        
                    except ValueError as e:
                        _logger.error(f"Error creating three-yearly date: {e}")
                        record.computed_date = default_time

            elif record.frequency_type == "immediate":
                record.computed_date = today
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
                max_days = calendar.monthrange(fields.Date.today().year, month)[1]
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
                            datetime(current_year, month, day)  # Use leap year to allow Feb 29
                        except ValueError:
                            raise AccessError(f"Invalid day {day} for month {month}")

    @api.onchange('frequency_type')
    def _onchange_frequency_type(self):
        if self.frequency_type == 'semi_annually':
            self.semi_annual_month1 = 1  # January
            self.semi_annual_month2 = 8  # August
            self.semi_annual_day1 = 28   # 28th
            self.semi_annual_day2 = 6    # 6th
            
            
    @api.constrains('three_year_day')
    def _check_three_year_day(self):
        for record in self:
            if record.frequency_type == 'three_yearly' and record.day_value:
                if not 1 <= record.three_year_day <= 28:
                    raise ValidationError(("Please select a day between 1 and 28 for three-yearly frequency."))
    
    @api.depends("due_date_value", "due_date_unit", "computed_date")
    
    def _compute_due_date(self):
        for record in self:
            if record.computed_date and record.due_date_unit in [
                "days",
                "hours",
                "minutes",
                "seconds",
                "weeks",
                "months",
                "years",
            ]:
                delta_args = {
                    record.due_date_unit: -record.due_date_value
                }
                record.due_date = record.computed_date + relativedelta(
                    **delta_args
                )
            else:
                # If computed_date is not available, set due_date to False or handle accordingly
                record.due_date = False

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
                # If computed_date is not available, set due_date to False or handle accordingly
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
        if record.due_date and record.escalation_date:
            record._schedule_due_dates()

            # send out email if the record frequency is immediate
        if record.frequency_type == "immediate":
            current_year = datetime.now().year
            global global_data
            global_data = {
                # "email_to": record.officer_responsible.email,
                "email_to": record.officer_responsible.email,
                "name": record.officer_responsible.name,
                "title": record.name.name,
                "upload_link": self._compute_upload_link(record.id),
                "email_from": os.getenv("EMAIL_FROM"),
                # "email_cc": record.officer_cc.email,
                "rulebook_name":  re.sub(r'<[^>]+>', '', record.type_of_return),

                "email_cc": self.mapped('officer_cc.email'),

                "due_date": self._compute_formatted_date(record.due_date),
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
        if "due_date" in vals and "escalation_date" in vals:
            if hasattr(self, "_schedule_due_dates"):
                self._schedule_due_dates()
        if "frequency_type" in vals and vals["frequency_type"] == "immediate":
            print("it came here")
            record = self.env["rulebook"].browse(self.id)
            current_year = datetime.now().year
            global global_data
            global_data = {
                "email_to": self.mapped('officer_responsible.email'),
                "name": self.mapped('first_line_escalation.name'),
                "title": self.mapped('name.name'),
                "upload_link": self._compute_upload_link(record.id),
                "email_from": os.getenv("EMAIL_FROM"),
                "rulebook_name":  re.sub(r'<[^>]+>', '', record.type_of_return),
                "email_cc": self.mapped('officer_cc.email'),
                "due_date": self._compute_formatted_date('due_date'),
                "current_year": current_year,
            }
            # global_data = {
            #     "email_to": record.officer_responsible.email,
            #     "name": record.first_line_escalation.name,
            #     "title": record.name.name,
            #     "upload_link": self._compute_upload_link(record.id),
            #     "email_from": os.getenv("EMAIL_FROM"),
            #     "email_cc": record.officer_cc.email,
            #     "due_date": self._compute_formatted_date(record.due_date),
            #     "current_year": current_year,
            # }

            print(record.officer_cc.email)

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
            "due_date_value" in vals
            or "due_date_unit" in vals
            or "escalation_date_value" in vals
            or "escalation_date_unit" in vals
            or "computed_date" in vals
            or "frequency_type" in vals
        ):
            self._schedule_due_dates()

        return result

    # def _compute_formatted_date(self, dt):
    #     # Extract day, month, year, and time components
    #     day = dt.day
    #     month = dt.strftime("%B")
    #     year = dt.year
    #     hour = dt.strftime("%-I")  # Remove leading zero from the hour
    #     minute = dt.strftime("%M")
    #     am_pm = dt.strftime("%p").lower()

    #     # Determine the correct ordinal suffix for the day
    #     if day % 10 == 1 and day != 11:
    #         day_suffix = "st"
    #     elif day % 10 == 2 and day != 12:
    #         day_suffix = "nd"
    #     elif day % 10 == 3 and day != 13:
    #         day_suffix = "rd"
    #     else:
    #         day_suffix = "th"

    #     # Format the final string
    #     formatted_date = f"{day}{day_suffix} of {month}, {year} by {hour}{am_pm}"

    #     return formatted_date if formatted_date else dt
    
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

            if record.due_date:
                self.env["calendar.event"].create(
                    {
                        "name": f"Internal Due Date for {record.id}",
                        "start": record.due_date,
                        "stop": record.due_date
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
        # now = datetime.now(pytz.timezone("Africa/Lagos"))
        now = datetime.now(pytz.timezone("Africa/Lagos")).replace(tzinfo=None)
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
            # print(event.name.split(" ")[-1])
            # print(rulebook_id)
            _logger.critical( f"from send_due_date_emails() : {event.name.split(' ')[-1]} ... Rulebook ID {rulebook_id} ")
            
            global_data = {
                "email_to": rulebook_id.first_line_escalation.email, 
                "first_line_escalation": rulebook_id.first_line_escalation.name,
                "rulebook_name":  re.sub(r'<[^>]+>', '', rulebook_id.type_of_return),
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
                "title":  re.sub(r'<[^>]+>', '', rulebook_id.type_of_return),
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

            # print(self.get_record_name(event.name.split(" ")[-1]))
            _logger.critical(
                f"Template Exists from send_due_date_emails() : {self.get_record_name(event.name.split(' ')[-1])}")

            rulebook_id = self.env["rulebook"].search(
                [("id", "=", event.name.split(" ")[-1])]
            )
            global_data = {
                "email_to": rulebook_id.officer_responsible.email,
                "name": rulebook_id.officer_responsible.name if rulebook_id.officer_responsible.name else rulebook_id.officer_responsible.email,
                # "title":  re.sub(r'<[^>]+>', '', rulebook_id.type_of_return),
                "title":  rulebook_id.type_of_return,
                "upload_link": self._compute_upload_link(rulebook_id.id),
                "email_from":  os.getenv("EMAIL_FROM"),
                # "rulebook_name":  re.sub(r'<[^>]+>', '', rulebook_id.type_of_return),
                "rulebook_name":   rulebook_id.type_of_return,
                "email_cc": rulebook_id.officer_cc.email,
                "due_date": self._compute_formatted_date(rulebook_id.due_date),
                "current_year": current_year,
            }
            _logger.critical(
                f"Rulebbok ID from send_due_date_emails() : {rulebook_id}")
            
            if (
                rulebook_id
                and rulebook_id.status == "active"
                and rulebook_id.frequency_type != "immediate"
            ):
                template_id = self.env.ref(
                    "rule_book.email_template_internal_due_date_"
                ).id
                template = self.env["mail.template"].browse(template_id)
                # print(template.exists())
                _logger.critical(
                    f"Template Exists from send_due_date_emails() : {template.exists()}")
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
        # current_date = fields.Datetime.now()  # Today's date
        current_date=fields.Datetime.now().astimezone(
            pytz.timezone('Africa/Lagos')).replace(tzinfo=None)

        rulebooks = self.env["reply.log"].search(
            [
                ("rulebook_compute_date", "<=", current_date),
                ("rulebook_id.is_recurring", "=", True),
            ]
        )

        for record in rulebooks:
            record._compute_next_due_date()

    
    
    def _compute_next_due_date(self):
        print("Updating next due date...")
        """Compute the next due date for the rulebook when the status is 'completed'."""
        for record in self:
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
                
            elif record.frequency_type == "bi_monthly":
                # Get the current day of the computed date
                current_day = record.computed_date.day
                
                # Determine if it's the first or second date of the month
                if current_day == record.bi_monthly_day1:
                    # If current is first day, next is second day of same month
                    next_due_date = record.computed_date.replace(day=record.bi_monthly_day2)
                else:
                    # If current is second day, next is first day of next month
                    next_due_date = record.computed_date + relativedelta(months=1)
                    next_due_date = next_due_date.replace(day=record.bi_monthly_day1)
                    
            elif record.frequency_type == "semi_annually":
                # Get current date components
                current_month = record.computed_date.month
                current_day = record.computed_date.day
                
                if current_month == record.semi_annual_month1:
                    # Move to second date of the year
                    next_due_date = record.computed_date.replace(
                        month=record.semi_annual_month2,
                        day=record.semi_annual_day2
                    )
                else:
                    # Move to first date of next year
                    next_due_date = record.computed_date.replace(
                        year=record.computed_date.year + 1,
                        month=record.semi_annual_month1,
                        day=record.semi_annual_day1
                    )
            elif record.frequency_type == "three_yearly":
                next_due_date = record.computed_date + relativedelta(years=3)
                
            elif record.frequency_type == "date":
                next_due_date = record.computed_date
                
            elif record.frequency_type == "immediate":
                next_due_date = record.computed_date
                
            else:
                next_due_date = record.computed_date
                
            record.next_due_date = next_due_date
            record.computed_date = next_due_date
            _logger.critical(
                f"NEXT DUE DATE : {next_due_date}")

            

    def send_reminder_email(self):
        """Send a reminder email to the responsible party if the due date is approaching."""
        today = fields.Datetime.now().astimezone(
            pytz.timezone('Africa/Lagos')).replace(tzinfo=None)
        for rulebook in self:
            if (
                rulebook.report_status != "completed"
                and rulebook.due_date <= today
            ):
                template_id = self.env.ref("module_name.reminder_email_template").id
                self.env["mail.template"].browse(template_id).send_mail(rulebook.id)
                rulebook.last_escalation_sent =  today

            # Notify first and second line escalation officers
            if (
                rulebook.escalation_date <= fields.Datetime.now()
                and rulebook.report_status != "completed"
            ):
                template_id = self.env.ref("module_name.escalation_email_template").id
                self.env["mail.template"].browse(template_id).send_mail(rulebook.id)
                rulebook.last_escalation_sent = today
                

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

        # Specify the Africa timezone (Lagos)
        africa_timezone = pytz.timezone("Africa/Lagos")

        # Get the current date and time in the Africa/Lagos timezone
        africa_now = datetime.now(africa_timezone)
        
        # Get today's date without time
        today_start = africa_now.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        today_end = africa_now.replace(
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
            
    def _post_email_to_reply_log(self, message):
        """Post a copy of the email message to the reply.log chatter"""
        # Create or get a reply log record
        ReplyLog = self.env['reply.log']
        reply_log = ReplyLog.search(
            [('name', '=', f'Log for {self.name}')], limit=1)
        if not reply_log:
            reply_log = ReplyLog.create({'name': f'Log for {self.name}'})

        # Get related partners from users/employees
        recipient_partners = []
        if message.email_to:
            # If sending to users
            users = self.env['res.users'].search(
                [('email', 'in', message.email_to.split(','))])
            recipient_partners.extend(users.mapped('partner_id').ids)

            # If sending to employees
            employees = self.env['hr.employee'].search(
                [('work_email', 'in', message.email_to.split(','))])
            recipient_partners.extend(
                employees.mapped('user_id.partner_id').ids)

        reply_log.message_post(
            body=message.body,
            subject=message.subject,
            message_type='email',
            subtype_id=self.env.ref('mail.mt_comment').id,
            email_from=message.email_from,
            # Only include partner_ids if we have any
            partner_ids=recipient_partners if recipient_partners else None,
            attachment_ids=message.attachment_ids.ids,
        )

    def message_post(self, **kwargs):
        """Override to copy messages to reply.log"""
        message = super().message_post(**kwargs)
        if kwargs.get('message_type') == 'email':
            self._post_email_to_reply_log(message)
        return message

    def submit_report(self):
        """Submit the report and notify responsible parties."""
        self.status = "submitted"
        self.rulebook_id.report_status = "submitted"
        self.rulebook_id.message_post(
            body="The report has been submitted for {}.".format(
                self.rulebook_id.name),
            partner_ids=[self.rulebook_id.responsible_id.partner_id.id],
            # partner_ids=[self.rulebook_id.responsible_id.partner_id.id],
        )
        # Add more actions like sending submission emails

    def get_lagos_date(self):
        # Get the current UTC time
        utc_now = datetime.now(pytz.UTC)

        # Convert to Lagos timezone
        lagos_tz = pytz.timezone('Africa/Lagos')
        lagos_now = utc_now.astimezone(lagos_tz)

        # Get just the date
        lagos_date = lagos_now.date()

        return lagos_date 

    


# the sty
