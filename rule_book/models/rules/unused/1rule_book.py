from dateutil.relativedelta import relativedelta
from odoo import models, fields, api
from datetime import datetime, timedelta
import calendar

class Rulebook(models.Model):
    _name = "rulebook"
    _description = "Rulebook"
    _rec_name = "name"
    _order = "id desc"
    _inherit = ["mail.thread"]

    # Existing fields remain the same until frequency handling...

    frequency_type = fields.Selection([
        ('once', 'One Time Only'),
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('biweekly', 'Bi-Weekly'),
        ('monthly', 'Monthly'),
        ('quarterly', 'Quarterly'),
        ('semi_annual', 'Semi-Annual'),
        ('yearly', 'Yearly'),
        ('custom', 'Custom')
    ], string='Frequency Type', required=True, default='monthly', tracking=True)

    # Custom frequency fields
    custom_interval = fields.Integer(
        string='Custom Interval',
        default=1,
        help='Number of units for custom frequency'
    )
    
    custom_interval_type = fields.Selection([
        ('days', 'Days'),
        ('weeks', 'Weeks'),
        ('months', 'Months'),
        ('years', 'Years')
    ], string='Custom Interval Type', default='months')

    # Specific date settings
    start_date = fields.Date(
        string='Start Date',
        required=True,
        default=fields.Date.context_today,
        tracking=True
    )

    end_date = fields.Date(
        string='End Date',
        tracking=True
    )

    specific_day = fields.Integer(
        string='Day of Month',
        help='Specific day of month (1-31)',
        default=1
    )

    specific_weekday = fields.Selection([
        ('0', 'Monday'),
        ('1', 'Tuesday'),
        ('2', 'Wednesday'),
        ('3', 'Thursday'),
        ('4', 'Friday'),
        ('5', 'Saturday'),
        ('6', 'Sunday')
    ], string='Day of Week')

    specific_week = fields.Selection([
        ('1', 'First'),
        ('2', 'Second'),
        ('3', 'Third'),
        ('4', 'Fourth'),
        ('5', 'Fifth'),
        ('last', 'Last')
    ], string='Week of Month')

    exclude_holidays = fields.Boolean(
        string='Exclude Holidays',
        default=True,
        help='If checked, due dates falling on holidays will be moved to the next business day'
    )

    @api.depends('frequency_type', 'start_date', 'specific_day', 'specific_weekday', 
                 'specific_week', 'custom_interval', 'custom_interval_type')
    def _compute_next_occurrence(self):
        """Compute the next occurrence date based on frequency settings"""
        for record in self:
            if not record.start_date:
                continue

            base_date = record.computed_date or record.start_date
            next_date = record._calculate_next_date(base_date)
            
            # Adjust for holidays if needed
            if record.exclude_holidays:
                next_date = record._adjust_for_holidays(next_date)
            
            record.computed_date = next_date

    def _calculate_next_date(self, base_date):
        """Calculate the next occurrence based on frequency type"""
        self.ensure_one()
        
        if self.frequency_type == 'once':
            return base_date
            
        today = fields.Date.today()
        if isinstance(base_date, datetime):
            base_date = base_date.date()

        if self.frequency_type == 'daily':
            next_date = base_date + timedelta(days=1)
        
        elif self.frequency_type == 'weekly':
            next_date = base_date + timedelta(weeks=1)
        
        elif self.frequency_type == 'biweekly':
            next_date = base_date + timedelta(weeks=2)
        
        elif self.frequency_type == 'monthly':
            next_date = self._calculate_monthly_date(base_date)
        
        elif self.frequency_type == 'quarterly':
            next_date = base_date + relativedelta(months=3)
        
        elif self.frequency_type == 'semi_annual':
            next_date = base_date + relativedelta(months=6)
        
        elif self.frequency_type == 'yearly':
            next_date = base_date + relativedelta(years=1)
        
        elif self.frequency_type == 'custom':
            next_date = self._calculate_custom_date(base_date)
        
        # Ensure we're not returning a date in the past
        return max(next_date, today)

    def _calculate_monthly_date(self, base_date):
        """Calculate next monthly occurrence with special handling"""
        if self.specific_day:
            # Handle specific day of month
            next_date = base_date + relativedelta(months=1)
            # Adjust to specific day, handling month end cases
            last_day = calendar.monthrange(next_date.year, next_date.month)[1]
            day = min(self.specific_day, last_day)
            return next_date.replace(day=day)
            
        elif self.specific_weekday and self.specific_week:
            # Handle specific weekday (e.g., "Last Thursday")
            next_date = base_date + relativedelta(months=1)
            return self._find_weekday_occurrence(next_date)
            
        return base_date + relativedelta(months=1)

    def _calculate_custom_date(self, base_date):
        """Calculate next date based on custom interval"""
        if self.custom_interval_type == 'days':
            return base_date + timedelta(days=self.custom_interval)
        elif self.custom_interval_type == 'weeks':
            return base_date + timedelta(weeks=self.custom_interval)
        elif self.custom_interval_type == 'months':
            return base_date + relativedelta(months=self.custom_interval)
        elif self.custom_interval_type == 'years':
            return base_date + relativedelta(years=self.custom_interval)
        return base_date

    def _find_weekday_occurrence(self, date):
        """Find specific weekday occurrence in a month (e.g., last Thursday)"""
        weekday = int(self.specific_weekday)
        week = self.specific_week
        
        if week == 'last':
            # Find the last occurrence of this weekday
            last_day = calendar.monthrange(date.year, date.month)[1]
            last_date = date.replace(day=last_day)
            while last_date.weekday() != weekday:
                last_date -= timedelta(days=1)
            return last_date
        else:
            # Find the nth occurrence
            week_num = int(week)
            first_day = date.replace(day=1)
            # Find first occurrence of weekday
            while first_day.weekday() != weekday:
                first_day += timedelta(days=1)
            # Add weeks to get to desired occurrence
            target_date = first_day + timedelta(weeks=week_num - 1)
            # If we've gone into next month, return last occurrence
            if target_date.month != date.month:
                return self._find_weekday_occurrence(date.replace(week='last'))
            return target_date

    def _adjust_for_holidays(self, date):
        """Adjust date if it falls on a holiday"""
        holiday_model = self.env['resource.calendar.leaves']
        while holiday_model.is_holiday(date):
            date += timedelta(days=1)
        return date

    @api.model
    def create(self, vals):
        """Override create to set initial computed_date"""
        record = super(Rulebook, self).create(vals)
        record._compute_next_occurrence()
        return record