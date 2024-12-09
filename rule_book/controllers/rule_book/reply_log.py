from odoo import http
from odoo.http import request
import logging
from odoo import models, fields, api, _
import pytz
# from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime

_logger = logging.getLogger(__name__)


class ReplyController(http.Controller):
    def get_lagos_date(self):
        # Get the current UTC time
        utc_now = datetime.now(pytz.UTC)

        # Convert to Lagos timezone
        lagos_tz = pytz.timezone('Africa/Lagos')
        lagos_now = utc_now.astimezone(lagos_tz)

        # Get just the date
        lagos_date = lagos_now.date()

        return lagos_date

    @http.route('/api/awaiting_replies/', auth='public')
    def get_awaiting_replies(self):
        _logger.info("Fetching awaiting replies...")
        now = datetime.now(pytz.timezone("Africa/Lagos")).replace(tzinfo=None)
        start_window = now - timedelta(minutes=29)
        end_window = now + timedelta(minutes=29)

        # Convert to Odoo Datetime format
        start_window_str = fields.Datetime.to_string(start_window)
        end_window_str = fields.Datetime.to_string(end_window)
        current_year = datetime.now().year
        now1 = datetime.now(pytz.timezone("Africa/Lagos")).replace(tzinfo=None)
        now2 = datetime.now()
        currentday = fields.Date.today()
        currentday2 = self.get_lagos_date()
        _logger.critical(
            f"now formatted: {now}, start formatted minus 30 minute{start_window} ,end  formatted plus 30 minute {end_window}")

        _logger.critical(
            f"this is the odoo string version,  start formatted minus 30 minute{start_window_str} ,end  formatted plus 30 minute {end_window_str}, year {current_year}")

        _logger.critical(
            f"Now1 timestamp, {now1} ,Now2 timestamp {now2} ,  current day {currentday} , current day formatted {currentday2}")

        # Fetch completed replies
        completed_replies = request.env['reply.log'].search(
            [("rulebook_status", "=", "completed")])

        # Extract unique rulebook_ids from completed replies
        completed_rulebook_ids = {
            reply.rulebook_id.id for reply in completed_replies if reply.rulebook_id}

        _logger.info(f"Completed rulebook IDs: {completed_rulebook_ids}")

        # Search for awaiting replies
        awaiting_replies = request.env['reply.log'].search([
            ("rulebook_status", "not in", [
             "completed", "reviewed", "pending"]),
            # Ensure we use rulebook_id here
            ("rulebook_id", "not in", list(completed_rulebook_ids))
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
        _logger.critical(result)

        return result
