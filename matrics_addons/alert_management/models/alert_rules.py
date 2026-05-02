import os

from odoo import models, fields, api
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from odoo.exceptions import ValidationError
import pytz
from pytz import timezone
import csv
import io
import base64, time, uuid
import smtplib
from time import sleep
import logging
import smtplib
from time import sleep
import logging
from collections import defaultdict
import re
import hashlib
import json

from .reference_selections import ALERT_SOURCE_SELECTION

_logger = logging.getLogger(__name__)


class alert_rules(models.Model):
    _name = "alert.rules"
    _description = "Alert Rules"
    _inherit = ["mail.thread", "mail.activity.mixin"]
    _order = "id desc"

    name = fields.Char(string="Name", required=True, Tracking=True)
    narration = fields.Html(string="narration", required=True, tracking=True)
    sql_text = fields.Many2one(
        "process.sql", string="SQL Query", required=True, tracking=True
    )
    frequency_id = fields.Many2one(
        "exception.frequency", string="Frequency", required=True, tracking=True
    )
    # process_category_id = fields.Many2one('process.category', string="Process Category", required=True)
    status = fields.Selection(
        [("1", "Active"), ("0", "Inactive")],
        default="1",  # The default value is an integer (1)
        string="Alert Status",
        tracking=True,
    )
    specific_email_recipients = fields.Many2many(
        "res.users",
        "alert_rules_email_rel",
        "alert_rules_id",
        "user_id",
        string="Specific Recipients",
        required=True,
        tracking=True,
    )
    alert_id = fields.Many2one("alert.group", string="Alert Group")
    first_owner = fields.Many2one("res.users", string="First Line Owner")
    second_owner = fields.Many2one("res.users", string="Second Line Owner")
    process_id = fields.Char(string="Process", tracking=True)
    model_id = fields.Selection(
        selection=ALERT_SOURCE_SELECTION,
        string="Model",
        tracking=True,
    )
    risk_rating = fields.Selection(
        selection=[("low", "Low"), ("medium", "Medium"), ("high", "High")],
        default="low",  # The default value is the first risk rating
        string="Risk Rating",
    )
    date_created = fields.Datetime(string="Created_Date", read_only=True)
    last_checked = fields.Datetime(string="Last_Checked", read_only=True)
    cron_string = fields.Char(
        string="Cron String",
        help="Cron string in the format '0 0 * * *' for daily at midnight, '0 * * * *' for hourly, etc.",
        tracking=True,
        required=True,
        default="*/5 * * * *",
    )
    recipients = fields.Text(
        string="All Target Recipients",
        compute="_compute_recipients",
        store=True,
        help="This field is automatically computed based on the specific recipients, alert group emails, and owners. It aggregates all email addresses that will receive the alert notifications.",
    )
    # alert_query is a related field to fetch the actual SQL query text from the linked process.sql record, making it easier to access the query directly from the alert rule.
    alert_query = fields.Text(
        string="Alert SQL Query",
        related="sql_text.query",
        store=True,
        help="This field displays the actual SQL query text from the linked process.sql record. It is stored for easier access and performance when processing the alert rules.",
    )

    @api.depends(
        "specific_email_recipients",
        "specific_email_recipients.email",
        "first_owner",
        "first_owner.email",
        "second_owner",
        "second_owner.email",
        "alert_id",
        "alert_id.email",
        "alert_id.email.email",
        "alert_id.email_cc",
        "alert_id.email_cc.email",
    )
    def _compute_recipients(self):
        for record in self:
            email_sources = []

            email_sources.extend(record.specific_email_recipients.mapped("email"))

            if record.first_owner:
                email_sources.append(record.first_owner.email)
            if record.second_owner:
                email_sources.append(record.second_owner.email)

            if record.alert_id:
                email_sources.extend(record.alert_id.email.mapped("email"))
                email_sources.extend(record.alert_id.email_cc.mapped("email"))

            unique_emails = {
                email.strip()
                for email in email_sources
                if email and email.strip()
            }

            record.recipients = ", ".join(sorted(unique_emails)) if unique_emails else ""

    @api.model
    def create(self, vals_list):

        vals_list["last_checked"] = fields.Datetime.now()

        if "date_created" not in vals_list:
            vals_list["date_created"] = fields.Datetime.now()

        res = super(alert_rules, self).create(vals_list)
        return res

    @api.onchange("sql_text")
    def onchange_sql_text(self):
        if self.sql_text:
            row = self.search([("sql_text.id", "=", self.sql_text.id)])

            if len(row) > 0:
                raise ValidationError(
                    f"alert rules for {self.sql_text.name} already exist"
                )

    def _check_manual_run_cooldown(self):
        """Raise UserError if this rule was manually run within the last 5 minutes."""
        from odoo.exceptions import UserError

        param_key = f"alert_rules.last_manual_run_{self.id}"
        last_run_str = self.env["ir.config_parameter"].sudo().get_param(param_key, "")
        if last_run_str:
            try:
                last_run = datetime.fromisoformat(last_run_str)
                elapsed = (datetime.utcnow() - last_run).total_seconds()
                if elapsed < 300:
                    remaining = int(300 - elapsed)
                    mins, secs = divmod(remaining, 60)
                    wait = f"{mins}m {secs}s" if mins else f"{secs}s"
                    raise UserError(
                        f'This rule was already run manually less than 5 minutes ago. '
                        f'Please wait {wait} before running it again.'
                    )
            except UserError:
                raise
            except Exception:
                pass

    def _record_manual_run(self):
        """Store the current UTC timestamp as the last manual run time for this rule."""
        param_key = f"alert_rules.last_manual_run_{self.id}"
        self.env["ir.config_parameter"].sudo().set_param(
            param_key, datetime.utcnow().isoformat()
        )

    def run_alert_rule(self):
        """Manually trigger this alert rule, sending all current query results."""
        from odoo.exceptions import UserError

        self.ensure_one()
        self._check_manual_run_cooldown()
        self._record_manual_run()

        if self.model_id == 'adverse.media':
            return self._run_adverse_media_alert_rule(force=True)

        if not self.recipients:
            raise UserError(
                f'No recipients configured for rule "{self.name}". '
                'Please add recipients under the Recipients tab before running.'
            )

        try:
            self._clear_alert_record_signatures(self)
            self.send_alert(self)
            self.write({'last_checked': fields.Datetime.now()})
        except Exception as e:
            _logger.error(f"Manual run failed for rule '{self.name}': {str(e)}")
            raise
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Alert Sent',
                'message': f'Alert rule "{self.name}" executed successfully.',
                'type': 'success',
                'sticky': False,
            },
        }

    def _run_adverse_media_alert_rule(self, force=False):
        """Notify keyword officers about unnotified new adverse media alerts.

        When force=True (manual Run), all current 'new' alerts are re-notified
        regardless of whether they were previously sent.  In scheduled mode only
        alerts that have not yet been notified are processed.
        """
        domain = [('status', '=', 'new'), ('active', '=', True)]
        if not force:
            domain.append(('officer_notified', '=', False))

        new_alerts = self.env['adverse.media.alert'].search(domain)

        if not new_alerts:
            _logger.info("Adverse media alert rule: no qualifying alerts to notify about")
            self.write({'last_checked': fields.Datetime.now()})
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'No New Alerts',
                    'message': 'No new adverse media alerts found.',
                    'type': 'info',
                    'sticky': False,
                },
            }

        media_records = new_alerts.mapped('media_id')
        sent = 0
        errors = []
        for am_rec in media_records:
            am_alerts = new_alerts.filtered(lambda a: a.media_id.id == am_rec.id)
            try:
                am_rec._notify_officers(am_alerts)
                sent += 1
            except Exception as e:
                partner_name = am_rec.partner_id.name or str(am_rec.id)
                _logger.error(
                    f"Adverse media alert rule: notification failed for {partner_name}: {str(e)}",
                    exc_info=True,
                )
                errors.append(partner_name)

        self.write({'last_checked': fields.Datetime.now()})

        if errors:
            msg = (
                f'Notified {sent} partner(s); failed for: {", ".join(errors)}. '
                'Check server logs for details.'
            )
            notif_type = 'warning'
        else:
            msg = f'Adverse media notifications sent for {sent} partner(s) ({len(new_alerts)} alert(s)).'
            notif_type = 'success'

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Alert Sent',
                'message': msg,
                'type': notif_type,
                'sticky': False,
            },
        }

    @api.model
    def process_alert_rules(self):

        alert_rules = self.search([("status", "=", "1")])

        if len(alert_rules) > 0:
            for rule in alert_rules:

                self.process(rule)
        else:
            pass

    def process(self, rule):

        last_checked = rule.last_checked
        unit = rule.frequency_id.name
        period = rule.frequency_id.period
        next_check = self.calculate_next_check(last_checked, unit, period)
        current_time_lagos = fields.Datetime.now()

        _logger.info("------------date info")
        _logger.info(f"last time checked {last_checked}")
        _logger.info(f"current time checked {current_time_lagos}")
        _logger.info(f"next checked {next_check}")
        if (
            last_checked
            and current_time_lagos.year == next_check.year
            and current_time_lagos.month == next_check.month
            and current_time_lagos.day == next_check.day
            and current_time_lagos.hour == next_check.hour
            and current_time_lagos.minute == next_check.minute
        ):
            rule.write({"last_checked": fields.Datetime.now()})
            _logger.info("sending now....")
            self.send_alert(rule)
            return

        # Check if scheduled time has elapsed, handling potential misses

        calculated_time = next_check
        _logger.info(calculated_time)
        if calculated_time < current_time_lagos:
            _logger.info("------------sending as time elapse")
            rule.write({"last_checked": fields.Datetime.now()})
            self.send_alert(rule)
            return
        else:
            # Handle the case where last_checked is None
            calculated_time = self.calculate_next_check(
                current_time_lagos - timedelta(minutes=1), unit, period
            )  # uses current time - 1 minute as the base.
            if calculated_time <= current_time_lagos:
                self.send_alert(rule)
                rule.write({"last_checked": fields.Datetime.now()})
                return

    def calculate_next_check(self, last_checked, unit, period):
        if unit == "minutes":
            return last_checked + timedelta(minutes=period)
        elif unit == "hourly":
            return last_checked + timedelta(hours=period)
        elif unit == "daily":
            return last_checked + timedelta(days=period)
        elif unit == "weekly":
            return last_checked + timedelta(weeks=period)
        elif unit == "monthly":
            return last_checked + relativedelta(months=period)
        elif unit == "yearly":
            return last_checked + relativedelta(years=period)
        else:
            raise ValidationError("Unsupported Unit")

    def format_currency(self, cell):
        if cell is not None and isinstance(cell, (int, float, str)):
            try:
                if re.match(r"[\$\d,.]+", str(cell)):
                    return f"{float(cell):,.2f}"
                else:
                    return cell
            except (ValueError, TypeError):
                return cell
        else:
            return cell

    def has_sql_aliases(self, query):
        # Strip unnecessary whitespace
        query = query.strip()

        # Patterns to detect various forms of SQL aliases
        patterns = [
            # Table alias pattern: "table alias" or "table as alias" (case insensitive for AS)
            r"\b\w+\b\s+(?!ON|JOIN|WHERE|FROM|SELECT|GROUP|ORDER|HAVING|UNION|EXCEPT|INTERSECT)\b\w+\b",
            r"\b\w+\b\s+(?i:AS)\s+\b\w+\b",
        ]

        # Check each pattern
        for pattern in patterns:
            if re.search(pattern, query):
                return True

        return False

    def append_branch_condition(self, query, branch_id):
        # Check if query uses aliases
        has_aliases = self.has_sql_aliases(query)

        # Process query based on presence of WHERE clause
        if " WHERE " in query.upper():
            # Fix multiple WHERE clauses if present
            parts = query.split(";")
            cleaned_parts = []

            first_where_found = False
            for part in parts:
                part = part.strip()
                if not part:
                    continue

                if " WHERE " in part.upper() and first_where_found:
                    # Replace second WHERE with AND
                    part = " AND " + part.split(" WHERE ", 1)[1]
                elif " WHERE " in part.upper():
                    first_where_found = True

                cleaned_parts.append(part)

            query = " ".join(cleaned_parts)

            # Now append our condition - use appropriate table reference
            if has_aliases:
                # If using aliases, we need to use the correct one
                query += " AND branch_id = %s"  # You'll need logic to determine correct alias
            else:
                query += " AND branch_id = %s"
        else:
            # No WHERE clause, add one
            if has_aliases:
                query += " WHERE branch_id = %s"  # You'll need logic to determine correct alias
            else:
                query += " WHERE branch_id = %s"

        return query, [branch_id]

    def format_query(self, rule):
        """
        Format SQL query to ensure it has a 'branch_id' column properly defined.

        This function identifies if branch_id already exists in the query (either directly or as an alias),
        and modifies the query as needed to ensure the column is properly present as 'branch_id'.

        If no branch_id can be added, returns the original query unchanged to avoid SQL errors.

        Returns:
            str: Modified SQL query with proper branch_id handling, or original query if modification fails
        """

        query = rule.sql_text.query

        # If query is empty or None, return empty string
        if not query:
            return ""

        # If the query uses * wildcard, return it unchanged
        if "*" in query:
            return query

        # Create a working copy to preserve case in the original query
        original_query = query.strip()
        query_lower = original_query.lower()

        # Handle queries that don't have a direct 'SELECT...FROM' structure
        if "select " not in query_lower or " from " not in query_lower:
            return original_query

        try:
            # Split the query into parts
            select_part_lower = (
                query_lower.split(" from ")[0].replace("select ", "").strip()
            )
            from_part_lower = "from " + query_lower.split(" from ", 1)[1].strip()

            # Check for various branch_id patterns in the SELECT clause
            # Case 1: column named exactly branch_id with no alias
            has_branch_id_column = re.search(
                r"\bbranch_id\b(?!\s+as)", select_part_lower
            )

            # Case 2: column.branch_id AS branch_id
            has_branch_id_alias = re.search(
                r"(\w+)\.branch_id\b\s+as\s+branch_id\b", select_part_lower
            )

            # Case 3: column.branch_id AS something_else
            other_branch_id_alias = re.search(
                r"(\w+)\.branch_id\b\s+as\s+(\w+)(?!\bbranch_id\b)", select_part_lower
            )

            # Case 4: column.branch_id with no AS (implicit naming)
            implicit_branch_id = re.search(
                r"(\w+)\.branch_id\b(?!\s+as)", select_part_lower
            )

            # If branch_id already exists with correct alias, return query unchanged
            if has_branch_id_column or has_branch_id_alias:
                return original_query

            # If branch_id exists with wrong alias, replace the alias
            elif other_branch_id_alias:
                table_alias = other_branch_id_alias.group(1)
                wrong_alias = other_branch_id_alias.group(2)

                # Find the actual case-sensitive version in the original query
                pattern = re.compile(
                    f"{re.escape(table_alias)}\\.branch_id\\s+as\\s+{re.escape(wrong_alias)}",
                    re.IGNORECASE,
                )
                match = pattern.search(original_query)
                if match:
                    original_text = match.group(0)
                    replacement = f"{table_alias}.branch_id AS branch_id"
                    return original_query.replace(original_text, replacement)

            # If branch_id column exists without explicit alias, add AS branch_id
            elif implicit_branch_id:
                table_alias = implicit_branch_id.group(1)

                # Find the actual case-sensitive version in the original query
                pattern = re.compile(
                    f"{re.escape(table_alias)}\\.branch_id\\b(?!\\s+as)", re.IGNORECASE
                )
                match = pattern.search(original_query)
                if match:
                    original_text = match.group(0)
                    replacement = f"{original_text} AS branch_id"
                    return original_query.replace(original_text, replacement)

            # If no branch_id found, try to add it from a branch table
            else:
                return self._add_branch_id_from_table(
                    original_query, select_part_lower, from_part_lower
                )

        except Exception as e:
            # Log the error but return original query to avoid breaking the system
            _logger.warning(f"Error formatting SQL query: {e}. Using original query.")
            return original_query

    def _add_branch_id_from_table(
        self, original_query, select_part_lower, from_part_lower
    ):
        """
        Attempt to add branch_id column by finding a branch table in the FROM clause.
        Returns original query if no suitable branch table is found.
        """
        try:
            # Look for potential branch tables in different patterns
            branch_patterns = [
                r"\b(\w*res_branch\w*)\s+(\w+)",  # branch_table alias
                r"\b(\w*res_branch\w*)\b(?!\s+\w)",  # branch_table without alias
                r"join\s+(\w*res_branch\w*)\s+(\w+)",  # JOIN branch_table alias
                r"join\s+(\w*res_branch\w*)\b(?!\s+\w)",  # JOIN branch_table without alias
            ]

            branch_reference = None

            for pattern in branch_patterns:
                match = re.search(pattern, from_part_lower)
                if match:
                    if len(match.groups()) >= 2 and match.group(2):
                        # Has alias
                        branch_reference = f"{match.group(2)}.id"
                    else:
                        # No alias, use table name
                        branch_reference = f"{match.group(1)}.id"
                    break

            # If we found a branch table reference, add it to the SELECT
            if branch_reference:
                # Find the original SELECT part to preserve casing
                select_start = original_query.lower().find("select")
                from_start = original_query.lower().find(" from ")
                original_select_part = original_query[
                    select_start + 6 : from_start
                ].strip()

                # Add branch_id to the SELECT clause
                if original_select_part.endswith(","):
                    modified_select = (
                        f"{original_select_part} {branch_reference} AS branch_id"
                    )
                else:
                    modified_select = (
                        f"{original_select_part}, {branch_reference} AS branch_id"
                    )

                # Preserve original SELECT keyword casing
                select_keyword = original_query[select_start : select_start + 6]
                from_part_original = original_query[from_start:]

                return f"{select_keyword} {modified_select} {from_part_original}"

            # If no branch table found, return original query unchanged
            # This prevents SQL errors from adding non-existent columns
            else:
                _logger.info(
                    "No branch table found in query. Returning original query unchanged."
                )
                return original_query

        except Exception as e:
            _logger.warning(
                f"Error adding branch_id from table: {e}. Using original query."
            )
            return original_query

    def create_csv(self, columns, rows):

        # check if branch_id is in cloumn and store the index
        pattern = re.compile(r"\bbranch\s*_?\s*id\b", re.IGNORECASE)
        branch_id_indices = [
            i for i, col in enumerate(columns) if pattern.fullmatch(col)
        ]

        # currency pattern
        currency_pattern = r"^-?\d+\.\d{2}$"

        # Create a CSV in memory
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        # Filter columns
        filtered_columns = [
            " ".join(th.split("_")).title()
            for th in columns
            if th.lower() != "branch_id"
        ]

        # Write headers to the CSV
        csv_writer.writerow(filtered_columns)
        # Write data rows to the CSV
        for row in rows:
            filtered_row = [
                cell for index, cell in enumerate(row) if index not in branch_id_indices
            ]
            # Format currency values in the filtered row
            formatted_row = [self.format_currency(cell) for cell in filtered_row]
            csv_writer.writerow(formatted_row)

        csv_content = csv_buffer.getvalue()
        csv_buffer.close()

        # Step 3: Base64 encode the CSV content
        encoded_content = base64.b64encode(csv_content.encode("utf-8")).decode("utf-8")

        return encoded_content

    def generate_table(self, columns, rows):

        # check if branch_id is in cloumn and store the index
        pattern = re.compile(r"\bbranch\s*_?\s*id\b", re.IGNORECASE)
        branch_id_indices = [
            i for i, col in enumerate(columns) if pattern.fullmatch(col)
        ]

        # check if currency_id is in cloumn and store the index
        currency_pattern = r"^-?\d+\.\d{2}$"

        # Create the HTML table for the email

        table_html = """
            <table class="table table-bordered table-hover container-fluid" style="min-width: 100vw; max-width: 100vw; border-collapse: collapse; font-family: Arial, sans-serif; border: none; overflow:auto;">
                <thead style="background-color: #007046; color: #fff; padding: 12px;">
                    <tr>
                        <!-- Table headers -->
                        {header_columns}
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
            """

        # Generate the table header
        header_html = "".join(
            [
                f"<th style='padding: 8px;'>{' '.join(header.split('_')).title()}</th>"
                for header in columns
                if header != "branch_id"
            ]
        )
        # Create the HTML table for the email

        # Generate the table rows
        rows_html = ""
        for row in rows[:10]:
            rows_html += "<tr>"
            for index, cell in enumerate(row):
                # Check currency
                if bool(re.search(currency_pattern, str(cell))) and cell is not None:
                    # Format the currency value
                    cell = f"{float(cell):,.2f}"  # Format with commas and two decimal places

                if index not in branch_id_indices:

                    rows_html += f"<td style='padding: 8px; border: 1px solid #ddd;'>{cell if cell is not None else ''}</td>"
            rows_html += "</tr>"

        # Insert the generated HTML into the main table structure
        table_html = table_html.format(header_columns=header_html, table_rows=rows_html)

        return table_html

    def prepare_email(self, rule, table_html, encoded_content, email, emailcc, row_count=0):
        """Render an alert.mail.template and send via mail.mail.

        Template lookup order: rule's model_id slug → 'alert' (default).
        All layout is driven by the Alert Templates configuration.
        """
        # --- resolve alert.mail.template ---
        model_code = (rule.model_id or '').replace('.', '_')
        mail_tpl = (
            self.env['alert.mail.template'].search([('code', '=', model_code)], limit=1)
            if model_code else
            self.env['alert.mail.template']
        )
        if not mail_tpl:
            mail_tpl = self.env['alert.mail.template'].search([('code', '=', 'alert')], limit=1)
        if not mail_tpl:
            raise ValidationError(
                "Default alert mail template (code='alert') not found. "
                "Please create it under Alert Templates."
            )

        source_label = dict(ALERT_SOURCE_SELECTION).get(
            rule.model_id, rule.model_id or "Alert Rules"
        )
        alert_id = f"Alert{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        email_from = (
            os.getenv("EmailFrom")
            or self.env.user.company_id.email
            or self.env.user.email
        )

        base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
        company = self.env.user.company_id
        logo_url = f"{base_url}/web/image/res.company/{company.id}/logo_web"
        logo_html = f'<img src="{logo_url}" alt="{company.name}" style="max-height:60px;display:block;" />'

        # --- render the full email body from the configurable template ---
        full_html = mail_tpl.render(
            logo=logo_html,
            alert_name=rule.name,
            alert_id=alert_id,
            row_count=row_count,
            table=table_html,
        )

        # --- create CSV attachment (stored in DB, not filestore) ---
        attachment_id = self.env["ir.attachment"].create({
            "name": "report.csv",
            "mimetype": "text/csv",
            "type": "binary",
            "db_datas": encoded_content,
        })

        to_str = ",".join([str(e) for e in email]) if email else "techsupport@novajii.com"
        cc_str = ",".join([str(e) for e in emailcc]) if emailcc else ""

        # --- audit history ---
        new_alert_history = self.env["alert.history"].create({
            "alert_id": alert_id,
            "attachment_data": attachment_id.id,
            "attachment_link": f"/web/content/{attachment_id.id}?download=true",
            "html_body": full_html,
            "ref_id": rule.model_id or "alert.rules",
            "process_id": rule.process_id,
            "risk_rating": rule.risk_rating,
            "last_checked": rule.last_checked,
            "email": to_str,
            "email_cc": cc_str,
            "narration": rule.narration,
            "name": rule.name,
            "source": source_label,
        })

        try:
            mail = self.env["mail.mail"].sudo().create({
                "subject": rule.name,
                "email_to": to_str,
                "email_cc": cc_str,
                "email_from": email_from,
                "body_html": full_html,
                "attachment_ids": [(4, attachment_id.id)],
                "auto_delete": False,
            })
            mail.send()

            if mail.state in ["exception", "cancel"]:
                _logger.error(f"Failed to send alert email: {mail.failure_reason}")
                new_alert_history.unlink()
            else:
                _logger.info(f"Alert email sent to {to_str} for rule '{rule.name}'")

        except Exception as e:
            if new_alert_history.exists():
                new_alert_history.unlink()
            raise

    def get_first_table_alias(self, query):
        pattern = r'FROM\s+[a-zA-Z0-9_"]+\s+([a-zA-Z0-9_"]+)'
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            return match.group(1)  # Only the alias is captured
        return None

    def _generate_record_signatures(self, rows, columns):
        """Generate unique signatures for each record to track individual changes"""
        try:
            signatures = set()

            for row in rows:
                # Create a normalized version of the row
                normalized_row = tuple(
                    ["<NULL>" if cell is None else str(cell) for cell in row]
                )

                # Generate a signature for this specific record
                record_string = json.dumps(normalized_row, sort_keys=True)
                signature = hashlib.md5(record_string.encode()).hexdigest()
                signatures.add(signature)

            return signatures

        except Exception as e:
            _logger.warning(f"Error generating record signatures: {e}")
            return set()

    def _get_previous_record_signatures(self, rule, branch_id=None):
        """Get the previous record signatures for this rule/branch combination"""
        hash_key = f"alert_records_{rule.id}"
        if branch_id:
            hash_key += f"_branch_{branch_id}"

        try:
            stored_signatures = (
                self.env["ir.config_parameter"].sudo().get_param(hash_key, default="")
            )
            if stored_signatures:
                return set(json.loads(stored_signatures))
            return set()
        except:
            return set()

    def _store_record_signatures(self, rule, signatures, branch_id=None):
        """Store the current record signatures for future comparison"""
        hash_key = f"alert_records_{rule.id}"
        if branch_id:
            hash_key += f"_branch_{branch_id}"

        try:
            signatures_json = json.dumps(list(signatures))
            self.env["ir.config_parameter"].sudo().set_param(hash_key, signatures_json)
        except Exception as e:
            _logger.warning(f"Error storing record signatures: {e}")

    def _get_new_records_only(self, rule, current_rows, columns, branch_id=None):
        """
        Compare current data with previous data and return only new records
        Returns: (new_records, has_new_records)
        """
        if not current_rows:
            return [], False

        # Generate signatures for current records
        current_signatures = self._generate_record_signatures(current_rows, columns)

        # Get previously seen signatures
        previous_signatures = self._get_previous_record_signatures(rule, branch_id)

        # Find new signatures (records we haven't seen before)
        new_signatures = current_signatures - previous_signatures

        if not new_signatures:
            _logger.info(
                f"No new records found for rule {rule.id}"
                + (f" branch {branch_id}" if branch_id else "")
            )
            return [], False

        # Filter current_rows to only include new records
        new_records = []
        for row in current_rows:
            # Generate signature for this row
            normalized_row = tuple(
                ["<NULL>" if cell is None else str(cell) for cell in row]
            )
            record_string = json.dumps(normalized_row, sort_keys=True)
            signature = hashlib.md5(record_string.encode()).hexdigest()

            # If this signature is new, include the record
            if signature in new_signatures:
                new_records.append(row)

        _logger.info(
            f"Found {len(new_records)} new records out of {len(current_rows)} total records for rule {rule.id}"
            + (f" branch {branch_id}" if branch_id else "")
        )

        # Store all current signatures (including old ones) for next comparison
        self._store_record_signatures(rule, current_signatures, branch_id)

        return new_records, True

    def _clear_alert_record_signatures(self, rule, branch_id=None):
        """Clear the stored signatures when no data exists"""
        hash_key = f"alert_records_{rule.id}"
        if branch_id:
            hash_key += f"_branch_{branch_id}"

        try:
            self.env["ir.config_parameter"].sudo().set_param(hash_key, "")
        except Exception as e:
            _logger.warning(f"Error clearing record signatures: {e}")

    def _find_branch_column_index(self, columns):
        """Find the index of branch_id column dynamically"""
        for i, column in enumerate(columns):
            if "branch_id" in column.lower():
                return i
        return None

    def _get_distinct_branches(self, rows, branchcode_index):
        """Extract distinct non-null branch codes from rows"""
        branches = []
        for row in rows:
            branchcode = row[branchcode_index]
            if branchcode and branchcode != "" and branchcode not in branches:
                branches.append(branchcode)
        return branches

    def _send_general_alert_with_new_records_only(self, rule, query, rows, columns):
        """Send alert to general alert group with only new records"""
        # Get only new records
        new_records, has_new_records = self._get_new_records_only(rule, rows, columns)

        if not has_new_records:
            return

        mailto = set()
        mailcc = set()

        # Build recipient lists
        for user in rule.alert_id.email:
            mailto.add(user.login)

        for user in rule.alert_id.email_cc:
            mailcc.add(user.login)

        for user in rule.specific_email_recipients:
            mailcc.add(user.login)

        # Only send if there are recipients and new records
        if (len(mailto) > 0 or len(mailcc) > 0) and new_records:
            encoded_content = self.create_csv(columns, new_records)
            table_html = self.generate_table(columns, new_records)
            self.prepare_email(rule, table_html, encoded_content, mailto, mailcc, row_count=len(new_records))

    def _send_branch_officer_alert_with_new_records_only(
        self, rule, query, branch_officer, branch_id
    ):
        """Send alert to specific branch officer with only new records"""
        # Modify query to filter by branch
        if query.endswith(";"):
            query = query[:-1]

        # --- STEP 1: Extract and remove existing ORDER BY ---
        order_by_clause = ""
        query_upper = query.upper()

        if "ORDER BY" in query_upper:
            order_by_index = query_upper.rindex("ORDER BY")
            order_by_clause = query[order_by_index:]  # Keep the ORDER BY part
            query = query[:order_by_index]  # Remove it from main query
            query_upper = query_upper[:order_by_index]

        # --- STEP 2: Add WHERE / AND clause ---
        if "WHERE" in query_upper:
            updated_query = f"{query} AND branch_id = '{branch_officer.branch_id.id}'"
        else:
            updated_query = f"{query} WHERE branch_id = '{branch_officer.branch_id.id}'"

        # --- STEP 3: Re-add ORDER BY at the end ---
        if order_by_clause:
            updated_query += " " + order_by_clause

        _logger.info(f"Branch-specific query for branch {branch_id}: {updated_query}")

        # Execute branch-specific query
        self.env.cr.execute(updated_query)
        branch_rows = self.env.cr.fetchall()
        branch_columns = [desc[0] for desc in self.env.cr.description]

        # Skip if no records for this branch
        if len(branch_rows) == 0:
            _logger.info(
                f"No records found for branch {branch_id}, clearing signatures"
            )
            self._clear_alert_record_signatures(rule, branch_id)
            return

        # Get only new records for this branch
        new_records, has_new_records = self._get_new_records_only(
            rule, branch_rows, branch_columns, branch_id
        )

        if not has_new_records:
            return

        encoded_content = self.create_csv(branch_columns, new_records)
        table_html = self.generate_table(branch_columns, new_records)

        self.prepare_email(
            rule, table_html, encoded_content, [branch_officer.officer.login], "",
            row_count=len(new_records),
        )

    def _send_internal_alert_with_new_records_only(self, rule, query, rows, columns):
        """Send internal alert with only new records"""
        # Get only new records
        new_records, has_new_records = self._get_new_records_only(rule, rows, columns)

        _logger.info("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
        _logger.info(has_new_records)
        _logger.info(new_records)

        if not has_new_records:
            return

        mailto = set()
        mailcc = set()

        # Build recipient lists for internal alerts
        if rule.first_owner:
            mailto.add(rule.first_owner.login)
        if rule.second_owner:
            mailcc.add(rule.second_owner.login)

        for user in rule.specific_email_recipients:
            mailto.add(user.login)

        for user in rule.alert_id.email_cc:
            mailcc.add(user.login)

        # Remove duplicates: if email exists in mailto, exclude from mailcc
        mailcc = mailcc - mailto

        encoded_content = self.create_csv(columns, rows)
        table_html = self.generate_table(columns, rows)
        self.prepare_email(rule, table_html, encoded_content, mailto, mailcc, row_count=len(rows))

    def _handle_branch_specific_alerts_with_new_records_only(
        self, rule, query, rows, columns, branchcode_index
    ):
        """Handle alerts that need to be partitioned by branch with new records only"""
        branches = self._get_distinct_branches(rows, branchcode_index)
        general_alert_sent = False

        _logger.info(f"Found {len(branches)} distinct branches: {branches}")

        for branch in branches:
            branch_officer = (
                self.env["control.officer"]
                .sudo()
                .search([("branch_id", "=", int(branch))])
            )

            if branch_officer and rule.alert_id.id == branch_officer.alert_id.id:
                _logger.info(
                    f"Sending branch-specific alert to {branch_officer.officer.name} for branch {branch}"
                )
                # Send branch-specific alert to branch officer with new records only
                self._send_branch_officer_alert_with_new_records_only(
                    rule, query, branch_officer, int(branch)
                )
            else:
                # No specific branch officer, send to general alert group (only once)
                if not general_alert_sent:
                    _logger.info(
                        f"No specific branch officer found for branch {branch}, sending to general alert group"
                    )
                    self._send_general_alert_with_new_records_only(
                        rule, query, rows, columns
                    )
                    general_alert_sent = True

    def send_alert(self, rule):
        """
        Main method to send alerts with new records only detection
        This prevents duplicate notifications by only sending records that haven't been seen before
        """
        if rule.model_id == 'adverse.media':
            rule._run_adverse_media_alert_rule(force=False)
            return

        try:
            query: str = self.format_query(rule)

            # Get alias for safe ORDER BY
            alias = self.get_first_table_alias(query)

            # Add ORDER BY safely if not already present
            if "ORDER BY" not in query.upper():
                if query.endswith(";"):
                    query = query[:-1]
                if alias:
                    query += f" ORDER BY {alias}.id DESC;"
                else:
                    query += " ORDER BY id DESC;"

            _logger.info(f"Processing alert rule: {rule.name}")
            _logger.info(f"Final query: {query}")

            # Execute initial query to get all data
            self.env.cr.execute(f"{query}")
            rows = self.env.cr.fetchall()
            columns = [desc[0] for desc in self.env.cr.description]

            # Early return if no records found
            if len(rows) == 0:
                _logger.info("No records found, clearing stored signatures")
                # Clear previous signatures since no data exists
                self._clear_alert_record_signatures(rule)
                return

            # Determine alert routing based on alert type
            if rule.alert_id.tag != "internal":
                # External alert handling - check for branch partitioning
                branchcode_index = self._find_branch_column_index(columns)

                if branchcode_index is not None:
                    _logger.info("Branch column found, handling branch-specific alerts")
                    # Handle branch-specific alerts with new records only
                    self._handle_branch_specific_alerts_with_new_records_only(
                        rule, query, rows, columns, branchcode_index
                    )
                else:
                    _logger.info(
                        "No branch column found, sending to general alert group"
                    )
                    # No branch column found, send to general alert group with new records only
                    self._send_general_alert_with_new_records_only(
                        rule, query, rows, columns
                    )
            else:
                _logger.info("Internal alert type, sending to internal recipients")
                # Internal alert handling with new records only
                self._send_internal_alert_with_new_records_only(
                    rule, query, rows, columns
                )

            _logger.info(f"Alert processing completed for rule: {rule.name}")

        except Exception as e:
            _logger.error(f"Error processing alert rule {rule.name}: {str(e)}")
            raise ValueError(f"Alert processing failed: {str(e)}")
