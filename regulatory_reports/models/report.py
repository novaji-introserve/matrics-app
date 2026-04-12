import base64
import html
import io
import logging

import sqlparse
import xlsxwriter

from odoo import _, api, fields, models
from odoo.exceptions import UserError, ValidationError
from sqlparse import sql
from sqlparse.tokens import DML, Keyword

_logger = logging.getLogger(__name__)


class RegulatoryReportRun(models.Model):
    _name = "res.regulatory.report.run"
    _description = "Regulatory Report Run"
    _order = "create_date desc, id desc"

    name = fields.Char(required=True)
    report_id = fields.Many2one(
        "res.regulatory.report",
        string="Report",
        required=True,
        ondelete="cascade",
        index=True,
    )
    query_text = fields.Text(string="Query", required=True, readonly=True)
    state = fields.Selection(
        [
            ("queued", "Queued"),
            ("running", "Running"),
            ("done", "Done"),
            ("failed", "Failed"),
        ],
        string="Status",
        default="queued",
        index=True,
    )
    row_count = fields.Integer(string="Rows Exported", readonly=True)
    file_name = fields.Char(string="File Name", readonly=True)
    attachment_id = fields.Many2one(
        "ir.attachment",
        string="Download File",
        readonly=True,
        ondelete="set null",
    )
    download_url = fields.Char(
        string="Download URL",
        compute="_compute_download_url",
        readonly=True,
    )
    error_message = fields.Text(string="Error", readonly=True)
    requested_by = fields.Many2one(
        "res.users",
        string="Requested By",
        default=lambda self: self.env.user,
        readonly=True,
    )
    download_ready = fields.Boolean(
        string="Download Ready",
        compute="_compute_download_ready",
    )

    @api.depends("attachment_id")
    def _compute_download_url(self):
        for rec in self:
            rec.download_url = (
                f"/web/content/{rec.attachment_id.id}?download=true"
                if rec.attachment_id
                else False
            )

    @api.depends("state", "attachment_id")
    def _compute_download_ready(self):
        for rec in self:
            rec.download_ready = rec.state == "done" and bool(rec.attachment_id)

    def action_download_file(self):
        self.ensure_one()
        if not self.download_ready:
            raise UserError(_("The file is not ready for download yet."))
        return {
            "type": "ir.actions.act_url",
            "url": self.download_url,
            "target": "self",
        }

    def generate_download_file(self):
        for rec in self:
            try:
                rec.write({"state": "running", "error_message": False})
                columns, row_count, file_content = rec.report_id._generate_excel_content(
                    rec.query_text
                )
                file_name = rec.report_id._build_export_filename()
                attachment = self.env["ir.attachment"].create(
                    {
                        "name": file_name,
                        "type": "binary",
                        "datas": base64.b64encode(file_content).decode("ascii"),
                        "mimetype": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "res_model": rec._name,
                        "res_id": rec.id,
                    }
                )
                rec.write(
                    {
                        "state": "done",
                        "row_count": row_count,
                        "file_name": file_name,
                        "attachment_id": attachment.id,
                    }
                )
            except Exception as exc:
                _logger.exception("Failed to generate regulatory report download")
                rec.write(
                    {
                        "state": "failed",
                        "error_message": str(exc),
                    }
                )


class RegulatoryReport(models.Model):
    _name = "res.regulatory.report"
    _description = "Regulatory Report"
    _order = "name, create_date desc"
    _inherit = ["mail.thread", "mail.activity.mixin"]

    name = fields.Char(string="Name", required=True, tracking=True)
    query_text = fields.Text(
        string="SQL Query",
        required=True,
        default="SELECT 1",
        tracking=True,
    )
    preview_html = fields.Html(
        string="Preview",
        readonly=True,
        sanitize=False,
    )
    preview_row_count = fields.Integer(string="Preview Rows", readonly=True)
    preview_truncated = fields.Boolean(string="Preview Truncated", readonly=True)
    last_preview_at = fields.Datetime(string="Last Preview At", readonly=True)
    latest_run_id = fields.Many2one(
        "res.regulatory.report.run",
        string="Latest Download",
        readonly=True,
        ondelete="set null",
    )
    latest_run_state = fields.Selection(
        related="latest_run_id.state",
        string="Latest Download Status",
        readonly=True,
    )
    latest_download_ready = fields.Boolean(
        string="Latest Download Ready",
        compute="_compute_latest_download_ready",
    )
    run_ids = fields.One2many(
        "res.regulatory.report.run",
        "report_id",
        string="Downloads",
        readonly=True,
    )

    @api.depends("latest_run_id.state", "latest_run_id.attachment_id")
    def _compute_latest_download_ready(self):
        for rec in self:
            rec.latest_download_ready = bool(
                rec.latest_run_id
                and rec.latest_run_id.state == "done"
                and rec.latest_run_id.attachment_id
            )

    @api.constrains("query_text")
    def _check_query_text(self):
        for rec in self:
            rec._validate_select_query(rec.query_text)

    def write(self, vals):
        if "query_text" in vals:
            vals.update(
                {
                    "preview_html": False,
                    "preview_row_count": 0,
                    "preview_truncated": False,
                    "last_preview_at": False,
                    "latest_run_id": False,
                }
            )
        return super().write(vals)

    def action_validate_query(self):
        self.ensure_one()
        self._validate_select_query(self.query_text)
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("SQL Validation"),
                "message": _("The query is a valid SELECT statement."),
                "type": "success",
                "sticky": False,
            },
        }

    def action_preview_report(self):
        self.ensure_one()
        columns, rows, truncated = self._execute_preview_query(self.query_text)
        self.write(
            {
                "preview_html": self._build_preview_html(columns, rows, truncated),
                "preview_row_count": len(rows),
                "preview_truncated": truncated,
                "last_preview_at": fields.Datetime.now(),
            }
        )
        return {
            "type": "ir.actions.client",
            "tag": "reload",
        }

    def action_queue_download(self):
        self.ensure_one()
        self._validate_select_query(self.query_text)
        run = self.env["res.regulatory.report.run"].create(
            {
                "name": f"{self.name} - {fields.Datetime.now()}",
                "report_id": self.id,
                "query_text": self.query_text,
                "requested_by": self.env.user.id,
                "state": "queued",
            }
        )
        self.write({"latest_run_id": run.id})
        run.with_delay(
            description=f"Generate regulatory report download: {self.name}",
            priority=20,
        ).generate_download_file()
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("Download Queued"),
                "message": _("The export has been queued. Refresh when the file is ready."),
                "type": "success",
                "sticky": False,
            },
        }

    def action_download_latest(self):
        self.ensure_one()
        if not self.latest_run_id:
            raise UserError(_("No download has been queued for this report yet."))
        return self.latest_run_id.action_download_file()

    def _build_export_filename(self):
        self.ensure_one()
        slug = "_".join((self.name or "regulatory_report").strip().split())
        slug = slug.lower() or "regulatory_report"
        return f"{slug}.xlsx"

    def _execute_preview_query(self, query_text):
        self._validate_select_query(query_text)
        self.env.cr.execute(query_text)
        columns = [col[0] for col in (self.env.cr.description or [])]
        preview_rows = self.env.cr.fetchmany(31)
        truncated = len(preview_rows) > 30
        rows = preview_rows[:30]
        return columns, rows, truncated

    def _generate_excel_content(self, query_text):
        self.ensure_one()
        self._validate_select_query(query_text)
        self.env.cr.execute(query_text)
        columns = [col[0] for col in (self.env.cr.description or [])]

        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {"in_memory": True})
        worksheet = workbook.add_worksheet("Report")
        header_format = workbook.add_format(
            {
                "bold": True,
                "font_size": 10,
                "font_color": "#0F172A",
                "bg_color": "#EAF1F8",
                "border": 1,
                "border_color": "#CBD5E1",
                "align": "left",
                "valign": "vcenter",
                "text_wrap": False,
            }
        )
        body_format_even = workbook.add_format(
            {
                "font_size": 10,
                "font_color": "#334155",
                "bg_color": "#FFFFFF",
                "border": 1,
                "border_color": "#E2E8F0",
                "align": "left",
                "valign": "top",
            }
        )
        body_format_odd = workbook.add_format(
            {
                "font_size": 10,
                "font_color": "#334155",
                "bg_color": "#F8FAFC",
                "border": 1,
                "border_color": "#E2E8F0",
                "align": "left",
                "valign": "top",
            }
        )

        total_rows = 0
        if columns:
            for col_idx, column_name in enumerate(columns):
                worksheet.write(0, col_idx, column_name, header_format)
                worksheet.set_column(col_idx, col_idx, min(max(len(str(column_name)) + 4, 14), 28))

        worksheet.freeze_panes(1, 0)

        while True:
            chunk = self.env.cr.fetchmany(1000)
            if not chunk:
                break
            for row_offset, row in enumerate(chunk, start=total_rows + 1):
                row_format = body_format_even if row_offset % 2 else body_format_odd
                for col_idx, value in enumerate(row):
                    worksheet.write(row_offset, col_idx, "" if value is None else value, row_format)
            total_rows += len(chunk)

        workbook.close()
        output.seek(0)
        return columns, total_rows, output.read()

    def _build_preview_html(self, columns, rows, truncated):
        if not columns:
            return "<div class='alert alert-info'>The query returned no columns.</div>"

        header_html = "".join(
            f"<th style='padding:9px 10px;border-bottom:1px solid #cbd5e1;border-right:1px solid #e2e8f0;background:#eaf1f8;color:#0f172a;text-align:left;font-size:11px;font-weight:700;white-space:nowrap;text-transform:uppercase;letter-spacing:0.04em;'>{html.escape(str(col))}</th>"
            for col in columns
        )
        body_rows = []
        for index, row in enumerate(rows):
            row_background = "#ffffff" if index % 2 == 0 else "#f8fafc"
            row_html = "".join(
                f"<td style='padding:8px 10px;border-bottom:1px solid #e2e8f0;border-right:1px solid #eef2f7;vertical-align:top;font-size:11px;line-height:1.35;color:#334155;background:{row_background};'>{html.escape('' if value is None else str(value))}</td>"
                for value in row
            )
            body_rows.append(f"<tr style='background:{row_background};'>{row_html}</tr>")

        if not body_rows:
            colspan = max(len(columns), 1)
            body_rows.append(
                f"<tr><td colspan='{colspan}' style='padding:14px 10px;border-bottom:1px solid #e2e8f0;background:#ffffff;font-size:11px;color:#64748b;'>No rows returned.</td></tr>"
            )

        note = ""
        if truncated:
            note = (
                "<p style='margin:0 0 10px 0;color:#64748b;font-size:11px;font-weight:600;'>"
                "Preview limited to the first 30 rows."
                "</p>"
            )

        return (
            "<div style='font-family:Inter, Segoe UI, Arial, sans-serif;'>"
            f"{note}"
            "<div style='overflow:auto;border:1px solid #d8e1eb;border-radius:10px;background:#ffffff;box-shadow:0 10px 30px rgba(15, 23, 42, 0.05);'>"
            "<table style='width:100%;border-collapse:separate;border-spacing:0;font-size:11px;'>"
            f"<thead><tr>{header_html}</tr></thead>"
            f"<tbody>{''.join(body_rows)}</tbody>"
            "</table>"
            "</div>"
            "</div>"
        )

    def _validate_select_query(self, query_text):
        query = (query_text or "").strip()
        if not query:
            raise ValidationError(_("SQL query is required."))

        statements = [stmt for stmt in sqlparse.parse(query) if stmt.tokens and stmt.value.strip()]
        if len(statements) != 1:
            raise ValidationError(_("Only a single SQL statement is allowed."))

        statement = statements[0]
        if not self._is_select_statement(statement):
            raise ValidationError(_("Only SELECT statements are allowed."))

        forbidden_keywords = {
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "ALTER",
            "CREATE",
            "TRUNCATE",
            "MERGE",
            "GRANT",
            "REVOKE",
            "COPY",
            "VACUUM",
            "CALL",
            "EXEC",
            "EXECUTE",
        }
        for token in statement.flatten():
            if token.ttype in Keyword and token.normalized in forbidden_keywords:
                raise ValidationError(
                    _("Only read-only SELECT statements are allowed.")
                )

        return True

    def _is_select_statement(self, statement):
        first_token = statement.token_first(skip_ws=True, skip_cm=True)
        if not first_token:
            return False

        if first_token.ttype is DML and first_token.normalized == "SELECT":
            return True

        if first_token.normalized == "WITH":
            return any(
                token.ttype is DML and token.normalized == "SELECT"
                for token in statement.flatten()
            )

        if isinstance(statement, sql.Statement):
            statement_type = statement.get_type()
            return statement_type == "SELECT"

        return False
