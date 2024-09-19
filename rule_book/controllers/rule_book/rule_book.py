from odoo import http
from odoo.http import request
from cryptography.fernet import Fernet
import logging
from werkzeug.datastructures import FileStorage
import base64
import logging
import os

_logger = logging.getLogger(__name__)


# key = Fernet.generate_key()
# print(key.decode())
# Use the following key for encryption and decryption
key = b"Gt9Q2_7Wrldc-k5YPUG3WzRmWOVY8OZuQrSNw_NJN3o="  # Replace with your generated key
cipher_suite = Fernet(key)


def encrypt_id(rulebook_id):
    """Encrypt the rulebook ID."""
    return cipher_suite.encrypt(str(rulebook_id).encode()).decode()


def decrypt_id(encrypted_id):
    """Decrypt the rulebook ID."""
    return int(cipher_suite.decrypt(encrypted_id.encode()).decode())


class RuleBookController(http.Controller):

    @http.route(
        "/submit_external_report",
        type="http",
        auth="public",
        methods=["POST"],
        csrf=False,
    )
    def submit_external_report(self, **kwargs):
        try:
            print("testing")

            # Create a new reply log record
            ReplyLog = request.env["reply.log"]

            # Get the decrypted rulebook_id from the kwargs
            rulebook_id = kwargs.get("rulebook_id")
            print(rulebook_id)
            print("testing")

            # Ensure rulebook_id is provided
            if not rulebook_id:
                raise ValueError("Rulebook ID is missing in the request")

            # Handle the file upload
            file_data = kwargs.get("document")
            if isinstance(file_data, FileStorage):
                document = base64.b64encode(file_data.read()).decode("utf-8")
            else:
                document = False

            # get the rule book data
            # Fetch the rulebook record using the provided ID
            rulebook = request.env["rulebook"].sudo().browse(int(rulebook_id))
            # Create the reply log record
            report = ReplyLog.sudo().create(
                {
                    "rulebook_id": rulebook_id,
                    "reply_content": kwargs.get("reply_content"),
                    "reporter": kwargs.get("reporter"),
                    "document": document,
                    "rulebook_status": "submitted",
                    "rulebook_compute_date": rulebook.computed_date,
                }
            )
            # getting the url of the reply log
            base_url = (
                request.env["ir.config_parameter"].sudo().get_param("web.base.url")
            )
            #
            url = f"{base_url}/web#id={report.id}&cids=1&menu_id=108&action=302&model=reply.log&view_type=form"
            # global data
            report.set_global_data(
                {
                "email_from": "icomply@bio.ng",
                "email_to": rulebook.first_line_escalation.email,
                "name": rulebook.name.name,
                "content": kwargs.get("reply_content"),
                "url_link": url,
            }
            ) 

            # Trigger alert to escalation officer
            self.trigger_escalation_alert(report)

            # Redirect to a thank-you page or another action
            return request.redirect("/thank_you")

        except ValueError as e:
            # Handle specific ValueErrors (e.g., missing rulebook_id)
            _logger.error(f"ValueError in submit_external_report: {str(e)}")
            # Optionally redirect to an error page or provide user feedback
            return request.redirect("/error?message=Invalid input")

        except Exception as e:
            # Handle other exceptions
            _logger.error(f"Exception in submit_external_report: {str(e)}")
            # Optionally redirect to an error page or provide user feedback
            return request.redirect(f"/error?message={str(e)}")

    def trigger_escalation_alert(self, report):
        # Logic for sending email to escalation officers

        template = request.env.ref("rule_book.email_template_escalation")
        template.sudo().send_mail(report.id, force_send=True)

    @http.route(
        "/report_submission/<string:encrypted_id>",
        type="http",
        auth="public",
        website=True,
    )
    def report_submission(self, encrypted_id):

        # Decrypt the rulebook ID from the URL
        rulebook_id = decrypt_id(encrypted_id)
        return request.render(
            "rule_book.external_submission_template",
            {
                "rulebook_id": rulebook_id,
            },
        )

    @http.route(
        "/encrypt/<string:encrypted_id>", type="http", auth="public", website=True
    )
    def testing(self, encrypted_id):

        # Decrypt the rulebook ID from the URL
        return encrypt_id(encrypted_id)

    @http.route("/error", type="http", auth="public", website=True)
    def error_page(self):
        error = request.params.get("message")
        # Decrypt the rulebook ID from the URL
        return error

    @http.route("/thank_you", type="http", auth="public", website=True)
    def thank_you_page(self):

        # Decrypt the rulebook ID from the URL
        return "report submitted Successfully"

    @http.route("/get_stored_document", type="http", auth="public", website=True)
    def get_stored_document(self):

        # Decrypt the rulebook ID from the URL
        return "report submitted Successfully"
