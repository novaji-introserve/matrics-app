import re
from odoo import http
from odoo.http import request
from cryptography.fernet import Fernet
import logging
from werkzeug.datastructures import FileStorage
import base64
import logging
import os
from datetime import datetime
from odoo import models, fields, api


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
            _logger.critical("Submitting external report...")

            # Get the decrypted rulebook_id from the kwargs
            rulebook_id = kwargs.get("rulebook_id")
            if not rulebook_id:
                raise ValueError("Rulebook ID is missing in the request")

            # Handle the file upload
            file_data = kwargs.get("document")
            document = False
            filename = None  # Initialize filename variable

            if isinstance(file_data, FileStorage):
                filename = file_data.filename  # Get the filename
                document = base64.b64encode(file_data.read()).decode("utf-8")
                _logger.info(f"Uploaded file: {filename}")  # Log the filename

            # Fetch the rulebook record using the provided ID
            rulebook = request.env["rulebook"].sudo().browse(int(rulebook_id))

            if not rulebook.exists():
                raise ValueError(f"Rulebook with ID {rulebook_id} does not exist.")

            reporter = rulebook.officer_responsible
            _logger.critical(f"Reporter Id for rulebook: {reporter}")
            _logger.critical(f"Reporter Name for rulebook: {reporter.name}")

            # Search for existing reply log
            reply_log = request.env["reply.log"].sudo().search(
                [('rulebook_id', '=', rulebook.id)], limit=1
            )

            # Prepare data for update or create
            reply_data = {
                "reply_content": kwargs.get("reply_content"),
                "reporter": reporter.id,
                "document": document,
                "document_filename": filename,  # Store the filename
                "rulebook_status": "submitted",
                "rulebook_compute_date": rulebook.computed_date,
                "reply_date": fields.Datetime.now(),
            }

            if reply_log:
                # Update the existing record
                reply_log.write(reply_data)
                _logger.info(f"Reply log updated for Rulebook ID {rulebook_id}.")
            else:
                # Create a new record
                reply_data["rulebook_id"] = rulebook.id
                reply_log = request.env["reply.log"].sudo().create(reply_data)
                _logger.info(f"New reply log created for Rulebook ID {rulebook_id}.")
           
            # Generate the URL for the reply log
            # Access the rulebook model
            rulebook_model = request.env['rulebook']
            # Call the `_record_link` method
            url = rulebook_model._record_link(reply_log.id,model_name='reply.log')

            # Prepare global data
            current_year = datetime.now().year
            global_data = {
                "email_from": "leonell4fame@gmail.com",
                "email_to": rulebook.first_line_escalation.email,
                "name":  rulebook.type_of_return,
                "title":  rulebook.name.name,
                "content": kwargs.get("reply_content"),
                "url_link": url,
                "current_year": current_year,
            }
            
            if rulebook.first_line_escalation:
                reply_log.set_global_data(global_data)
            # Trigger alert to escalation officer
                self.trigger_escalation_alert(reply_log)

            # Redirect to a thank-you page
            return request.redirect(f"/thank_you?reporter={reporter.name}")

        except ValueError as e:
            _logger.error(f"ValueError in submit_external_report: {str(e)}")
            return request.redirect("/error?message=Invalid input")

        except Exception as e:
            _logger.error(f"Exception in submit_external_report: {str(e)}")
            return request.redirect(f"/error?message={str(e)}")

   
    def trigger_escalation_alert(self, report):
        # Logic for sending email to escalation officers
        template = request.env.ref("rule_book.email_template_escalation")
        if template:
            template.sudo().send_mail(report.id, force_send=True)
        else:
            _logger.critical(
                "Email template 'rule_book.email_template_escalation' not found.")


    @http.route(
        "/report_submission/<string:encrypted_id>",
        type="http",
        auth="public",
        website=True,
    )
    
    
    def report_submission(self, encrypted_id):
        # Decrypt the rulebook ID from the URL
        rulebook_id = decrypt_id(encrypted_id)
        rulebook = request.env["rulebook"].sudo().browse(int(rulebook_id))
        if not rulebook:
            error_message="The Rulebook No longer exist on the system"
            return request.redirect(f"/error?message={error_message}")
        current_year = datetime.now().year
        return request.render(
            "rule_book.external_submission_template",
            {
                "rulebook_id": rulebook_id,
                "current_year": current_year,
                "rulebook":   rulebook.type_of_return if rulebook.type_of_return else "",
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
        current_year = datetime.now().year
        reporter = request.params.get("reporter")
        # Log the filename
        _logger.critical(f"Reporter Id for rulebook: {reporter}")
        
        # Pass the relevant details to the template
        return request.render(
            "rule_book.thank_you_page_template",
            {
                "current_year": current_year,
                "report_submitter": reporter if reporter else "Unknown",
            },
        )
    

    @http.route("/get_stored_document", type="http", auth="public", website=True)
    def get_stored_document(self):

        # Decrypt the rulebook ID from the URL
        return "report submitted Successfully"
