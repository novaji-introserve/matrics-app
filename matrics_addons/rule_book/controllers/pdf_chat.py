# controllers/pdf_chat_controller.py
from odoo import http
from odoo.http import request

class PdfChatController(http.Controller):

    @http.route('/pdf_chat/upload', type='http', auth='user', methods=['POST'], csrf=False)
    def upload_pdf(self, **kwargs):
        """Handle PDF upload and initialize chat."""
        file = kwargs.get('pdf_file')
        pdf_chat = request.env['pdf.chat'].create_from_pdf(file)

        return request.redirect(f'/pdf_chat/{pdf_chat.id}')

    @http.route('/pdf_chat/<int:chat_id>/ask', type='json', auth='user')
    def ask_question(self, chat_id, question):
        """Handle asking questions related to the uploaded PDF."""
        pdf_chat = request.env['pdf.chat'].browse(chat_id)
        response = pdf_chat.chat_with_pdf(question)

        return {'response': response}
