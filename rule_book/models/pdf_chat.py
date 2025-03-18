# models/pdf_chat.py
import base64
import io
from collections import Counter
import os
import binascii  # Make sure this import is added


import requests

from odoo import models, fields, api
import PyPDF2
# import ollama
from odoo.exceptions import ValidationError, UserError
from PIL import Image
import pytesseract
from pdf2image import convert_from_bytes
from odoo.exceptions import UserError

from dotenv import load_dotenv
import logging

_logger = logging.getLogger(__name__)


load_dotenv()

class PdfChat(models.Model):
    _name = 'pdf.chat'
    _description = 'PDF Chat'
    _rec_name="pdf_file_name"

    user_id = fields.Many2one('res.users', ondelete='cascade',
                              string='User', required=True, default=lambda self: self.env.user)
    rulebook_id = fields.Many2one(
        'rulebook.title', string='Rulebook', required=True, ondelete='cascade', index=True)
    pdf_file_name = fields.Char(string='PDF File Name', compute='_compute_pdf_file_name' )
    extracted_text = fields.Text(
        string='Extracted Text', readonly=True, index=False)
    chat_logs = fields.One2many(
        'pdf.chat.log', 'pdf_chat_id', string='Chat Logs', ondelete='cascade', index=True)
    # Temporary field for user input
    user_question = fields.Text(string='User Question')
    # extracted_text_search = fields.Char(
    #     string='Extracted Text Search', index=True,
    #     compute='_compute_extracted_text_search', store=True)


    
    
    # @api.depends('extracted_text')
    # def _compute_extracted_text_search(self):
    #     for record in self:
    #         if record.extracted_text:
    #             # Store only the first 1000 characters for search purposes
    #             record.extracted_text_search = record.extracted_text[:1000]
    #         else:
    #             record.extracted_text_search = False

    
    def _get_pdf_content(self, rulebook):
        """Retrieve the actual PDF content from the rulebook."""
        if not rulebook.file:
            return None

        # Try to get the attachment directly
        attachment = self.env['ir.attachment'].search([
            ('res_model', '=', 'rulebook.title'),
            ('res_id', '=', rulebook.id),
            ('res_field', '=', 'file')
        ], limit=1)

        if attachment:
            _logger.info(
                f"Found attachment: {attachment.name}, size: {attachment.file_size}")

            # Try a direct approach to get the file contents
            try:
                # For Odoo v14+ with attachment store
                if hasattr(attachment, '_full_path'):
                    store_fname = attachment.store_fname
                    if store_fname:
                        full_path = attachment._full_path(store_fname)
                        _logger.info(f"Reading file directly from: {full_path}")
                        with open(full_path, 'rb') as f:
                            file_content = f.read()
                            return file_content

                # Another approach for getting the binary data
                if hasattr(attachment, 'raw'):
                    _logger.info("Using attachment.raw to get data")
                    return attachment.raw

                # For database storage
                _logger.info("Using standard datas field")
                raw_datas = attachment.datas

                # If it's a string (base64), decode it
                if isinstance(raw_datas, str):
                    # Remove any padding issues
                    padding = len(raw_datas) % 4
                    if padding:
                        raw_datas += '=' * (4 - padding)

                    try:
                        return base64.b64decode(raw_datas)
                    except Exception as e:
                        _logger.error(f"Failed to decode attachment.datas: {e}")

                return raw_datas
            except Exception as e:
                _logger.error(f"Error accessing attachment data: {e}")

        # If all else fails, try the original file
        return rulebook.file
    
    @api.depends('rulebook_id')
    def _compute_pdf_file_name(self):
        """Compute method to set the PDF file name and extract text for chatting."""
        for record in self:
            record.pdf_file_name = None
            record.extracted_text = None
            
           
            if not record.rulebook_id:
                continue

            rulebook = record.rulebook_id
            record.pdf_file_name = rulebook.file_name or None

            if not rulebook.file:
                _logger.warning(f"No file found for rulebook ID {rulebook.id}")
                continue

            # Attempt to get the actual PDF content
            try:
                pdf_content = self._get_pdf_content(rulebook)
                if not pdf_content:
                    _logger.error("Could not retrieve PDF content")
                    record.extracted_text = "Error: Could not retrieve the PDF file"
                    continue

                # Check if the content is actually a PDF
                if isinstance(pdf_content, bytes) and not pdf_content.startswith(b'%PDF'):
                    _logger.error("Content does not appear to be a valid PDF")
                    # Try to decode it if it might be base64
                    try:
                        decoded = base64.b64decode(pdf_content)
                        if decoded.startswith(b'%PDF'):
                            pdf_content = decoded
                            _logger.info(
                                "Successfully decoded content to valid PDF")
                    except:
                        pass

                # Extract text from the PDF
                pdf_text = self._extract_text_from_pdf(pdf_content)

                if pdf_text and not pdf_text.startswith("Error:"):
                    record.extracted_text = pdf_text
                    _logger.info(
                        f"Successfully extracted {len(pdf_text)} characters of text")
                else:
                    _logger.warning(f"Text extraction failed: {len(pdf_text)}")
                    record.extracted_text = pdf_text
            except Exception as e:
                _logger.error(f"Error in PDF processing: {e}")
                record.extracted_text = f"Error processing PDF: {str(e)}"
            
    def _extract_text_from_pdf(self, pdf_file):
        """Extract text from the provided binary PDF file."""
        if not pdf_file:
            _logger.error("No PDF file provided")
            return "No PDF content available"

        # Make sure we're working with binary data
        if isinstance(pdf_file, str):
            try:
                pdf_file = base64.b64decode(pdf_file)
            except Exception as e:
                _logger.error(f"Failed to decode base64 string: {e}")
                return "Error: Could not decode PDF data"

        # Safety check - is this actually a PDF?
        if not pdf_file.startswith(b'%PDF'):
            _logger.error(
                "Data does not appear to be a valid PDF (missing PDF header)")
            # Log a sample of the data for debugging
            sample = pdf_file[:100].hex()
            _logger.info(f"First 100 bytes (hex): {sample}")
            return "Error: File does not appear to be a valid PDF"

        # Get a file-like object
        pdf_stream = io.BytesIO(pdf_file)

        try:
            # Try PyPDF2
            reader = PyPDF2.PdfReader(pdf_stream, strict=False)
            text = ""
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"

            if text.strip():
                return text

            # If no text extracted, fallback to OCR
            return self._extract_text_from_image_pdf(pdf_file)
        except Exception as e:
            _logger.error(f"PyPDF2 error: {e}")
            # Fallback to OCR
            return self._extract_text_from_image_pdf(pdf_file)

    def _process_pdf_stream(self, pdf_stream):
        """Process a PDF stream and extract text."""
        try:
            pdf_reader = PyPDF2.PdfReader(pdf_stream)
            extracted_text = ""

            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                text = page.extract_text()

                # If no text is found, fallback to OCR
                if not text or text.isspace():
                    # Reset the stream to start for OCR processing
                    pdf_stream.seek(0)
                    ocr_text = self._extract_text_from_image_pdf(
                        pdf_stream.getvalue())
                    extracted_text += ocr_text if ocr_text else ""
                    break
                extracted_text += text

            return extracted_text
        except Exception as pdf_err:
            _logger.error(f"Error with PyPDF2: {str(pdf_err)}")
            # Handle image-based PDF by converting each page to an image and applying OCR
            pdf_stream.seek(0)  # Reset stream position
            return self._extract_text_from_image_pdf(pdf_stream.getvalue())

    def _extract_text_from_image_pdf(self, pdf_data):
            """Convert PDF pages to images and use OCR."""
            try:
                _logger.info("Attempting OCR conversion of PDF")
                pages = convert_from_bytes(pdf_data)
                text = ""
                
                for i, page in enumerate(pages):
                    _logger.info(f"Processing page {i+1} with OCR")
                    page_text = pytesseract.image_to_string(page)
                    text += page_text + "\n"
                    
                if not text.strip():
                    return "OCR processing did not extract any text"
                    
                return text
            except Exception as e:
                _logger.error(f"OCR processing error: {e}")
                return f"Error during OCR processing: {str(e)}"
    
    def chat_with_pdf(self):
        for record in self:
            """Handle chat based on the extracted text and user's question."""
            if not record.user_question:
                return
            # Construct the prompt based on the PDF content and user's question
            prompt = f"Based on the following document content: {record.extracted_text}\n\nUser question: {record.user_question}"
            response = self.query_gemini_api(prompt)

            # Save the question and response as chat logs
            self.env['pdf.chat.log'].create({
                'pdf_chat_id': self.id,
                'question': record.user_question,
                'response': response,
            })

            # Clear the temporary field after processing
            self.user_question = False

            return {
                'type': 'ir.actions.client',
                'tag': 'reload',
            }
    
  
    def query_gemini_api(self,prompt):
        apikey = os.getenv("GEMINI_API")
        """Function to send the prompt to the Gemini API and return the response."""
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={apikey}"
        headers = {
            'Content-Type': 'application/json',
        }

        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt}
                    ]
                }
            ]
        }

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            response.raise_for_status()  # Raise an error for bad responses
            
            # Extract the text from the response
            candidates = response.json().get('candidates', [])
            if candidates:
                # Assuming you want the text from the first candidate
                text_parts = candidates[0].get('content', {}).get('parts', [])
                if text_parts:
                    return ''.join(part['text'] for part in text_parts)

        except requests.exceptions.HTTPError as http_err:
            # Handle HTTP errors
            _logger.info(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            # Handle connection errors
            _logger.info(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            # Handle timeout errors
            _logger.info(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            # Handle any other request errors
            _logger.info(f"An error occurred: {req_err}")
        except Exception as e:
            # Handle unexpected errors
            _logger.info(f"An unexpected error occurred: {e}")

        return "An unexpected error occurred: {e}"  # Return None or an appropriate value in case of an errorresponses


class PdfChatLog(models.Model):
    _name = 'pdf.chat.log'
    _description = 'PDF Chat Log'

    pdf_chat_id = fields.Many2one(
        'pdf.chat', ondelete='cascade', string='PDF Chat', required=True)
    question = fields.Text(string='Question', )
    response = fields.Text(string='Response')
    timestamp = fields.Datetime(string='Timestamp', default=fields.Datetime.now)


    @api.model
    def get_most_asked_questions(self):
        # Fetch all records where the question field is not empty
        logs = self.search([('question', '!=', False)])

        # Count occurrences of each question
        question_counter = Counter(logs.mapped('question'))

        # Get the top 5 most asked questions
        most_asked_questions = question_counter.most_common(5)

        # Prepare the result with questions and their counts
        result = []
        for question, count in most_asked_questions:
            result.append({
                'question': question,
                'count': count,
            })

        # For debugging or logging
        _logger.info(f"results {result}")

        return result

