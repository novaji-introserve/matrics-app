# models/pdf_chat.py
import base64
import io
from collections import Counter
import os

import requests

from odoo import models, fields, api
import PyPDF2
# import ollama
from odoo.exceptions import ValidationError, UserError
import PyPDF2
from PIL import Image
import pytesseract
from pdf2image import convert_from_bytes
from odoo.exceptions import UserError

from dotenv import load_dotenv

load_dotenv()

class PdfChat(models.Model):
    _name = 'pdf.chat'
    _description = 'PDF Chat'
    _rec_name="pdf_file_name"

    user_id = fields.Many2one('res.users', string='User', required=True, default=lambda self: self.env.user)
    rulebook_id = fields.Many2one('rulebook.title', string='Rulebook', required=True)
    pdf_file_name = fields.Char(string='PDF File Name', compute='_compute_pdf_file_name', store=True)
    extracted_text = fields.Text(string='Extracted Text', readonly=True)
    chat_logs = fields.One2many('pdf.chat.log', 'pdf_chat_id', string='Chat Logs')
    user_question = fields.Text(string='User Question')  # Temporary field for user input

    @api.depends('rulebook_id')
    def _compute_pdf_file_name(self):
        """Compute method to set the PDF file name and extract text for chatting."""
        for record in self:
            if record.rulebook_id:
                rulebook = record.rulebook_id
                # Assume rulebook has 'pdf_file' (binary) and 'title' (pdf file name)
                record.pdf_file_name = rulebook.file_name

                # Extract the text from the PDF
                pdf_file = rulebook.file  # Binary PDF file stored in the rulebook
                print('i am pdf file')
                print(pdf_file)

                if pdf_file:
                    pdf_text = self._extract_text_from_pdf(pdf_file)
                    record.extracted_text = pdf_text
                    print(pdf_text)

                    # Optionally, initiate an initial query to Ollama based on the PDF content (e.g., summary)
                    # initial_prompt = f"Provide a brief summary of the document content:\n\n{pdf_text}"
                    # summary = self.query_llama_model(initial_prompt)
                    # # Add initial summary or chat log if needed (optional)
                    # self.env['pdf.chat.log'].create({
                    #     'pdf_chat_id': record.id,
                    #     'question': 'Initial PDF Summary',
                    #     'response': summary,
                    # })
   
    
    def _extract_text_from_pdf(self, pdf_file):
        """Extract text from the provided binary PDF file, handling both text-based and image-based PDFs."""
        try:
            # Decode the base64-encoded binary file
            decoded_pdf_data = base64.b64decode(pdf_file)
            # Convert the decoded binary data into a byte stream
            pdf_stream = io.BytesIO(decoded_pdf_data)
        
            # Try reading the PDF using PyPDF2 (for text-based PDFs)
            try:
                pdf_reader = PyPDF2.PdfReader(pdf_stream)
                extracted_text = ""
            
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                
                    # If no text is found, fallback to OCR
                    if not text or text.isspace():
                        extracted_text += self._extract_text_from_image_pdf(decoded_pdf_data)
                        break
                    extracted_text += text
            
                return extracted_text
        
            except Exception:  # Changed from PyPDF2.errors.PdfReadError
                # Handle image-based PDF by converting each page to an image and applying OCR
                return self._extract_text_from_image_pdf(decoded_pdf_data)
        except Exception as e:
            raise UserError(f"Error reading PDF file: {str(e)}")
        

    def _extract_text_from_image_pdf(self, pdf_data):
        """Convert image-based PDF pages to text using OCR."""
        extracted_text = ""
        try:
            # Convert PDF pages to images
            pages = convert_from_bytes(pdf_data)
            # Perform OCR on each page
            for page in pages:
                text = pytesseract.image_to_string(page)
                extracted_text += text + "\n"
    
        except Exception as e:
            raise UserError(f"Error extracting text from image-based PDF: {str(e)}")
    
        return extracted_text
    
    # def query_llama_model(self, prompt):
    #     """Query the LLaMA model using the extracted text and user prompt."""
    #     model = "llama3.1"
    #     messages = [{"role": "user", "content": prompt}]
    #     response = ollama.chat(model=model, messages=messages)

    #     # Extract only the 'message.content' part from the response
    #     response_content = response.get('message', {}).get('content', '')
        # return response_content

    def chat_with_pdf(self):
        for record in self:
            """Handle chat based on the extracted text and user's question."""
            if not record.user_question:
                return
            print('i was called')
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
            print(f"HTTP error occurred: {http_err}")  # Handle HTTP errors
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")  # Handle connection errors
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")  # Handle timeout errors
        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred: {req_err}")  # Handle any other request errors
        except Exception as e:
            print(f"An unexpected error occurred: {e}")  # Handle unexpected errors

        return "An unexpected error occurred: {e}"  # Return None or an appropriate value in case of an errorresponses


class PdfChatLog(models.Model):
    _name = 'pdf.chat.log'
    _description = 'PDF Chat Log'

    pdf_chat_id = fields.Many2one('pdf.chat', string='PDF Chat', required=True)
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
        print(result)
        print("i am result")

        return result

