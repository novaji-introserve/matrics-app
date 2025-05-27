from odoo import http, fields
from odoo.http import request, content_disposition
from PyPDF2 import PdfFileReader
from pdf2image import convert_from_bytes
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import fitz
from reportlab.lib import colors
from pathlib import Path
import base64
from io import BytesIO

class PDFController(http.Controller):

    def safe_str(self, value):
        if isinstance(value, float):
            return f"{value:,.2f}"
        return str(value) if value not in [None, False] else 'N/A'
    
    def extract_pdf_as_image_pymupdf(self, pdf_data):
        """Extract first page of PDF as image using PyMuPDF"""
        try:
            pdf_doc = fitz.open(stream=pdf_data, filetype="pdf")
            page = pdf_doc[0] 
            
            # Render page as image
            mat = fitz.Matrix(2.0, 2.0) 
            pix = page.get_pixmap(matrix=mat)
            img_data = pix.tobytes("png")
            pdf_doc.close()
            
            return BytesIO(img_data)
        except Exception as e:
            print(f"Error with PyMuPDF: {e}")
            return None
    def format_text_case(self, text):
        """Format text from ALL CAPS to proper case"""
        if not text or not isinstance(text, str):
            return text
        
        formatted = text.title()
        
        # Handle common words that shouldn't be title case
        exceptions = ['Of', 'And', 'The', 'For', 'In', 'On', 'At', 'To', 'A', 'An']
        words = formatted.split()
        
        for i, word in enumerate(words):
            if i > 0 and word in exceptions:
                words[i] = word.lower()
        
        return ' '.join(words)

    @http.route('/compliance/pdf_report/<int:record_id>', type='http', auth="user")
    def generate_pdf_report(self, record_id, **kwargs):
        LOGO_PATH = "custom_addons/icomply_odoo/compliance_management/static/img/alt_bank_logo_.png"
        record = request.env['res.partner.edd'].browse(record_id)
        if not record.exists():
            return request.not_found()

        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
                                rightMargin=72, leftMargin=72,
                                topMargin=36, bottomMargin=72)  
        
        # Define styles
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(name='Question', fontSize=12, leading=14, textColor=colors.black, spaceAfter=6))
        styles.add(ParagraphStyle(name='Answer', fontSize=12, leading=14, textColor=colors.darkblue, leftIndent=20, spaceAfter=12))
        styles.add(ParagraphStyle(name='SectionHeader', fontSize=14, leading=16, textColor=colors.darkgreen, 
                                spaceBefore=20, spaceAfter=10, fontName='Helvetica-Bold'))
        styles.add(ParagraphStyle(name='BankTitle', fontSize=16, leading=18, textColor=colors.black, 
                                alignment=1, spaceBefore=10, spaceAfter=10, fontName='Helvetica-Bold'))

        # Updated signature styles - all BLACK now
        styles.add(ParagraphStyle(name='SignatureLabel', fontSize=12, leading=14, textColor=colors.black,  
                                alignment=1, spaceBefore=2, fontName='Helvetica-Bold'))  
        styles.add(ParagraphStyle(name='SignatureName', fontSize=12, leading=14, textColor=colors.black,  
                                alignment=1, spaceBefore=8, fontName='Helvetica-Bold'))
        styles.add(ParagraphStyle(name='SignatureDate', fontSize=11, leading=13, textColor=colors.black, 
                                alignment=1, spaceBefore=4))
        styles.add(ParagraphStyle(name='SignatureInitials', fontSize=10, leading=11, textColor=colors.black, 
                                alignment=1, spaceBefore=12))


        story = []
        
        # Add Bank Logo Section (
        if Path(LOGO_PATH).exists():
            try:
                logo = Image(LOGO_PATH, width=1.5*inch, height=0.75*inch)  # Smaller dimensions
                logo.hAlign = 'CENTER'
                story.append(logo)
                story.append(Spacer(1, 8)) 
            except:
                pass  
        
        # Bank Name and Report Title
        story.append(Paragraph("DUE DILIGENCE REPORT", styles['BankTitle']))
        story.append(Spacer(1, 12))  


        # Helper functions
        def format_yes_no_with_checkboxes(value, question):
            """Format yes/no questions with single checkbox (checked=Yes, unchecked=No)"""
            if isinstance(value, bool):
                is_yes = value
            else:
                is_yes = str(value).lower() in ['yes', 'true', '1'] if value else False
            
            checkbox = "[✓]" if is_yes else "[X]"
            return checkbox

        if record.customer_type == "individual":
            # Sections
            def add_section(title, fields):
                story.append(Paragraph(title, styles['SectionHeader']))
                for field, question in fields:
                    story.append(Paragraph(question, styles['Question']))
                    
                    
                    if '.' in field:
                        field_parts = field.split('.')
                        value = record
                        for part in field_parts:
                            value = getattr(value, part, None)
                            if value is None:
                                break
                    else:
                        value = getattr(record, field, None)
                    

                    if hasattr(value, 'name') and not isinstance(value, str): 
                        value = value.name
                    elif isinstance(value, (list, tuple)) and hasattr(value, 'mapped'):  
                        value = ", ".join(value.mapped('name')) or 'N/A'
                    elif hasattr(value, '__iter__') and not isinstance(value, str):  
                        try:
                            value = ", ".join([str(item.name) if hasattr(item, 'name') else str(item) for item in value])
                        except:
                            value = str(value)
                    
                    if  isinstance(value,str) and value:
                        value = self.format_text_case(value)
                    
                    story.append(Paragraph(self.safe_str(value), styles['Answer']))
            # Customer Profile
            add_section("Customer Profile Information", [
                ('customer_id', "Customer name"),
                ('customer_address', "Residential Address"),
                ('brief_customer_profile', "Please give a brief profile of the subject customer?"),
                ('nature_of_business', "What is the specific nature of the customers business/occupation/employment?"),
                ('employment_position', "Employment Position?"),
                ('employer_name', "Name of employer/company?"),
            ])

            # Residency
            story.append(Paragraph("Residency Information", styles['SectionHeader']))
            residency_fields = [
            ('residency_status', "Residency Status?"),
                ('applicable_country_ids', "Residential Countries?"),
            ]
            for field, question in residency_fields:
                story.append(Paragraph(question, styles['Question']))
                if field == 'residency_status':
                    value = getattr(record, field, None)
                    if value:
                        value = dict(record._fields[field].selection).get(value)
                elif field == 'applicable_country_ids':
                    value = ", ".join(record.applicable_country_ids.mapped('name')) if record.applicable_country_ids else 'N/A'
                else:
                    value = getattr(record, field, None)
                story.append(Paragraph(self.safe_str(value), styles['Answer']))
                
            # Financial Details
            story.append(Paragraph("Financial Details", styles['SectionHeader']))
            financial_fields = [
                ('expected_monthly_income', "What is the expected monthly income/inflow of the customer?"),
                ('estimated_net_worth', "What is the estimated total net worth of the customer?"),
                ('inflow_purpose', "What would be the purpose of the inflows routed into the customer's account?"),
                ('inflow_document', "Provide supporting documents if applicable?"),
                ('outflow_purpose', "What would be the expected purpose of the outflows from the account?"),
                ('outflow_document', "Provide supporting documents if applicable?"),
            ]
            for field, question in financial_fields:
                story.append(Paragraph(question, styles['Question']))
                if field in ['expected_monthly_income', 'estimated_net_worth']:
                    value = self.safe_str(getattr(record, field, None))
                elif field in ['inflow_document', 'outflow_document']:
                    value = "Attach evidence" if getattr(record, field, False) else 'N/A'
                else:
                    value = getattr(record, field, None) or 'N/A'
                story.append(Paragraph(value, styles['Answer']))
                
            # PEP Information
            story.append(Paragraph("PEP Information", styles['SectionHeader']))
            is_pep = getattr(record, 'is_customer_pep', False)
            question = "Is the customer a Politically Exposed Person (PEP) or associated to a PEP?"
            formatted_value = format_yes_no_with_checkboxes(is_pep, question)
            story.append(Paragraph(question, styles['Question']))
            story.append(Paragraph(formatted_value, styles['Answer']))

            # Only show 'relationship_with_pep' if 'is_customer_pep' is True
            if is_pep:
                story.append(Paragraph("", styles['Question']))
                relationship = getattr(record, 'relationship_with_pep', None)
                story.append(Paragraph(self.safe_str(relationship), styles['Answer']))


                            
        else:
            # Sections
            def add_section(title, fields):
                story.append(Paragraph(title, styles['SectionHeader']))
                for field, question in fields:
                    story.append(Paragraph(question, styles['Question']))
                    
                    
                    if '.' in field:
                        field_parts = field.split('.')
                        value = record
                        for part in field_parts:
                            value = getattr(value, part, None)
                            if value is None:
                                break
                    else:
                        value = getattr(record, field, None)
                    

                    if hasattr(value, 'name') and not isinstance(value, str): 
                        value = value.name
                    elif isinstance(value, (list, tuple)) and hasattr(value, 'mapped'):  
                        value = ", ".join(value.mapped('name')) or 'N/A'
                    elif hasattr(value, '__iter__') and not isinstance(value, str): 
                        try:
                            value = ", ".join([str(item.name) if hasattr(item, 'name') else str(item) for item in value])
                        except:
                            value = str(value)

                    if  isinstance(value,str) and value:
                        value = self.format_text_case(value)
                    
                    story.append(Paragraph(self.safe_str(value), styles['Answer']))
            # Customer Profile
            add_section("Customer Profile Information", [
                ('customer_id', "Entity Name"),
                ('customer_address', "Business Address"),
                ('brief_customer_profile', "Please give a brief profile of the subject customer?"),
                ('nature_of_business', "What is the specific nature of the customers business/occupation/employment?"),
                ('kycc_info', "What types/nature of clientele/customers does this business provide services/goods to?"),
            ])

            # Company Registration and Legal Information
            add_section("Company Registration & Legal Information", [
                ('company_registration_number', "Company Registration Number (RC/BN)"),
                ('signatories_details', "Signatories' details (Names)"),
                ('beneficial_owner_details', "Beneficial Owner details (List the names of the Directors/Shareholders/Person of Significant Control)"),
            ])

            # Residency
            story.append(Paragraph("Citizenship Information", styles['SectionHeader']))
            residency_fields = [
            ('dual_citizenship_info', "Do any signatories/directors/shareholders hold dual citizenship or residence in other jurisdictions?"),
            ('citizenship_country_ids', "Citizenship Countries"),
            ]
            for field, question in residency_fields:
                story.append(Paragraph(question, styles['Question']))
                if field == 'dual_citizenship_info':
                    value = getattr(record, field, None)
                elif field == 'citizenship_country_ids':
                    value = ", ".join(record.applicable_country_ids.mapped('name')) if record.applicable_country_ids else 'N/A'
                else:
                    value = getattr(record, field, None)
                story.append(Paragraph(self.safe_str(value), styles['Answer']))
                
            # Financial Details
            story.append(Paragraph("Financial Details", styles['SectionHeader']))
            financial_fields = [
                ('high_risk_industries', "Does the customer have any direct or indirect link to high-risk industries (e.g. Crypto, Financial Services, DNFBPs)?"),
                ('inflow_purpose', "What would be the purpose of the inflows routed into the customer's account?"),
                ('inflow_document', "Provide supporting documents if applicable?"),
                ('outflow_purpose', "What would be the expected purpose of the outflows from the account?"),
                ('outflow_document', "Provide supporting documents if applicable?"),
            ]
            for field, question in financial_fields:
                story.append(Paragraph(question, styles['Question']))
                if field == 'high_risk_industries':
                    value = getattr(record, field, None)
                    if isinstance(value, bool):
                        formatted_value = format_yes_no_with_checkboxes(value, question)
                        story.append(Paragraph(formatted_value, styles['Answer']))
                    else:
                        story.append(Paragraph(self.safe_str(value), styles['Answer']))
                elif field in ['inflow_document', 'outflow_document']:
                    value = "Attach evidence" if getattr(record, field, False) else 'N/A'
                    story.append(Paragraph(value, styles['Answer']))
                else:
                    value = getattr(record, field, None) or 'N/A'
                    story.append(Paragraph(value, styles['Answer']))
                    
            # PEP Information
            story.append(Paragraph("PEP Information", styles['SectionHeader']))

        
            pep_association_value = getattr(record, 'pep_association', False)
            pep_association_question = "Is any Director/Shareholder/Signatory a Politically Exposed Person (PEP) or associated with a PEP?"
            story.append(Paragraph(pep_association_question, styles['Question']))
            story.append(Paragraph(format_yes_no_with_checkboxes(pep_association_value, pep_association_question), styles['Answer']))

            if pep_association_value:
                pep_details = getattr(record, 'pep_relationship_details', None)
                story.append(Paragraph("", styles['Question']))
                story.append(Paragraph(self.safe_str(pep_details), styles['Answer']))

            # Handle 'third_party_involvement' and its related details
            third_party_value = getattr(record, 'third_party_involvement', False)
            third_party_question = "Are there any third-party, proxies, nominees or legal reps managing the entity/account?"
            story.append(Paragraph(third_party_question, styles['Question']))
            story.append(Paragraph(format_yes_no_with_checkboxes(third_party_value, third_party_question), styles['Answer']))

            if third_party_value:
                third_party_details = getattr(record, 'third_party_details', None)
                story.append(Paragraph("", styles['Question']))
                story.append(Paragraph(self.safe_str(third_party_details), styles['Answer']))


        # Source of Funds/Wealth
        story.append(Paragraph("Source of Funds/Wealth", styles['SectionHeader']))
        source_fields = [
            ('expected_source_of_funds', "What is the expected source of funds into the customer's account?"),
            ('source_of_funds_document', "Kindly provide documentary evidence if available"),
            ('source_of_wealth', "What is the source of wealth of the customer?"),
            ('source_of_wealth_document', "Kindly provide documentary evidence if available"),
        ]
        for field, question in source_fields:
            story.append(Paragraph(question, styles['Question']))
            if field in ['expected_source_of_funds', 'source_of_wealth']:
                value = getattr(record, field, None) or 'N/A'
            else:
                value = "Attach evidence" if getattr(record, field, False) else 'N/A'
            story.append(Paragraph(self.safe_str(value), styles['Answer']))

        # Media Publicity
        story.append(Paragraph("Media Publicity", styles['SectionHeader']))

        # Get the value of 'has_negative_media'
        has_negative_media = getattr(record, 'has_negative_media', False)
        media_question = "Is there any negative news about the customer or the customer's activities?"
        story.append(Paragraph(media_question, styles['Question']))
        story.append(Paragraph(format_yes_no_with_checkboxes(has_negative_media, media_question), styles['Answer']))

        if has_negative_media:
            # Show details if negative media exists
            details = getattr(record, 'negative_media_details', None)
            story.append(Paragraph("Provide details", styles['Question']))
            story.append(Paragraph(self.safe_str(details), styles['Answer']))

            # Show document prompt
            story.append(Paragraph("Attach evidence", styles['Question']))
            story.append(Paragraph("Attach evidence", styles['Answer']))


        # Cross-Border Transactions
        story.append(Paragraph("Cross-Border Transactions", styles['SectionHeader']))

        # Fetch and show main yes/no question
        cross_border_value = getattr(record, 'cross_border_transaction', False)
        main_question = "Is the customer expected to engage in transactions involving cross-border fund transfers?"
        story.append(Paragraph(main_question, styles['Question']))
        story.append(Paragraph(format_yes_no_with_checkboxes(cross_border_value, main_question), styles['Answer']))

        # Conditionally show jurisdictions if cross-border transactions are expected
        if cross_border_value:
            jurisdictions = ", ".join(record.cross_border_jurisdictions.mapped('name')) if record.cross_border_jurisdictions else 'N/A'
            story.append(Paragraph("Specify jurisdictions involved", styles['Question']))
            story.append(Paragraph(self.safe_str(jurisdictions), styles['Answer']))


        # Additional Comments
        story.append(Paragraph("Additional Comments", styles['SectionHeader']))
        story.append(Paragraph("Any Other Comments?", styles['Question']))
        story.append(Paragraph(self.safe_str(record.other_comments), styles['Answer']))

        # Attestation
        story.append(Spacer(1, 24))
        story.append(Paragraph("CERTIFICATION", styles['SectionHeader']))
        story.append(Paragraph(
            "I hereby attest that the information provided above and in the enclosed documents is accurate to the best of my knowledge.",
            styles['Question']))
        story.append(Spacer(1, 24))
        
        # Signature table data
        signature_data = []

        # Approving Officer Column
        approving_officer = []
        # Signature Image/Line
        if hasattr(record, 'approving_officer_signature') and record.approving_officer_signature:
            try:
                file_data = base64.b64decode(record.approving_officer_signature)
                
                if file_data[:4] == b'%PDF':
                    # Try PyMuPDF first (most reliable)
                    img_buffer = self.extract_pdf_as_image_pymupdf(file_data)
                    
                    if img_buffer:
                        signature_img_ao = Image(img_buffer, width=3*inch, height=1.2*inch)
                        approving_officer.append(signature_img_ao)
                    else:
                        officer_name = getattr(record, 'approving_officer_name', 'Officer')
                        placeholder_text = f"[PDF Signature: {officer_name}]"
                        approving_officer.append(Paragraph(placeholder_text, styles['SignatureName']))
                else:
                    img_buffer = BytesIO(file_data)
                    signature_img_ao = Image(img_buffer, width=3*inch, height=1.2*inch)
                    approving_officer.append(signature_img_ao)
                    
            except Exception as e:
                print(f"Error processing approving officer signature: {e}")
                # Black signature line
                approving_officer.append(Paragraph("_____________________________", styles['SignatureName']))
        else:
            # Black signature line for empty signatures  
            approving_officer.append(Paragraph("_____________________________", styles['SignatureName']))

        approving_officer.append(Paragraph("APPROVING OFFICER SIGNATURE", styles['SignatureLabel']))
        approving_officer.append(Paragraph(getattr(record, 'approving_officer_name', '___________________'), 
                                        styles['SignatureName']))
        approving_officer.append(Paragraph(f"Date: {record.date_approved}", styles['SignatureDate']))

        # Responsible Officer Column
        responsible_officer = []
        # Signature Image/Line
        if hasattr(record, 'responsible_officer_signature') and record.responsible_officer_signature:
            try:
                file_data = base64.b64decode(record.responsible_officer_signature)
                
                if file_data[:4] == b'%PDF':
                    img_buffer = self.extract_pdf_as_image_pymupdf(file_data)
                    
                    if img_buffer:
                        signature_img_ro = Image(img_buffer, width=3*inch, height=1.2*inch)
                        responsible_officer.append(signature_img_ro)
                    else:
                        officer_name = getattr(record, 'responsible_officer_name', 'Officer')
                        placeholder_text = f"[PDF Signature: {officer_name}]"
                        responsible_officer.append(Paragraph(placeholder_text, styles['SignatureName']))
                else:
                    img_buffer = BytesIO(file_data)
                    signature_img_ro = Image(img_buffer, width=3*inch, height=1.2*inch)
                    responsible_officer.append(signature_img_ro)
                    
            except Exception as e:
                print(f"Error processing responsible officer signature: {e}")
                # Black signature line
                responsible_officer.append(Paragraph("_____________________________", styles['SignatureName']))
        else:
            # Black signature line for empty signatures
            responsible_officer.append(Paragraph("_____________________________", styles['SignatureName']))

        responsible_officer.append(Paragraph("RESPONSIBLE OFFICER SIGNATURE", styles['SignatureLabel']))
        responsible_officer.append(Paragraph(getattr(record, 'responsible_officer_name', '___________________'), 
                                        styles['SignatureName']))
        responsible_officer.append(Paragraph(f"Date: {record.date_reviewed}", styles['SignatureDate']))

        # Updated table
        signature_table = Table([[approving_officer, responsible_officer]], 
                            colWidths=[3.75*inch, 3.75*inch],
                            hAlign='CENTER')

        signature_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('TOPPADDING', (0, 0), (-1, -1), 0),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
        ]))

        story.append(signature_table)
        story.append(Spacer(1, 24))
        
        # Stamp/Seal section
        stamp_line = Table([[""]], colWidths=[7*inch], hAlign='CENTER')
        stamp_line.setStyle(TableStyle([
            ('LINEBELOW', (0, 0), (0, 0), 1, colors.grey),
            ('ALIGN', (0, 0), (0, 0), 'CENTER'),
            ('PADDING', (0, 0), (0, 0), 10),
        ]))
        
        story.append(stamp_line)
        story.append(Paragraph("Bank Compliance Stamp", 
                              ParagraphStyle(name='StampLabel', fontSize=9, textColor=colors.grey,
                                            alignment=1, spaceBefore=2)))
        # Build and return PDF
        doc.build(story)
        buffer.seek(0)
        filename = f"Due_Diligence_Report_{record.name.replace(' ', '_')}.pdf"
        return request.make_response(
            buffer.getvalue(),
            headers=[
                ('Content-Type', 'application/pdf'),
                ('Content-Disposition', content_disposition(filename)),
            ]
        )