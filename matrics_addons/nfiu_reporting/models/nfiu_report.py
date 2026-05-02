from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
from odoo.modules.module import get_module_resource
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta, time


THRESHOLD_AMT = 10000000
REPORTING_DAYS_PAST = 1095
REPORT_NAME_PREFIX = 'NFIU'
SCHEMA_NOT_FOUND_MESSAGE = _('Validation schema not found. Please upload a schema to be used for validation.')

class NFIUReport(models.Model):
    _name = 'nfiu.report'
    _description = 'Financial Intelligence Report'
    _order = 'submission_date desc'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Report Name', required=True)
    entity_id = fields.Many2one(comodel_name='nfiu.entity', string='Reporting Entity', required=True, tracking=True)
    rentity_id = fields.Integer(string='Reporting Entity ID', related='entity_id.identification_id',required=True,tracking=True)
    rentity_branch = fields.Char(string='Reporting Entity Branch',related='entity_id.branch_id.name',size=255,tracking=True)
    submission_code = fields.Selection([
        ('E', 'Electronic'),
        ('M', 'Manual')
    ], string='Submission Code', required=True, default='E',tracking=True)
    
    report_code = fields.Selection([
        ('AIF', 'Additional Information File'),
        ('CTR', 'Currency Transaction Report'),
        ('EFT', 'Electronic/Express Funds Transfer (EFT)'),
        ('IFT', 'International Funds Transfer'),
        ('LRFI', 'Local Request For Information'),
        ('M', 'Manual'),
        ('SAR', 'Suspicious Activity Report'),
        ('STR', 'Suspicious Transaction Report'),
        ('UTR', 'Unusual Transaction Report'),
    ], string='Report Code', required=True,tracking=True)
    
    report_type = fields.Selection([
        ('CTR', 'CTR - Currency Transaction Report'),
        ('FTR', 'FTR - Foreign Currency Transaction Report'),
        ('STR','STR - Suspicious Transaction Report'),
        ('UTR', 'UTR - Unusual Transaction Report'),
        ('AIF', 'Additional Information File'),
        ('EFT', 'Electronic/Express Funds Transfer (EFT)'),
        ('IFT', 'International Funds Transfer'),
        ('LRFI', 'Local Request For Information'),
        ('M', 'Manual'),
        ('SAR', 'Suspicious Activity Report'),
    ], string='Report Type', required=True, tracking=True, compute='_compute_report_type', store=True, precompute=True)
    
    entity_reference = fields.Char(string='Entity Reference',related='entity_id.entity_reference', size=255,tracking=True)
    fiu_ref_number = fields.Char(string='FIU Reference Number',related='entity_id.fiu_reference', size=255,tracking=True)
    submission_date = fields.Datetime(string='Submission Date', required=True, default=fields.Datetime.now)
    currency_code_local = fields.Selection([
        ('NGN', 'Naira'),
        ('USD', 'US Dollar'),
        ('EUR', 'Euro'),
        ('GBP', 'British Pound'),
        ('CAD', 'Canadian Dollars'),
        ('CNY', 'Chinese Yen'),
    ], string='Reporting Currency', required=True, default='NGN',tracking=True)
    
    reason = fields.Text(string='Reason',)
    action = fields.Text(string='Action')

    # Reporting person details
    reporting_person_id = fields.Many2one('nfiu.person', string='Reporting Person', required=True,tracking=True)
    location_id = fields.Many2one('nfiu.address', string='Location',tracking=True,required=True)
    transaction_cnt = fields.Integer(string='Transaction Count', compute='_compute_transaction_count', store=True)
    # Report indicators
    indicator_ids = fields.Many2many('nfiu.indicator', string='Report Indicators',required=True,tracking=True)
    
    # XML generation
    xml_content = fields.Text(string='Generated XML')
    xml_file = fields.Binary(string='XML File')
    xml_filename = fields.Char(string='XML Filename',tracking=True)
    
    state = fields.Selection([
        ('draft', 'Draft'),
        ('generated', 'XML Generated'),
        ('validated', 'Validated'),
        ('submitted', 'Submitted'),
        ('error', 'Error')
    ], string='State', default='draft',tracking=True)
    date_from = fields.Date(string='Period Start',
                            required=True, index=True, tracking=True)
    date_to = fields.Date(string='Period End',
                          required=True, index=True, tracking=True)
    validation_message = fields.Text(string='Validation Message',tracking=True)

    @api.model
    def create(self, vals):
        if not vals.get('name'):
            vals['name'] = self.env['ir.sequence'].next_by_code('nfiu.report') or '/'
        return super(NFIUReport, self).create(vals)
    
    @api.depends('report_code', 'currency_code_local', 'entity_id.local_currency.currency_id.name')
    def _compute_report_type(self):
        for report in self:
            if not report.report_code:
                report.report_type = False
            elif report.report_code == 'CTR':
                entity_currency_name = report.entity_id.local_currency.currency_id.name
                report.report_type = report.report_code
                if (
                    report.currency_code_local
                    and entity_currency_name
                    and report.currency_code_local.upper() != entity_currency_name.upper()
                ):
                    report.report_type = "FTR"
            else:
                report.report_type = report.report_code
                    

    @api.depends('currency_code_local', 'date_from', 'date_to')
    def _compute_transaction_count(self):
        transaction_model = self.env['res.customer.transaction']

        for report in self:
            report.transaction_cnt = 0
            if not report.currency_code_local or not report.date_from or not report.date_to:
                continue

            filters = report.get_report_filters()
            if not filters:
                continue

            report.transaction_cnt = transaction_model.search_count(
                filters
            )
    
    def set_draft(self):
        self.ensure_one()
        self.write({'state':'draft'})
        
    def set_generated(self):
        self.ensure_one()
        self.write({'state':'generated'})

    def action_download_report(self):
        self.ensure_one()
        if not self.xml_file:
            raise ValidationError(_("No XML file is available for download."))

        filename = self.xml_filename or f"{self.name or 'nfiu_report'}.xml"
        return {
            'type': 'ir.actions.act_url',
            'url': f'/web/content/{self._name}/{self.id}/xml_file/{filename}?download=true',
            'target': 'self',
        }

    def get_reporting_location(self):
        """Determine the reporting location based on the entity's addresses"""
        if self.location_id:
            # For simplicity, we take the first address as the reporting location
            return f"{self.location_id.address}, {self.location_id.town}, {self.location_id.city}"
        return None

    def _format_sql_datetime(self, value, field_label):
        if not value:
            raise ValidationError(_("%s is required for NFIU XML generation.") % field_label)
        return fields.Datetime.to_string(value).replace(' ', 'T')

    def _sanitize_text(self, value, max_length, field_label, required=False):
        text = '' if value is None else str(value).strip()
        if required and not text:
            raise ValidationError(_("%s is required for NFIU XML generation.") % field_label)
        return text[:max_length] if text else text

    def _add_text_element(self, parent, tag, value, max_length, field_label, required=False):
        text = self._sanitize_text(value, max_length, field_label, required=required)
        if required or text:
            ET.SubElement(parent, tag).text = text
        return text

    def _build_report_name(self):
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"{self.entity_id.commercial_name.strip().replace(' ', '_').upper()}_{self.report_type}_{self.name.strip().replace(' ', '_').upper()}_{timestamp}"

    def _get_transaction_date_domain(self):
        self.ensure_one()
        if not self.date_from or not self.date_to:
            return []

        start_dt = datetime.combine(self.date_from, time.min)
        end_dt = datetime.combine(self.date_to, time.max)
        return [
            ('date_created', '>=', fields.Datetime.to_string(start_dt)),
            ('date_created', '<=', fields.Datetime.to_string(end_dt)),
        ]

    def generate_xml(self):
        """Generate XML content according to goAML schema"""
        self.ensure_one()
        
        # Create root element
        root = ET.Element('report')
        
        # Add basic report elements
        ET.SubElement(root, 'rentity_id').text = str(self.rentity_id)
        if self.rentity_branch:
            ET.SubElement(root, 'rentity_branch').text = self.rentity_branch
        ET.SubElement(root, 'submission_code').text = self.submission_code
        ET.SubElement(root, 'report_code').text = self.report_code
        
        if self.entity_reference:
            ET.SubElement(root, 'entity_reference').text = self.entity_reference
        if self.fiu_ref_number:
            ET.SubElement(root, 'fiu_ref_number').text = self.fiu_ref_number
            
        ET.SubElement(root, 'submission_date').text = self.submission_date.strftime('%Y-%m-%dT%H:%M:%S')
        ET.SubElement(root, 'currency_code_local').text = self.currency_code_local
        
        # Add reporting person
        reporting_person = ET.SubElement(root, 'reporting_person')
        self._add_reporting_person_xml(reporting_person, self.reporting_person_id)
        
        # Add location if present
        if self.location_id:
            location = ET.SubElement(root, 'location')
            self._add_address_xml(location, self.location_id)
        
        # Add reason and action
        if self.reason:
            ET.SubElement(root, 'reason').text = self.reason
        if self.action:
            ET.SubElement(root, 'action').text = self.action
        filters = self.get_report_filters()
        transactions = self.env['res.customer.transaction'].search(filters)
        self.transaction_cnt = len(transactions)
        for transaction in transactions:
            trans_elem = ET.SubElement(root, 'transaction')
            self._add_transaction_xml(trans_elem, transaction)
        
        # Add report indicators
        if self.indicator_ids:
            indicators = ET.SubElement(root, 'report_indicators')
            for indicator in self.indicator_ids:
                ET.SubElement(indicators, 'indicator').text = indicator.code
        
        # Convert to string with pretty formatting
        ET.indent(root, space="  ")
        xml_string = ET.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')
        
        # Update record
        self.xml_content = xml_string
        self.xml_file = base64.b64encode(xml_string.encode('utf-8'))
        self.xml_filename = self._build_report_name() + '.xml'
        self.state = 'generated'
        
        return True

    def get_report_filters(self):
        """
        Build the transaction search domain for this report.

        The base domain always includes the selected date range and currency.
        CTR/FTR add threshold rules for signed amounts:
        - debit transactions must be below -threshold_amount
        - credit transactions must be above threshold_amount
        STR adds the suspicious transaction flags and does not use threshold rules.
        All other report types use only the base date-range and currency filters.
        """
        self.ensure_one()

        currency = self.env['res.currency'].search([('name', '=', self.currency_code_local)], limit=1)
        if not currency:
            return []
        filters = self._get_transaction_date_domain() + [('currency_id', '=', currency.id)]
        threshold = self.env['nfiu.currency.threshold'].search([('currency_id', '=', currency.id)], limit=1)
        threshold_amount = threshold.threshold if threshold else THRESHOLD_AMT
        report_kind = self.report_type
        if report_kind in ('CTR', 'FTR'):
            filters += [
                '|',
                '&',
                ('transaction_type', '=', 'D'),
                ('amount', '<', threshold_amount * -1),
                '&',
                ('transaction_type', '=', 'C'),
                ('amount', '>', threshold_amount),
            ]
        elif report_kind == 'STR':
            filters += [
                ('report_nfiu', '=', True),
                ('suspicious_transaction', '=', True),
            ]
        return filters
# Additional methods for automatic report generation
    def action_create_from_transactions(self):
        """Create NFIU report from existing res.customer.transaction transactions"""
        # This method can be used to automatically create reports from existing Odoo transactions
        Transaction = self.env['res.customer.transaction']
        
        # Define criteria for suspicious transactions
        threshold_amount = 25  # 10 million NGN threshold
        
        # Find transactions above threshold
        suspicious_transactions = Transaction.search([
            ('amount', '>', threshold_amount),
            'currency_id','=',121,
            ('date_created', '>=', fields.Date.today() - timedelta(days=REPORTING_DAYS_PAST)),
        ])
        
        # Create NFIU transactions from these
        for t in suspicious_transactions:
            t.report_fiu()
            '''
            self.env['nfiu.transaction'].create({
                'report_id': self.id,
                'transaction_number': trans.name,
                'description': trans.name,
                'date_transaction': trans.date_created,
                'amount_local': trans.amount,
                'transaction_location': 'Head Office',  # Default location
                'transmode_code': 'E',  # Electronic transfer
            })
            '''
        
        
        return True

    @api.model
    def schedule_automatic_reports(self):
        """Schedule automatic generation of threshold reports"""
        # This method can be called by a cron job
        
        # Find all transactions above threshold in the last day
        since = fields.Date.today() - timedelta(days=1000)
        
        Transaction = self.env['res.customer.transaction']
        threshold_transactions = Transaction.search([
            ('amount', '>', THRESHOLD_AMT),  # 10M NGN threshold
            ('date_created', '=', since),
        ])
        
        if threshold_transactions:
            # Create automatic CTR report
            report = self.create({
                'name': f'Auto CTR {since}',
                'report_code': 'CTR',
                'submission_code': 'E',
                'rentity_id': 1,  # Your entity ID
                'submission_date': fields.Datetime.now(),
                'currency_code_local': 'NGN',
                'reporting_person_id': 1,  # Default reporting person
                'reason': 'Automatic threshold report generation',
            })
            
            # Add transactions to report
            for trans in threshold_transactions:
                pass
                '''
                self.env['nfiu.transaction'].create({
                    'report_id': report.id,
                    'transaction_number': trans.name,
                    'description': f'Threshold transaction: {trans.name}',
                    'date_transaction': trans.date_created,
                    'amount_local': trans.amount,
                    'transaction_location': 'System Generated',
                    'transmode_code': 'E',
                })
                '''
            
            # Add threshold indicator
            threshold_indicator = self.env.ref('nfiu_reporting.indicator_threshold')
            report.indicator_ids = [(4, threshold_indicator.id)]
            
            # Auto-generate and validate XML
            report.generate_xml()
            report.validate_xml()
        
        return True

    def _add_reporting_person_xml(self, parent, person):
        """Add person XML elements"""
        if not person:
            return
        # Add basic person details   
        reporting_gender = person.gender or getattr(self.entity_id, 'gender', False) or '-'
        ET.SubElement(parent, 'gender').text = self._sanitize_text(reporting_gender, 1, 'Reporting person gender')
        if person.title:
            self._add_text_element(parent, 'title', person.title, 30, 'Reporting person title')
        self._add_text_element(parent, 'first_name', person.first_name, 100, 'Reporting person first name', required=True)
        if person.middle_name:
            self._add_text_element(parent, 'middle_name', person.middle_name, 100, 'Reporting person middle name')
        # Add prefix if available
        if person.prefix:
            self._add_text_element(parent, 'prefix', person.prefix, 100, 'Reporting person prefix')
        self._add_text_element(parent, 'last_name', person.last_name, 100, 'Reporting person last name', required=True)
        
        if person.birthdate:
            ET.SubElement(parent, 'birthdate').text = self._format_sql_datetime(person.birthdate, 'Reporting person birthdate')
        # Add birth place if available
        if person.birth_place:
            self._add_text_element(parent, 'birth_place', person.birth_place, 255, 'Reporting person birth place')
        # Add mother's name if available
        if person.mothers_name:
            self._add_text_element(parent, 'mothers_name', person.mothers_name, 100, "Reporting person mother's name")
        # Add alias and SSN if available
        if person.alias:
            self._add_text_element(parent, 'alias', person.alias, 100, 'Reporting person alias')
        # Add SSN if available
        if person.ssn:
            self._add_text_element(parent, 'ssn', person.ssn, 25, 'Reporting person SSN')
            
        # Add passport info
        if person.passport_number:
            self._add_text_element(parent, 'passport_number', person.passport_number, 255, 'Reporting person passport number', required=True)
            if person.passport_country:
                self._add_text_element(parent, 'passport_country', person.passport_country, 2, 'Reporting person passport country')
        
        if person.id_number:
            self._add_text_element(parent, 'id_number', person.id_number, 255, 'Reporting person ID number')
            
        # Add nationalities and residence
        if person.nationality1:
            self._add_text_element(parent, 'nationality1', person.nationality1, 2, 'Reporting person nationality 1')
        if person.nationality2:
            self._add_text_element(parent, 'nationality2', person.nationality2, 2, 'Reporting person nationality 2')
        if person.nationality3:
            self._add_text_element(parent, 'nationality3', person.nationality3, 2, 'Reporting person nationality 3')
        if person.residence:
            self._add_text_element(parent, 'residence', person.residence, 2, 'Reporting person residence')
            
        # Add occupation and employer
        if person.occupation:
            self._add_text_element(parent, 'occupation', person.occupation, 255, 'Reporting person occupation')
        if person.employer_name:
            self._add_text_element(parent, 'employer_name', person.employer_name, 255, 'Reporting person employer name')
            
        # Add source of wealth
        if person.source_of_wealth:
            self._add_text_element(parent, 'source_of_wealth', person.source_of_wealth, 255, 'Reporting person source of wealth')

    def _add_person_xml(self, parent, person):
        """Add person XML elements"""
        if not person:
            return
        # Add basic person details   
        ET.SubElement(parent, 'gender').text = self._sanitize_text(person.gender or '-', 1, 'Transaction person gender')
        if person.title:
            self._add_text_element(parent, 'title', person.title, 30, 'Transaction person title')
        self._add_text_element(parent, 'first_name', person.firstname, 100, 'Transaction person first name', required=True)
        if person.middlename:
            self._add_text_element(parent, 'middle_name', person.middlename, 100, 'Transaction person middle name')
        # Add prefix if available
        if person.prefix:
            self._add_text_element(parent, 'prefix', person.prefix, 100, 'Transaction person prefix')
        self._add_text_element(parent, 'last_name', person.lastname, 100, 'Transaction person last name', required=True)
        
        if person.dob:
            ET.SubElement(parent, 'birthdate').text = self._format_sql_datetime(person.dob, 'Transaction person birthdate')
        # Add birth place if available
        if person.birth_place:
            self._add_text_element(parent, 'birth_place', person.birth_place, 255, 'Transaction person birth place')
        # Add mother's name if available
        if person.mothers_name:
            self._add_text_element(parent, 'mothers_name', person.mothers_name, 100, "Transaction person mother's name")
        # Add alias and SSN if available
        if person.alias:
            self._add_text_element(parent, 'alias', person.alias, 100, 'Transaction person alias')
        # Add SSN if available
        if person.ssn:
            self._add_text_element(parent, 'ssn', person.ssn, 25, 'Transaction person SSN')
            
        # Add passport info
        if person.passport_number:
            self._add_text_element(parent, 'passport_number', person.passport_number, 255, 'Transaction person passport number', required=True)
            if person.passport_country:
                self._add_text_element(parent, 'passport_country', person.passport_country, 2, 'Transaction person passport country')
        
        if person.id_number:
            self._add_text_element(parent, 'id_number', person.id_number, 255, 'Transaction person ID number')
            
        # Add nationalities and residence
        if person.nationality1:
            self._add_text_element(parent, 'nationality1', person.nationality1, 2, 'Transaction person nationality 1')
        if person.nationality2:
            self._add_text_element(parent, 'nationality2', person.nationality2, 2, 'Transaction person nationality 2')
        if person.nationality3:
            self._add_text_element(parent, 'nationality3', person.nationality3, 2, 'Transaction person nationality 3')
        if person.residence:
            self._add_text_element(parent, 'residence', person.residence, 2, 'Transaction person residence')
            
        # Add occupation and employer
        if person.occupation:
            self._add_text_element(parent, 'occupation', person.occupation, 255, 'Transaction person occupation')
        if person.employer_name:
            self._add_text_element(parent, 'employer_name', person.employer_name, 255, 'Transaction person employer name')
            
        # Add source of wealth
        if person.source_of_wealth:
            self._add_text_element(parent, 'source_of_wealth', person.source_of_wealth, 255, 'Transaction person source of wealth')

    def _add_address_xml(self, parent, address):
        """Add address XML elements"""
        if not address:
            return
            
        ET.SubElement(parent, 'address_type').text = self._sanitize_text(address.address_type or 'P', 1, 'Address type', required=True)
        self._add_text_element(parent, 'address', address.address, 100, 'Address', required=True)
        if address.town:
            self._add_text_element(parent, 'town', address.town, 255, 'Town')
        self._add_text_element(parent, 'city', address.city, 255, 'City', required=True)
        if address.zip:
            self._add_text_element(parent, 'zip', address.zip, 10, 'ZIP')
        self._add_text_element(parent, 'country_code', address.country_code or 'NG', 2, 'Country code', required=True)
        if address.state:
            self._add_text_element(parent, 'state', address.state, 255, 'State')

    def _add_transaction_xml(self, parent, transaction):
        """Add transaction XML elements"""
        transaction_number = (
            transaction.transaction_number
            or transaction.name
            or transaction.internal_ref_number
            or str(transaction.id)
        )
        transaction_number = self._sanitize_text(transaction_number, 50, 'Transaction number', required=True)
        ET.SubElement(parent, 'transactionnumber').text = transaction_number
        if transaction.internal_ref_number:
            self._add_text_element(parent, 'internal_ref_number', transaction.internal_ref_number, 50, 'Internal reference number')
        self._add_text_element(parent, 'transaction_location', transaction.transaction_location or self.get_reporting_location(), 255, 'Transaction location', required=True)
        self._add_text_element(parent, 'transaction_description', transaction.narration or '', 4000, 'Transaction description', required=True)
        ET.SubElement(parent, 'date_transaction').text = self._format_sql_datetime(transaction.date_created, 'Transaction date')
        
        if transaction.teller:
            self._add_text_element(parent, 'teller', transaction.teller, 50, 'Teller')
        if transaction.authorized:
            self._add_text_element(parent, 'authorized', transaction.authorized, 50, 'Authorized by')
            
        if transaction.value_date:
            ET.SubElement(parent, 'value_date').text = self._format_sql_datetime(transaction.value_date, 'Value date')
            
        ET.SubElement(parent, 'transmode_code').text = self._sanitize_text(transaction.transmode_code or 'A', 2, 'Transaction mode code', required=True)
        if transaction.transmode_comment:
            self._add_text_element(parent, 'transmode_comment', transaction.transmode_comment, 50, 'Transaction mode comment')
            
        amount_local = transaction.amount_local if transaction.amount_local is not None else transaction.amount
        if amount_local is None:
            raise ValidationError(_("Transaction amount is required for NFIU XML generation."))
        ET.SubElement(parent, 'amount_local').text = str(abs(amount_local))
        # Add parties (from/to), we need to resolve the customer to person or entity
        from_person = transaction.customer_id
        to_person = transaction.customer_id
        if transaction.transaction_type == 'C':
            from_person = transaction.customer_id
            to_person = transaction.customer_id
        else:
            from_person = transaction.customer_id
            to_person = transaction.customer_id
        
        if from_person:
            from_elem = ET.SubElement(parent, 't_from')
            ET.SubElement(from_elem, 'from_funds_code').text = self._sanitize_text(transaction.from_funds_code or 'A', 2, 'From funds code', required=True)
            if transaction.from_funds_comment:
                self._add_text_element(from_elem, 'from_funds_comment', transaction.from_funds_comment, 255, 'From funds comment')
            
            from_person = ET.SubElement(from_elem, 'from_person')
            self._add_person_xml(from_person, transaction.customer_id)
            self._add_text_element(from_elem, 'from_country', transaction.from_country or 'NG', 2, 'From country', required=True)
            
        if to_person:
            to_elem = ET.SubElement(parent, 't_to')
            ET.SubElement(to_elem, 'to_funds_code').text = self._sanitize_text(transaction.to_funds_code or 'A', 2, 'To funds code', required=True)
            if transaction.to_funds_comment:
                self._add_text_element(to_elem, 'to_funds_comment', transaction.to_funds_comment, 255, 'To funds comment')
                
            to_person = ET.SubElement(to_elem, 'to_person')
            self._add_person_xml(to_person, transaction.customer_id)
            self._add_text_element(to_elem, 'to_country', transaction.to_country or 'NG', 2, 'To country', required=True)

    def validate_xml(self):
        """Validate generated XML against XSD schema"""
        self.ensure_one()
        
        if not self.xml_content:
            raise ValidationError("Please generate XML first")
        
        try:
            # Get XSD schema content (assuming it's stored as attachment or in data)
            xsd_content = self._get_xsd_schema()
            
            # Parse XSD schema
            xsd_doc = etree.fromstring(xsd_content)
            xsd_schema = etree.XMLSchema(xsd_doc)
            
            # Parse XML content
            xml_doc = etree.fromstring(self.xml_content.encode('utf-8'))
            
            # Validate
            if xsd_schema.validate(xml_doc):
                self.state = 'validated'
                self.validation_message = "XML is valid according to goAML schema"
            else:
                self.state = 'error'
                errors = []
                for error in xsd_schema.error_log:
                    errors.append(f"Line {error.line}: {error.message}")
                self.validation_message = "\n".join(errors)
                
        except ValidationError as e:
            self.state = 'error'
            self.validation_message = str(e)
            raise
        except Exception as e:
            self.state = 'error'
            self.validation_message = f"Validation error: {str(e)}"
            
        return True

    def _get_xsd_schema(self):
        """Get XSD schema content"""
        params = self.env['ir.config_parameter'].sudo()

        module_xsd_path = params.get_param('nfiu_reporting.xsd_module_path')
        if module_xsd_path:
            path_parts = [part for part in module_xsd_path.split('/') if part]
            schema_file_path = get_module_resource('nfiu_reporting', *path_parts)
            if schema_file_path:
                with open(schema_file_path, 'rb') as schema_file:
                    return schema_file.read()

        # Odoo 16 catches FileNotFoundError in _file_read and returns b'' when a
        # filestore file is missing, so we check for falsy datas rather than catching
        # FileNotFoundError here — just skip to the next source if data is absent.
        attachment_id = params.get_param('nfiu_reporting.xsd_attachment_id')
        if attachment_id:
            attachment = self.env['ir.attachment'].sudo().browse(int(attachment_id)).exists()
            if attachment and attachment.datas:
                return base64.b64decode(attachment.datas)

        # Backward-compatible fallback for databases that still use the legacy attachment xmlid.
        attachment = self.env.ref('nfiu_reporting.nfiu_schema_attachment', raise_if_not_found=False)
        if attachment and attachment.datas:
            return base64.b64decode(attachment.datas)

        # Always try the bundled module XSD as a last resort.
        bundled_path = get_module_resource('nfiu_reporting', 'data', 'NFIU_goAML_4_5_Schema.xsd')
        if bundled_path:
            with open(bundled_path, 'rb') as f:
                return f.read()

        raise ValidationError(SCHEMA_NOT_FOUND_MESSAGE)

    def action_submit(self):
        """Submit the report (placeholder for actual submission logic)"""
        self.ensure_one()
        
        if self.state != 'validated':
            raise ValidationError("Report must be validated before submission")
            
        # Here you would implement the actual submission logic
        # This could involve API calls, file uploads, etc.
        
        self.state = 'submitted'
        return True
