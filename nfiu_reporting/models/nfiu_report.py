from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


THRESHOLD_AMT = 10000000
REPORTING_DAYS_PAST = 1095

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
        ('EFT', 'Electronic/Express Funds Transfer'),
        ('IFT', 'International Funds Transfer'),
        ('LRFI', 'Local Request For Information'),
        ('M', 'Manual'),
        ('SAR', 'Suspicious Activity Report'),
        ('STR', 'Suspicious Transaction Report'),
        ('UTR', 'Unusual Transaction Report'),
    ], string='Report Code', required=True,tracking=True)
    
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
    location_id = fields.Many2one('nfiu.address', string='Location',tracking=True)
    
    # Related transactions
    #transaction_ids = fields.One2many('nfiu.transaction', 'report_id', string='Transactions')
    
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
    
    def set_draft(self):
        self.ensure_one()
        self.write({'state':'draft'})
        
    def set_generated(self):
        self.ensure_one()
        self.write({'state':'generated'})

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
        currencies= self.env['res.currency'].search([('name', '=', self.currency_code_local)], limit=1)
        for c in currencies:
            currency_id = c.id
        # Add transactions
        
        transactions = self.env['res.customer.transaction'].search([('currency_id', '=', currency_id), ('report_nfiu', '=', True), ('date_created', '>=', self.date_from), ('date_created', '<=', self.date_to)])
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
        self.xml_filename = f"FIU_Report_{self.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
        self.state = 'generated'
        
        return True


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
        ET.SubElement(parent, 'gender').text = person.gender or '-'
        if person.title:
            ET.SubElement(parent, 'title').text = person.title or ''
        ET.SubElement(parent, 'first_name').text = person.first_name or ''
        if person.middle_name:
            ET.SubElement(parent, 'middle_name').text = person.middle_name
        # Add prefix if available
        if person.prefix:
            ET.SubElement(parent, 'prefix').text = person.prefix
        ET.SubElement(parent, 'last_name').text = person.last_name
        
        if person.birthdate:
            ET.SubElement(parent, 'birthdate').text = person.birthdate.strftime('%Y-%m-%dT%H:%M:%S')
        # Add birth place if available
        if person.birth_place:
            ET.SubElement(parent, 'birth_place').text = person.birth_place
        # Add mother's name if available
        if person.mothers_name:
            ET.SubElement(parent, 'mothers_name').text = person.mothers_name
        # Add alias and SSN if available
        if person.alias:
            ET.SubElement(parent, 'alias').text = person.alias
        # Add SSN if available
        if person.ssn:
            ET.SubElement(parent, 'ssn').text = person.ssn
            
        # Add passport info
        if person.passport_number:
            ET.SubElement(parent, 'passport_number').text = person.passport_number
            if person.passport_country:
                ET.SubElement(parent, 'passport_country').text = person.passport_country
        
        if person.id_number:
            ET.SubElement(parent, 'id_number').text = person.id_number
            
        # Add nationalities and residence
        if person.nationality1:
            ET.SubElement(parent, 'nationality1').text = person.nationality1
        if person.nationality2:
            ET.SubElement(parent, 'nationality2').text = person.nationality2
        if person.nationality3:
            ET.SubElement(parent, 'nationality3').text = person.nationality3
        if person.residence:
            ET.SubElement(parent, 'residence').text = person.residence
            
        # Add occupation and employer
        if person.occupation:
            ET.SubElement(parent, 'occupation').text = person.occupation
        if person.employer_name:
            ET.SubElement(parent, 'employer_name').text = person.employer_name
            
        # Add source of wealth
        if person.source_of_wealth:
            ET.SubElement(parent, 'source_of_wealth').text = person.source_of_wealth

    def _add_person_xml(self, parent, person):
        """Add person XML elements"""
        if not person:
            return
        # Add basic person details   
        ET.SubElement(parent, 'gender').text = person.gender or '-'
        if person.title:
            ET.SubElement(parent, 'title').text = person.title or ''
        ET.SubElement(parent, 'first_name').text = person.firstname or ''
        if person.middlename:
            ET.SubElement(parent, 'middle_name').text = person.middlename
        # Add prefix if available
        if person.prefix:
            ET.SubElement(parent, 'prefix').text = person.prefix
        ET.SubElement(parent, 'last_name').text = person.lastname
        
        if person.dob:
            ET.SubElement(parent, 'birthdate').text = person.dob.strftime('%Y-%m-%dT%H:%M:%S')
        # Add birth place if available
        if person.birth_place:
            ET.SubElement(parent, 'birth_place').text = person.birth_place
        # Add mother's name if available
        if person.mothers_name:
            ET.SubElement(parent, 'mothers_name').text = person.mothers_name
        # Add alias and SSN if available
        if person.alias:
            ET.SubElement(parent, 'alias').text = person.alias
        # Add SSN if available
        if person.ssn:
            ET.SubElement(parent, 'ssn').text = person.ssn
            
        # Add passport info
        if person.passport_number:
            ET.SubElement(parent, 'passport_number').text = person.passport_number
            if person.passport_country:
                ET.SubElement(parent, 'passport_country').text = person.passport_country
        
        if person.id_number:
            ET.SubElement(parent, 'id_number').text = person.id_number
            
        # Add nationalities and residence
        if person.nationality1:
            ET.SubElement(parent, 'nationality1').text = person.nationality1
        if person.nationality2:
            ET.SubElement(parent, 'nationality2').text = person.nationality2
        if person.nationality3:
            ET.SubElement(parent, 'nationality3').text = person.nationality3
        if person.residence:
            ET.SubElement(parent, 'residence').text = person.residence
            
        # Add occupation and employer
        if person.occupation:
            ET.SubElement(parent, 'occupation').text = person.occupation
        if person.employer_name:
            ET.SubElement(parent, 'employer_name').text = person.employer_name
            
        # Add source of wealth
        if person.source_of_wealth:
            ET.SubElement(parent, 'source_of_wealth').text = person.source_of_wealth

    def _add_address_xml(self, parent, address):
        """Add address XML elements"""
        if not address:
            return
            
        ET.SubElement(parent, 'address_type').text = address.address_type or 'P'
        ET.SubElement(parent, 'address').text = address.address or ''
        if address.town:
            ET.SubElement(parent, 'town').text = address.town
        ET.SubElement(parent, 'city').text = address.city or ''
        if address.zip:
            ET.SubElement(parent, 'zip').text = address.zip
        ET.SubElement(parent, 'country_code').text = address.country_code or 'NG'
        if address.state:
            ET.SubElement(parent, 'state').text = address.state

    def _add_transaction_xml(self, parent, transaction):
        """Add transaction XML elements"""
        ET.SubElement(parent, 'transactionnumber').text = transaction.transaction_number
        if transaction.internal_ref_number:
            ET.SubElement(parent, 'internal_ref_number').text = transaction.internal_ref_number
        ET.SubElement(parent, 'transaction_location').text = transaction.transaction_location or ''
        ET.SubElement(parent, 'transaction_description').text = transaction.narration or ''
        ET.SubElement(parent, 'date_transaction').text = transaction.date_created.strftime('%Y-%m-%dT%H:%M:%S')
        
        if transaction.teller:
            ET.SubElement(parent, 'teller').text = transaction.teller
        if transaction.authorized:
            ET.SubElement(parent, 'authorized').text = transaction.authorized
            
        if transaction.value_date:
            ET.SubElement(parent, 'value_date').text = transaction.value_date.strftime('%Y-%m-%dT%H:%M:%S')
            
        ET.SubElement(parent, 'transmode_code').text = transaction.transmode_code or 'A'
        if transaction.transmode_comment:
            ET.SubElement(parent, 'transmode_comment').text = transaction.transmode_comment
            
        ET.SubElement(parent, 'amount_local').text = str(abs(transaction.amount_local))
        default_entity = self.env.ref('nfiu_reporting.nfiu_entity_1')
        # Add parties (from/to), we need to resolve the customer to person or entity
        if transaction.transaction_type == 'C':
            transaction.from_person_id = transaction.customer_id
            transaction.to_person_id = transaction.customer_id
            transaction.to_entity_id = default_entity      
        else:
            transaction.to_person_id = transaction.customer_id
            transaction.from_person_id = transaction.customer_id
            transaction.to_entity_id = default_entity
        
        if transaction.from_person_id:
            from_elem = ET.SubElement(parent, 't_from')
            ET.SubElement(from_elem, 'from_funds_code').text = transaction.from_funds_code or 'A'
            if transaction.from_funds_comment:
                ET.SubElement(from_elem, 'from_funds_comment').text = transaction.from_funds_comment
            
            from_person = ET.SubElement(from_elem, 'from_person')
            self._add_person_xml(from_person, transaction.from_person_id)
            ET.SubElement(from_elem, 'from_country').text = transaction.from_country or 'NG'
            
        if transaction.to_person_id:
            to_elem = ET.SubElement(parent, 't_to')
            ET.SubElement(to_elem, 'to_funds_code').text = transaction.to_funds_code or 'A'
            if transaction.to_funds_comment:
                ET.SubElement(to_elem, 'to_funds_comment').text = transaction.to_funds_comment
                
            to_person = ET.SubElement(to_elem, 'to_person')
            self._add_person_xml(to_person, transaction.to_person_id)
            ET.SubElement(to_elem, 'to_country').text = transaction.to_country or 'NG'

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
                
        except Exception as e:
            self.state = 'error'
            self.validation_message = f"Validation error: {str(e)}"
            
        return True

    def _get_xsd_schema(self):
        """Get XSD schema content"""
        # Look for the XSD schema file in the module's data folder
        schema_path = self.env.ref('nfiu_reporting.nfiu_schema_attachment').datas
        if schema_path:
            return base64.b64decode(schema_path)

    def action_submit(self):
        """Submit the report (placeholder for actual submission logic)"""
        self.ensure_one()
        
        if self.state != 'validated':
            raise ValidationError("Report must be validated before submission")
            
        # Here you would implement the actual submission logic
        # This could involve API calls, file uploads, etc.
        
        self.state = 'submitted'
        return True


