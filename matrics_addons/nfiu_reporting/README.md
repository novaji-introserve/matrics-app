# NFIU goAML Reporting Module for Odoo

This module provides functionality to generate XML reports compliant with the Nigerian Financial Intelligence Unit (NFIU) goAML schema for financial intelligence reporting.

## Features

- **Transaction Management**: Create and manage financial transactions that need to be reported to NFIU
- **Person & Entity Management**: Maintain records of persons and entities involved in transactions
- **Report Generation**: Generate various types of reports (STR, CTR, UTR, etc.)
- **XML Generation**: Automatically format data into goAML-compliant XML
- **Schema Validation**: Validate generated XML against the official NFIU XSD schema
- **Automatic Reporting**: Set up automatic threshold-based report generation

## Installation

1. Copy this module to your Odoo addons directory
2. Install the module through Odoo Apps interface
3. Upload the NFIU goAML XSD schema file:
   - Go to Settings > Technical > Attachments
   - Create a new attachment with external ID 'nfiu_reporting.nfiu_schema_attachment'
   - Upload the 'NFIU_goAML_4_5_Schema_25122020.xsd' file

## Configuration

1. **Set up Reporting Entity**:
   - Configure your organization's NFIU entity ID
   - Set up the default reporting person
   - Configure addresses and contact information

2. **Configure Report Indicators**:
   - Review and customize the predefined report indicators
   - Add institution-specific indicators as needed

3. **Set up Automatic Reporting** (Optional):
   - Configure threshold amounts for automatic CTR generation
   - Set up cron jobs to run `schedule_automatic_reports()` method

## Usage

### Manual Report Creation

1. Go to **NFIU Reporting > Reports**
2. Click **Create** to create a new report
3. Fill in the report details:
   - Select report type (STR, CTR, UTR, etc.)
   - Add reporting person information
   - Specify reason and action taken
4. Add transactions to the report
5. Select appropriate report indicators
6. Click **Generate XML** to create the XML content
7. Click **Validate XML** to validate against schema
8. Click **Submit Report** when ready

### Automatic Report Generation

The module can automatically generate CTR (Currency Transaction Reports) for transactions exceeding the threshold amount:

```python
# Run this method via cron job or manually
self.env['nfiu.report'].schedule_automatic_reports()
```

### Export Multiple Reports

1. Go to **NFIU Reporting > Export Reports**
2. Select date range or specific reports
3. Choose report type filter
4. Click **Export to XML**

## Technical Details

### Models

- `nfiu.report`: Main report model
- `nfiu.transaction`: Individual transactions within a report
- `nfiu.person`: Person entities involved in transactions
- `nfiu.entity`: Business entities
- `nfiu.address`: Address information
- `nfiu.account`: Bank account information
- `nfiu.indicator`: Report indicators/flags

### XML Generation

The module generates XML that complies with the goAML 4.5 schema. Key features:

- Proper XML structure according to NFIU requirements
- Support for all major goAML elements
- Validation against XSD schema
- UTF-8 encoding with XML declaration

### Integration with Accounting

The module can integrate with Odoo's accounting module to automatically detect suspicious transactions:

- Monitor account.move.line for threshold amounts
- Auto-generate reports for qualifying transactions
- Link to existing customer and vendor records

## Customization

### Adding Custom Indicators

```python
# Create custom indicators
self.env['nfiu.indicator'].create({
    'name': 'Custom Suspicious Pattern',
    'code': 'CUSTOM1',
    'category': 'MA',
    'description': 'Institution-specific suspicious pattern'
})
```

### Custom Transaction Detection

Extend the `schedule_automatic_reports()` method to add custom logic for detecting suspicious transactions.

## Compliance Notes

- Ensure all required fields are filled before XML generation
- Validate XML against schema before submission
- Keep audit trail of all generated reports
- Follow NFIU guidelines for reporting thresholds and timeframes

## Troubleshooting

### XML Validation Errors

1. Check that all required fields are filled
2. Verify date formats (YYYY-MM-DDTHH:MM:SS)
3. Ensure numeric fields contain valid numbers
4. Check that country codes are valid 2-letter ISO codes

### Performance Considerations

- For large datasets, consider batch processing
- Archive old reports periodically
- Index frequently searched fields
- Monitor database size for transaction tables

## Support

For technical support or customization requests, contact your Odoo implementation partner.

## License

This module is provided under the same license as Odoo Community Edition.
"""
