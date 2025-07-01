# Input and output file paths
import xml.etree.ElementTree as ET
import re
import logging
import os


logger = logging.getLogger(__name__)

# Input and output file paths
input_file = '/home/novaji/odoo/icomply_odoo/compliance_management/data/demo_data/risk_assessment/product_service_assessment.xml'
output_file = 'risk_assessment_product_plans.xml'

# Function to convert customer type name to code
def name_to_code(name):
    clean_name = re.sub(r'[^\w\s]', '', name)
    clean_name = re.sub(r'\s+', '_', clean_name.strip())
    return clean_name.upper()

# Function to create narration from description
def create_narration(name, description):
    return f"Risk assessment for {name.lower()} clients. {description}"

# Function to dump XML structure for debugging
def dump_xml_structure(element, level=0, max_depth=5):
    if level > max_depth:
        return
    indent = "  " * level
    text = element.text.strip() if element.text else ''
    logger.debug(f"{indent}Tag: {element.tag}, Attributes: {element.attrib}, Text: {text}")
    for child in element:
        dump_xml_structure(child, level + 1, max_depth)

# Function to check file content
def log_file_content(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            logger.debug(f"Raw file content (first 1000 chars):\n{content[:1000]}")
            if len(content) > 1000:
                logger.debug("... (content truncated)")
    except Exception as e:
        logger.error(f"Failed to read file content: {str(e)}")

# Function to check for unescaped ampersands
def check_for_unescaped_ampersands(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for i, line in enumerate(lines, 1):
                if '&' in line and not '&' in line and not '<' in line and not '>' in line:
                    logger.warning(f"Possible unescaped ampersand found at line {i}: {line.strip()}")
    except Exception as e:
        logger.error(f"Failed to check file for unescaped ampersands: {str(e)}")

try:
    # Check if input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file {input_file} not found")

    # Log file content
    logger.info(f"Checking content of {input_file}")
    log_file_content(input_file)

    # Check for unescaped ampersands
    logger.info(f"Checking {input_file} for unescaped ampersands")
    check_for_unescaped_ampersands(input_file)

    # Parse the input XML file
    logger.info(f"Parsing input file: {input_file}")
    tree = ET.parse(input_file)
    root = tree.getroot()

    # Log the root element and structure
    logger.info(f"Root element tag: {root.tag}")
    logger.info(f"First few child elements: {[child.tag for child in root[:5]]}")
    logger.debug("Dumping XML structure:")
    dump_xml_structure(root)

    # Find all record elements with model="res.risk.assessment" or similar
    customer_types = root.findall(".//record[@model='res.risk.assessment']")
    if not customer_types:
        logger.warning("No records found with model='res.risk.assessment', trying case-insensitive search")
        # Try a more flexible search for any record elements
        customer_types = [record for record in root.findall(".//record") if record.get('model', '').lower() == 'res.risk.assessment']
        logger.info(f"Found {len(customer_types)} customer types after case-insensitive search")

    logger.info(f"Found {len(customer_types)} customer types (records with model='res.risk.assessment')")

    # List to store records
    records = []

    # Process each customer type
    for customer_type in customer_types:
        # Find name and recommendation fields
        name_elem = customer_type.find("field[@name='name']")
        recommendation_elem = customer_type.find("field[@name='recommendation']")
        record_id = customer_type.get('id')

        if name_elem is None or not name_elem.text:
            logger.warning(f"Skipping record with id={record_id}, no valid name field")
            continue
        if record_id is None:
            logger.warning(f"Skipping record with no id attribute, name={name_elem.text.strip()}")
            continue

        name = name_elem.text.strip()
        # Handle HTML entities
        name = name.replace('&', '&')
        description = recommendation_elem.text.strip() if recommendation_elem and recommendation_elem.text else f"Apply enhanced compliance monitoring for {name.lower()}."

        logger.info(f"Processing customer type: {name}, record id: {record_id}")
        logger.debug(f"Description: {description}")

        # Escape single quotes for SQL query
        escaped_name = name.replace("'", "''")

        # Generate record data
        record_id_output = f"risk_plan_{name_to_code(name).lower()}"
        code = name_to_code(name)
        # sql_query = f"SELECT 1 FROM res_partner WHERE id = %s AND customer_industry_id IN (SELECT id FROM customer_industry WHERE name = '{escaped_name}')"
        sql_query = f"SELECT 1  FROM res_partner_account a INNER JOIN res_partner r ON r.id = a.customer_id WHERE r.id = %s and a.category = '{escaped_name}')"
        narration = create_narration(name, description)

        # Create record dictionary
        record = {
            'id': record_id_output,
            'name': f"{name} Analysis",
            'code': code,
            'sql_query': sql_query,
            'priority': '10',
            'state': 'active',
            'narration': narration,
            'risk_score': '5',
            'compute_score_from': 'risk_assessment',
            'risk_assessment_ref': record_id,  # Use the record's id attribute
            'use_composite_calculation': 'True',
            'universe_id': 'risk_universe_customer_types'
        }
        records.append(record)

    logger.info(f"Generated {len(records)} records")

    # Create XML structure
    output_root = ET.Element('odoo')
    data = ET.SubElement(output_root, 'data', {'noupdate': '1'})

    # Add each record
    for record in records:
        record_elem = ET.SubElement(data, 'record', {
            'id': record['id'],
            'model': 'res.compliance.risk.assessment.plan'
        })

        ET.SubElement(record_elem, 'field', {'name': 'name'}).text = record['name']
        ET.SubElement(record_elem, 'field', {'name': 'code'}).text = record['code']
        ET.SubElement(record_elem, 'field', {'name': 'sql_query'}).text = record['sql_query']
        ET.SubElement(record_elem, 'field', {'name': 'priority'}).text = record['priority']
        ET.SubElement(record_elem, 'field', {'name': 'state'}).text = record['state']
        ET.SubElement(record_elem, 'field', {'name': 'narration'}).text = record['narration']
        ET.SubElement(record_elem, 'field', {'name': 'risk_score'}).text = record['risk_score']
        ET.SubElement(record_elem, 'field', {'name': 'compute_score_from'}).text = record['compute_score_from']
        ET.SubElement(record_elem, 'field', {'name': 'risk_assessment', 'ref': record['risk_assessment_ref']})
        ET.SubElement(record_elem, 'field', {'name': 'use_composite_calculation'}).text = record['use_composite_calculation']
        ET.SubElement(record_elem, 'field', {'name': 'universe_id', 'ref': record['universe_id']})

    # Write to output file
    logger.info(f"Writing to output file: {output_file}")
    ET.ElementTree(output_root).write(output_file, encoding='utf-8', xml_declaration=True)

    print(f"\nXML file generated successfully: {output_file}")

except FileNotFoundError as e:
    logger.error(str(e))
    print(str(e))
except ET.ParseError as e:
    logger.error(f"Failed to parse input XML: {str(e)}")
    print(f"Error: Invalid XML in {input_file}: {str(e)}")
    logger.info("Check for unescaped ampersands or other invalid XML characters around the reported line and column.")
except Exception as e:
    logger.error(f"Unexpected error: {str(e)}")
    print(f"Error: {str(e)}")
output_file = 'risk_assessment_plans.xml'

# Function to convert customer type name to code
def name_to_code(name):
    clean_name = re.sub(r'[^\w\s]', '', name)
    clean_name = re.sub(r'\s+', '_', clean_name.strip())
    return clean_name.upper()

# Function to create narration from description
def create_narration(name, description):
    return f"Risk assessment for {name.lower()} clients. {description}"

# Function to dump XML structure for debugging
def dump_xml_structure(element, level=0, max_depth=5):
    if level > max_depth:
        return
    indent = "  " * level
    text = element.text.strip() if element.text else ''
    logger.debug(f"{indent}Tag: {element.tag}, Attributes: {element.attrib}, Text: {text}")
    for child in element:
        dump_xml_structure(child, level + 1, max_depth)

# Function to check for unescaped ampersands
def check_for_unescaped_ampersands(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for i, line in enumerate(lines, 1):
                if '&' in line and not '&' in line and not '&lt;' in line and not '&gt;' in line:
                    logger.warning(f"Possible unescaped ampersand found at line {i}: {line.strip()}")
    except Exception as e:
        logger.error(f"Failed to check file for unescaped ampersands: {str(e)}")

try:
    # Check if input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file {input_file} not found")

    # Check for unescaped ampersands
    logger.info(f"Checking {input_file} for unescaped ampersands")
    check_for_unescaped_ampersands(input_file)

    # Parse the input XML file
    logger.info(f"Parsing input file: {input_file}")
    tree = ET.parse(input_file)
    root = tree.getroot()

    # Log the root element and structure
    logger.info(f"Root element tag: {root.tag}")
    logger.info(f"First few child elements: {[child.tag for child in root[:5]]}")
    logger.debug("Dumping XML structure:")
    dump_xml_structure(root)

    # Find all record elements with model="res.risk.assessment"
    customer_types = root.findall(".//record[@model='res.risk.assessment']")
    logger.info(f"Found {len(customer_types)} customer types (records with model='res.risk.assessment')")

    # List to store records
    records = []

    # Process each customer type
    for customer_type in customer_types:
        # Find name and recommendation fields
        name_elem = customer_type.find("field[@name='name']")
        recommendation_elem = customer_type.find("field[@name='recommendation']")
        record_id = customer_type.get('id')

        if name_elem is None or not name_elem.text:
            logger.warning(f"Skipping record with id={record_id}, no valid name field")
            continue
        if record_id is None:
            logger.warning(f"Skipping record with no id attribute, name={name_elem.text.strip()}")
            continue

        name = name_elem.text.strip()
        # Handle HTML entities
        name = name.replace('&', '&')
        description = recommendation_elem.text.strip() if recommendation_elem and recommendation_elem.text else f"Apply enhanced compliance monitoring for {name.lower()}."

        logger.info(f"Processing customer type: {name}, record id: {record_id}")
        logger.debug(f"Description: {description}")

        # Escape single quotes for SQL query
        escaped_name = name.replace("'", "''")

        # Generate record data
        record_id_output = f"risk_product_plan_{name_to_code(name).lower()}"
        code = name_to_code(name)
        sql_query = f"SELECT 1 FROM res_partner WHERE id = %s AND customer_industry_id IN (SELECT id FROM customer_industry WHERE name = '{escaped_name}')"
        narration = create_narration(name, description)

        # Create record dictionary
        record = {
            'id': record_id_output,
            'name': f"{name} Analysis",
            'code': code,
            'sql_query': sql_query,
            'priority': '10',
            'state': 'active',
            'narration': narration,
            'risk_score': '5',
            'compute_score_from': 'risk_assessment',
            'risk_assessment_ref': record_id,  # Use the record's id attribute
            'use_composite_calculation': 'True',
            'universe_id': 'risk_universe_customer_types'
        }
        records.append(record)

    logger.info(f"Generated {len(records)} records")

    # Create XML structure
    output_root = ET.Element('odoo')
    data = ET.SubElement(output_root, 'data', {'noupdate': '1'})

    # Add each record
    for record in records:
        record_elem = ET.SubElement(data, 'record', {
            'id': record['id'],
            'model': 'res.compliance.risk.assessment.plan'
        })

        ET.SubElement(record_elem, 'field', {'name': 'name'}).text = record['name']
        ET.SubElement(record_elem, 'field', {'name': 'code'}).text = record['code']
        ET.SubElement(record_elem, 'field', {'name': 'sql_query'}).text = record['sql_query']
        ET.SubElement(record_elem, 'field', {'name': 'priority'}).text = record['priority']
        ET.SubElement(record_elem, 'field', {'name': 'state'}).text = record['state']
        ET.SubElement(record_elem, 'field', {'name': 'narration'}).text = record['narration']
        ET.SubElement(record_elem, 'field', {'name': 'risk_score'}).text = record['risk_score']
        ET.SubElement(record_elem, 'field', {'name': 'compute_score_from'}).text = record['compute_score_from']
        ET.SubElement(record_elem, 'field', {'name': 'risk_assessment', 'ref': record['risk_assessment_ref']})
        ET.SubElement(record_elem, 'field', {'name': 'use_composite_calculation'}).text = record['use_composite_calculation']
        ET.SubElement(record_elem, 'field', {'name': 'universe_id', 'ref': record['universe_id']})

    # Write to output file
    logger.info(f"Writing to output file: {output_file}")
    ET.ElementTree(output_root).write(output_file, encoding='utf-8', xml_declaration=True)

    print(f"\nXML file generated successfully: {output_file}")

except FileNotFoundError as e:
    logger.error(str(e))
    print(str(e))
except ET.ParseError as e:
    logger.error(f"Failed to parse input XML: {str(e)}")
    print(f"Error: Invalid XML in {input_file}: {str(e)}")
    logger.info("Check for unescaped ampersands or other invalid XML characters around the reported line and column.")
except Exception as e:
    logger.error(f"Unexpected error: {str(e)}")
    print(f"Error: {str(e)}")