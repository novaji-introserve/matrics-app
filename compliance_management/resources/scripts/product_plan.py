# import xml.etree.ElementTree as ET
# import os

# # Specify the file path for the input and output XML (replace with your actual file path)
# INPUT_XML_FILE_PATH = "./../../data/demo_data/risk_assessment/product_service_assessments.xml"
# OUTPUT_XML_FILE_PATH = "./../../data/demo_data/risk_assessment/product_plans.xml"

# def parse_risk_assessments(file_path):
#     """
#     Parse XML file to extract risk assessments and their associated subject codes.
#     """
#     if not os.path.exists(file_path):
#         raise FileNotFoundError(f"XML file not found at: {file_path}")

#     risk_assessments = []
    
#     tree = ET.parse(file_path)
#     root = tree.getroot()
    
#     for record in root.findall("record"):
#         if record.get('model') != "res.risk.assessment":
#             continue  # Skip non-assessment records (e.g., res.risk.assessment.line)
        
#         assessment = {}
#         assessment['id'] = record.get('id')
        
#         for field in record.findall('field'):
#             field_name = field.get('name')
#             if field_name == 'name':
#                 assessment['name'] = field.text
#             elif field_name == 'subject_id':
#                 subject_id = field.get('ref')
#                 # Extract subject_code from subject_id (e.g., compliance_inst_fc_sterling_alumni_account_subject → sterling_alumni_account)
#                 if subject_id and subject_id.startswith("compliance_inst_fc_") and subject_id.endswith("_subject"):
#                     assessment['subject_code'] = subject_id[len("compliance_inst_fc_"):-len("_subject")]
        
#         # Exclude ambiguous entries
#         ambiguous_codes = {'test', 'curpdor', 'qa_prod', 'richard_mccall'}
#         if 'subject_code' in assessment and assessment['subject_code'] not in ambiguous_codes:
#             risk_assessments.append(assessment)
    
#     return risk_assessments

# def generate_risk_assessment_plan_xml(assessments):
#     """
#     Generate XML for risk assessment plans for each assessment.
#     """
#     root = ET.Element("odoo")
    
#     for assessment in assessments:
#         plan_id = f"risk_plan_{assessment['subject_code']}"
#         plan = ET.SubElement(root, "record", id=plan_id, model="res.compliance.risk.assessment.plan")
#         ET.SubElement(plan, "field", name="name").text = f"Risk Analysis For {assessment['name']}"
#         ET.SubElement(plan, "field", name="code").text = assessment['subject_code'].upper()
#         ET.SubElement(plan, "field", name="sql_query").text = (
#             f"SELECT 1\n"
#             f"FROM res_partner_account a\n"
#             f"INNER JOIN res_partner r ON r.id = a.customer_id\n"
#             f"WHERE r.id = %s and a.category = '{assessment['subject_code']}'"
#         )
#         ET.SubElement(plan, "field", name="priority").text = "1"
#         ET.SubElement(plan, "field", name="state").text = "active"
#         ET.SubElement(plan, "field", name="narration").text = f"Analysis for customers using {assessment['name'].lower()} product"
#         ET.SubElement(plan, "field", name="compute_score_from").text = "risk_assessment"
#         ET.SubElement(plan, "field", name="risk_assessment", attrib={"ref": assessment['id']})
#         ET.SubElement(plan, "field", name="use_composite_calculation").text = "True"
#         ET.SubElement(plan, "field", name="universe_id", attrib={"ref": "compliance_inst_fc_risk_univ_22"})
    
#     return root

# def save_to_xml(root, output_file=OUTPUT_XML_FILE_PATH):
#     """Save XML tree to a file."""
#     tree = ET.ElementTree(root)
#     tree.write(output_file, encoding='utf-8', xml_declaration=True)
#     print(f"Generated XML saved to: {output_file}")

# def main():
#     try:
#         # Parse the input XML file
#         risk_assessments = parse_risk_assessments(INPUT_XML_FILE_PATH)
        
#         # Print summary
#         print(f"Total Risk Assessments Processed: {len(risk_assessments)}")
        
#         # Generate and save risk assessment plan XML
#         xml_root = generate_risk_assessment_plan_xml(risk_assessments)
#         save_to_xml(xml_root)
        
#     except FileNotFoundError as e:
#         print(e)
#     except Exception as e:
#         print(f"Error processing XML: {e}")

# main()

import xml.etree.ElementTree as ET
import os
import re

# Specify the file path for the input and output XML (replace with your actual file path)
INPUT_XML_FILE_PATH = "./../../data/demo_data/risk_assessment/product_service_assessments.xml"
OUTPUT_XML_FILE_PATH = "./../../data/demo_data/risk_assessment/product_plans.xml"

def clean_id(text):
    """Clean text to make it suitable for XML IDs."""
    # Replace any non-alphanumeric characters with underscores
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', text)
    # Remove multiple consecutive underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    # Ensure it starts with a letter or underscore
    if cleaned and cleaned[0].isdigit():
        cleaned = '_' + cleaned
    return cleaned

def parse_risk_assessments(file_path):
    """
    Parse XML file to extract risk assessments and their associated subject codes.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"XML file not found at: {file_path}")

    risk_assessments = []
    
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    for record in root.findall("record"):
        if record.get('model') != "res.risk.assessment":
            continue  # Skip non-assessment records (e.g., res.risk.assessment.line)
        
        assessment = {}
        assessment['id'] = clean_id(record.get('id'))  # Clean the assessment ID
        
        for field in record.findall('field'):
            field_name = field.get('name')
            if field_name == 'name':
                assessment['name'] = field.text
            elif field_name == 'subject_id':
                subject_id = field.get('ref')
                # Extract subject_code from subject_id (e.g., compliance_inst_fc_sterling_alumni_account_subject → sterling_alumni_account)
                if subject_id and subject_id.startswith("compliance_inst_fc_") and subject_id.endswith("_subject"):
                    raw_subject_code = subject_id[len("compliance_inst_fc_"):-len("_subject")]
                    assessment['subject_code'] = clean_id(raw_subject_code)  # Clean the subject code
        
        # Exclude ambiguous entries
        ambiguous_codes = {'test', 'curpdor', 'qa_prod', 'richard_mccall'}
        if 'subject_code' in assessment and assessment['subject_code'] not in ambiguous_codes:
            risk_assessments.append(assessment)
    
    return risk_assessments

def generate_risk_assessment_plan_xml(assessments):
    """
    Generate XML for risk assessment plans for each assessment.
    """
    root = ET.Element("odoo")
    
    for assessment in assessments:
        # Clean the subject code for use in IDs and generate clean plan ID
        clean_subject_code = clean_id(assessment['subject_code'])
        plan_id = f"risk_plan_{clean_subject_code}"
        
        plan = ET.SubElement(root, "record", id=plan_id, model="res.compliance.risk.assessment.plan")
        ET.SubElement(plan, "field", name="name").text = f"Risk Analysis For {assessment['name']}"
        ET.SubElement(plan, "field", name="code").text = assessment['subject_code'].upper()
        ET.SubElement(plan, "field", name="sql_query").text = (
            f"SELECT 1\n"
            f"FROM res_partner_account a\n"
            f"INNER JOIN res_partner r ON r.id = a.customer_id\n"
            f"WHERE r.id = %s and a.category = '{assessment['subject_code'].split('_')[-1]}'"
        )
        ET.SubElement(plan, "field", name="priority").text = "1"
        ET.SubElement(plan, "field", name="state").text = "active"
        ET.SubElement(plan, "field", name="narration").text = f"Analysis for customers using {assessment['name'].lower()} product"
        ET.SubElement(plan, "field", name="compute_score_from").text = "risk_assessment"
        ET.SubElement(plan, "field", name="risk_assessment", attrib={"ref": assessment['id']})
        ET.SubElement(plan, "field", name="use_composite_calculation").text = "True"
        ET.SubElement(plan, "field", name="universe_id", attrib={"ref": "compliance_inst_fc_risk_univ_22"})
    
    return root

def save_to_xml(root, output_file=OUTPUT_XML_FILE_PATH):
    """Save XML tree to a file."""
    tree = ET.ElementTree(root)
    tree.write(output_file, encoding='utf-8', xml_declaration=True)
    print(f"Generated XML saved to: {output_file}")

def main():
    try:
        # Parse the input XML file
        risk_assessments = parse_risk_assessments(INPUT_XML_FILE_PATH)
        
        # Print summary
        print(f"Total Risk Assessments Processed: {len(risk_assessments)}")
        
        # Generate and save risk assessment plan XML
        xml_root = generate_risk_assessment_plan_xml(risk_assessments)
        save_to_xml(xml_root)
        
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"Error processing XML: {e}")


main()