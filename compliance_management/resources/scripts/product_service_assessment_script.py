import xml.etree.ElementTree as ET
import os

# Specify the file path for the input and output XML (replace with your actual file path)
INPUT_XML_FILE_PATH = "./../../data/demo_data/risk_assessment/product_service_subject.xml"
OUTPUT_XML_FILE_PATH = "./../../data/demo_data/risk_assessment/product_service_assessments.xml"

def fix_typos(subject):
    """Fix known typos in the name field."""
    if subject['name'] == "Disapora Vantage Gold":
        subject['name'] = "Diaspora Vantage Gold"
    if subject['name'] == "Monsterling Recurring Deposit Account EUR":
        subject['name'] = "Sterling Recurring Deposit Account EUR"
    return subject

def parse_risk_subjects(file_path):
    """
    Parse XML file to extract risk subjects, fix duplicates and typos, and exclude ambiguous entries.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"XML file not found at: {file_path}")

    risk_subjects = []
    seen_codes = set()
    duplicates = []

    tree = ET.parse(file_path)
    root = tree.getroot()
    
    for record in root.findall("record"):
        subject = {}
        subject['id'] = record.get('id')
        subject['model'] = record.get('model')
        
        for field in record.findall('field'):
            field_name = field.get('name')
            
            if field_name == 'name':
                subject['name'] = field.text
            elif field_name == 'code':
                subject['code'] = field.text
            elif field_name == 'universe_id':
                subject['universe_id'] = field.get('ref')
        
        subject = fix_typos(subject)
        
        # Handle duplicate codes
        if subject['code'] in seen_codes:
            duplicates.append(subject['code'])
            if subject['code'] == "savings_non_individual":
                subject['code'] = "savings_non_individual_2"
                subject['id'] = subject['id'].replace("savings_non_individual", "savings_non_individual_2")
        else:
            seen_codes.add(subject['code'])
        
        # Exclude ambiguous entries
        ambiguous_codes = {'test', 'curpdor', 'qa_prod', 'richard_mccall'}
        if subject['code'] not in ambiguous_codes:
            risk_subjects.append(subject)
    
    return risk_subjects, duplicates

def generate_risk_assessment_xml(subjects):
    """
    Generate XML for risk assessments and risk assessment lines for each subject.
    """
    root = ET.Element("odoo")
    
    risk_categories = [
        {"name": "Money Laundering", "category_id": "compliance_inst_fc_risk_category_1", "impact": "24.00", "score": "6"},
        {"name": "Terrorism Financing", "category_id": "compliance_inst_fc_risk_category_2", "impact": "22.00", "score": "6"},
        {"name": "Proliferation Financing", "category_id": "compliance_inst_fc_risk_category_3", "impact": "18.00", "score": "4"},
        {"name": "Sanction Risk", "category_id": "compliance_inst_fc_risk_category_53", "impact": "21.00", "score": "6"},
        {"name": "Bribery and Corruption", "category_id": "compliance_inst_fc_risk_category_42", "impact": "19.00", "score": "4"}
    ]
    
    for subject in subjects:
        # Generate res.risk.assessment record
        assessment_id = f"res_risk_assessment_{subject['code']}"
        assessment = ET.SubElement(root, "record", id=assessment_id, model="res.risk.assessment")
        ET.SubElement(assessment, "field", name="name").text = subject['name']
        ET.SubElement(assessment, "field", name="narration").text = f"Risk assessment for {subject['name']} accounts."
        ET.SubElement(assessment, "field", name="recommendation").text = f"Strengthen transaction monitoring for {subject['name'].lower()} usage."
        ET.SubElement(assessment, "field", name="subject_id", attrib={"ref": subject['id']})
        ET.SubElement(assessment, "field", name="universe_id", attrib={"ref": "compliance_inst_fc_risk_univ_22"})
        ET.SubElement(assessment, "field", name="assessment_type_id", attrib={"ref": "compliance_inst_assessment_type_inst"})
        ET.SubElement(assessment, "field", name="type_id", attrib={"ref": "compliance_inst_risk_type_3"})
        
        # Generate res.risk.assessment.line records
        for risk in risk_categories:
            line_id = f"res_risk_assessment_line_{subject['code']}_{risk['name'].lower().replace(' ', '_')}"
            line = ET.SubElement(root, "record", id=line_id, model="res.risk.assessment.line")
            ET.SubElement(line, "field", name="name").text = risk['name']
            ET.SubElement(line, "field", name="implication", attrib={"eval": "[(6, 0, [ref('compliance_management.implication_regulatory_fine_aml_failure')])]"})
            ET.SubElement(line, "field", name="existing_controls", attrib={"eval": "[(6, 0, [ref('compliance_management.risk_control_fcra_access_controls')])]"})
            ET.SubElement(line, "field", name="planned_mitigation", attrib={"eval": "[(6, 0, [ref('compliance_management.mitigation_1_demo')])]"})
            ET.SubElement(line, "field", name="category_id", attrib={"ref": risk['category_id']})
            ET.SubElement(line, "field", name="department_id", attrib={"ref": "hr_department_compliance"})
            ET.SubElement(line, "field", name="risk_assessment_id", attrib={"ref": assessment_id})
            ET.SubElement(line, "field", name="inherent_risk_score").text = "9"
            ET.SubElement(line, "field", name="control_effectiveness_score").text = "8"
            ET.SubElement(line, "field", name="residual_risk_impact").text = risk['impact']
            ET.SubElement(line, "field", name="residual_risk_score").text = risk['score']
            ET.SubElement(line, "field", name="residual_risk_probability").text = "75"
    
    return root

def save_to_xml(root, output_file=OUTPUT_XML_FILE_PATH):
    """Save XML tree to a file."""
    tree = ET.ElementTree(root)
    tree.write(output_file, encoding='utf-8', xml_declaration=True)
    print(f"Generated XML saved to: {output_file}")

def main():
    try:
        # Parse the input XML file
        risk_subjects, duplicates = parse_risk_subjects(INPUT_XML_FILE_PATH)
        
        # Print summary
        print(f"Total Risk Subjects Processed: {len(risk_subjects)}")
        if duplicates:
            print(f"Duplicates Found (Resolved): {duplicates}")
        
        # Generate and save risk assessment XML
        xml_root = generate_risk_assessment_xml(risk_subjects)
        save_to_xml(xml_root)
        
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"Error processing XML: {e}")


main()