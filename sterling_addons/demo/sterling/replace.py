import os
import sys
import re

def process_file(file_path):
    """Process a single file, replacing 'compliance_management' with 'sterling_addons'
    in field tags with subject_id or risk_assessment_id attributes."""
    try:
        # Read the file content
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Define patterns to match field tags with the required attributes
        # Pattern 1: ref attribute comes before name attribute
        pattern1 = re.compile(r'<field\b[^>]*?ref="compliance_management\.([^"]+)"[^>]*?name="(subject_id|risk_assessment|risk_assessment_id)"[^>]*?>', re.DOTALL)
        # Pattern 2: name attribute comes before ref attribute
        pattern2 = re.compile(r'<field\b[^>]*?name="(subject_id|risk_assessment|risk_assessment_id)"[^>]*?ref="compliance_management\.([^"]+)"[^>]*?>', re.DOTALL)
        
        # Apply replacements
        new_content = pattern1.sub(lambda m: m.group(0).replace('compliance_management.', ''), content)
        new_content = pattern2.sub(lambda m: m.group(0).replace('compliance_management.', ''), new_content)
        
        # Check if any changes were made
        if new_content != content:
            # Write the modified content back to the file
            with open(file_path, 'w') as f:
                f.write(new_content)
            print(f"Updated: {file_path}")
        else:
            print(f"No changes needed in: {file_path}")
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def process_directory(directory_path):
    """Recursively process all files in the given directory."""
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            process_file(file_path)

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <file_path_or_directory>")
        sys.exit(1)
    
    path = sys.argv[1]
    
    if os.path.isfile(path):
        process_file(path)
    elif os.path.isdir(path):
        process_directory(path)
    else:
        print(f"Error: {path} is not a valid file or directory")
        sys.exit(1)

if __name__ == "__main__":
    main()