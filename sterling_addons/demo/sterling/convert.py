import os
import re
import argparse

def convert_file_content(file_path):
    """
    Reads a file, converts all 'curXXX' to 'CURXXX', and writes it back.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # This regex finds one or more letters followed by one or more digits.
        # It's case-insensitive to avoid re-processing files,
        # but the lambda function ensures the output is always uppercase.
        pattern = re.compile(r"([a-zA-Z]+\d+)", re.IGNORECASE)
        
        # We use a lambda function to get the matched string (m.group(1))
        # and return its uppercase version.
        new_content = pattern.sub(lambda m: m.group(1).upper(), content)

        # Only write back if content actually changed
        if new_content != content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Updated: {file_path}")
        else:
            print(f"Checked (no changes): {file_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    """
    Main function to parse arguments and process files or directories.
    """
    parser = argparse.ArgumentParser(
        description="Convert 'curXXX' to 'CURXXX' in Odoo XML files."
    )
    parser.add_argument(
        'path', 
        nargs='+', 
        help="One or more file paths or directory paths to process."
    )
    args = parser.parse_args()

    for path in args.path:
        if os.path.isfile(path):
            # It's a single file
            if path.endswith('.xml'):
                convert_file_content(path)
            else:
                print(f"Skipping non-XML file: {path}")
        
        elif os.path.isdir(path):
            # It's a directory, walk through it
            for root, _, files in os.walk(path):
                for file in files:
                    if file.endswith('.xml'):
                        file_path = os.path.join(root, file)
                        convert_file_content(file_path)
        
        else:
            print(f"Path does not exist: {path}")

if __name__ == "__main__":
    main()