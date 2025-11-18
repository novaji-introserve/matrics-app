#!/usr/bin/env python3
import re
import sys
import os
import glob
import argparse

def add_prefix_to_refs(file_path, output_dir=None, prefix="compliance_management."):
    """Add prefix to ref attributes in an XML file that don't already have it."""
    try:
        # Read the XML file
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Regular expression to find ref attributes without the prefix
        pattern = rf'ref="(?!{re.escape(prefix)})([^"]*)"'
        
        # Count original matches
        original_matches = len(re.findall(pattern, content))
        
        # Replace with the prefixed version
        modified_content = re.sub(pattern, rf'ref="{prefix}\1"', content)
        
        # Count new matches to determine how many were changed
        new_matches = len(re.findall(rf'ref="{re.escape(prefix)}', modified_content))
        changes_made = new_matches - len(re.findall(rf'ref="{re.escape(prefix)}', content))
        
        # Determine output path
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            base_name = os.path.basename(file_path)
            output_path = os.path.join(output_dir, base_name)
        else:
            output_path = file_path
        
        # Write the modified content
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(modified_content)
        
        return {
            "file": file_path,
            "output": output_path,
            "changes": changes_made,
            "success": True
        }
    
    except Exception as e:
        return {
            "file": file_path,
            "error": str(e),
            "success": False
        }

def process_files(files, output_dir=None, prefix="compliance_management."):
    """Process multiple files."""
    results = []
    for file_pattern in files:
        # Handle wildcards in file paths
        matching_files = glob.glob(file_pattern)
        
        if not matching_files:
            results.append({
                "file": file_pattern,
                "error": "No matching files found",
                "success": False
            })
            continue
            
        for file_path in matching_files:
            result = add_prefix_to_refs(file_path, output_dir, prefix)
            results.append(result)
    
    return results

def main():
    parser = argparse.ArgumentParser(description='Add prefix to ref attributes in XML files')
    parser.add_argument('files', nargs='+', help='XML files to process (supports wildcards)')
    parser.add_argument('-o', '--output-dir', help='Output directory for modified files')
    parser.add_argument('-p', '--prefix', default="compliance_management.", 
                        help='Prefix to add (default: compliance_management.)')
    
    args = parser.parse_args()
    
    results = process_files(args.files, args.output_dir, args.prefix)
    
    # Print summary
    success_count = sum(1 for r in results if r["success"])
    failure_count = len(results) - success_count
    changes_count = sum(r.get("changes", 0) for r in results if r["success"])
    
    print(f"\nProcessed {len(results)} files:")
    print(f"  - Success: {success_count}")
    print(f"  - Failed: {failure_count}")
    print(f"  - Total ref attributes modified: {changes_count}")
    
    # Print details
    print("\nDetails:")
    for result in results:
        if result["success"]:
            if result["changes"] > 0:
                print(f"✅ {result['file']} → {result['output']} ({result['changes']} refs modified)")
            else:
                print(f"✓ {result['file']} → {result['output']} (no changes needed)")
        else:
            print(f"❌ {result['file']}: {result['error']}")

if __name__ == "__main__":
    main()