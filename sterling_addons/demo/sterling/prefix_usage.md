# Process a single file
python add_prefix.py data.xml

# Process multiple specific files
python add_prefix.py file1.xml file2.xml file3.xml

# Use wildcards to process all XML files in a directory
python add_prefix.py *.xml

# Save output to a different directory
python add_prefix.py *.xml --output-dir modified_files

# Specify a different prefix
python add_prefix.py *.xml --prefix custom_module.