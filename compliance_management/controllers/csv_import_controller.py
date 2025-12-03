import json
import logging
import os
import tempfile
import base64
from datetime import datetime
import random
import string
import mimetypes
import magic
import re
from pathlib import Path
from odoo import http, _, api
from odoo.http import request, Response
from ..services.websocket.connection import send_message
import traceback
import xlsxwriter
from io import BytesIO
import socket

_logger = logging.getLogger(__name__)

class FileSecurityValidator:
    """Enhanced file security validation with comprehensive protection against script injection"""
    
    # Allowed file types with strict validation requirements
    ALLOWED_FILE_TYPES = {
        'csv': {
            'extensions': ['.csv'],
            'mime_types': ['text/csv'],  # Only strict CSV MIME type
            'magic_numbers': [],  # CSV validated by structure, not magic numbers
            'max_size': None  # Will be set from class constants
        },
        'excel': {
            'extensions': ['.xlsx', '.xls'],
            'mime_types': [
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'application/vnd.ms-excel'
            ],
            'magic_numbers': [
                b'PK\x03\x04',  # XLSX (ZIP format)
                b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'  # XLS (OLE format)
            ],
            'max_size': None  # Will be set from class constants
        }
    }
    
    # Configuration constants - avoid hardcoding
    DEFAULT_MAX_FILENAME_LENGTH = 100
    MIN_FILE_SIZE_BYTES = 10
    GLOBAL_MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB
    CSV_MAX_SIZE = 100 * 1024 * 1024  # 100MB
    EXCEL_MAX_SIZE = 50 * 1024 * 1024  # 50MB
    DATA_RATIO_THRESHOLD = 0.8  # 80% data characters required for CSV
    
    # Comprehensive patterns to block malicious content
    BLOCKED_FILENAME_PATTERNS = [
        # Executable files
        r'\.exe$', r'\.bat$', r'\.cmd$', r'\.com$', r'\.scr$', r'\.msi$',
        # Script files  
        r'\.php$', r'\.py$', r'\.pl$', r'\.rb$', r'\.sh$', r'\.bash$',
        r'\.js$', r'\.vbs$', r'\.ps1$', r'\.jsp$', r'\.asp$', r'\.aspx$',
        # Archive files
        r'\.zip$', r'\.rar$', r'\.7z$', r'\.tar$', r'\.gz$', r'\.bz2$',
        # System/config files
        r'\.dll$', r'\.sys$', r'\.ini$', r'\.cfg$', r'\.conf$',
        # Hidden files and path traversal
        r'^\.',  # Files starting with dot
        r'\.\.',  # Path traversal attempts
        # SQL injection in filenames
        r"[';\"]\s*(drop|delete|update|insert|exec|union|select)", 
        r"--", r"/\*.*\*/",
        # Path traversal variations
        r"\.\.[\\/]", r"[\\/]\.\.", r"%2e%2e", r"%2f", r"%5c",
        # Suspicious filename patterns (but not blocking normal names)
        r"(script|malicious|hack|exploit|payload)\.csv$",  # Obviously suspicious names
    ]
    
    # Context-aware script detection patterns - only match when followed by code syntax
    SCRIPT_EXECUTION_PATTERNS = {
        # Python script patterns - only when followed by code syntax
        'python_imports': [
            r'\bimport\s+(os|sys|subprocess|socket|urllib|shutil|pickle)\s*[;\n]',  # Dangerous imports with code syntax
            r'\bfrom\s+(os|sys|subprocess|socket|urllib|shutil)\s+import\s+\w+',  # From imports
            # For Excel cells: dangerous imports (fixed - removed anchors)
            r'\bimport\s+(os|sys|subprocess|socket|urllib|shutil|pickle)\b',  # Match dangerous imports anywhere
            r'\bfrom\s+(os|sys|subprocess|socket|urllib|shutil)\s+import\s+\w+',  # From imports anywhere
        ],
        'python_execution': [
            r'\b(exec|eval|compile)\s*\(',  # Direct execution functions
            r'\bos\.(system|popen|spawn|execv?)\s*\(',  # OS command execution
            r'\bsubprocess\.(run|call|Popen|check_output)\s*\(',  # Subprocess execution
            r'__import__\s*\(',  # Dynamic imports
        ],
        
        # Shell script patterns
        'shell_execution': [
            r'#!/bin/(sh|bash|zsh|csh|tcsh|ksh)',  # Shebang lines (always suspicious)
            r'\b(rm|mv|cp|dd|chmod|chown|sudo|su)\s+[-/\w]',  # Commands with arguments
            r'\b(curl|wget|nc|netcat|telnet)\s+[-\w]',  # Network commands with args
            r'[;&|]\s*(rm|dd|mkfs|sudo)\s+',  # Chained dangerous commands
            r'\$\([^)]*\)',  # Command substitution
            r'`[^`]+`',  # Backtick command substitution
        ],
        
        # SQL injection patterns - context-aware
        'sql_injection': [
            r"';\s*(drop|delete|insert|update)\s+(table|from|into)",  # SQL with proper syntax
            r'\bunion\s+select\s+',  # UNION SELECT attacks
            r'--\s*[a-zA-Z].*[;\n]',  # SQL comments followed by statements
            r'\)\s*;\s*(drop|delete|insert)\s+\w+',  # After closing parenthesis with target
        ],
        
        # Web script injection
        'web_scripts': [
            r'<script[^>]*>.*</script>',  # Complete script tags
            r'javascript:\s*[a-zA-Z_$][\w$]*\s*\(',  # JavaScript function calls
            r'<\?php\s+.*\?>',  # PHP code blocks
            r'<%[^>]*%>',  # ASP/JSP code blocks
            r'on(load|error|click|focus|mouse|key)\s*=\s*["\'][^"\']*["\']',  # Event handlers with values
        ],
        
        # Command injection patterns
        'command_injection': [
            r'(system|shell_exec|passthru|exec)\s*\(\s*["\'][^"\']*["\']',  # Function calls with string args
            r'proc_open\s*\(',  # Process execution
            r'popen\s*\(\s*["\'][^"\']*["\']',  # Pipe open with commands
        ],
        
        # File system attacks
        'file_attacks': [
            r'\.\./(\.\./)+(etc|bin|usr|var)',  # Path traversal to system dirs
            r'file:///(etc|bin|usr|var|windows)',  # File protocol to system dirs
            r'["\']/(etc|bin|usr|var)/(passwd|shadow|hosts)["\']',  # Direct system file access
        ],
        
        # Excel formula injection
        'excel_formula_injection': [
            r'=\s*(cmd|powershell|system|exec)\s*\(',  # Executable formulas
            r'=\s*HYPERLINK\s*\(\s*["\']https?://[^"\']*["\']',  # External hyperlinks
            r'=\s*(IMPORTDATA|IMPORTXML|IMPORTHTML)\s*\(',  # Data import functions
        ]
    }
    
    @classmethod 
    def _initialize_file_types(cls):
        """Initialize file type configurations with class constants"""
        cls.ALLOWED_FILE_TYPES['csv']['max_size'] = cls.CSV_MAX_SIZE
        cls.ALLOWED_FILE_TYPES['excel']['max_size'] = cls.EXCEL_MAX_SIZE
    
    @staticmethod
    def validate_filename(filename):
        """Enhanced filename validation with comprehensive security checks"""
        if not filename:
            raise ValueError("Filename is required")
        
        original_filename = filename
        
        # Remove any path components (security: prevent path traversal)
        # Handle both Unix and Windows path separators
        filename = os.path.basename(filename.replace('\\', '/'))
        
        # Additional path traversal checks
        if filename != original_filename:
            raise ValueError("Path traversal attempt detected in filename")
        
        # Check for dangerous patterns
        filename_lower = filename.lower()
        for pattern in FileSecurityValidator.BLOCKED_FILENAME_PATTERNS:
            if re.search(pattern, filename_lower, re.IGNORECASE):
                raise ValueError(f"Potentially dangerous filename pattern detected: {filename}")
        
        # Strict filename length check
        if len(filename) > FileSecurityValidator.DEFAULT_MAX_FILENAME_LENGTH:
            raise ValueError(f"Filename too long (max {FileSecurityValidator.DEFAULT_MAX_FILENAME_LENGTH} characters)")
        
        # Enhanced character validation
        if '\x00' in filename:
            raise ValueError("Null byte detected in filename")
        
        # Check for any control characters
        for char in filename:
            if ord(char) < 32 and char not in '\t\n\r':
                raise ValueError(f"Invalid control character detected in filename: {repr(char)}")
        
        # Check for potentially dangerous Unicode characters
        dangerous_unicode = ['\u202e', '\u200e', '\u200f']  # RTL override, LTR marks
        for char in dangerous_unicode:
            if char in filename:
                raise ValueError("Potentially dangerous Unicode character detected in filename")
        
        # Validate extension is allowed
        file_ext = Path(filename).suffix.lower()
        allowed_extensions = []
        for file_type in FileSecurityValidator.ALLOWED_FILE_TYPES.values():
            allowed_extensions.extend(file_type['extensions'])
        
        if file_ext not in allowed_extensions:
            raise ValueError(f"File extension '{file_ext}' not allowed. Allowed: {', '.join(allowed_extensions)}")
        
        # Additional check: ensure filename has proper extension
        if '.' not in filename or len(file_ext) < 2:
            raise ValueError("Invalid file extension format")
        
        return filename
    
    @staticmethod
    def validate_file_content(file_data, filename):
        """Enhanced file content validation with comprehensive security checks"""
        # Initialize file types configuration if needed
        FileSecurityValidator._initialize_file_types()
        if not file_data:
            raise ValueError("File content is empty")
        
        file_ext = Path(filename).suffix.lower()
        
        # Get file type configuration
        file_type_config = None
        for config in FileSecurityValidator.ALLOWED_FILE_TYPES.values():
            if file_ext in config['extensions']:
                file_type_config = config
                break
        
        if not file_type_config:
            raise ValueError(f"Unsupported file type: {file_ext}")
        
        # Check file size against type-specific limits
        if len(file_data) > file_type_config['max_size']:
            raise ValueError(f"File size exceeds maximum limit of {file_type_config['max_size'] // (1024*1024)}MB for {file_ext} files")
        
        # Minimum file size check (prevent empty/tiny malicious files)
        if len(file_data) < FileSecurityValidator.MIN_FILE_SIZE_BYTES:
            raise ValueError("File appears to be empty or corrupted. Please ensure your file contains data and try uploading again.")
        
        # CRITICAL: Scan for malicious content patterns FIRST
        FileSecurityValidator._scan_malicious_content(file_data, filename)
        
        # Then validate specific file format
        if file_ext == '.csv':
            FileSecurityValidator._validate_csv_structure(file_data)
        elif file_ext in ['.xlsx', '.xls']:
            FileSecurityValidator._validate_excel_content(file_data, file_ext)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")
        
        return True
    
    @staticmethod
    def _scan_malicious_content(file_data, filename=None):
        """Context-aware malicious content detection following SRP"""
        # Step 1: Check for binary executable signatures (SRP: Binary validation)
        FileSecurityValidator._check_binary_signatures(file_data)
        
        # Step 2: Determine file type for context-aware scanning
        file_ext = filename.lower() if filename else ""
        is_excel_file = file_ext.endswith(('.xlsx', '.xls'))
        is_csv_file = file_ext.endswith('.csv')
        
        # Step 3: For Excel files, extract and scan cell content
        if is_excel_file:
            # CRITICAL: Extract and scan actual cell content from Excel files
            try:
                import zipfile
                from io import BytesIO
                import xml.etree.ElementTree as ET
                
                with zipfile.ZipFile(BytesIO(file_data), 'r') as zip_file:
                    cell_texts = []
                    headers_found = []
                    
                    # Extract shared strings for reference
                    shared_strings_map = {}
                    try:
                        shared_strings = zip_file.read('xl/sharedStrings.xml')
                        root = ET.fromstring(shared_strings)
                        for i, t_elem in enumerate(root.iter()):
                            if t_elem.tag.endswith('}t') and t_elem.text:
                                shared_strings_map[i] = t_elem.text
                                cell_texts.append(t_elem.text)
                    except:
                        pass
                    
                    # Extract worksheet content and check for headers in first row
                    for file_info in zip_file.infolist():
                        if file_info.filename.startswith('xl/worksheets/'):
                            try:
                                worksheet = zip_file.read(file_info.filename)
                                root = ET.fromstring(worksheet)
                                
                                # STEP 1: Check for proper headers in first row (A1, B1, C1, etc.)
                                first_row_cells = []
                                for row in root.iter():
                                    if row.tag.endswith('}row') and row.get('r') == '1':  # First row
                                        for cell in row.iter():
                                            if cell.tag.endswith('}c'):  # Cell element
                                                cell_ref = cell.get('r', '')
                                                if cell_ref and cell_ref[0] in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                                                    cell_value = None
                                                    # Check for shared string reference
                                                    for v_elem in cell.iter():
                                                        if v_elem.tag.endswith('}v') and v_elem.text:
                                                            if cell.get('t') == 's':  # Shared string
                                                                try:
                                                                    ss_index = int(v_elem.text)
                                                                    cell_value = shared_strings_map.get(ss_index, v_elem.text)
                                                                except:
                                                                    cell_value = v_elem.text
                                                            else:
                                                                cell_value = v_elem.text
                                                    # Check for inline string
                                                    for t_elem in cell.iter():
                                                        if t_elem.tag.endswith('}t') and t_elem.text:
                                                            cell_value = t_elem.text
                                                    
                                                    if cell_value:
                                                        first_row_cells.append((cell_ref, cell_value))
                                        break  # Only check first row
                                
                                # Validate headers
                                if first_row_cells:
                                    headers_found.extend([cell[1] for cell in first_row_cells])
                                    FileSecurityValidator._validate_excel_headers(first_row_cells)
                                
                                # Extract all cell content for malicious content scanning
                                for elem in root.iter():
                                    # Extract formulas and inline text
                                    if elem.tag.endswith('}f') and elem.text:
                                        cell_texts.append(elem.text)
                                    elif elem.tag.endswith('}t') and elem.text:
                                        cell_texts.append(elem.text)
                                    elif elem.tag.endswith('}v') and elem.text:
                                        cell_texts.append(elem.text)
                            except:
                                continue
                    
                    # STEP 2: Ensure we found valid headers
                    if not headers_found:
                        raise ValueError("Empty/Invalid file content")
                    
                    _logger.info(f"Headers found: {headers_found[:10]}")  # Log first 10 headers
                    
                    # STEP 3: Scan all extracted cell content for malicious patterns
                    if cell_texts:
                        combined_text = '\n'.join(cell_texts)
                        _logger.info(f"Scanning {len(cell_texts)} Excel cell values, {len(combined_text)} total characters")
                        FileSecurityValidator._detect_script_execution(combined_text, is_csv=False, is_excel=True)
                    
            except Exception as e:
                # If we can't read Excel content, fail securely
                # raise ValueError(f"Empty/Invalid file content")
                raise ValueError(f"{str(e)}")
            
            return True
        
        # Step 4: For CSV and other text files, decode content for analysis
        content = FileSecurityValidator._decode_content_safely(file_data)
        
        if content:
            # Step 5: Context-aware script detection with file type awareness
            FileSecurityValidator._detect_script_execution(content, is_csv_file, is_excel_file)
        
        return True
    
    @staticmethod
    def _check_binary_signatures(file_data):
        """Check for executable binary signatures (SRP: Binary validation only)"""
        binary_signatures = [
            (b'\x7fELF', "ELF executable header detected"),
            (b'MZ\x90\x00', "Windows executable header detected"),
            (b'\xff\xfe', "UTF-16 BOM potentially hiding content"),
            (b'\xfe\xff', "UTF-16 BE BOM potentially hiding content"),
        ]
        
        for signature, error_msg in binary_signatures:
            if file_data.startswith(signature):
                raise ValueError(error_msg)
    
    @staticmethod
    def _decode_content_safely(file_data):
        """Safely decode file content for text analysis (SRP: Content decoding only)"""
        encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                return file_data.decode(encoding)
            except UnicodeDecodeError:
                continue
        
        # If all decodings fail, this might be binary data - that's fine
        return None
    
    @staticmethod
    def _scan_excel_cell_content(file_data):
        """Extract and scan actual cell content from Excel files (SRP: Excel-specific validation)"""
        try:
            import zipfile
            from io import BytesIO
            import xml.etree.ElementTree as ET
            
            with zipfile.ZipFile(BytesIO(file_data), 'r') as zip_file:
                # Extract text content from cells only - ignore XML structure
                cell_texts = []
                
                # Read shared strings (where most text content is stored)
                try:
                    shared_strings = zip_file.read('xl/sharedStrings.xml')
                    root = ET.fromstring(shared_strings)
                    for t_elem in root.iter():
                        if t_elem.tag.endswith('}t') and t_elem.text:
                            cell_texts.append(t_elem.text)
                except:
                    pass
                
                # Read worksheet cells for inline strings and formulas
                for file_info in zip_file.infolist():
                    if file_info.filename.startswith('xl/worksheets/'):
                        try:
                            worksheet = zip_file.read(file_info.filename)
                            root = ET.fromstring(worksheet)
                            for elem in root.iter():
                                # Check for formula injection in formulas
                                if elem.tag.endswith('}f') and elem.text:
                                    FileSecurityValidator._check_excel_formula(elem.text)
                                # Check inline strings
                                elif elem.tag.endswith('}t') and elem.text:
                                    cell_texts.append(elem.text)
                        except:
                            continue
                
                # CRITICAL: Scan extracted cell content as plain text
                if cell_texts:
                    combined_text = '\n'.join(cell_texts)
                    _logger.info(f"Scanning {len(cell_texts)} cell values, {len(combined_text)} total characters")
                    # Only scan the actual cell content, not XML structure
                    FileSecurityValidator._detect_script_execution(combined_text, is_csv=False, is_excel=True)
                else:
                    # SECURITY: If we found no cell content, the file might be suspicious or our extraction failed
                    _logger.warning("No cell content found in file - potential security concern")
                    # Continue with minimal validation rather than failing, but log the concern
                
                # Note: We deliberately do NOT scan raw XML content as it contains
                # legitimate XML attributes that can trigger false positives
                    
        except zipfile.BadZipFile:
            raise ValueError("Corrupted XLSX file - invalid ZIP structure")
        except Exception as e:
            # If we can't read the Excel file, that's suspicious
            _logger.error(f"Excel validation failed: {str(e)}")
            raise ValueError(f"Could not validate file structure - potential security threat")
            # raise ValueError(f"Could not validate Excel file structure - potential security threat: {str(e)}")
    
    @staticmethod
    def _check_excel_formula(formula):
        """Check Excel formulas for malicious content (SRP: Formula validation only)"""
        dangerous_formulas = [
            r'=.*cmd\s*\|',  # Command execution
            r'=.*powershell',  # PowerShell execution  
            r'=.*system\s*\(',  # System calls
            r'=.*exec\s*\(',  # Exec calls
        ]
        
        for pattern in dangerous_formulas:
            if re.search(pattern, formula, re.IGNORECASE):
                raise ValueError("Malicious formula detected")
    
    @staticmethod
    def _detect_script_execution(content, is_csv=False, is_excel=False):
        """Context-aware script execution detection (SRP: Script detection only)"""
        # Normalize content for consistent matching
        content_normalized = FileSecurityValidator._normalize_content(content)
        
        # Debug: Log sample of content being scanned
        sample_content = content_normalized[:500] + "..." if len(content_normalized) > 500 else content_normalized
        _logger.info(f"Content sample being scanned: {repr(sample_content)}")
        
        # Debug: Quick test for obvious malicious content
        if 'import os' in content_normalized:
            _logger.error(f"DIRECT MATCH: Found 'import os' in content!")
        if 'import sys' in content_normalized:
            _logger.error(f"DIRECT MATCH: Found 'import sys' in content!")
        if 'import socket' in content_normalized:
            _logger.error(f"DIRECT MATCH: Found 'import socket' in content!")
        
        # For data files (CSV/Excel), be more lenient with certain patterns
        categories_to_check = FileSecurityValidator.SCRIPT_EXECUTION_PATTERNS.keys()
        
        # Skip shell execution checks for Excel cell content (too many false positives)
        if is_excel:
            categories_to_check = [k for k in categories_to_check if k != 'shell_execution']
        
        # Check each category of script patterns
        _logger.info(f"Checking categories: {list(categories_to_check)} for {'Excel' if is_excel else 'CSV' if is_csv else 'Unknown'} content")
        
        for category in categories_to_check:
            patterns = FileSecurityValidator.SCRIPT_EXECUTION_PATTERNS[category]
            _logger.info(f"Checking {category} with {len(patterns)} patterns")
            
            for pattern in patterns:
                try:
                    matches = list(re.finditer(pattern, content_normalized, re.IGNORECASE | re.MULTILINE | re.DOTALL))
                    if matches:
                        _logger.info(f"Found {len(matches)} matches for {category} pattern: {pattern}")
                        for match in matches:
                            _logger.info(f"Match found: '{match.group(0)}' at position {match.start()}-{match.end()}")
                            # Additional context validation for certain patterns
                            if FileSecurityValidator._validate_script_context(match, content_normalized, category, is_csv, is_excel):
                                _logger.error(f"SECURITY THREAT DETECTED: {category} pattern matched - '{match.group(0)}'")
                                raise ValueError(f"Empty/Invalid file content")
                            else:
                                _logger.info(f"Match dismissed by context validation: '{match.group(0)}'")
                    # else:
                    #     _logger.info(f"No matches for {category} pattern: {pattern}")
                except re.error:
                    # Skip invalid regex patterns
                    _logger.warning(f"Invalid regex pattern in {category}: {pattern}")
                    continue
    
    @staticmethod
    def _normalize_content(content):
        """Normalize content for consistent pattern matching (SRP: Content normalization)"""
        # Remove excessive whitespace but preserve structure for context analysis
        normalized = re.sub(r'\s+', ' ', content)
        # Remove common obfuscation attempts
        normalized = normalized.replace('\x00', '')  # Remove null bytes
        return normalized
    
    @staticmethod
    def _validate_script_context(match, content, category, is_csv=False, is_excel=False):
        """Validate if a pattern match is actually executable code vs. legitimate data (SRP: Context validation)"""
        matched_text = match.group(0)
        start_pos = match.start()
        end_pos = match.end()
        
        # Get surrounding context (50 chars before and after)
        context_start = max(0, start_pos - 50)
        context_end = min(len(content), end_pos + 50)
        context = content[context_start:context_end]
        
        # Context-specific validation rules with file type awareness
        if category == 'python_imports':
            # Allow "import/export business" but block "import os"
            return FileSecurityValidator._is_python_import_execution(matched_text, context)
        
        elif category == 'sql_injection':
            # Allow "I want to select a product" but block "'; DROP TABLE"
            return FileSecurityValidator._is_sql_injection_attempt(matched_text, context)
        
        elif category == 'shell_execution':
            # For data files, be more careful - many false positives
            if is_csv or is_excel:
                # Only flag if it really looks like a command with arguments
                return FileSecurityValidator._is_actual_shell_command(matched_text, context)
            return True
        
        elif category in ['web_scripts', 'command_injection']:
            # These are almost always malicious in data context
            return True
        
        elif category == 'excel_formula_injection':
            # Only relevant for Excel files
            return is_excel
        
        elif category == 'file_attacks':
            # Path traversal attempts are always suspicious
            return True
        
        # Default: if we're not sure, err on the side of security
        return True
    
    @staticmethod
    def _is_actual_shell_command(matched_text, context):
        """Check if this is actually a shell command vs. regular text (SRP: Shell command validation)"""
        # Look for command-like indicators
        command_indicators = [
            r'[;&|]\s*\w+\s+',  # Command chaining
            r'^\s*\w+\s+-\w+',  # Command with flags
            r'\$\(',  # Command substitution
            r'`.*`',  # Backtick substitution
            r'#!/',  # Shebang
        ]
        
        for indicator in command_indicators:
            if re.search(indicator, context, re.IGNORECASE):
                return True
        
        return False
    
    @staticmethod
    def _is_python_import_execution(matched_text, context):
        """Check if this is actual Python import vs. business term (SRP: Python context validation)"""
        # Look for code indicators around the import
        code_indicators = [
            r'[;\n]\s*import\s+',  # Import after statement separator
            r'import\s+\w+\s*[;\n]',  # Import followed by statement separator
            r'from\s+\w+\s+import\s+',  # From-import syntax
            r'import\s+\w+\s*\.',  # Import followed by method call
        ]
        
        for indicator in code_indicators:
            if re.search(indicator, context, re.IGNORECASE):
                return True
        
        return False
    
    @staticmethod
    def _is_sql_injection_attempt(matched_text, context):
        """Check if this is actual SQL injection vs. legitimate data (SRP: SQL context validation)"""
        # Look for SQL injection indicators
        injection_indicators = [
            r"';\s*(drop|delete|insert|update)",  # Statement termination followed by SQL
            r"union\s+select\s+",  # UNION SELECT attack
            r"--.*[;\n]",  # SQL comment followed by statement
            r"\)\s*;\s*(drop|delete)",  # Parenthesis closure followed by dangerous SQL
        ]
        
        for indicator in injection_indicators:
            if re.search(indicator, context, re.IGNORECASE):
                return True
        
        return False
    
    @staticmethod
    def _validate_csv_structure(file_data):
        """Strict CSV structure validation"""
        try:
            # Decode with strict encoding requirements
            content = file_data.decode('utf-8-sig')  # Only UTF-8 allowed for CSV
        except UnicodeDecodeError:
            raise ValueError("CSV files must be UTF-8 encoded")
        
        if not content.strip():
            raise ValueError("CSV file is empty")
        
        lines = content.strip().split('\n')
        if len(lines) < 1:
            raise ValueError("CSV must have at least one line")
        
        # Validate CSV structure using csv module
        import csv
        from io import StringIO
        
        try:
            # Test parsing as CSV
            csv_reader = csv.reader(StringIO(content))
            rows = list(csv_reader)
            
            if len(rows) < 1:
                raise ValueError("No valid CSV rows found")
            
            # Check that all rows have consistent column count
            if len(rows) > 1:
                header_cols = len(rows[0]) if rows[0] else 0
                if header_cols == 0:
                    raise ValueError("CSV header row is empty")
                
                for i, row in enumerate(rows[1:], start=2):
                    if len(row) != header_cols and len(row) > 0:  # Allow empty rows
                        raise ValueError(f"Inconsistent column count in row {i}: expected {header_cols}, got {len(row)}")
            
            # Additional validation: ensure it looks like real data
            if len(rows) == 1:
                # Only header, that's suspicious for a data file
                _logger.warning("CSV file contains only header row")
            
        except csv.Error as e:
            raise ValueError(f"Invalid CSV format: {str(e)}")
        
        # Final check: ensure reasonable content ratio
        # CSV should be mostly data, not code-like content
        data_chars = sum(1 for c in content if c.isalnum() or c in ',"\n\r\t ')
        total_chars = len(content)
        
        if total_chars > 0 and (data_chars / total_chars) < FileSecurityValidator.DATA_RATIO_THRESHOLD:
            raise ValueError(f"File contains too many non-data characters for a CSV file (threshold: {FileSecurityValidator.DATA_RATIO_THRESHOLD*100}%)")
        
        return True
    
    @staticmethod
    def _validate_excel_content(file_data, file_ext):
        """Validate Excel file content using magic numbers and content scanning"""
        if len(file_data) < 8:
            raise ValueError("File too small to be valid Excel file")
        
        # Check magic numbers
        if file_ext == '.xlsx':
            # XLSX files are ZIP archives, should start with ZIP signature
            if not file_data.startswith(b'PK\x03\x04'):
                raise ValueError("Invalid XLSX file format")
            # Additional content scanning for XLSX files
            FileSecurityValidator._scan_excel_cell_content(file_data)
        elif file_ext == '.xls':
            # XLS files use OLE format
            if not file_data.startswith(b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'):
                raise ValueError("Invalid XLS file format")
            # For XLS files, we can only do basic binary scanning
            FileSecurityValidator._scan_malicious_content(file_data)
        
        return True
    
    
    @staticmethod
    def validate_mime_type(file_data, filename):
        """Strict MIME type validation with no permissive fallbacks"""
        mime_type = None
        
        try:
            # Use python-magic for content-based detection
            mime_type = magic.from_buffer(file_data, mime=True)
        except Exception as e:
            # NO fallback to filename-based detection for security
            raise ValueError(f"Could not detect MIME type from file content: {str(e)}")
        
        if not mime_type:
            raise ValueError("Could not determine file MIME type from content")
        
        file_ext = Path(filename).suffix.lower()
        
        # Get expected MIME types for this extension
        expected_mime_types = []
        for file_type_info in FileSecurityValidator.ALLOWED_FILE_TYPES.values():
            if file_ext in file_type_info['extensions']:
                expected_mime_types = file_type_info['mime_types']
                break
        
        if not expected_mime_types:
            raise ValueError(f"File extension '{file_ext}' is not in allowed types")
        
        # STRICT matching - no permissive rules
        if mime_type not in expected_mime_types:
            # Special handling for CSV - but much more strict
            if file_ext == '.csv':
                # Only allow text/csv or text/plain, NOT any text/*
                if mime_type not in ['text/csv', 'text/plain']:
                    raise ValueError(f"CSV file has invalid MIME type '{mime_type}'. Expected 'text/csv' or 'text/plain'")
            else:
                raise ValueError(f"File MIME type '{mime_type}' does not match expected types {expected_mime_types} for {file_ext}")
        
        # Additional signature validation for Excel files
        if file_ext in ['.xlsx', '.xls']:
            FileSecurityValidator._validate_excel_signatures(file_data, file_ext)
        
        return True
    
    @staticmethod
    def _validate_excel_signatures(file_data, file_ext):
        """Validate Excel file signatures"""
        if len(file_data) < 8:
            raise ValueError("File too small for Excel format")
        
        file_type_config = None
        for config in FileSecurityValidator.ALLOWED_FILE_TYPES.values():
            if file_ext in config['extensions']:
                file_type_config = config
                break
        
        if not file_type_config or not file_type_config['magic_numbers']:
            return True
        
        # Check if file starts with any of the expected magic numbers
        valid_signature = False
        for magic_number in file_type_config['magic_numbers']:
            if file_data.startswith(magic_number):
                valid_signature = True
                break
        
        if not valid_signature:
            expected_sigs = [sig.hex() for sig in file_type_config['magic_numbers']]
            actual_sig = file_data[:8].hex()
            raise ValueError(f"Invalid {file_ext} file signature. Expected one of {expected_sigs}, got {actual_sig}")
        
        return True

    @staticmethod
    def _validate_excel_headers(first_row_cells):
        """Validate that Excel file has proper column headers (SRP: Header validation)"""
        if not first_row_cells:
            raise ValueError("File must contain column headers in the first row")
        
        # Extract header values
        headers = [cell[1].strip().lower() if isinstance(cell[1], str) else str(cell[1]).strip().lower() 
                  for cell in first_row_cells if cell[1]]
        
        if not headers:
            raise ValueError("File headers cannot be empty")
        
        # Check for minimum number of headers
        if len(headers) < 1:
            raise ValueError("File must have at least one column header")
        
        # Check for proper column headers (should contain at least some meaningful text)
        meaningful_headers = [h for h in headers if len(h) > 1 and not h.isdigit() and h not in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']]
        if len(meaningful_headers) == 0:
            raise ValueError("File must contain meaningful column headers (not just numbers or single characters)")
        
        # Headers should look like column names, not code
        malicious_header_patterns = [
            r'^import\s+\w+$',  # Python imports as headers
            r'^from\s+\w+\s+import',  # From imports
            r'^def\s+\w+\s*\(',  # Function definitions
            r'^class\s+\w+',  # Class definitions
            r'^#!/',  # Shebang lines
            r'^<script',  # Script tags
            r'^<\?php',  # PHP tags
            r'^\s*(rm|mv|cp|dd|sudo|su)\s+',  # Shell commands
            r'^\s*(exec|eval|system)\s*\(',  # Execution functions
            r'^select\s+\*\s+from',  # SQL queries
            r'^drop\s+table',  # SQL drops
        ]
        
        # Check each header for malicious patterns
        for header in headers:
            for pattern in malicious_header_patterns:
                if re.search(pattern, header, re.IGNORECASE):
                    raise ValueError(f"Invalid header detected: '{header}' looks like executable code, not a column name")
        
        # Headers should be reasonable length (not huge code blocks)
        for header in headers:
            if len(header) > 100:  # Reasonable header length limit
                raise ValueError(f"Header too long: '{header[:50]}...' (max 100 characters)")
        
        # Check for suspiciously repetitive patterns (like code)
        if len(headers) > 5:
            # If most headers start with same pattern, might be code
            import_count = sum(1 for h in headers if h.startswith(('import ', 'from ')))
            if import_count > len(headers) * 0.5:  # More than 50% are imports
                raise ValueError("Too many import-like headers detected - this looks like code, not data headers")

class CSVImportController(http.Controller):
    UPLOAD_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "media", "uploads"
    )

    def __init__(self):
        super(CSVImportController, self).__init__()
        # SECURITY: Create upload directory with secure permissions
        if not os.path.exists(self.UPLOAD_DIR):
            os.makedirs(self.UPLOAD_DIR, mode=0o700)  # Owner access only
        else:
            # Ensure existing directory has secure permissions
            try:
                os.chmod(self.UPLOAD_DIR, 0o700)
            except OSError:
                _logger.warning(f"Could not set secure permissions on upload directory: {self.UPLOAD_DIR}")

    # @http.route("/csv_import/upload", type="http", auth="user")
    # def csv_upload_page(self):
    #     """Render the CSV upload page"""
    #     return request.render("csv_import.csv_import_upload_form", {})

    @http.route("/csv_import/get_import_models", type="json", auth="user")
    def get_import_models(self, search_term=None, limit=50, offset=0):
        """Get available models for import directly from ir.model"""
        try:
            self._send_message("Fetching available models from database...", "info")

            # WHITELISTED MODELS: Only allow specific screening list models for import
            # This restricts the upload functionality to only sanctioned/screening list models
            allowed_models = [
                "res.partner.greylist",      # Grey List - Suspicious entities
                "res.partner.watchlist",     # Watch List - Entities under monitoring
                "res.pep",                   # Politically Exposed Persons
                "pep.list",                  # PEP List - Additional PEP data
                "res.partner.blacklist",     # Black List - Sanctioned/banned entities
            ]

            # Build domain with whitelist filter
            domain = [
                ("transient", "=", False),
                ("model", "in", allowed_models),  # Only allow whitelisted models
            ]

            # PREVIOUS IMPLEMENTATION (COMMENTED OUT):
            # The code below allowed ALL non-system models to be imported, which exposed
            # too many internal models to users. This has been restricted to only screening
            # list models for security and user experience improvements.
            #
            # domain = [
            #     ("transient", "=", False),
            #     ("model", "not ilike", "ir.%"),      # Excluded infrastructure models
            #     ("model", "not ilike", "base.%"),    # Excluded base system models
            #     ("model", "not ilike", "bus.%"),     # Excluded bus/messaging models
            #     ("model", "not ilike", "base_%"),    # Excluded base-prefixed models
            # ]

            # Apply search term filter if provided
            if search_term:
                domain += [
                    "|",
                    ("name", "ilike", search_term),
                    ("model", "ilike", search_term),
                ]
            _logger.info(f"Searching ir.model with domain: {domain}")
            total_count = request.env["ir.model"].sudo().search_count(domain)
            fields_to_fetch = ["id", "name", "model"]
            if "description" in request.env["ir.model"]._fields:
                fields_to_fetch.append("description")
            ir_models = (
                request.env["ir.model"]
                .sudo()
                .search_read(
                    domain=domain,
                    fields=fields_to_fetch,
                    limit=limit,
                    offset=offset,
                    order="name",
                )
            )
            models = []
            for ir_model in ir_models:
                model_name = ir_model["model"]
                if model_name in request.env:
                    try:
                        model_obj = request.env[model_name].sudo()
                        if model_obj._abstract or not model_obj._table:
                            continue
                        try:
                            request.env.cr.execute(
                                f"""
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables 
                                    WHERE table_name = %s
                                )
                            """,
                                (model_obj._table,),
                            )
                            table_exists = request.env.cr.fetchone()[0]
                            if not table_exists:
                                continue
                        except Exception as e:
                            _logger.debug(
                                f"Skipping model {model_name}, table check failed: {str(e)}"
                            )
                            continue
                        description = (
                            ir_model.get("description", False)
                            or f"Import data into {ir_model['name']}"
                        )
                        models.append(
                            {
                                "id": ir_model["id"],
                                "name": ir_model["name"],
                                "model_name": model_name,
                                "description": description,
                                "template_filename": f"{model_name.replace('.', '_')}_template.xlsx",
                            }
                        )
                    except Exception as e:
                        _logger.debug(f"Skipping model {model_name}: {str(e)}")
                        continue
            self._send_message(
                f"Loaded {len(models)} available models for import", "success"
            )
            return {"models": models, "total": len(models)}
        except Exception as e:
            error_msg = f"Error loading import models: {str(e)}"
            _logger.exception(error_msg)
            self._send_message(error_msg, "error")
            return {"models": [], "total": 0, "error": error_msg}

    @http.route("/csv_import/get_model_fields", type="json", auth="user")
    def get_model_fields(self, model_id):
        """Get importable fields for a specific model"""
        try:
            ir_model = request.env["ir.model"].sudo().browse(int(model_id))
            if not ir_model.exists():
                self._send_message(
                    f"Error: Model with ID {model_id} not found", "error"
                )
                return {"error": "Model not found"}
            model_name = ir_model.model
            if model_name not in request.env:
                return {"error": f"Model {model_name} is not accessible"}
            self._send_message(f"Getting fields for model: {ir_model.name}", "info")
            model_obj = request.env[model_name]
            importable_fields = []
            required_fields = []
            for field_name, field in model_obj._fields.items():
                if (
                    not field.store
                    or field.type in ["many2many", "one2many", "binary", "reference"]
                    or (field.compute and not field.inverse)
                ):
                    continue
                field_info = {
                    "name": field_name,
                    "string": field.string,
                    "type": field.type,
                    "required": field.required,
                    "relation": (
                        field.comodel_name
                        if field.type in ["many2one", "many2many"]
                        else False
                    ),
                }
                importable_fields.append(field_info)
                if field.required:
                    required_fields.append(field_name)
            self._send_message(
                f"Loaded {len(importable_fields)} fields for model {ir_model.name}",
                "success",
            )
            return {
                "fields": importable_fields,
                "required_fields": required_fields,
            }
        except Exception as e:
            error_msg = f"Error getting model fields: {str(e)}"
            _logger.exception(error_msg)
            self._send_message(error_msg, "error")
            return {"error": error_msg}

    # Duplicate method removed - keeping only the first implementation

    @http.route("/csv_import/get_table_columns", type="json", auth="user")
    def get_table_columns(self, model_id):
        """Get columns for a specific model table"""
        try:
            ir_model = request.env["ir.model"].sudo().browse(int(model_id))
            if not ir_model.exists():
                self._send_message(f"Error: Model with ID {model_id} not found", "error")
                return {"error": "Model not found"}
                
            model_name = ir_model.model
            if model_name not in request.env:
                return {"error": f"Model {model_name} is not accessible"}
                
            self._send_message(f"Getting columns for table: {ir_model.name}", "info")
            model_obj = request.env[model_name]
            
            columns = []
            for field_name, field in model_obj._fields.items():
                # Skip non-storable fields, computed fields without inverse, and complex relation fields
                if (not field.store or 
                    field.type in ["many2many", "one2many", "binary", "reference"] or
                    (field.compute and not field.inverse)):
                    continue
                    
                columns.append({
                    "name": field_name,
                    "string": field.string,
                    "type": field.type,
                    "required": field.required,
                    "relation": field.comodel_name if field.type == "many2one" else False,
                })
                    
            self._send_message(f"Loaded {len(columns)} columns for table {ir_model.name}", "success")
            return {
                "columns": columns,
            }
        except Exception as e:
            error_msg = f"Error getting model columns: {str(e)}"
            _logger.exception(error_msg)
            self._send_message(error_msg, "error")
            return {"error": error_msg}

    @http.route(
        "/csv_import/upload_chunk",
        type="http",
        auth="user",
        methods=["POST"],
        csrf=False,
    )
    def upload_chunk(self, **post):
        """Handle chunked file uploads with comprehensive security validation"""
        try:
            chunk_number = request.httprequest.headers.get("X-Chunk-Number")
            total_chunks = request.httprequest.headers.get("X-Total-Chunks")
            file_id = request.httprequest.headers.get("X-File-Id")
            original_filename = request.httprequest.headers.get("X-Original-Filename")
            model_id = request.httprequest.headers.get("X-Model-Id")
            
            # SECURITY: Validate filename first (before any processing)
            try:
                if original_filename:
                    sanitized_filename = FileSecurityValidator.validate_filename(original_filename)
                    _logger.info(f"Processing chunk {chunk_number}/{total_chunks} for {sanitized_filename}")
                else:
                    return Response(
                        json.dumps({"error": "Filename is required for security validation"}),
                        content_type="application/json",
                        status=400,
                    )
            except Exception as e:
                _logger.warning(f"Security validation failed for filename '{original_filename}': {str(e)}")
                
                # Customize error message based on validation failure type
                error_message = str(e)
                if "appears to be empty or corrupted" in error_message:
                    user_message = "Upload failed: Empty/Invalid file content"
                elif "must contain meaningful column headers" in error_message:
                    user_message = "Upload failed: Empty/Invalid file content."
                elif "Script execution detected" in error_message or "malicious content" in error_message.lower():
                    user_message = "Upload failed: Empty/Invalid file content."
                elif "Invalid file extension" in error_message or "not allowed" in error_message:
                    user_message = "Upload failed: File type not supported. Please upload a CSV or Excel file."
                elif "File size exceeds" in error_message:
                    user_message = "Upload failed: File size is too large. Please reduce file size and try again."
                elif "Invalid" in error_message and ("CSV" in error_message or "Excel" in error_message):
                    user_message = "Upload failed: File format is invalid or corrupted. Please check your file and try again."
                else:
                    # Fallback for any other validation errors
                    user_message = f"Upload failed: {error_message}"
                
                return Response(
                    json.dumps({"error": user_message}),
                    content_type="application/json",
                    status=400,
                )
                
            if not all([chunk_number, total_chunks, file_id, model_id]):
                return Response(
                    json.dumps({"error": "Missing required headers"}),
                    content_type="application/json",
                    status=400,
                )
            try:
                chunk_number = int(chunk_number)
                total_chunks = int(total_chunks)
                model_id = int(model_id)
            except ValueError:
                return Response(
                    json.dumps({"error": "Invalid number format in headers"}),
                    content_type="application/json",
                    status=400,
                )
            ir_model = request.env["ir.model"].sudo().browse(model_id)
            if not ir_model.exists():
                return Response(
                    json.dumps({"error": f"Invalid model ID: {model_id}"}),
                    content_type="application/json",
                    status=400,
                )
            chunk_data = None
            if "chunk" in request.httprequest.files:
                chunk_file = request.httprequest.files["chunk"]
                chunk_data = chunk_file.read()
                _logger.info(f"Got chunk from files, size: {len(chunk_data)} bytes")
            elif "chunk" in request.httprequest.form:
                chunk_data = request.httprequest.form["chunk"]
                if isinstance(chunk_data, str):
                    try:
                        chunk_data = base64.b64decode(chunk_data)
                        _logger.info(
                            f"Decoded base64 chunk, size: {len(chunk_data)} bytes"
                        )
                    except:
                        _logger.warning("Failed to decode base64, treating as raw data")
            elif request.httprequest.data:
                chunk_data = request.httprequest.data
                _logger.info(f"Got chunk from raw data, size: {len(chunk_data)} bytes")
            if not chunk_data:
                _logger.error("No chunk data found in request")
                return Response(
                    json.dumps({"error": "No chunk file provided"}),
                    content_type="application/json",
                    status=400,
                )
            temp_dir = tempfile.gettempdir()
            chunk_dir = os.path.join(temp_dir, "odoo_csv_import", file_id)
            os.makedirs(chunk_dir, exist_ok=True)
            chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_number}")
            with open(chunk_path, "wb") as f:
                if isinstance(chunk_data, str):
                    f.write(chunk_data.encode("utf-8"))
                else:
                    f.write(chunk_data)
            self._send_message(
                f"Successfully saved chunk {chunk_number + 1} of {total_chunks}",
                "success",
            )
            if chunk_number == total_chunks - 1:
                self._send_message(
                    "Final chunk received. Starting file reassembly and security validation...", "info"
                )
                return self._handle_final_chunk(
                    file_id, total_chunks, sanitized_filename, ir_model, chunk_dir
                )
            return Response(
                json.dumps(
                    {
                        "status": "success",
                        "message": "Chunk received successfully",
                        "chunk_number": chunk_number,
                    }
                ),
                content_type="application/json",
            )
        except Exception as e:
            _logger.error(f"Unexpected error in upload_chunk: {str(e)}")

            _logger.error(f"Traceback: {traceback.format_exc()}")
            try:
                self._send_message(f"Error processing chunk: {str(e)}", "error")
            except:
                pass
            return Response(
                json.dumps({"error": f"Server error: {str(e)}"}),
                content_type="application/json",
                status=500,
            )

    def _handle_final_chunk(self, file_id, total_chunks, original_filename, ir_model, chunk_dir):
        """
        Process the final chunk with improved transaction management and consistent handling for all operations
        """
        final_path = None
        try:
            # File processing steps (consistent for all modes)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            name, ext = os.path.splitext(original_filename)
            unique_filename = f"{name}_{timestamp}_{random_suffix}{ext}"
            batch_folder = f"batch_{ir_model.model}_{timestamp}"
            batch_dir = os.path.join(self.UPLOAD_DIR, batch_folder)
            os.makedirs(batch_dir, exist_ok=True)
            final_path = os.path.join(batch_dir, unique_filename)
            
            # Assemble file from chunks - CRITICAL: This must complete before any processing
            self._send_message("Assembling file from chunks...", "info")
            assembled_data = b''
            
            # Assemble chunks in memory first for security validation
            for i in range(total_chunks):
                chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
                if not os.path.exists(chunk_path):
                    self._send_message(f"Missing chunk {i}", "error")
                    return Response(
                        json.dumps({"error": f"Missing chunk {i}"}),
                        content_type="application/json",
                        status=500,
                    )
                with open(chunk_path, "rb") as infile:
                    assembled_data += infile.read()
            
            # SECURITY: Comprehensive file validation before saving to disk
            try:
                self._send_message("Performing security validation...", "info")
                
                # Validate file content and structure
                FileSecurityValidator.validate_file_content(assembled_data, original_filename)
                
                # Validate MIME type matches file extension  
                FileSecurityValidator.validate_mime_type(assembled_data, original_filename)
                
                self._send_message("Security validation passed", "success")
                
            except Exception as e:
                # Customize websocket message based on validation failure type
                error_message = str(e)
                if "appears to be empty or corrupted" in error_message:
                    websocket_message = "Upload failed: Empty/Invalid file content"
                elif "must contain meaningful column headers" in error_message:
                    websocket_message = "Upload failed: Empty/Invalid file content"
                elif "Script execution detected" in error_message or "malicious content" in error_message.lower():
                    websocket_message = "Upload failed: Empty/Invalid file content"
                elif "Invalid file extension" in error_message or "not allowed" in error_message:
                    websocket_message = "Upload failed: File type not supported"
                elif "File size exceeds" in error_message:
                    websocket_message = "Upload failed: File size is too large"
                elif "Invalid" in error_message and ("CSV" in error_message or "Excel" in error_message):
                    websocket_message = "Upload failed: Empty/Invalid file content"
                else:
                    # Fallback for any other validation errors
                    websocket_message = f"Upload failed: {error_message}"
                
                self._send_message(websocket_message, "error")
                _logger.warning(f"File rejected during security validation: {original_filename} - {str(e)}")
                
                # Clean up chunks
                try:
                    self._cleanup_chunks(chunk_dir, total_chunks)
                except:
                    pass
                    
                return Response(
                    json.dumps({
                        # "error": f"File rejected by security validation"
                        "error": f"{str(e)}"
                    }),
                    content_type="application/json",
                    status=400,
                )
            
            # If validation passes, write to disk with secure permissions
            with open(final_path, "wb") as outfile:
                outfile.write(assembled_data)
            
            # SECURITY: Set restrictive file permissions (owner read/write only)
            try:
                os.chmod(final_path, 0o600)
            except OSError as e:
                _logger.warning(f"Could not set secure file permissions: {str(e)}")
            
            # Clear sensitive data from memory
            assembled_data = None
            
            file_size = os.path.getsize(final_path)
            self._send_message(
                f"File successfully validated and saved: {self._format_bytes(file_size)}",
                "success",
            )
            
            # Determine content type
            content_type = "text/csv"
            if original_filename.lower().endswith((".xlsx", ".xls")):
                content_type = (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            # Get operation mode parameters
            delete_mode = request.httprequest.headers.get("X-Delete-Mode") == "true"
            unique_identifier_field = request.httprequest.headers.get("X-Unique-Identifier", "")

            import_log_id = None
            
            # Create import log record with complete file information
            with request.env.registry.cursor() as new_cr:
                try:
                    env = api.Environment(new_cr, request.env.uid, request.env.context)
                    with open(final_path, "rb") as infile:
                        file_content = base64.b64encode(infile.read())
                    
                    import_log = (
                        env["import.log"]
                        .sudo()
                        .create(
                            {
                                "name": f"Import {unique_filename}",
                                "file_name": unique_filename,
                                "original_filename": original_filename,
                                "content_type": content_type,
                                "ir_model_id": ir_model.id,
                                "file": file_content,
                                "status": "pending",
                                "batch_folder": batch_folder,
                                "file_path": final_path,
                                "uploaded_by": request.env.user.id,
                                # Delete mode parameters
                                "delete_mode": delete_mode,
                                "unique_identifier_field": unique_identifier_field if delete_mode else False,
                            }
                        )
                    )
                    import_log_id = import_log.id
                    new_cr.commit()
                    self._send_message(
                        "File successfully uploaded and saved", "success"
                    )
                except Exception as e:
                    new_cr.rollback()
                    raise e
            
            # Clean up temporary chunks after successful file assembly
            self._cleanup_chunks(chunk_dir, total_chunks)
            
            # Now start the appropriate processing - BOTH modes now wait for complete file assembly
            return self._start_processing_after_assembly(
                import_log_id, 
                delete_mode, 
                unique_filename, 
                final_path,
                ir_model
            )
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            error_message = f"Error handling final chunk: {str(e)}"
            
            _logger.error(error_message)
            _logger.error(error_trace)
            
            self._send_message(f"Error processing file: {str(e)}", "error")
            
            # Clean up on error
            if final_path and os.path.exists(final_path):
                try:
                    os.unlink(final_path)
                except:
                    pass
                    
            return Response(
                json.dumps({"error": error_message}),
                content_type="application/json",
                status=500,
            )
            
    def _cleanup_chunks(self, chunk_dir, total_chunks):
        """Clean up temporary chunk files after successful file assembly"""
        try:
            for i in range(total_chunks):
                chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
                if os.path.exists(chunk_path):
                    os.unlink(chunk_path)
            
            # Try to remove the chunk directory
            try:
                os.rmdir(chunk_dir)
            except OSError:
                # Directory not empty or other issue, but that's okay
                pass
                
            self._send_message("Cleaned up temporary chunk files", "info")
            
        except Exception as e:
            _logger.warning(f"Error cleaning up chunks: {str(e)}")
            # Don't fail the main process for cleanup issues

    def _start_processing_after_assembly(self, import_log_id, delete_mode, unique_filename, final_path, ir_model):
        """Start the appropriate processing after complete file assembly"""
        
        # Check if queue_job is available
        use_queue = False
        try:
            queue_job_installed = (
                request.env["ir.module.module"]
                .sudo()
                .search(
                    [("name", "=", "queue_job"), ("state", "=", "installed")],
                    limit=1,
                )
            )
            use_queue = bool(queue_job_installed)
        except Exception as e:
            _logger.warning(f"Error checking for queue_job module: {str(e)}")
        
        # Process based on operation mode
        if delete_mode:
            return self._start_archive_operation(
                import_log_id, 
                use_queue, 
                unique_filename, 
                ir_model
            )
        else:
            return self._start_import_operation(
                import_log_id, 
                use_queue, 
                unique_filename, 
                final_path
            )

    def _start_archive_operation(self, import_log_id, use_queue, unique_filename, ir_model):
        """Start archive/deletion operation with proper job handling"""
        
        with request.env.registry.cursor() as proc_cr:
            env = api.Environment(proc_cr, request.env.uid, request.env.context)
            import_log = env["import.log"].sudo().browse(import_log_id)
            
            self._send_message("Processing in archive mode...", "info")
            
            if use_queue and hasattr(import_log, "with_delay"):
                self._send_message("Queueing archive operation as background job...", "info")
                job = import_log.with_delay(
                    description=f"Archive from {ir_model.model} using {import_log.unique_identifier_field}",
                    channel="csv_import",
                    priority=15
                ).queue_delete_operation()
                proc_cr.commit()
                
                return Response(
                    json.dumps({
                        "status": "success",
                        "import_id": import_log_id,
                        "message": "File upload complete, archive operation queued",
                        "mode": "delete",
                        "filename": unique_filename
                    }),
                    content_type="application/json"
                )
            else:
                # Process directly without queueing
                self._send_message("Starting archive operation directly...", "info")
                try:
                    # Use a separate thread to avoid blocking the request
                    import threading
                    thread = threading.Thread(
                        target=lambda: import_log.queue_delete_operation()
                    )
                    thread.start()
                    
                    # Return response immediately without waiting for completion
                    return Response(
                        json.dumps({
                            "status": "success",
                            "import_id": import_log_id,
                            "message": "File upload complete, archive operation started",
                            "mode": "delete",
                            "filename": unique_filename
                        }),
                        content_type="application/json"
                    )
                except Exception as e:
                    _logger.error(f"Error starting archive operation: {str(e)}")
                    return Response(
                        json.dumps({
                            "status": "warning",
                            "import_id": import_log_id,
                            "message": "File upload complete, but archive operation could not be started automatically. Please start it manually.",
                            "error": str(e),
                            "mode": "delete",
                            "filename": unique_filename
                        }),
                        content_type="application/json"
                    )

    def _start_import_operation(self, import_log_id, use_queue, unique_filename, final_path):
        """Start regular import operation with proper job handling"""
        
        with request.env.registry.cursor() as proc_cr:
            env = api.Environment(proc_cr, request.env.uid, request.env.context)
            import_log = env["import.log"].sudo().browse(import_log_id)
            
            if use_queue and hasattr(import_log, "with_delay"):
                self._send_message("Queueing file for batch processing...", "info")
                try:
                    job = (
                        import_log.sudo()
                        .with_delay(
                            description=f"Process CSV Import {import_log_id}",
                            channel="csv_import",
                        )
                        .process_file()
                    )
                    proc_cr.commit()
                    self._send_message(f"Import job queued for processing", "success")
                    return Response(
                        json.dumps(
                            {
                                "status": "success",
                                "import_id": import_log_id,
                                "message": "File upload complete, processing queued",
                                "filename": unique_filename,
                                "file_path": final_path,
                            }
                        ),
                        content_type="application/json",
                    )
                except Exception as e:
                    proc_cr.rollback()
                    _logger.error(f"Error creating queue job: {str(e)}")
                    use_queue = False
                    
            if not use_queue:
                try:
                    self._send_message(
                        "Starting direct processing (no job queue)...", "info"
                    )
                    
                    # Use threading to avoid blocking the response for large files
                    import threading
                    
                    def process_import():
                        """Process import in background thread"""
                        try:
                            # Create a new cursor for the background processing
                            with request.env.registry.cursor() as bg_cr:
                                bg_env = api.Environment(bg_cr, request.env.uid, request.env.context)
                                bg_import_log = bg_env["import.log"].sudo().browse(import_log_id)
                                result = bg_import_log.process_file()
                                bg_cr.commit()
                                
                                # Log the result
                                if result.get("success", False):
                                    _logger.info(f"Import {import_log_id} completed successfully")
                                else:
                                    _logger.warning(f"Import {import_log_id} completed with warnings: {result.get('message', 'No message')}")
                        except Exception as bg_e:
                            _logger.error(f"Error in background import processing: {str(bg_e)}")
                    
                    # Start background processing
                    thread = threading.Thread(target=process_import)
                    thread.daemon = True  # Thread will not prevent program from exiting
                    thread.start()
                    
                    # Return immediate response
                    return Response(
                        json.dumps(
                            {
                                "status": "success",
                                "import_id": import_log_id,
                                "message": "File upload complete, processing started in background",
                                "filename": unique_filename,
                                "file_path": final_path,
                            }
                        ),
                        content_type="application/json",
                    )
                except Exception as e:
                    proc_cr.rollback()
                    _logger.error(f"Error starting direct processing: {str(e)}")
                    return Response(
                        json.dumps(
                            {
                                "status": "warning",
                                "import_id": import_log_id,
                                "message": "File upload complete. Please start processing manually.",
                                "error": str(e),
                                "filename": unique_filename,
                                "file_path": final_path,
                            }
                        ),
                        content_type="application/json",
                    )
           
    @http.route("/csv_import/start_import", type="json", auth="user")
    def start_import(self, import_id):
        """Start the import process with proper transaction management"""
        import_id = int(import_id)
        with request.env.registry.cursor() as cr:
            try:
                env = api.Environment(cr, request.env.uid, request.env.context)
                import_log = env["import.log"].sudo().browse(import_id)
                if not import_log.exists():
                    return {"success": False, "error": "Import not found"}
                result = import_log.process_file()
                cr.commit()
                return result
            except Exception as e:
                cr.rollback()
                _logger.error(f"Error starting import: {str(e)}")
                return {"success": False, "error": str(e)}
    
    @http.route("/csv_import/start_delete", type="json", auth="user")
    def start_delete_operation(self, import_id):
        """Start or resume an archive operation for an import"""
        import_id = int(import_id)
        
        try:
            import_log = request.env["import.log"].sudo().browse(import_id)
            if not import_log.exists():
                return {"success": False, "error": "Import not found"}
                
            if not import_log.delete_mode:
                return {"success": False, "error": "This import is not configured for archive mode"}
            
            # Always process directly to avoid threading issues
            self._send_message("Starting archive operation directly...", "info")
            result = import_log.queue_delete_operation()
            return {
                "success": True,
                "message": "Archive operation executed directly",
                "result": result
            }
        except Exception as e:
            _logger.error(f"Error starting archive operation: {str(e)}")
            return {"success": False, "error": str(e)}

    @http.route(
        "/csv_import/download_template/<int:model_id>", type="http", auth="user"
    )
    def download_template(self, model_id, **kw):
        """Download a template file for any model"""
        try:
            ir_model = request.env["ir.model"].sudo().browse(int(model_id))
            if not ir_model.exists():
                return Response(
                    json.dumps({"error": "Model not found"}),
                    content_type="application/json",
                    status=404,
                )
            model_name = ir_model.model
            if model_name not in request.env:
                return Response(
                    json.dumps({"error": f"Model {model_name} is not accessible"}),
                    content_type="application/json",
                    status=400,
                )
            self._send_message(f"Generating template for {ir_model.name}...", "info")
            fields_result = self.get_model_fields(model_id)
            if "error" in fields_result:
                return Response(
                    json.dumps({"error": fields_result["error"]}),
                    content_type="application/json",
                    status=500,
                )
            fields = fields_result["fields"]
            content = self._generate_template(ir_model, fields)
            if not content:
                return Response(
                    json.dumps({"error": "Could not generate template"}),
                    content_type="application/json",
                    status=500,
                )
            filename = f"{model_name.replace('.', '_')}_template.xlsx"
            self._send_message(f"Downloading template for {ir_model.name}...", "info")
            return request.make_response(
                content,
                headers=[
                    (
                        "Content-Type",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    ),
                    ("Content-Disposition", f'attachment; filename="{filename}"'),
                ],
            )
        except Exception as e:
            _logger.error(f"Error downloading template: {str(e)}")
            self._send_message(f"Error generating template: {str(e)}", "error")
            return Response(
                json.dumps({"error": f"Error downloading template: {str(e)}"}),
                content_type="application/json",
                status=500,
            )

    def _generate_template(self, ir_model, fields):
        """Generate a template XLSX file for any model"""
        try:
            output = BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet(
                ir_model.name[:31] 
            )

            header_format = workbook.add_format({"bold": True, "bg_color": "#E6E6E6"})
            for col, field in enumerate(fields):
                worksheet.write(0, col, field["string"], header_format)
                worksheet.set_column(col, col, max(len(field["string"]), 15))

            help_format = workbook.add_format({"italic": True, "font_color": "#808080"})
            for col, field in enumerate(fields):
                info = f"{field['name']} ({field['type']})"
                if field["required"]:
                    info += " (Required)"
                worksheet.write(1, col, info, help_format)

            sample_format = workbook.add_format({"font_color": "#0070C0"})
            for col, field in enumerate(fields):
                sample_value = self._get_sample_value(field)
                worksheet.write(2, col, sample_value, sample_format)

            for col, field in enumerate(fields):
                if field["type"] == "boolean":
                    worksheet.data_validation(
                        3,
                        col,
                        1000,
                        col,
                        {
                            "validate": "list",
                            "source": ["TRUE", "FALSE", "Yes", "No", "1", "0"],
                        },
                    )
                elif field["type"] == "selection":
                    pass
                elif field["type"] == "date":
                    worksheet.data_validation(
                        3,
                        col,
                        1000,
                        col,
                        {
                            "validate": "date",
                            "criteria": "between",
                            "minimum": "1900-01-01",
                            "maximum": "2100-12-31",
                        },
                    )

            worksheet.freeze_panes(1, 0)

            workbook.close()
            content = output.getvalue()
            output.close()

            return content

        except Exception as e:
            _logger.error(f"Error generating template: {str(e)}")
            return None

    def _get_sample_value(self, field):
        """Get a sample value for a field based on its type"""
        field_type = field["type"]
        if field_type == "char":
            return "Sample Text"
        elif field_type == "text":
            return "Sample longer text content"
        elif field_type == "integer":
            return 42
        elif field_type == "float":
            return 42.5
        elif field_type == "monetary":
            return 100.00
        elif field_type == "date":
            return datetime.now().strftime("%Y-%m-%d")
        elif field_type == "datetime":
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elif field_type == "boolean":
            return "Yes"
        elif field_type == "many2one":
            return "External ID or Database ID"
        elif field_type == "selection":
            return "Selection Value"
        else:
            return ""

    def _format_bytes(self, size):
        """Format bytes to human readable format"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"

    def _send_message(self, message, message_type="info"):
        """Send log message to frontend and log to server with safe error handling"""
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)
        log_level(f"[{message_type.upper()}] {message}")
        try:
            send_message(request.env, message, message_type, request.env.user.id)
        except Exception as e:
            _logger.warning(f"Failed to send log message: {str(e)}")

    @http.route("/csv_import/ws_status", type="json", auth="user")
    def get_websocket_status(self, **kw):
        from ..services.websocket.manager import get_server_status

        status = get_server_status()

        status["port_test"] = False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect(("localhost", int(status["port"])))
            s.close()
            status["port_test"] = True
        except:
            pass
        return status

    @http.route("/csv_import/start_ws_server", type="json", auth="user")
    def start_websocket_server(self, **kw):
        from ..services.websocket.manager import start_websocket_server

        return {"success": start_websocket_server()}
