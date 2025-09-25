import unittest
import tempfile
import os
from unittest.mock import patch, MagicMock
from ..controllers.csv_import_controller import FileSecurityValidator


class TestFileSecurityValidation(unittest.TestCase):
    """Comprehensive security validation tests"""
    
    def test_validate_filename_allowed_extensions(self):
        """Test that allowed file extensions pass validation"""
        valid_files = [
            'test.csv',
            'data.xlsx', 
            'report.xls',
            'Test File.csv',
            'data-2024.xlsx'
        ]
        
        for filename in valid_files:
            with self.subTest(filename=filename):
                result = FileSecurityValidator.validate_filename(filename)
                self.assertEqual(result, filename)
    
    def test_validate_filename_blocked_extensions(self):
        """Test that dangerous file extensions are blocked"""
        dangerous_files = [
            'malware.exe',
            'script.php',
            'backdoor.py',
            'virus.sh',
            'trojan.bat',
            'malicious.js',
            'evil.dll',
            'hack.zip',
            'bad.rar'
        ]
        
        for filename in dangerous_files:
            with self.subTest(filename=filename):
                with self.assertRaises(ValueError) as context:
                    FileSecurityValidator.validate_filename(filename)
                self.assertIn("File type not allowed", str(context.exception))
    
    def test_validate_filename_path_traversal_protection(self):
        """Test protection against path traversal attacks"""
        path_traversal_attempts = [
            '../../../etc/passwd',
            '..\\..\\windows\\system32\\config\\sam',
            '/etc/shadow',
            'C:\\Windows\\System32\\drivers\\etc\\hosts',
            '....//....//etc//passwd',
            '.htaccess',
            '.ssh/id_rsa'
        ]
        
        for filename in path_traversal_attempts:
            with self.subTest(filename=filename):
                with self.assertRaises(ValueError):
                    FileSecurityValidator.validate_filename(filename)
    
    def test_validate_filename_special_characters(self):
        """Test handling of special characters in filenames"""
        invalid_files = [
            'file\x00.csv',  # Null byte
            'file\x01.csv',  # Control character
            'file\x1f.csv',  # Control character
            '',  # Empty filename
            'a' * 300 + '.csv'  # Too long filename
        ]
        
        for filename in invalid_files:
            with self.subTest(filename=filename):
                with self.assertRaises(ValueError):
                    FileSecurityValidator.validate_filename(filename)
    
    def test_validate_csv_content_valid(self):
        """Test validation of valid CSV content"""
        valid_csv = b"Name,Age,City\nJohn,30,New York\nJane,25,Los Angeles"
        
        # Should not raise any exception
        result = FileSecurityValidator._validate_csv_content(valid_csv)
        self.assertTrue(result)
    
    def test_validate_csv_content_malicious(self):
        """Test detection of malicious content in CSV"""
        malicious_csvs = [
            b'<script>alert("xss")</script>,data,more',
            b'=cmd|"/c calc"!A1,data,more',
            b'<?php system($_GET["cmd"]); ?>,data,more',
            b'<%eval request("cmd")%>,data,more',
            b'javascript:alert("xss"),data,more',
            b'vbscript:msgbox("xss"),data,more'
        ]
        
        for csv_content in malicious_csvs:
            with self.subTest(csv_content=csv_content):
                with self.assertRaises(ValueError) as context:
                    FileSecurityValidator._validate_csv_content(csv_content)
                self.assertIn("potentially malicious content", str(context.exception))
    
    def test_validate_excel_content_valid_xlsx(self):
        """Test validation of valid XLSX file (ZIP format)"""
        # XLSX files start with ZIP signature
        valid_xlsx = b'PK\x03\x04' + b'x' * 100  # Mock XLSX content
        
        result = FileSecurityValidator._validate_excel_content(valid_xlsx, '.xlsx')
        self.assertTrue(result)
    
    def test_validate_excel_content_valid_xls(self):
        """Test validation of valid XLS file (OLE format)"""
        # XLS files start with OLE signature
        valid_xls = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1' + b'x' * 100
        
        result = FileSecurityValidator._validate_excel_content(valid_xls, '.xls')
        self.assertTrue(result)
    
    def test_validate_excel_content_invalid_format(self):
        """Test rejection of invalid Excel file formats"""
        invalid_files = [
            (b'invalid content', '.xlsx'),
            (b'not ole format', '.xls'),
            (b'', '.xlsx'),  # Empty file
            (b'short', '.xls')  # Too short
        ]
        
        for content, ext in invalid_files:
            with self.subTest(content=content[:10], ext=ext):
                with self.assertRaises(ValueError):
                    FileSecurityValidator._validate_excel_content(content, ext)
    
    def test_validate_file_content_size_limit(self):
        """Test file size validation"""
        # Create content larger than 2GB limit
        large_content = b'x' * (FileSecurityValidator.MAX_FILE_SIZE + 1)
        
        with self.assertRaises(ValueError) as context:
            FileSecurityValidator.validate_file_content(large_content, 'test.csv')
        self.assertIn("exceeds maximum limit", str(context.exception))
    
    def test_validate_file_content_empty(self):
        """Test handling of empty file content"""
        with self.assertRaises(ValueError) as context:
            FileSecurityValidator.validate_file_content(b'', 'test.csv')
        self.assertIn("empty", str(context.exception))
    
    @patch('magic.from_buffer')
    def test_validate_mime_type_csv(self, mock_magic):
        """Test MIME type validation for CSV files"""
        mock_magic.return_value = 'text/csv'
        
        csv_content = b'name,age\nJohn,30'
        
        # Should not raise exception
        result = FileSecurityValidator.validate_mime_type(csv_content, 'test.csv')
        self.assertTrue(result)
    
    @patch('magic.from_buffer')
    def test_validate_mime_type_mismatch(self, mock_magic):
        """Test MIME type mismatch detection"""
        mock_magic.return_value = 'application/x-executable'
        
        # Executable MIME type with CSV extension should fail
        malicious_content = b'fake csv content'
        
        with self.assertRaises(ValueError) as context:
            FileSecurityValidator.validate_mime_type(malicious_content, 'fake.csv')
        self.assertIn("does not match expected type", str(context.exception))
    
    def test_integration_malicious_file_rejection(self):
        """Integration test: ensure malicious files are completely rejected"""
        # Test various attack vectors
        attack_vectors = [
            {
                'filename': 'backdoor.php.csv',  # Double extension attack
                'content': b'<?php system($_GET["cmd"]); ?>'
            },
            {
                'filename': '../../etc/passwd.csv',  # Path traversal
                'content': b'root:x:0:0:root:/root:/bin/bash'
            },
            {
                'filename': 'script.csv',
                'content': b'<script>alert("XSS")</script>,data,more'
            }
        ]
        
        for attack in attack_vectors:
            with self.subTest(attack=attack['filename']):
                # At least one validation should fail for each attack
                validation_failed = False
                
                try:
                    FileSecurityValidator.validate_filename(attack['filename'])
                except ValueError:
                    validation_failed = True
                
                try:
                    FileSecurityValidator.validate_file_content(attack['content'], attack['filename'])
                except ValueError:
                    validation_failed = True
                
                self.assertTrue(validation_failed, 
                              f"Security validation should have rejected: {attack['filename']}")


class TestUploadEndpointSecurity(unittest.TestCase):
    """Test security at the HTTP endpoint level"""
    
    def setUp(self):
        """Set up test environment"""
        # These would need to be adapted for actual Odoo testing framework
        pass
    
    def test_chunk_upload_filename_validation(self):
        """Test that chunk upload validates filenames"""
        # This would test the actual HTTP endpoint
        # Implementation depends on testing framework
        pass
    
    def test_chunk_upload_rejects_malicious_files(self):
        """Test that malicious files are rejected at upload"""
        # This would test with actual malicious file uploads
        pass


if __name__ == '__main__':
    unittest.main()