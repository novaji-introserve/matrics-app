# -*- coding: utf-8 -*-

import unittest
import logging
from unittest.mock import Mock, patch
from ..services.security_service import SecurityService

_logger = logging.getLogger(__name__)

class TestSQLInjectionProtection(unittest.TestCase):
    """Test cases for SQL injection protection mechanisms."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.security_service = SecurityService()
    
    def test_malicious_sql_queries_blocked(self):
        """Test that malicious SQL queries are blocked."""
        
        # Test cases with malicious SQL patterns
        malicious_queries = [
            "SELECT * FROM users; DROP TABLE users; --",
            "SELECT * FROM users WHERE id = 1 OR 1=1",
            "SELECT * FROM users UNION SELECT * FROM passwords",
            "SELECT * FROM users; EXEC xp_cmdshell('dir'); --",
            "SELECT pg_sleep(10)",
            "SELECT * FROM users WHERE name = 'admin' AND password = '' OR 'x'='x'",
            "SELECT * FROM users; waitfor delay '00:00:10'; --",
            "SELECT * FROM users WHERE id IN (SELECT COUNT(*) FROM information_schema.tables)",
            "SELECT * FROM users WHERE id = 1; DELETE FROM users WHERE id > 1; --"
        ]
        
        for query in malicious_queries:
            with self.subTest(query=query):
                is_safe, error_msg = self.security_service.validate_sql_query(query)
                self.assertFalse(is_safe, f"Query should be blocked: {query}")
                self.assertIsNotNone(error_msg, "Error message should be provided")
                _logger.info(f"Blocked malicious query: {query[:50]}...")
    
    def test_legitimate_queries_allowed(self):
        """Test that legitimate SQL queries are allowed."""
        
        legitimate_queries = [
            "SELECT COUNT(*) FROM res_partner WHERE active = true",
            "SELECT name, email FROM res_partner WHERE branch_id IN (1, 2, 3)",
            "SELECT * FROM res_compliance_stat WHERE scope = 'risk' ORDER BY id",
            "SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE date >= '2024-01-01'",
            "SELECT DISTINCT category FROM res_compliance_stat WHERE state = 'active'",
            "SELECT p.name, b.name FROM res_partner p JOIN res_branch b ON p.branch_id = b.id"
        ]
        
        for query in legitimate_queries:
            with self.subTest(query=query):
                is_safe, error_msg = self.security_service.validate_sql_query(query)
                self.assertTrue(is_safe, f"Legitimate query should be allowed: {query}")
                _logger.info(f"Allowed legitimate query: {query[:50]}...")
    
    def test_parameter_sanitization(self):
        """Test that SQL parameters are properly sanitized."""
        
        test_cases = [
            ("'; DROP TABLE users; --", "&#x27;; DROP TABLE users; --"),
            ("admin' OR '1'='1", "admin&#x27; OR &#x27;1&#x27;=&#x27;1"),
            ('<script>alert("xss")</script>', '&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;'),
            ("normal_string", "normal_string"),
            (123, 123),
            ([1, 2, 3], [1, 2, 3])
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = self.security_service.sanitize_sql_parameter(input_val)
                if isinstance(expected, str):
                    self.assertEqual(result, expected)
                else:
                    self.assertEqual(result, expected)
                _logger.info(f"Sanitized: {input_val} -> {result}")
    
    def test_time_based_injection_detection(self):
        """Test detection of time-based SQL injection attempts."""
        
        time_based_attacks = [
            "SELECT * FROM users WHERE id = 1; SELECT pg_sleep(10); --",
            "SELECT * FROM users WHERE id = 1 AND (SELECT * FROM (SELECT SLEEP(10))a)",
            "SELECT * FROM users WHERE id = 1; waitfor delay '00:00:10'; --",
            "SELECT * FROM users WHERE id = 1 OR benchmark(10000000,SHA1(1))",
            "SELECT * FROM users WHERE id = 1; SELECT dbms_pipe.receive_message('a',10) FROM dual; --"
        ]
        
        for query in time_based_attacks:
            with self.subTest(query=query):
                is_safe, error_msg = self.security_service.validate_sql_query(query)
                self.assertFalse(is_safe, f"Time-based attack should be blocked: {query}")
                self.assertIn("dangerous pattern", error_msg.lower())
                _logger.info(f"Blocked time-based attack: {query[:50]}...")
    
    def test_union_based_injection_detection(self):
        """Test detection of UNION-based SQL injection attempts."""
        
        union_attacks = [
            "SELECT name FROM users WHERE id = 1 UNION SELECT password FROM admin",
            "SELECT * FROM products WHERE id = 1 UNION ALL SELECT * FROM users",
            "SELECT name FROM users WHERE id = 1 UNION SELECT NULL, username, password FROM admin"
        ]
        
        for query in union_attacks:
            with self.subTest(query=query):
                is_safe, error_msg = self.security_service.validate_sql_query(query)
                self.assertFalse(is_safe, f"UNION attack should be blocked: {query}")
                _logger.info(f"Blocked UNION attack: {query[:50]}...")
    
    def test_safe_where_condition_building(self):
        """Test safe WHERE condition building with parameterization."""
        
        test_cases = [
            ("user_id", "=", 123),
            ("name", "LIKE", "%admin%"),
            ("branch_id", "IN", [1, 2, 3]),
            ("status", "!=", "inactive")
        ]
        
        for field, operator, value in test_cases:
            with self.subTest(field=field, operator=operator, value=value):
                try:
                    condition, params = self.security_service.build_safe_where_condition(
                        field, operator, value
                    )
                    self.assertIsNotNone(condition)
                    self.assertIsInstance(params, tuple)
                    _logger.info(f"Built safe condition: {condition} with params: {params}")
                except Exception as e:
                    self.fail(f"Failed to build safe condition: {e}")
    
    def test_invalid_field_names_rejected(self):
        """Test that invalid field names are rejected."""
        
        invalid_fields = [
            "user_id; DROP TABLE users",
            "name' OR '1'='1",
            "../../etc/passwd",
            "user_id UNION SELECT password",
            "field'; EXEC xp_cmdshell('cmd'); --"
        ]
        
        for field in invalid_fields:
            with self.subTest(field=field):
                with self.assertRaises(Exception):
                    self.security_service.build_safe_where_condition(field, "=", "value")
                _logger.info(f"Rejected invalid field name: {field}")
    
    def test_request_data_validation(self):
        """Test request data validation and sanitization."""
        
        # Test malicious request data
        malicious_data = {
            "sql_query": "SELECT * FROM users; DROP TABLE users; --",
            "user_input": "<script>alert('xss')</script>",
            "normal_field": "normal_value"
        }
        
        try:
            # This should raise an exception for the malicious SQL query
            self.security_service.validate_and_sanitize_request_data(malicious_data)
            self.fail("Should have raised exception for malicious SQL query")
        except Exception as e:
            self.assertIn("Invalid query", str(e))
            _logger.info(f"Blocked malicious request data: {str(e)}")
        
        # Test safe request data
        safe_data = {
            "name": "cco",
            "email": "cco",
            "branch_id": 72
        }
        
        try:
            result = self.security_service.validate_and_sanitize_request_data(safe_data)
            self.assertIsInstance(result, dict)
            _logger.info("Allowed safe request data")
        except Exception as e:
            self.fail(f"Safe data should not raise exception: {e}")

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    unittest.main(verbosity=2)
    
    
    
    
    
    
# #!/usr/bin/env python3
# """
# SQL Injection Protection Tests for File Upload System

# This module tests various SQL injection attack vectors that could be attempted
# through file uploads and ensures they are properly blocked by our security measures.
# """

# import unittest
# from unittest.mock import patch
# from ..controllers.csv_import_controller import FileSecurityValidator


# class TestSQLInjectionProtection(unittest.TestCase):
#     """Test protection against SQL injection attacks through file uploads"""
    
#     def test_filename_sql_injection_protection(self):
#         """Test that SQL injection attempts in filenames are blocked"""
#         sql_injection_filenames = [
#             "'; DROP TABLE users; --.csv",
#             "test'; INSERT INTO admin VALUES ('hacker', 'password'); --.csv", 
#             "data'; UPDATE users SET password='hacked' WHERE id=1; --.csv",
#             "file'; DELETE FROM * WHERE 1=1; --.csv",
#             "report' UNION SELECT * FROM passwords --.csv",
#             "data.csv'; EXEC xp_cmdshell('format c:'); --",
#             "test.csv' OR '1'='1",
#             "file.csv\"; DROP DATABASE production; --"
#         ]
        
#         for filename in sql_injection_filenames:
#             with self.subTest(filename=filename):
#                 with self.assertRaises(ValueError) as context:
#                     FileSecurityValidator.validate_filename(filename)
#                 # Should fail due to invalid characters or dangerous patterns
#                 self.assertTrue(
#                     "Invalid characters" in str(context.exception) or
#                     "File type not allowed" in str(context.exception) or
#                     "not allowed" in str(context.exception)
#                 )
    
#     def test_malicious_script_filenames(self):
#         """Test that script file extensions are blocked"""
#         script_files = [
#             "backdoor.php.csv",  # Double extension attack
#             "script.py.csv", 
#             "malware.exe.csv",
#             "trojan.bat.csv",
#             "virus.sh.csv"
#         ]
        
#         for filename in script_files:
#             with self.subTest(filename=filename):
#                 with self.assertRaises(ValueError):
#                     FileSecurityValidator.validate_filename(filename)

#     def test_path_traversal_protection(self):
#         """Test protection against path traversal attacks"""
#         path_traversal_attempts = [
#             "../../../etc/passwd.csv",
#             "..\\..\\windows\\system32\\config\\sam.csv", 
#             "....//....//etc//shadow.csv",
#             "/etc/hosts.csv",
#             "~/.ssh/id_rsa.csv"
#         ]
        
#         for filename in path_traversal_attempts:
#             with self.subTest(filename=filename):
#                 with self.assertRaises(ValueError):
#                     FileSecurityValidator.validate_filename(filename)


# if __name__ == '__main__':
#     unittest.main()
    