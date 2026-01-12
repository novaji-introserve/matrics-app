import os
import time
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ElasticSearchLogger:
    def __init__(self):
        # Elasticsearch connection settings
        self.elasticsearch_config = {
            "uri": os.getenv("ELASTICSEARCH_URI", "http://172.20.110.75:9200"),
            "default_index": os.getenv("ELASTICSEARCH_INDEX", "icomply-sterling-logs"),
            "username": os.getenv("ELASTICSEARCH_USERNAME", ""),
            "password": os.getenv("ELASTICSEARCH_PASSWORD", ""),
            "enable_ssl": os.getenv("ELASTICSEARCH_ENABLE_SSL", "false").lower() == "true",
            "ignore_ssl_errors": os.getenv("ELASTICSEARCH_IGNORE_SSL_ERRORS", "true").lower() == "true"
        }
        
        # Log file settings
        self.log_file_path = os.getenv("LOG_PATH", "/data/Altbank/ServerLog.log")
        self.last_position = 0
        
        # Initialize Elasticsearch client
        self.es_client = self.initialize_elasticsearch_client()
        
    def initialize_elasticsearch_client(self):
        try:
            client_options = {
                "hosts": [self.elasticsearch_config["uri"]],
                "request_timeout": 30,
                "retry_on_timeout": True
            }

            # Add authentication if provided
            if self.elasticsearch_config["username"] and self.elasticsearch_config["password"]:
                client_options["basic_auth"] = (
                    self.elasticsearch_config["username"],
                    self.elasticsearch_config["password"]
                )

            # Configure SSL settings
            if self.elasticsearch_config["uri"].startswith("https://"):
                client_options["verify_certs"] = not self.elasticsearch_config["ignore_ssl_errors"]

            es = Elasticsearch(**client_options)
            
            # Test connection
            if es.ping():
                print("Connected to Elasticsearch successfully!")
                return es
            else:
                print("Failed to connect to Elasticsearch!")
                return None
                
        except Exception as e:
            print(f"Error connecting to Elasticsearch: {str(e)}")
            return None
            
    def parse_log_line(self, line):
        """Parse an Odoo log line into structured data"""
        try:
            # Basic parsing - you may need to adjust this based on your log format
            parts = line.split(' ', 3)  # Split into timestamp, level, module, message
            
            if len(parts) >= 4:
                timestamp_str = parts[0] + ' ' + parts[1]
                level = parts[2].strip(':')
                message = parts[3].strip()
                
                # Create structured log entry
                log_entry = {
                    "timestamp": timestamp_str,
                    "@timestamp": datetime.now().isoformat(),
                    "level": level,
                    "message": message,
                    "source": "ServerLog.log",
                    "server_ip": "172.20.160.112"  # Using the IP you provided
                }
                return log_entry
            return None
        except Exception as e:
            print(f"Error parsing log line: {str(e)}")
            return None
            
    def ship_to_elasticsearch(self, log_entry):
        """Send log entry to Elasticsearch"""
        if not self.es_client:
            print("Elasticsearch client not initialized")
            return False
            
        try:
            print(f"Sending log to index: {self.elasticsearch_config['default_index']}")
            print(f"Log entry: {json.dumps(log_entry)}")
            
            # Ship to index
            response = self.es_client.index(
                index=self.elasticsearch_config['default_index'],
                document=log_entry
            )
            
            print(f"Elasticsearch response: {response}")
            return True
        except Exception as e:
            print(f"Error shipping log to Elasticsearch: {str(e)}")
            return False
            
    def truncate_log_file(self, max_size_mb=100):
        """Truncate the log file if it exceeds the specified size"""
        try:
            # Check current file size
            file_size_mb = os.path.getsize(self.log_file_path) / (1024 * 1024)
            
            if file_size_mb > max_size_mb:
                print(f"Log file exceeds {max_size_mb}MB, truncating...")
                
                # Read the last 1000 lines (adjust as needed)
                with open(self.log_file_path, 'r') as f:
                    lines = f.readlines()
                    last_lines = lines[-1000:]
                
                # Write only the last lines back to the file
                with open(self.log_file_path, 'w') as f:
                    f.writelines(last_lines)
                    
                # Reset the file position tracker
                self.last_position = os.path.getsize(self.log_file_path)
                
                print(f"Log file truncated to {len(last_lines)} lines")
        except Exception as e:
            print(f"Error truncating log file: {str(e)}")
            
    def monitor_log_file(self):
        """Continuously monitor the log file for new entries"""
        if not os.path.exists(self.log_file_path):
            print(f"Log file not found: {self.log_file_path}")
            return
            
        print(f"Starting to monitor {self.log_file_path}")
        
        # Get initial file position
        self.last_position = os.path.getsize(self.log_file_path)
        
        while True:
            try:
                # Check if file has grown
                current_size = os.path.getsize(self.log_file_path)
                
                if current_size > self.last_position:
                    # Open file and read new lines
                    with open(self.log_file_path, 'r') as f:
                        f.seek(self.last_position)
                        new_lines = f.readlines()
                        
                    # Process each new line
                    for line in new_lines:
                        log_entry = self.parse_log_line(line.strip())
                        if log_entry:
                            self.ship_to_elasticsearch(log_entry)
                            
                    # Update position
                    self.last_position = current_size
                    
                # Check if log file needs truncating to keep it "light"
                self.truncate_log_file()
                    
                # Sleep before next check
                time.sleep(1)
                
            except Exception as e:
                print(f"Error monitoring log file: {str(e)}")
                time.sleep(5)  # Wait longer after an error

# Run as a standalone script
if __name__ == "__main__":
    logger = ElasticSearchLogger()
    
    # Start monitoring the log file
    logger.monitor_log_file()