# -*- coding: utf-8 -*-

import json
import logging
import uuid
import time
import os
from datetime import datetime
from odoo import http, tools
from odoo.http import request, Response
from werkzeug.wrappers import Response as WerkzeugResponse

_logger = logging.getLogger(__name__)

# Enable detailed logging
logging.basicConfig(level=logging.DEBUG)
_logger.setLevel(logging.DEBUG)

# Create a file logger for debugging
log_dir = os.path.expanduser('~/odoo_websocket_logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
file_handler = logging.FileHandler(os.path.join(log_dir, 'websocket_debug.log'))
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)

class DebugWebSocketController(http.Controller):
    """Debug controller with detailed logging"""
    
    @http.route('/ws/test', type='http', auth='public')
    def test_websocket_page(self):
        """Serve a test HTML page for WebSocket testing"""
        _logger.info("Serving WebSocket test page")
        
        html = """<!DOCTYPE html>
<html>
<head>
    <title>WebSocket Test</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        #log { height: 300px; overflow-y: scroll; background: #f3f3f3; padding: 10px; border: 1px solid #ccc; }
        .success { color: green; }
        .error { color: red; }
        .warning { color: orange; }
        button, input { margin: 5px; padding: 5px; }
    </style>
</head>
<body>
    <h1>WebSocket Connection Tester</h1>
    
    <div>
        <label for="wsUrl">WebSocket URL:</label>
        <input type="text" id="wsUrl" size="40" value="ws://localhost:8073/websocket" />
        <button id="connect">Connect</button>
        <button id="disconnect">Disconnect</button>
    </div>
    
    <div>
        <input type="text" id="message" placeholder="Message to send" />
        <button id="send">Send</button>
    </div>
    
    <h3>Connection Log:</h3>
    <pre id="log"></pre>
    
    <script>
        let socket = null;
        const log = document.getElementById('log');
        
        function logMessage(msg, type = 'info') {
            const time = new Date().toTimeString().split(' ')[0];
            const div = document.createElement('div');
            div.className = type;
            div.textContent = `[${time}] ${msg}`;
            log.appendChild(div);
            log.scrollTop = log.scrollHeight;
        }
        
        document.getElementById('connect').addEventListener('click', function() {
            if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
                logMessage('Already connected or connecting', 'warning');
                return;
            }
            
            const wsUrl = document.getElementById('wsUrl').value;
            logMessage(`Connecting to ${wsUrl}...`);
            
            try {
                socket = new WebSocket(wsUrl);
                
                socket.onopen = function() {
                    logMessage('Connection established!', 'success');
                };
                
                socket.onmessage = function(event) {
                    logMessage(`Received: ${event.data}`, 'success');
                };
                
                socket.onclose = function(event) {
                    if (event.wasClean) {
                        logMessage(`Connection closed cleanly, code=${event.code}, reason=${event.reason}`, 'warning');
                    } else {
                        logMessage('Connection died', 'error');
                    }
                    socket = null;
                };
                
                socket.onerror = function(error) {
                    logMessage(`Error: ${error.message || 'Unknown error'}`, 'error');
                };
            } catch (err) {
                logMessage(`Failed to create WebSocket: ${err.message}`, 'error');
            }
        });
        
        document.getElementById('disconnect').addEventListener('click', function() {
            if (!socket || socket.readyState !== WebSocket.OPEN) {
                logMessage('Not connected', 'warning');
                return;
            }
            
            logMessage('Closing connection...');
            socket.close(1000, 'User disconnected');
        });
        
        document.getElementById('send').addEventListener('click', function() {
            if (!socket || socket.readyState !== WebSocket.OPEN) {
                logMessage('Not connected', 'error');
                return;
            }
            
            const message = document.getElementById('message').value;
            if (!message) {
                logMessage('Please enter a message to send', 'warning');
                return;
            }
            
            socket.send(message);
            logMessage(`Sent: ${message}`);
        });
    </script>
</body>
</html>
        """
        
        return Response(html, content_type='text/html')
    
    @http.route('/ws/debug', type='http', auth='public')
    def debug_info(self):
        """Display debugging information"""
        _logger.info("Accessing debug info page")
        
        info = {
            'timestamp': datetime.now().isoformat(),
            'odoo_version': tools.config.get('version', 'Unknown'),
            'server_port': tools.config.get('http_port', 8069),
            'longpolling_port': tools.config.get('longpolling_port', 8073),
            'nginx_info': self._get_nginx_info(),
            'environment': {
                'python_version': os.sys.version,
                'working_dir': os.getcwd(),
                'user': os.getlogin() if hasattr(os, 'getlogin') else 'Unknown',
                'pid': os.getpid(),
            },
            'request_info': {
                'host': request.httprequest.host,
                'url': request.httprequest.url,
                'user_agent': request.httprequest.user_agent.string if request.httprequest.user_agent else 'Unknown',
                'headers': dict(request.httprequest.headers),
            }
        }
        
        json_info = json.dumps(info, indent=2)
        
        # Create the HTML without using f-strings to avoid syntax issues
        html = """<!DOCTYPE html>
<html>
<head>
    <title>WebSocket Debug Info</title>
    <style>
        body { font-family: monospace; margin: 20px; }
        pre { background: #f5f5f5; padding: 10px; overflow-x: auto; }
        .section { margin-bottom: 20px; }
    </style>
</head>
<body>
    <h1>WebSocket Debugging Information</h1>
    
    <div class="section">
        <h2>Server Configuration</h2>
        <pre>SERVER_INFO</pre>
    </div>
    
    <div class="section">
        <h2>WebSocket Test</h2>
        <p>For testing WebSocket connections, visit: <a href="/ws/test">/ws/test</a></p>
    </div>
    
    <div class="section">
        <h2>WebSocket Connection Test</h2>
        <button id="testWs">Test WebSocket Connection</button>
        <pre id="wsResult">Click the button to test...</pre>
    </div>
    
    <script>
        document.getElementById('testWs').addEventListener('click', function() {
            const result = document.getElementById('wsResult');
            result.textContent = 'Testing WebSocket connection...';
            
            try {
                // Try to connect to different WebSocket endpoints
                testWebSocket('ws://' + window.location.hostname + ':8073/websocket', 'Custom WebSocket (8073)');
                testWebSocket('ws://' + window.location.hostname + '/websocket', 'Proxied WebSocket');
                testWebSocket('ws://' + window.location.hostname + '/csv_import/ws', 'Custom WebSocket');
            } catch (err) {
                result.textContent += '\\nError: ' + err.message;
            }
            
            function testWebSocket(url, name) {
                result.textContent += '\\nTesting ' + name + ' at ' + url;
                
                const socket = new WebSocket(url);
                
                socket.onopen = function() {
                    result.textContent += '\\n✅ Connected to ' + name + '!';
                    socket.close();
                };
                
                socket.onerror = function() {
                    result.textContent += '\\n❌ Failed to connect to ' + name;
                };
            }
        });
    </script>
</body>
</html>
        """
        
        # Replace placeholder with actual JSON data
        html = html.replace('SERVER_INFO', json_info)
        
        return Response(html, content_type='text/html')
    
    def _get_nginx_info(self):
        """Try to detect Nginx configuration"""
        headers = request.httprequest.headers
        
        info = {
            'detected': False,
            'headers': {},
        }
        
        # Check for common Nginx headers
        nginx_headers = ['X-Nginx-Version', 'Server', 'X-Proxy', 'X-Forwarded-For', 'X-Real-IP']
        for header in nginx_headers:
            if header.lower() in headers:
                info['headers'][header] = headers.get(header)
                info['detected'] = True
                
        # Check if server header contains Nginx
        server = headers.get('Server', '')
        if 'nginx' in server.lower():
            info['detected'] = True
            
        return info
