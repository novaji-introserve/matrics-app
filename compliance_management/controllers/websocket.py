# -*- coding: utf-8 -*-
"""
WebSocket Controller
==================
HTTP endpoints for WebSocket configuration and integration with existing clients.
"""

from datetime import datetime
import logging
from odoo import http, fields
from odoo.http import request, Response
from odoo.tools import config
from ..services.websocket.manager import get_server_status, restart_websocket_server
from ..services.websocket.connection import send_message

_logger = logging.getLogger(__name__)

class WebSocketController(http.Controller):
    """Controller for WebSocket-related HTTP endpoints"""
    
    @http.route('/csv_import/ws', type='http', auth='user')
    def websocket_route(self):
        """
        Placeholder route for WebSocket connections.
        The actual WebSocket handling happens in the standalone server,
        but this route is needed for Odoo to register the endpoint.
        """
        _logger.info("WebSocket route accessed via HTTP, not WebSocket")
        return Response(
            "This endpoint requires a WebSocket connection. "
            "It should be accessed with a WebSocket client, not HTTP.",
            content_type="text/plain", 
            status=400
        )

    @http.route('/csv_import/ws_config', type='json', auth='user')
    def get_websocket_config(self):
        """
        Return WebSocket configuration to the client.
        This endpoint is used by frontend JavaScript to get connection details.
        """
        # Get the host from the request or config
        host = request.httprequest.host.split(':')[0]  # Use current hostname
        port = int(config.get("websocket_port", 8072))
        
        # Return configuration compatible with existing client code
        return {
            'host': host,
            'port': port,
            'path': '/csv_import/ws'
        }
    
    @http.route('/csv_import/send_log', type='json', auth='user')
    def send_log(self, message, message_type='info', **kw):
        """
        Send a log message via WebSocket or Bus.
        This endpoint is used by frontend JavaScript to send log messages.
        """
        user_id = request.env.user.id
        
        # Use the utility function to send the message
        success = send_message(
            request.env, 
            message, 
            message_type=message_type, 
            user_id=user_id
        )
        
        return {"status": "success" if success else "error"}
    
    @http.route('/csv_import/ws_status', type='http', auth='user')
    def websocket_status(self):
        """
        Display WebSocket server status.
        This endpoint provides a status page for the WebSocket server.
        """
        status = get_server_status()
        is_running = status.get('is_running', False)
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>WebSocket Server Status</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
                h1 {{ color: #333; }}
                .status {{ padding: 10px; margin: 10px 0; border-radius: 4px; }}
                .running {{ background-color: #dff0d8; color: #155724; }}
                .stopped {{ background-color: #f2dede; color: #721c24; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .button {{ display: inline-block; padding: 8px 16px; margin: 10px 0; 
                          background-color: #4CAF50; color: white; border: none; 
                          border-radius: 4px; cursor: pointer; text-decoration: none; }}
                .button.warning {{ background-color: #f0ad4e; }}
            </style>
        </head>
        <body>
            <h1>WebSocket Server Status</h1>
            
            <div class="status {'running' if is_running else 'stopped'}">
                <strong>Status:</strong> {'Running' if is_running else 'Stopped'}
            </div>
            
            <h2>Server Information</h2>
            <table>
                <tr><th>Process ID</th><td>{status.get('pid', 'N/A')}</td></tr>
                <tr><th>Port</th><td>{status.get('port', 'N/A')}</td></tr>
                <tr><th>Log File</th><td>{status.get('log_file', 'N/A')}</td></tr>
                <tr><th>Active Connections</th><td>{status.get('connection_count', 0)}</td></tr>
                <tr><th>Uptime</th><td>{status.get('uptime', 0)} seconds</td></tr>
            </table>
            
            <form method="post" action="/csv_import/ws_restart">
                <input type="hidden" name="csrf_token" value="{request.csrf_token()}"/>
                <button type="submit" class="button warning">Restart WebSocket Server</button>
            </form>
            
            <h2>Connection Test</h2>
            <p>Use the button below to test the WebSocket connection from your browser:</p>
            <button onclick="testConnection()" class="button">Test Connection</button>
            <div id="test-result" style="margin-top: 10px; padding: 10px; border: 1px solid #ddd;"></div>
            
            <script>
                function testConnection() {{
                    const resultDiv = document.getElementById('test-result');
                    resultDiv.innerHTML = 'Testing connection...';
                    
                    // Get current hostname and protocol
                    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                    const wsHost = window.location.hostname;
                    const wsPort = {status.get('port', 8072)};
                    const wsUrl = `${{wsProtocol}}//${{wsHost}}:${{wsPort}}/csv_import/ws`;
                    
                    try {{
                        resultDiv.innerHTML += `<br>Connecting to: ${{wsUrl}}`;
                        const socket = new WebSocket(wsUrl);
                        
                        socket.onopen = () => {{
                            resultDiv.innerHTML += '<br><span style="color:green">✓ Connected successfully!</span>';
                            
                            // Send an auth message to match your client's expectations
                            const testMsg = JSON.stringify({{
                                type: "auth",
                                user_id: 1,  // Admin user for test
                                session_id: "test-session"
                            }});
                            
                            socket.send(testMsg);
                            resultDiv.innerHTML += `<br>Sent auth request: ${{testMsg}}`;
                        }};
                        
                        socket.onmessage = (event) => {{
                            resultDiv.innerHTML += `<br>Received: ${{event.data}}`;
                            
                            // After receiving auth response, send a test log message
                            try {{
                                const data = JSON.parse(event.data);
                                if (data.type === "connected") {{
                                    // Send test log message
                                    const logMsg = JSON.stringify({{
                                        type: "log_message",
                                        message: "Test log message from status page",
                                        message_type: "info"
                                    }});
                                    socket.send(logMsg);
                                    resultDiv.innerHTML += `<br>Sent log message: ${{logMsg}}`;
                                }}
                            }} catch (e) {{
                                console.error("Error parsing message:", e);
                            }}
                        }};
                        
                        socket.onerror = (error) => {{
                            resultDiv.innerHTML += '<br><span style="color:red">✗ Connection error!</span>';
                        }};
                        
                        socket.onclose = () => {{
                            resultDiv.innerHTML += '<br>Connection closed';
                        }};
                        
                        // Close after 5 seconds
                        setTimeout(() => {{
                            if (socket.readyState === WebSocket.OPEN) {{
                                socket.close();
                            }}
                        }}, 5000);
                        
                    }} catch (error) {{
                        resultDiv.innerHTML += `<br><span style="color:red">Error: ${{error.message}}</span>`;
                    }}
                }}
            </script>
        </body>
        </html>
        """
        
        return Response(html, content_type='text/html')
    
    @http.route('/csv_import/ws_restart', type='http', auth='user', methods=['POST'], csrf=True)
    def restart_websocket(self, **post):
        """
        Restart the WebSocket server.
        This endpoint allows administrators to restart the WebSocket server.
        """
        # Only allow users with system access to restart the server
        if not request.env.user.has_group('base.group_system'):
            return Response("Permission denied", content_type="text/plain", status=403)
            
        success = restart_websocket_server()
        
        if success:
            return Response(
                "<html><body><h1>WebSocket Server Restarted</h1>"
                "<p>The WebSocket server was successfully restarted.</p>"
                "<p><a href='/csv_import/ws_status'>Return to status page</a></p></body></html>",
                content_type="text/html"
            )
        else:
            return Response(
                "<html><body><h1>WebSocket Server Restart Failed</h1>"
                "<p>Failed to restart the WebSocket server. Check server logs for details.</p>"
                "<p><a href='/csv_import/ws_status'>Return to status page</a></p></body></html>",
                content_type="text/html"
            )
            
    @http.route("/csv_import/log", type="json", auth="user")
    def log_message(self, message, message_type="info", **kw):
        """Log a message from client to server"""
        user_id = request.env.user.id
        log_data = {
            "message": message,
            "message_type": message_type,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Log to server
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)
        log_level(f"[USER {user_id}] {message}")

        # Add to message bus for long polling
        request.env["bus.bus"]._sendone(
            f"csv_import_logs_{user_id}", "log_message", log_data
        )

        return {"status": "success"}

    @http.route("/csv_import/poll_logs", type="json", auth="user")
    def poll_logs(self, last_id=0, **kw):
        """Long polling endpoint for log messages"""
        user_id = request.env.user.id

        # Get messages from the bus
        return request.env["bus.bus"]._poll(
            channels=[f"csv_import_logs_{user_id}"], last=last_id
        )
