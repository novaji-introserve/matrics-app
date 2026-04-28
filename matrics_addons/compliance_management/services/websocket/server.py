# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# Standalone WebSocket Server
# ==========================
# This is a standalone WebSocket server for Odoo that runs in a separate process.
# It provides real-time communication capabilities for Odoo clients.

# This file is meant to be run as a separate process from the Odoo server,
# which helps avoid issues with gevent monkey patching and threading conflicts.
# """

# import sys
# import os
# import json
# import logging
# import signal
# import time
# from datetime import datetime

# # Configure logging
# LOG_FILE = '/tmp/odoo_websocket.log'
# PID_FILE = '/tmp/odoo_websocket.pid'

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s] %(message)s',
#     handlers=[
#         logging.StreamHandler(),
#         logging.FileHandler(LOG_FILE)
#     ]
# )
# logger = logging.getLogger("odoo-websocket")

# # Import required packages
# try:
#     import gevent
#     from gevent import monkey, pywsgi
#     from geventwebsocket.handler import WebSocketHandler
    
#     # Apply monkey patches for gevent - this is safe here because
#     # we're in a separate process from Odoo
#     monkey.patch_all()
# except ImportError as e:
#     logger.error(f"Required packages not installed: {e}")
#     logger.error("Install with: pip install gevent geventwebsocket")
#     sys.exit(1)

# # Global variables
# connections = {}  # Maps client IDs to WebSocket connections
# user_connections = {}  # Maps user IDs to lists of client IDs
# session_mappings = {}  # Maps session IDs to client IDs
# connection_counter = 0
# start_time = time.time()

# def handle_websocket(ws, client_id):
#     """
#     Handle an individual WebSocket connection throughout its lifecycle.
    
#     Args:
#         ws: The WebSocket connection object
#         client_id: Unique ID for this connection
#     """
#     global connections, user_connections, session_mappings
    
#     try:
#         # Send welcome message
#         ws.send(json.dumps({
#             "type": "connected",
#             "client_id": client_id,
#             "message": "Connected to Odoo WebSocket server",
#             "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         }))
        
#         logger.info(f"Client {client_id} connected")
        
#         # Handle messages in a loop until the connection is closed
#         while not ws.closed:
#             message = ws.receive()
#             if message is None:
#                 break
                
#             logger.info(f"Received from {client_id}: {message}")
            
#             try:
#                 # Parse and handle the message
#                 data = json.loads(message)
#                 message_type = data.get("type", "unknown")
                
#                 # Handle different message types
#                 if message_type == "auth":
#                     # Handle authentication
#                     user_id = data.get("user_id")
#                     session_id = data.get("session_id")
                    
#                     if user_id:
#                         logger.info(f"Authenticated client {client_id} as user {user_id}")
                        
#                         # Register the user connection
#                         user_key = f"user_{user_id}"
#                         if user_key not in user_connections:
#                             user_connections[user_key] = []
#                         if client_id not in user_connections[user_key]:
#                             user_connections[user_key].append(client_id)
                        
#                         # Map session to client if provided
#                         if session_id:
#                             session_mappings[session_id] = client_id
                            
#                         # Send confirmation
#                         ws.send(json.dumps({
#                             "type": "auth_response",
#                             "success": True,
#                             "message": "Authentication successful",
#                             "user_id": user_id
#                         }))
#                     else:
#                         # Authentication without user ID
#                         ws.send(json.dumps({
#                             "type": "auth_response",
#                             "success": False,
#                             "message": "Authentication failed: No user ID provided"
#                         }))
                
#                 elif message_type == "log_message":
#                     # Handle log messages
#                     content = data.get("message", "")
#                     log_type = data.get("message_type", "info")
                    
#                     # Log it server-side
#                     log_level = getattr(logger, log_type, logger.info)
#                     log_level(f"Log from client {client_id}: {content}")
                    
#                     # Echo back with timestamp
#                     ws.send(json.dumps({
#                         "type": "log_response",
#                         "message": content,
#                         "message_type": log_type,
#                         "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                     }))
                    
#                     # Broadcast to other connections of the same user if applicable
#                     user_id = data.get("user_id")
#                     if user_id:
#                         broadcast_to_user(user_id, content, log_type, exclude=client_id)
                
#                 elif message_type == "ping":
#                     # Handle ping messages for keeping connections alive
#                     ws.send(json.dumps({
#                         "type": "pong",
#                         "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                     }))
                
#                 elif message_type == "status":
#                     # Return server status information
#                     status = get_server_status()
#                     ws.send(json.dumps({
#                         "type": "status_response",
#                         "status": status,
#                         "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                     }))
                
#                 else:
#                     # Echo unknown messages back
#                     ws.send(json.dumps({
#                         "type": "echo",
#                         "message": f"Received unknown message type: {message_type}",
#                         "original": data,
#                         "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                     }))
                    
#             except json.JSONDecodeError:
#                 # Handle invalid JSON
#                 ws.send(json.dumps({
#                     "type": "error",
#                     "message": "Invalid JSON format",
#                     "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 }))
                
#             except Exception as e:
#                 # Handle other errors
#                 logger.exception(f"Error handling message: {e}")
#                 ws.send(json.dumps({
#                     "type": "error",
#                     "message": f"Server error: {str(e)}",
#                     "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 }))
                
#     except Exception as e:
#         logger.exception(f"WebSocket handler error: {e}")
#     finally:
#         # Clean up the connection
#         cleanup_connection(client_id)
#         logger.info(f"Client {client_id} disconnected")

# def broadcast_to_user(user_id, message, message_type="info", exclude=None):
#     """
#     Broadcast a message to all connections for a specific user.
    
#     Args:
#         user_id: The user ID to broadcast to
#         message: The message content
#         message_type: The message type (info, warning, error, success)
#         exclude: Optional client ID to exclude from the broadcast
#     """
#     user_key = f"user_{user_id}"
    
#     if user_key not in user_connections:
#         return
        
#     payload = {
#         "type": "broadcast",
#         "message": message,
#         "message_type": message_type,
#         "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         "source": "server"
#     }
    
#     # Send to all connections for this user except excluded one
#     for client_id in list(user_connections[user_key]):
#         if client_id == exclude:
#             continue
            
#         if client_id in connections:
#             try:
#                 connections[client_id].send(json.dumps(payload))
#             except Exception as e:
#                 logger.error(f"Error sending to client {client_id}: {e}")
#                 cleanup_connection(client_id)

# def cleanup_connection(client_id):
#     """
#     Clean up references when a client disconnects.
    
#     Args:
#         client_id: The client ID to clean up
#     """
#     global connections, user_connections, session_mappings
    
#     # Remove from connections dict
#     if client_id in connections:
#         del connections[client_id]
    
#     # Remove from user connections lists
#     for user_key in list(user_connections.keys()):
#         if client_id in user_connections[user_key]:
#             user_connections[user_key].remove(client_id)
#             if not user_connections[user_key]:
#                 del user_connections[user_key]
    
#     # Remove from session mappings
#     for session_id, mapped_client in list(session_mappings.items()):
#         if mapped_client == client_id:
#             del session_mappings[session_id]

# def get_server_status():
#     """
#     Get the current status of the WebSocket server.
    
#     Returns:
#         dict: Status information about the server
#     """
#     uptime = int(time.time() - start_time)
#     hours, remainder = divmod(uptime, 3600)
#     minutes, seconds = divmod(remainder, 60)
    
#     return {
#         'uptime': uptime,
#         'uptime_human': f"{hours:02}:{minutes:02}:{seconds:02}",
#         'connections': len(connections),
#         'users': len(user_connections),
#         'sessions': len(session_mappings),
#         'start_time': datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
#     }

# def websocket_app(environ, start_response):
#     """
#     WSGI application for handling WebSocket and HTTP requests.
    
#     This is the main entry point for the WebSocket server.
#     """
#     global connections, connection_counter
    
#     path = environ.get('PATH_INFO', '')
#     client_ip = environ.get('REMOTE_ADDR', 'unknown')
    
#     logger.info(f"Request: {path} from {client_ip}")
    
#     # Handle WebSocket connections
#     if environ.get('HTTP_UPGRADE', '').lower() == 'websocket':
#         # Get WebSocket handler from environ
#         ws = environ.get('wsgi.websocket')
#         if not ws:
#             logger.error("WebSocket not available in environment")
#             start_response('400 Bad Request', [('Content-Type', 'text/plain')])
#             return [b'WebSocket upgrade failed']
        
#         # Generate client ID and store connection
#         connection_counter += 1
#         client_id = f"client_{connection_counter}"
#         connections[client_id] = ws
        
#         # Handle in dedicated function
#         handle_websocket(ws, client_id)
#         return []
    
#     # Handle HTTP requests based on path
#     if path == '/status':
#         # Return server status as JSON
#         status = get_server_status()
#         status_json = json.dumps(status, indent=2)
        
#         start_response('200 OK', [('Content-Type', 'application/json')])
#         return [status_json.encode('utf-8')]
    
#     elif path == '/csv_import/ws':
#         # Information page for the WebSocket endpoint
#         start_response('200 OK', [('Content-Type', 'text/html')])
#         return [b"""
#         <html>
#             <head><title>Odoo WebSocket Endpoint</title></head>
#             <body>
#                 <h1>Odoo WebSocket Endpoint</h1>
#                 <p>This endpoint is for WebSocket connections. Use a WebSocket client to connect.</p>
#             </body>
#         </html>
#         """]
    
#     # Default response for all other paths - status page
#     status = get_server_status()
    
#     start_response('200 OK', [('Content-Type', 'text/html')])
#     return [f"""
#     <!DOCTYPE html>
#     <html>
#     <head>
#         <title>Odoo WebSocket Server</title>
#         <style>
#             body {{ font-family: Arial, sans-serif; margin: 2em; line-height: 1.6; }}
#             table {{ border-collapse: collapse; width: 100%; margin: 1em 0; }}
#             th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
#             th {{ background-color: #f2f2f2; }}
#             .status {{ padding: 0.5em; border-radius: 4px; display: inline-block; }}
#             .running {{ background-color: #d4edda; color: #155724; }}
#             pre {{ background-color: #f8f9fa; padding: 1em; overflow: auto; }}
#         </style>
#     </head>
#     <body>
#         <h1>Odoo WebSocket Server</h1>
        
#         <div class="status running">
#             <strong>Status:</strong> Running
#         </div>
        
#         <h2>Server Information</h2>
#         <table>
#             <tr><th>Uptime</th><td>{status['uptime_human']}</td></tr>
#             <tr><th>Started</th><td>{status['start_time']}</td></tr>
#             <tr><th>Active Connections</th><td>{status['connections']}</td></tr>
#             <tr><th>Active Users</th><td>{status['users']}</td></tr>
#             <tr><th>Active Sessions</th><td>{status['sessions']}</td></tr>
#             <tr><th>Process ID</th><td>{os.getpid()}</td></tr>
#         </table>
        
#         <h2>WebSocket Endpoints</h2>
#         <ul>
#             <li><code>/csv_import/ws</code> - Main WebSocket endpoint</li>
#             <li><code>/status</code> - JSON status information</li>
#         </ul>
        
#         <h2>WebSocket Test</h2>
#         <div>
#             <button onclick="testConnection()">Test Connection</button>
#             <div id="result"></div>
#         </div>
        
#         <script>
#             function testConnection() {{
#                 const resultDiv = document.getElementById('result');
#                 resultDiv.innerHTML = '<p>Testing connection...</p>';
                
#                 try {{
#                     const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
#                     const wsUrl = wsProtocol + '//' + window.location.host + '/csv_import/ws';
                    
#                     resultDiv.innerHTML += `<p>Connecting to: ${{wsUrl}}</p>`;
#                     const socket = new WebSocket(wsUrl);
                    
#                     socket.onopen = () => {{
#                         resultDiv.innerHTML += '<p style="color:green">Connected successfully!</p>';
                        
#                         // Send a test message
#                         const testMsg = JSON.stringify({{
#                             type: "ping",
#                             timestamp: new Date().toISOString()
#                         }});
#                         socket.send(testMsg);
#                         resultDiv.innerHTML += `<p>Sent: ${{testMsg}}</p>`;
#                     }};
                    
#                     socket.onmessage = (event) => {{
#                         resultDiv.innerHTML += `<p>Received: ${{event.data}}</p>`;
#                     }};
                    
#                     socket.onerror = (error) => {{
#                         resultDiv.innerHTML += `<p style="color:red">Error: ${{error}}</p>`;
#                     }};
                    
#                     socket.onclose = () => {{
#                         resultDiv.innerHTML += '<p>Connection closed</p>';
#                     }};
                    
#                     // Close after 5 seconds
#                     setTimeout(() => {{
#                         if (socket.readyState === WebSocket.OPEN) {{
#                             socket.close();
#                         }}
#                     }}, 5000);
                    
#                 }} catch (error) {{
#                     resultDiv.innerHTML += `<p style="color:red">Error: ${{error.message}}</p>`;
#                 }}
#             }}
#         </script>
#     </body>
#     </html>
#     """.encode('utf-8')]

# def start_server(host='0.0.0.0', port=8073):
#     """
#     Start the WebSocket server.
    
#     Args:
#         host: Host address to bind to
#         port: Port to listen on
#     """
#     global start_time
#     start_time = time.time()
    
#     logger.info(f"Starting WebSocket server on {host}:{port}")
    
#     # Create the server
#     server = pywsgi.WSGIServer(
#         (host, port),
#         websocket_app,
#         handler_class=WebSocketHandler
#     )
    
#     # Handle signals for clean shutdown
#     def shutdown_handler(signum, frame):
#         logger.info(f"Received signal {signum}, shutting down...")
#         server.stop()
#         sys.exit(0)
    
#     signal.signal(signal.SIGINT, shutdown_handler)
#     signal.signal(signal.SIGTERM, shutdown_handler)
    
#     # Write PID to file for management
#     with open(PID_FILE, 'w') as f:
#         f.write(str(os.getpid()))
    
#     # Start the server
#     try:
#         logger.info(f"WebSocket server running on {host}:{port}")
#         logger.info(f"Process ID: {os.getpid()}")
#         logger.info("Press Ctrl+C to stop")
#         server.serve_forever()
#     except KeyboardInterrupt:
#         logger.info("Server stopped by user")
#     except Exception as e:
#         logger.exception(f"Server error: {e}")
#     finally:
#         logger.info("WebSocket server shutting down")
        
#         # Clean up PID file
#         try:
#             if os.path.exists(PID_FILE):
#                 os.remove(PID_FILE)
#         except:
#             pass

# if __name__ == "__main__":
#     # Parse command-line arguments
#     port = 8073
#     if len(sys.argv) > 1:
#         try:
#             port = int(sys.argv[1])
#         except ValueError:
#             logger.error(f"Invalid port: {sys.argv[1]}")
#             sys.exit(1)
    
#     # Start the server
#     start_server(port=port)


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone WebSocket Server
==========================
This is a standalone WebSocket server for Odoo that runs in a separate process.
It provides real-time communication capabilities for Odoo clients.

This file is meant to be run as a separate process from the Odoo server,
which helps avoid issues with gevent monkey patching and threading conflicts.
"""

import sys
import os
import json
import logging
import signal
import time
from datetime import datetime

# Configure logging
LOG_FILE = '/tmp/odoo_websocket.log'
PID_FILE = '/tmp/odoo_websocket.pid'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger("odoo-websocket")

# Import required packages without monkey patching yet
try:
    import gevent
    from gevent import monkey, pywsgi
    from geventwebsocket.handler import WebSocketHandler
    
    # Only apply monkey patches when this script is run directly
    # NOT when it's imported by another module
    if __name__ == "__main__":
        logger.info("Applying gevent monkey patches")
        monkey.patch_all()
        logger.info("Gevent monkey patches applied successfully")
except ImportError as e:
    logger.error(f"Required packages not installed: {e}")
    logger.error("Install with: pip install gevent geventwebsocket")
    sys.exit(1)

# Global variables
connections = {}  # Maps client IDs to WebSocket connections
user_connections = {}  # Maps user IDs to lists of client IDs
session_mappings = {}  # Maps session IDs to client IDs
connection_counter = 0
start_time = time.time()

def handle_websocket(ws, client_id):
    """
    Handle an individual WebSocket connection throughout its lifecycle.
    
    Args:
        ws: The WebSocket connection object
        client_id: Unique ID for this connection
    """
    global connections, user_connections, session_mappings
    
    try:
        # Send welcome message
        ws.send(json.dumps({
            "type": "connected",
            "client_id": client_id,
            "message": "Connected to Odoo WebSocket server",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }))
        
        logger.info(f"Client {client_id} connected")
        
        # Handle messages in a loop until the connection is closed
        while not ws.closed:
            message = ws.receive()
            if message is None:
                break
                
            logger.info(f"Received from {client_id}: {message}")
            
            try:
                # Parse and handle the message
                data = json.loads(message)
                message_type = data.get("type", "unknown")
                
                # Handle different message types
                if message_type == "auth":
                    # Handle authentication
                    user_id = data.get("user_id")
                    session_id = data.get("session_id")
                    
                    if user_id:
                        logger.info(f"Authenticated client {client_id} as user {user_id}")
                        
                        # Register the user connection
                        user_key = f"user_{user_id}"
                        if user_key not in user_connections:
                            user_connections[user_key] = []
                        if client_id not in user_connections[user_key]:
                            user_connections[user_key].append(client_id)
                        
                        # Map session to client if provided
                        if session_id:
                            session_mappings[session_id] = client_id
                            
                        # Send confirmation
                        ws.send(json.dumps({
                            "type": "connected",  # Match your existing client expectations
                            "success": True,
                            "message": "WebSocket authenticated successfully",
                            "user_id": user_id
                        }))
                    else:
                        # Authentication without user ID
                        ws.send(json.dumps({
                            "type": "error",
                            "message": "Authentication failed: No user ID provided"
                        }))
                
                elif message_type == "log_message":
                    # Handle log messages
                    content = data.get("message", "")
                    log_type = data.get("message_type", "info")
                    
                    # Log it server-side
                    log_level = getattr(logger, log_type, logger.info)
                    log_level(f"Log from client {client_id}: {content}")
                    
                    # Echo back with timestamp - match your existing client format
                    ws.send(json.dumps({
                        "type": "log_message",
                        "message": content,
                        "message_type": log_type,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }))
                    
                    # Broadcast to other connections of the same user if applicable
                    user_id = data.get("user_id")
                    if user_id:
                        broadcast_to_user(user_id, content, log_type, exclude=client_id)
                
                elif message_type == "ping":
                    # Handle ping messages for keeping connections alive
                    ws.send(json.dumps({
                        "type": "pong",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }))
                
                else:
                    # Echo unknown messages back
                    ws.send(json.dumps({
                        "type": "echo",
                        "message": f"Received unknown message type: {message_type}",
                        "original": data,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }))
                    
            except json.JSONDecodeError:
                # Handle invalid JSON
                ws.send(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON format",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }))
                
            except Exception as e:
                # Handle other errors
                logger.exception(f"Error handling message: {e}")
                ws.send(json.dumps({
                    "type": "error",
                    "message": f"Server error: {str(e)}",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }))
                
    except Exception as e:
        logger.exception(f"WebSocket handler error: {e}")
    finally:
        # Clean up the connection
        cleanup_connection(client_id)
        logger.info(f"Client {client_id} disconnected")

def broadcast_to_user(user_id, message, message_type="info", exclude=None):
    """
    Broadcast a message to all connections for a specific user.
    
    Args:
        user_id: The user ID to broadcast to
        message: The message content
        message_type: The message type (info, warning, error, success)
        exclude: Optional client ID to exclude from the broadcast
    """
    user_key = f"user_{user_id}"
    
    if user_key not in user_connections:
        return
        
    payload = {
        "type": "log_message",  # Match your existing client expectations
        "message": message,
        "message_type": message_type,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    
    # Send to all connections for this user except excluded one
    for client_id in list(user_connections[user_key]):
        if client_id == exclude:
            continue
            
        if client_id in connections:
            try:
                connections[client_id].send(json.dumps(payload))
            except Exception as e:
                logger.error(f"Error sending to client {client_id}: {e}")
                cleanup_connection(client_id)

def cleanup_connection(client_id):
    """
    Clean up references when a client disconnects.
    
    Args:
        client_id: The client ID to clean up
    """
    global connections, user_connections, session_mappings
    
    # Remove from connections dict
    if client_id in connections:
        del connections[client_id]
    
    # Remove from user connections lists
    for user_key in list(user_connections.keys()):
        if client_id in user_connections[user_key]:
            user_connections[user_key].remove(client_id)
            if not user_connections[user_key]:
                del user_connections[user_key]
    
    # Remove from session mappings
    for session_id, mapped_client in list(session_mappings.items()):
        if mapped_client == client_id:
            del session_mappings[session_id]

def get_server_status():
    """
    Get the current status of the WebSocket server.
    
    Returns:
        dict: Status information about the server
    """
    uptime = int(time.time() - start_time)
    hours, remainder = divmod(uptime, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return {
        'uptime': uptime,
        'uptime_human': f"{hours:02}:{minutes:02}:{seconds:02}",
        'connections': len(connections),
        'users': len(user_connections),
        'sessions': len(session_mappings),
        'start_time': datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
    }

def websocket_app(environ, start_response):
    """
    WSGI application for handling WebSocket and HTTP requests.
    
    This is the main entry point for the WebSocket server.
    """
    global connections, connection_counter
    
    path = environ.get('PATH_INFO', '')
    client_ip = environ.get('REMOTE_ADDR', 'unknown')
    
    logger.info(f"Request: {path} from {client_ip}")
    
    # Handle WebSocket connections
    if environ.get('HTTP_UPGRADE', '').lower() == 'websocket':
        # Get WebSocket handler from environ
        ws = environ.get('wsgi.websocket')
        if not ws:
            logger.error("WebSocket not available in environment")
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return [b'WebSocket upgrade failed']
        
        # Generate client ID and store connection
        connection_counter += 1
        client_id = f"client_{connection_counter}"
        connections[client_id] = ws
        
        # Handle in dedicated function
        handle_websocket(ws, client_id)
        return []
    
    # Handle HTTP requests based on path
    if path == '/status':
        # Return server status as JSON
        status = get_server_status()
        status_json = json.dumps(status, indent=2)
        
        start_response('200 OK', [('Content-Type', 'application/json')])
        return [status_json.encode('utf-8')]
    
    elif path == '/csv_import/ws':
        # Information page for the WebSocket endpoint
        start_response('200 OK', [('Content-Type', 'text/html')])
        return [b"""
        <html>
            <head><title>Odoo WebSocket Endpoint</title></head>
            <body>
                <h1>Odoo WebSocket Endpoint</h1>
                <p>This endpoint is for WebSocket connections. Use a WebSocket client to connect.</p>
            </body>
        </html>
        """]
    
    # Default response for all other paths - status page
    status = get_server_status()
    
    start_response('200 OK', [('Content-Type', 'text/html')])
    return [f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Odoo WebSocket Server</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 2em; line-height: 1.6; }}
            table {{ border-collapse: collapse; width: 100%; margin: 1em 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .status {{ padding: 0.5em; border-radius: 4px; display: inline-block; }}
            .running {{ background-color: #d4edda; color: #155724; }}
            pre {{ background-color: #f8f9fa; padding: 1em; overflow: auto; }}
        </style>
    </head>
    <body>
        <h1>Odoo WebSocket Server</h1>
        
        <div class="status running">
            <strong>Status:</strong> Running
        </div>
        
        <h2>Server Information</h2>
        <table>
            <tr><th>Uptime</th><td>{status['uptime_human']}</td></tr>
            <tr><th>Started</th><td>{status['start_time']}</td></tr>
            <tr><th>Active Connections</th><td>{status['connections']}</td></tr>
            <tr><th>Active Users</th><td>{status['users']}</td></tr>
            <tr><th>Active Sessions</th><td>{status['sessions']}</td></tr>
            <tr><th>Process ID</th><td>{os.getpid()}</td></tr>
        </table>
        
        <h2>WebSocket Endpoints</h2>
        <ul>
            <li><code>/csv_import/ws</code> - Main WebSocket endpoint</li>
            <li><code>/status</code> - JSON status information</li>
        </ul>
        
        <h2>WebSocket Test</h2>
        <div>
            <button onclick="testConnection()">Test Connection</button>
            <div id="result"></div>
        </div>
        
        <script>
            function testConnection() {{
                const resultDiv = document.getElementById('result');
                resultDiv.innerHTML = '<p>Testing connection...</p>';
                
                try {{
                    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                    const wsUrl = wsProtocol + '//' + window.location.host + '/csv_import/ws';
                    
                    resultDiv.innerHTML += `<p>Connecting to: ${{wsUrl}}</p>`;
                    const socket = new WebSocket(wsUrl);
                    
                    socket.onopen = () => {{
                        resultDiv.innerHTML += '<p style="color:green">Connected successfully!</p>';
                        
                        // Send a test message
                        const testMsg = JSON.stringify({{
                            type: "ping",
                            timestamp: new Date().toISOString()
                        }});
                        socket.send(testMsg);
                        resultDiv.innerHTML += `<p>Sent: ${{testMsg}}</p>`;
                    }};
                    
                    socket.onmessage = (event) => {{
                        resultDiv.innerHTML += `<p>Received: ${{event.data}}</p>`;
                    }};
                    
                    socket.onerror = (error) => {{
                        resultDiv.innerHTML += `<p style="color:red">Error: ${{error}}</p>`;
                    }};
                    
                    socket.onclose = () => {{
                        resultDiv.innerHTML += '<p>Connection closed</p>';
                    }};
                    
                    // Close after 5 seconds
                    setTimeout(() => {{
                        if (socket.readyState === WebSocket.OPEN) {{
                            socket.close();
                        }}
                    }}, 5000);
                    
                }} catch (error) {{
                    resultDiv.innerHTML += `<p style="color:red">Error: ${{error.message}}</p>`;
                }}
            }}
        </script>
    </body>
    </html>
    """.encode('utf-8')]

def start_server(host='0.0.0.0', port=8073):
    """
    Start the WebSocket server.
    
    Args:
        host: Host address to bind to
        port: Port to listen on
    """
    global start_time
    start_time = time.time()
    
    logger.info(f"Starting WebSocket server on {host}:{port}")
    
    # Create the server
    server = pywsgi.WSGIServer(
        (host, port),
        websocket_app,
        handler_class=WebSocketHandler
    )
    
    # Handle signals for clean shutdown
    def shutdown_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        server.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Write PID to file for management
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    
    # Start the server
    try:
        logger.info(f"WebSocket server running on {host}:{port}")
        logger.info(f"Process ID: {os.getpid()}")
        logger.info("Press Ctrl+C to stop")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.exception(f"Server error: {e}")
    finally:
        logger.info("WebSocket server shutting down")
        
        # Clean up PID file
        try:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
        except:
            pass

# This block only runs when the script is executed directly
# NOT when it's imported by another module
if __name__ == "__main__":
    # Parse command-line arguments
    port = 8073
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            logger.error(f"Invalid port: {sys.argv[1]}")
            sys.exit(1)
    
    # Start the server
    start_server(port=port)