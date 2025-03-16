# -*- coding: utf-8 -*-
import json
import logging
import threading
import uuid
import time
import os
from contextlib import contextmanager

from odoo import api, http, models, registry, SUPERUSER_ID
from odoo.http import request, Response
from odoo.tools import config
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import NotFound

_logger = logging.getLogger(__name__)

# Global registry of connected clients
WS_CONNECTIONS = {}
WS_CLIENT_IDS = {}  # Maps session_id to client_id

try:
    import gevent
    from gevent import pywsgi
    from geventwebsocket.handler import WebSocketHandler
    GEVENT_AVAILABLE = True
except ImportError:
    GEVENT_AVAILABLE = False
    _logger.warning("gevent and geventwebsocket are not installed. WebSocket functionality will be disabled.")


class WebSocketManager:
    """WebSocket Manager for handling real-time connections"""

    def __init__(self):
        self.websocket_server = None
        self.websocket_thread = None
        self.is_running = False
        self.host = config.get("websocket_host", "localhost")
        self.port = int(config.get("websocket_port", 8072))
        self.url_map = Map([
            Rule('/csv_import/ws', endpoint=self.handle_websocket)
        ])
        
    def start(self):
        """Start the WebSocket server in a separate thread"""
        if not GEVENT_AVAILABLE:
            _logger.error("Cannot start WebSocket server: gevent and geventwebsocket packages are required.")
            return False
                
        if self.is_running:
            _logger.warning("WebSocket server is already running.")
            return True
                
        try:
            # Define the WSGI application as before...
            def websocket_app(environ, start_response):
                # same as your existing code
                pass
            
            # Create the server
            self.websocket_server = pywsgi.WSGIServer(
                (self.host, self.port), 
                websocket_app,
                handler_class=WebSocketHandler
            )
            
            # Start the server without blocking (this is key)
            self.websocket_server.start()
            
            # Set is_running flag
            self.is_running = True
            _logger.info("WebSocket server started on %s:%s", self.host, self.port)
            return True
            
        except Exception as e:
            _logger.exception("Failed to start WebSocket server: %s", e)
            return False

    # def start(self):
    #     """Start the WebSocket server in a separate thread"""
    #     if not GEVENT_AVAILABLE:
    #         _logger.error("Cannot start WebSocket server: gevent and geventwebsocket packages are required.")
    #         return False
            
    #     if self.is_running:
    #         _logger.warning("WebSocket server is already running.")
    #         return True
            
    #     try:
    #         # Define the WSGI application
    #         def websocket_app(environ, start_response):
    #             # Match the URL against our URL map
    #             try:
    #                 urls = self.url_map.bind_to_environ(environ)
    #                 endpoint, args = urls.match()
    #                 return endpoint(environ, start_response)
    #             except NotFound:
    #                 # Pass unmatched paths to a simple 404 response
    #                 start_response('404 Not Found', [('Content-Type', 'text/plain')])
    #                 return [b'Not Found']
    #             except Exception as e:
    #                 _logger.exception("WebSocket error: %s", e)
    #                 start_response('500 Internal Server Error', [('Content-Type', 'text/plain')])
    #                 return [b'Internal Server Error']
            
    #         # Create and start the server
    #         self.websocket_server = pywsgi.WSGIServer(
    #             (self.host, self.port), 
    #             websocket_app,
    #             handler_class=WebSocketHandler
    #         )
            
    #         # Start in a separate thread
    #         self.websocket_thread = threading.Thread(target=self._run_server, daemon=True)
    #         self.websocket_thread.start()
            
    #         # Wait a moment to ensure the server starts
    #         time.sleep(1)
            
    #         if self.websocket_server:
    #             self.is_running = True
    #             _logger.info("WebSocket server started on %s:%s", self.host, self.port)
    #             return True
    #         else:
    #             _logger.error("Failed to start WebSocket server")
    #             return False
    #     except Exception as e:
    #         _logger.exception("Failed to start WebSocket server: %s", e)
    #         return False
            
    # def _run_server(self):
    #     """Run the WebSocket server (called in a thread)"""
    #     try:
    #         self.websocket_server.serve_forever()
    #     except Exception as e:
    #         _logger.exception("WebSocket server error: %s", e)
    #         self.is_running = False

    def _run_server(self):
        """Run the WebSocket server (called in a thread)"""
        try:
            # Import gevent modules here to ensure they're available
            import gevent.monkey
            # Patch only what's needed in this thread
            gevent.monkey.patch_socket()
            gevent.monkey.patch_ssl()
            
            # Now start the server
            _logger.info("WebSocket server starting to serve...")
            self.websocket_server.serve_forever()
        except Exception as e:
            _logger.exception("WebSocket server error: %s", e)
            self.is_running = False
            
    def stop(self):
        """Stop the WebSocket server"""
        if not self.is_running:
            return
            
        try:
            if self.websocket_server:
                self.websocket_server.stop()
                
            if self.websocket_thread and self.websocket_thread.is_alive():
                self.websocket_thread.join(timeout=2)
                
            self.is_running = False
            _logger.info("WebSocket server stopped")
        except Exception as e:
            _logger.exception("Error stopping WebSocket server: %s", e)
            
    def handle_websocket(self, environ, start_response):
        """Handle WebSocket connections for CSV import"""
        # Check if it's a WebSocket upgrade request
        if environ.get("HTTP_UPGRADE", "").lower() != "websocket":
            start_response("400 Bad Request", [("Content-Type", "text/plain")])
            return [b"WebSocket connection expected"]
        
        # Get the WebSocket from the environment
        websocket = environ.get("wsgi.websocket")
        if not websocket:
            start_response("400 Bad Request", [("Content-Type", "text/plain")])
            return [b"WebSocket upgrade failed"]
        
        # Generate a unique client ID
        client_id = str(uuid.uuid4())
        WS_CONNECTIONS[client_id] = websocket
        
        # Get user and session ID from headers or cookies
        session_id = None
        user_id = None
        
        try:
            # Set up session info
            session_cookie = environ.get('HTTP_COOKIE', '')
            if 'session_id' in session_cookie:
                # Extract session ID from cookie
                for cookie in session_cookie.split(';'):
                    cookie = cookie.strip()
                    if cookie.startswith('session_id='):
                        session_id = cookie.split('=')[1]
                        break
                
                # If we have a session ID, try to get the user ID
                if session_id:
                    # This must be done with a new cursor since we're in a different thread
                    db_name = environ.get('HTTP_X_ODOO_DB') or config.get('db_name')
                    if db_name:
                        with self._get_cursor(db_name) as cr:
                            env = api.Environment(cr, SUPERUSER_ID, {})
                            session = env['ir.http'].session_info()
                            user_id = session.get('uid', None)
                            
                            if user_id:
                                # Store the connection by user
                                user_key = f"user_{user_id}"
                                if user_key not in WS_CONNECTIONS:
                                    WS_CONNECTIONS[user_key] = []
                                WS_CONNECTIONS[user_key].append(client_id)
                                
                                # Map session to client
                                WS_CLIENT_IDS[session_id] = client_id
            
            # Send a welcome message
            websocket.send(json.dumps({
                "type": "connected",
                "message": "WebSocket connected successfully",
                "client_id": client_id,
                "user_id": user_id,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }))
            
            _logger.info(f"WebSocket client {client_id} connected, user_id: {user_id}")
            
            # Keep the connection open and handle messages
            while not websocket.closed:
                message = websocket.receive()
                if message is None:
                    break
                    
                try:
                    # Parse message
                    data = json.loads(message)
                    
                    # Handle authentication
                    if data.get("type") == "auth":
                        user_id = data.get("user_id")
                        session_id = data.get("session_id")
                        
                        if user_id:
                            # Store the connection by user
                            user_key = f"user_{user_id}"
                            if user_key not in WS_CONNECTIONS:
                                WS_CONNECTIONS[user_key] = []
                            WS_CONNECTIONS[user_key].append(client_id)
                            
                            # Map session to client
                            if session_id:
                                WS_CLIENT_IDS[session_id] = client_id
                            
                            # Send confirmation
                            websocket.send(json.dumps({
                                "type": "connected",
                                "message": "WebSocket authenticated successfully"
                            }))
                            
                            _logger.info(f"WebSocket client {client_id} authenticated as user {user_id}")
                    
                    # Handle log messages
                    elif data.get("type") == "log_message":
                        # Forward to appropriate handler
                        message_content = data.get("message", "")
                        message_type = data.get("message_type", "info")
                        
                        # Log locally for debugging
                        _logger.info(f"WS Log ({message_type}): {message_content}")
                        
                        # If user_id is available, broadcast to all user's connections
                        if user_id:
                            self._broadcast_to_user(user_id, message_content, message_type)
                
                except json.JSONDecodeError:
                    _logger.warning(f"Received invalid JSON from WebSocket client {client_id}")
                    websocket.send(json.dumps({
                        "type": "error", 
                        "message": "Invalid message format"
                    }))
                
                except Exception as e:
                    _logger.error(f"Error handling WebSocket message: {e}")
                    websocket.send(json.dumps({
                        "type": "error", 
                        "message": f"Server error: {str(e)}"
                    }))
        except Exception as e:
            _logger.error(f"WebSocket connection error: {e}")
        finally:
            # Clean up connection
            self._cleanup_connection(client_id, user_id, session_id)
            
        return []
            
    def _broadcast_to_user(self, user_id, message, message_type="info"):
        """Send a message to all connections for a specific user"""
        user_key = f"user_{user_id}"
        
        if user_key not in WS_CONNECTIONS:
            return
            
        payload = {
            "type": "log_message",
            "message": message,
            "message_type": message_type,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Get client IDs for this user
        client_ids = WS_CONNECTIONS[user_key]
        
        # Send to all connections for this user
        for client_id in list(client_ids):
            if client_id in WS_CONNECTIONS:
                try:
                    WS_CONNECTIONS[client_id].send(json.dumps(payload))
                except Exception as e:
                    _logger.error(f"Error sending WebSocket message to client {client_id}: {e}")
                    # This client might be disconnected, clean it up
                    self._cleanup_connection(client_id, user_id)
    
    def _cleanup_connection(self, client_id, user_id=None, session_id=None):
        """Clean up a disconnected WebSocket connection"""
        # Remove client connection
        if client_id in WS_CONNECTIONS:
            del WS_CONNECTIONS[client_id]
        
        # Clean up user connection list
        if user_id:
            user_key = f"user_{user_id}"
            if user_key in WS_CONNECTIONS and client_id in WS_CONNECTIONS[user_key]:
                WS_CONNECTIONS[user_key].remove(client_id)
                if not WS_CONNECTIONS[user_key]:
                    del WS_CONNECTIONS[user_key]
        
        # Clean up session mapping
        if session_id and session_id in WS_CLIENT_IDS:
            del WS_CLIENT_IDS[session_id]
            
        _logger.info(f"WebSocket client {client_id} disconnected")
        
    @contextmanager
    def _get_cursor(self, db_name):
        """Get a cursor in the given database"""
        db_registry = registry(db_name)
        with db_registry.cursor() as cr:
            yield cr


# Global singleton for the WebSocket server
websocket_manager = None

def start_websocket_server():
    """Start the WebSocket server if enabled"""
    global websocket_manager
    
    if not GEVENT_AVAILABLE:
        _logger.warning("WebSocket server not started because gevent is not installed")
        return
        
    try:
        ws_enabled = config.get('enable_websockets', False)
        if not ws_enabled:
            _logger.info("WebSocket server not enabled in configuration")
            return
            
        # Start the WebSocket manager
        websocket_manager = WebSocketManager()
        success = websocket_manager.start()
        
        if success:
            _logger.info("WebSocket server started successfully")
        else:
            _logger.error("Failed to start WebSocket server")
    except Exception as e:
        _logger.exception("Error starting WebSocket server: %s", e)


def stop_websocket_server():
    """Stop the WebSocket server"""
    global websocket_manager
    
    if websocket_manager:
        websocket_manager.stop()
        websocket_manager = None
        _logger.info("WebSocket server stopped")


class WebSocketController(http.Controller):
    """Controller for WebSocket endpoints"""
    
    @http.route('/csv_import/ws', type='http', auth='user')
    def websocket_route(self):
        """Placeholder route for WebSocket connections"""
        # The actual WebSocket handling happens in the WebSocketManager
        # This is just a placeholder route to ensure Odoo registers the endpoint
        _logger.info("WebSocket route accessed via HTTP, not WebSocket")
        return Response("This endpoint requires a WebSocket connection", content_type="text/plain", status=400)
    
    @http.route('/csv_import/send_log', type='json', auth='user')
    def send_log(self, message, message_type='info', **kw):
        """Send a log message via WebSocket or Bus"""
        user_id = request.env.user.id
        
        # Try WebSocket first
        if websocket_manager:
            websocket_manager._broadcast_to_user(user_id, message, message_type)
            
        # Also send through the bus as fallback
        try:
            payload = {
                "message": message,
                "message_type": message_type,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            request.env['bus.bus']._sendone(
                f"csv_import_logs_{user_id}",
                "log_message",
                payload
            )
        except Exception as e:
            _logger.error(f"Error sending log via bus: {e}")
            
        return {"status": "success"}





# import json
# import logging
# import threading
# import uuid
# from contextlib import contextmanager

# import werkzeug
# from werkzeug.serving import make_server

# from odoo import api, models, tools, fields
# from odoo.service.server import Worker
# from odoo.tools import config
# from geventwebsocket.handler import WebSocketHandler
# from odoo.addons.bus.websocket import WebsocketConnectionHandler

# _logger = logging.getLogger(__name__)

# # Global registry of connected clients
# ws_connections = {}
# ws_client_ids = {}  # Maps session_id to client_id


# class WebSocketWorker(Worker):
#     """WebSocket worker for handling real-time connections"""

#     def __init__(self, multi):
#         super(WebSocketWorker, self).__init__(multi)
#         self.websocket_server = None
#         self.websocket_server_thread = None

#     def start(self):
#         super(WebSocketWorker, self).start()
#         self._start_websocket_server()

#     def stop(self):
#         super(WebSocketWorker, self).stop()
#         if self.websocket_server:
#             self.websocket_server.shutdown()
#             self.websocket_server_thread.join()

#     def _start_websocket_server(self):
#         websocket_host = config.get("websocket_host", "localhost")
#         websocket_port = config.get("websocket_port", 8072)

#         app = WebSocketApp()

#         try:
#             self.websocket_server = make_server(
#                 websocket_host, websocket_port, app, threaded=True
#             )
#             self.websocket_server_thread = threading.Thread(
#                 target=self.websocket_server.serve_forever
#             )
#             self.websocket_server_thread.daemon = True
#             self.websocket_server_thread.start()

#             _logger.info(
#                 f"WebSocket server started on {websocket_host}:{websocket_port}"
#             )
#         except Exception as e:
#             _logger.error(f"Failed to start WebSocket server: {e}")


# class WebSocketApp(werkzeug.routing.Map):
#     """WSGI application for handling WebSocket connections"""

#     def __init__(self):
#         super(WebSocketApp, self).__init__(
#             [
#                 werkzeug.routing.Rule("/csv_import/ws", endpoint=self.handle_websocket),
#                 werkzeug.routing.Rule(
#                     "/websocket", endpoint=self.handle_odoo_websocket
#                 ),
#             ]
#         )

#     def __call__(self, environ, start_response):
#         urls = self.bind_to_environ(environ)
#         try:
#             endpoint, args = urls.match()
#             return endpoint(environ, start_response, **args)
#         except werkzeug.exceptions.HTTPException as e:
#             return e(environ, start_response)

#     def handle_websocket(self, environ, start_response):
#         """Handle WebSocket connections for CSV import"""
#         # Check if it's a WebSocket upgrade request
#         if environ.get("HTTP_UPGRADE", "").lower() != "websocket":
#             start_response("400 Bad Request", [("Content-Type", "text/plain")])
#             return [b"WebSocket connection expected"]

#         # Handle WebSocket handshake
#         try:
#             # This allows the WebSocketHandler to take over the connection
#             handler = WebSocketHandler(environ, start_response)
#             websocket = environ.get("wsgi.websocket")

#             if not websocket:
#                 start_response("400 Bad Request", [("Content-Type", "text/plain")])
#                 return [b"WebSocket upgrade failed"]

#             self._handle_websocket_connection(websocket, environ)
#             return []
#         except ImportError:
#             _logger.error(
#                 "geventwebsocket is not installed. WebSocket support disabled."
#             )
#             start_response(
#                 "500 Internal Server Error", [("Content-Type", "text/plain")]
#             )
#             return [b"WebSocket support is not enabled on the server"]

#     def handle_odoo_websocket(self, environ, start_response):
#         """Handle WebSocket connections for Odoo's built-in chat and notifications"""
#         # This is just a pass-through to Odoo's built-in WebSocket handler
#         try:
#             handler = WebsocketConnectionHandler(environ, start_response)
#             return handler(environ, start_response)
#         except ImportError:
#             _logger.error("Odoo Bus WebSocket module not found")
#             start_response(
#                 "500 Internal Server Error", [("Content-Type", "text/plain")]
#             )
#             return [b"WebSocket support is not available"]

#     def _handle_websocket_connection(self, websocket, environ):
#         """Handle a WebSocket connection for CSV import"""
#         client_id = str(uuid.uuid4())
#         ws_connections[client_id] = websocket

#         user_id = None
#         session_id = None

#         try:
#             # Keep connection open and handle messages
#             while not websocket.closed:
#                 message = websocket.receive()
#                 if message is None:
#                     break

#                 try:
#                     # Parse message
#                     data = json.loads(message)

#                     # Handle authentication
#                     if data.get("type") == "auth":
#                         user_id = data.get("user_id")
#                         session_id = data.get("session_id")

#                         if user_id and session_id:
#                             # Store the connection by user
#                             user_key = f"user_{user_id}"
#                             if user_key not in ws_connections:
#                                 ws_connections[user_key] = []
#                             ws_connections[user_key].append(client_id)

#                             # Map session to client
#                             ws_client_ids[session_id] = client_id

#                             # Send confirmation
#                             websocket.send(
#                                 json.dumps(
#                                     {
#                                         "type": "connected",
#                                         "message": "WebSocket connected successfully",
#                                     }
#                                 )
#                             )

#                             _logger.info(
#                                 f"WebSocket client {client_id} authenticated as user {user_id}"
#                             )

#                             # Send through the logs channel too for verification
#                             self._send_log_message(
#                                 user_id, "WebSocket connected successfully", "success"
#                             )

#                     # Handle log messages
#                     elif data.get("type") == "log_message":
#                         if not user_id:
#                             websocket.send(
#                                 json.dumps(
#                                     {"type": "error", "message": "Not authenticated"}
#                                 )
#                             )
#                             continue

#                         # Forward to all connections for this user
#                         self._send_log_message(
#                             user_id,
#                             data.get("message", ""),
#                             data.get("message_type", "info"),
#                         )

#                 except json.JSONDecodeError:
#                     _logger.warning(
#                         f"Received invalid JSON from WebSocket client {client_id}"
#                     )
#                     websocket.send(
#                         json.dumps(
#                             {"type": "error", "message": "Invalid message format"}
#                         )
#                     )

#                 except Exception as e:
#                     _logger.error(f"Error handling WebSocket message: {e}")
#                     websocket.send(
#                         json.dumps({"type": "error", "message": "Server error"})
#                     )
#         except Exception as e:
#             _logger.error(f"WebSocket connection error: {e}")
#         finally:
#             # Clean up connection
#             if client_id in ws_connections:
#                 del ws_connections[client_id]

#             # Clean up user connection list
#             if user_id:
#                 user_key = f"user_{user_id}"
#                 if user_key in ws_connections and client_id in ws_connections[user_key]:
#                     ws_connections[user_key].remove(client_id)
#                     if not ws_connections[user_key]:
#                         del ws_connections[user_key]

#             # Clean up session mapping
#             if session_id and session_id in ws_client_ids:
#                 del ws_client_ids[session_id]

#             _logger.info(f"WebSocket client {client_id} disconnected")

#     def _send_log_message(self, user_id, message, message_type="info"):
#         """Send a log message to a specific user"""
#         user_key = f"user_{user_id}"

#         if user_key not in ws_connections:
#             return

#         payload = {
#             "type": "log_message",
#             "message": message,
#             "message_type": message_type,
#             "timestamp": tools.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         }

#         # Send to all connections for this user
#         for client_id in ws_connections[user_key]:
#             if client_id in ws_connections:
#                 try:
#                     ws_connections[client_id].send(json.dumps(payload))
#                 except Exception as e:
#                     _logger.error(
#                         f"Error sending WebSocket message to client {client_id}: {e}"
#                     )


# class WebSocketManager(models.AbstractModel):
#     """Model for sending WebSocket messages from Odoo code"""

#     _name = "websocket.manager"
#     _description = "WebSocket Manager"

#     @api.model
#     def send_log_message(self, user_id, message, message_type="info"):
#         """Send a log message to a specific user via WebSocket"""
#         user_key = f"user_{user_id}"

#         if user_key not in ws_connections:
#             # Fallback to bus if WebSocket isn't connected
#             self._send_via_bus(user_id, message, message_type)
#             return

#         payload = {
#             "type": "log_message",
#             "message": message,
#             "message_type": message_type,
#             "timestamp": fields.Datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         }

#         # Send to all connections for this user
#         for client_id in ws_connections[user_key]:
#             if client_id in ws_connections:
#                 try:
#                     ws_connections[client_id].send(json.dumps(payload))
#                 except Exception as e:
#                     _logger.error(
#                         f"Error sending WebSocket message to client {client_id}: {e}"
#                     )
#                     # Fallback to bus
#                     self._send_via_bus(user_id, message, message_type)

#     def _send_via_bus(self, user_id, message, message_type="info"):
#         """Send a message via Odoo's bus channel as fallback"""
#         try:
#             channel = f"csv_import_logs_{user_id}"
#             payload = {
#                 "type": "log_message",
#                 "message": message,
#                 "message_type": message_type,
#                 "timestamp": fields.Datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#             }

#             self.env["bus.bus"]._sendone(channel, "log_message", payload)
#         except Exception as e:
#             _logger.error(f"Error sending message via bus: {e}")
