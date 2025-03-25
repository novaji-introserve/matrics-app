import logging
import json
from datetime import datetime

_logger = logging.getLogger(__name__)

# Keeps track of active WebSocket connections
WS_CONNECTIONS = {}


def send_log_message(env, message, message_type="info", user_id=None, group=None):
    """
    Send log messages to clients via WebSocket or Bus channels.

    Args:
        env: Odoo environment
        message (str): The message to be logged
        message_type (str): Type of message ('info', 'error', 'success', 'warning')
        user_id (int): Target user ID (optional)
        group (str): The channel group to send to (optional)
    """
    try:
        # Create message data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_data = {
            "message": message,
            "message_type": message_type,
            "timestamp": timestamp,
        }

        # First log to server
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)

        log_level(f"[{message_type.upper()}] {message}")

        # Try WebSocket first (if available)
        if _has_websocket_support():
            sent = _send_via_websocket(log_data, user_id, group)
            if sent:
                return

        # Fallback to Bus
        _send_via_bus(env, log_data, user_id, group)

    except Exception as e:
        _logger.error(f"Error sending log message: {str(e)}")
        _logger.error(f"Original message was: {message}")


def _has_websocket_support():
    """Check if WebSocket support is available"""
    try:
        from addons.bus.websocket import WebsocketConnectionHandler

        return True
    except ImportError:
        return False


def _send_via_websocket(log_data, user_id=None, group=None):
    """Send message via WebSocket if connections exist"""
    try:
        if not WS_CONNECTIONS:
            return False

        # Determine target connections
        if user_id and user_id in WS_CONNECTIONS:
            # Send to specific user
            connections = [WS_CONNECTIONS[user_id]]
        elif group and group in WS_CONNECTIONS:
            # Send to group
            connections = WS_CONNECTIONS[group]
        else:
            # Broadcast to all connections
            connections = [conn for conns in WS_CONNECTIONS.values() for conn in conns]

        if not connections:
            return False

        # Send to each connection
        message = json.dumps(log_data)
        for conn in connections:
            try:
                conn.write_message(message)
            except Exception as e:
                _logger.error(f"Error sending WebSocket message: {str(e)}")

        return True

    except Exception as e:
        _logger.error(f"WebSocket send error: {str(e)}")
        return False


def _send_via_bus(env, log_data, user_id=None, group=None):
    """Send message via Odoo Bus channel as fallback"""
    try:
        # Determine channel
        if user_id:
            channel = f"csv_import_logs_{user_id}"
        elif group:
            channel = f"csv_import_logs_{group}"
        else:
            channel = "csv_import_logs"

        # Send through bus
        env["bus.bus"]._sendone(channel, "log_message", log_data)
        return True

    except Exception as e:
        _logger.error(f"Bus send error: {str(e)}")
        return False


def register_connection(connection, user_id=None, group=None):
    """Register a new WebSocket connection"""
    if user_id:
        if user_id not in WS_CONNECTIONS:
            WS_CONNECTIONS[user_id] = []
        WS_CONNECTIONS[user_id].append(connection)

    if group:
        if group not in WS_CONNECTIONS:
            WS_CONNECTIONS[group] = []
        WS_CONNECTIONS[group].append(connection)

    _logger.info(f"Registered new WebSocket connection for user {user_id}")


def unregister_connection(connection, user_id=None, group=None):
    """Unregister a WebSocket connection"""
    try:
        if user_id and user_id in WS_CONNECTIONS:
            if connection in WS_CONNECTIONS[user_id]:
                WS_CONNECTIONS[user_id].remove(connection)
                if not WS_CONNECTIONS[user_id]:
                    del WS_CONNECTIONS[user_id]

        if group and group in WS_CONNECTIONS:
            if connection in WS_CONNECTIONS[group]:
                WS_CONNECTIONS[group].remove(connection)
                if not WS_CONNECTIONS[group]:
                    del WS_CONNECTIONS[group]

        _logger.info(f"Unregistered WebSocket connection for user {user_id}")

    except Exception as e:
        _logger.error(f"Error unregistering connection: {str(e)}")
