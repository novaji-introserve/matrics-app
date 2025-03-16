/** @odoo-module **/
import { browser } from "@web/core/browser/browser";
import { registry } from "@web/core/registry";
import { session } from "@web/session";

/**
 * Terminal Service - Provides a console-like interface for log messages
 * 
 * Features:
 * - Real-time message display with severity levels
 * - WebSocket support with long-polling fallback
 * - Message filtering and searching
 */
export const terminalService = {
    dependencies: ['bus_service', 'rpc'],

    start(env, { bus_service, rpc }) {
        let socket = null;
        let connected = false;
        let reconnectTimer = null;
        let reconnectAttempts = 0;
        const logs = [];
        const listeners = new Set();
        const MAX_LOGS = 1000;
        const MAX_RECONNECT_ATTEMPTS = 5;
        const RECONNECT_DELAY = 3000; // 3 seconds

        // Initialize WebSocket if supported
        async function connectWebSocket() {
            if (typeof WebSocket === 'undefined') {
                console.log('WebSockets not supported by your browser, using long-polling');
                startBusListening();
                return;
            }

            // Check if we already have a connection
            if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
                console.log('WebSocket already connected or connecting');
                return;
            }

            try {
                // First, get the WebSocket configuration from the server
                const wsConfig = await rpc('/csv_import/ws_config');

                // Construct the WebSocket URL using the configuration
                const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
                const wsUrl = `${protocol}://${wsConfig.host}:${wsConfig.port}${wsConfig.path}`;

                console.log(`Attempting to connect to WebSocket at ${wsUrl}`);

                socket = new WebSocket(wsUrl);

                socket.onopen = () => {
                    connected = true;
                    reconnectAttempts = 0;
                    clearTimeout(reconnectTimer);
                    notifyListeners('WebSocket connected', 'success');

                    // Send authentication
                    socket.send(JSON.stringify({
                        type: 'auth',
                        user_id: session.uid,
                        session_id: session.session_id
                    }));
                };

                socket.onmessage = (event) => {
                    try {
                        const data = JSON.parse(event.data);
                        if (data.type === 'log_message') {
                            addLog(data.message, data.message_type, data.timestamp);
                        } else if (data.type === 'connected') {
                            console.log('WebSocket authenticated:', data.message);
                        } else if (data.type === 'error') {
                            console.error('WebSocket error message:', data.message);
                        }
                    } catch (e) {
                        console.error('Error parsing WebSocket message:', e);
                    }
                };

                socket.onclose = (event) => {
                    connected = false;
                    if (event.wasClean) {
                        notifyListeners(`WebSocket closed: ${event.reason}`, 'warning');
                    } else {
                        notifyListeners('WebSocket connection lost', 'warning');
                    }

                    // Try to reconnect after delay
                    if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
                        reconnectAttempts++;
                        const delay = RECONNECT_DELAY * reconnectAttempts;
                        console.log(`Will try to reconnect WebSocket in ${delay / 1000}s (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);

                        clearTimeout(reconnectTimer);
                        reconnectTimer = setTimeout(() => {
                            if (!connected) {
                                connectWebSocket();
                            }
                        }, delay);
                    } else {
                        console.log('Max WebSocket reconnection attempts reached, falling back to long-polling');
                        startBusListening();
                    }
                };

                socket.onerror = (error) => {
                    console.error('WebSocket error:', error);
                    notifyListeners('WebSocket error, will try to reconnect', 'error');
                };
            } catch (e) {
                connected = false;
                notifyListeners('Error setting up WebSocket, using long-polling', 'error');
                console.error('WebSocket setup error:', e);
                startBusListening();
            }
        }

        // Rest of the code remains the same...
        function startBusListening() {
            try {
                const channel = `csv_import_logs_${env.services.user.userId}`;
                bus_service.addChannel(channel);

                // Use addEventListener for notifications
                bus_service.addEventListener('notification', (ev) => {
                    const notifications = ev.detail;
                    if (Array.isArray(notifications)) {
                        notifications.forEach((notification) => {
                            if (notification.type === 'log_message' && notification.payload) {
                                addLog(notification.payload.message, notification.payload.message_type, notification.payload.timestamp);
                            }
                        });
                    }
                });

                console.log('Bus service listening enabled as fallback');
                addLog('Terminal connected via bus service (long-polling)', 'info');
            } catch (e) {
                console.warn('Error setting up bus service:', e);
                addLog(`Error setting up bus service: ${e.message}`, 'error');
            }
        }

        // Add a log entry
        function addLog(message, type = 'info', timestamp = null) {
            if (!timestamp) {
                timestamp = new Date().toLocaleTimeString();
            }

            // Add to logs array
            logs.push({ message, type, timestamp });

            // Limit logs length
            if (logs.length > MAX_LOGS) {
                logs.shift();
            }

            // Notify listeners
            notifyListeners(message, type, timestamp);
        }

        // Notify all registered listeners
        function notifyListeners(message, type, timestamp) {
            listeners.forEach(listener => {
                try {
                    listener(message, type, timestamp);
                } catch (e) {
                    console.error('Error in terminal listener:', e);
                }
            });
        }

        // Send a log message to the server
        async function sendLog(message, type = 'info') {
            // Display locally immediately
            addLog(message, type);

            // Try WebSocket first if connected
            if (connected && socket && socket.readyState === WebSocket.OPEN) {
                try {
                    socket.send(JSON.stringify({
                        type: 'log_message',
                        message: message,
                        message_type: type
                    }));
                    return;
                } catch (error) {
                    console.warn('Error sending log via WebSocket, falling back to RPC:', error);
                }
            }

            // Fallback to RPC
            try {
                await rpc('/csv_import/send_log', {
                    message,
                    message_type: type
                });
            } catch (error) {
                console.error('Error sending log via RPC:', error);
                // Still add the log locally even if server send fails
                addLog(`Failed to send log to server: ${error.message}`, 'warning');
            }
        }

        // Initialize WebSocket and bus listening
        try {
            // Try WebSocket first
            connectWebSocket();

            // Also start bus listening as a parallel channel
            // The server will send to both to ensure delivery
            startBusListening();

            // Add initial log
            addLog('Terminal service initialized', 'info');
        } catch (e) {
            console.error('Error initializing terminal service:', e);
            // Add initial error log
            addLog(`Error initializing terminal service: ${e.message}`, 'error');
        }

        // Return public interface
        return {
            /**
             * Add a new log entry
             * 
             * @param {string} message - The log message
             * @param {string} [type='info'] - Log type (info, success, warning, error)
             */
            addLog,

            /**
             * Send a log message to the server and add it locally
             * 
             * @param {string} message - The log message
             * @param {string} [type='info'] - Log type (info, success, warning, error)
             * @returns {Promise} - Promise resolving when the log is sent
             */
            sendLog,

            /**
             * Register a listener for new log entries
             * 
             * @param {Function} listener - Callback function(message, type, timestamp)
             * @returns {Function} - Function to unregister the listener
             */
            onLog(listener) {
                listeners.add(listener);
                return () => {
                    listeners.delete(listener);
                };
            },

            /**
             * Get all logged messages
             * 
             * @returns {Array} - Array of log entries
             */
            getLogs() {
                return [...logs];
            },

            /**
             * Clear all logs
             */
            clearLogs() {
                logs.length = 0;
                notifyListeners('Logs cleared', 'info');
            },

            /**
             * Check if WebSocket is connected
             * 
             * @returns {boolean} - True if WebSocket is connected
             */
            isWebSocketConnected() {
                return connected;
            },

            /**
             * Manually reconnect WebSocket
             */
            reconnectWebSocket() {
                reconnectAttempts = 0;
                connectWebSocket();
            }
        };
    }
};

registry.category("services").add("terminal", terminalService);