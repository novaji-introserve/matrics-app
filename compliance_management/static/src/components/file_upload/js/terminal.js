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

            // Log to console for debugging
            console.log(`[Terminal ${type}] ${message}`);
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

        // Initialize WebSocket if supported
        async function connectWebSocket() {
            // Log connection attempt immediately
            addLog('Attempting to connect to WebSocket...', 'info');

            if (typeof WebSocket === 'undefined') {
                addLog('WebSockets not supported by your browser, using long-polling', 'warning');
                startBusListening();
                return;
            }

            // Check if we already have a connection
            if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
                addLog('WebSocket already connected or connecting', 'info');
                return;
            }

            try {
                // First, get the WebSocket configuration from the server
                addLog('Fetching WebSocket configuration...', 'info');
                const wsConfig = await rpc('/csv_import/ws_config');

                // Log the configuration
                console.log('WebSocket configuration:', wsConfig);

                // Construct the WebSocket URL
                const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
                const wsUrl = `${protocol}://${wsConfig.host}:${wsConfig.port}${wsConfig.path}`;

                addLog(`Connecting to WebSocket at ${wsUrl}`, 'info');
                console.log(`Attempting to connect to WebSocket at ${wsUrl}`);

                socket = new WebSocket(wsUrl);

                socket.onopen = () => {
                    connected = true;
                    reconnectAttempts = 0;
                    clearTimeout(reconnectTimer);
                    addLog('WebSocket connected successfully!', 'success');

                    // Send authentication
                    const authMsg = {
                        type: 'auth',
                        user_id: session.uid,
                        session_id: session.session_id
                    };

                    console.log('Sending authentication:', authMsg);
                    socket.send(JSON.stringify(authMsg));
                };

                socket.onmessage = (event) => {
                    try {
                        console.log('WebSocket message received:', event.data);
                        const data = JSON.parse(event.data);

                        if (data.type === 'log_message') {
                            addLog(data.message, data.message_type, data.timestamp);
                        } else if (data.type === 'connected') {
                            addLog(`WebSocket authenticated: ${data.message}`, 'success');
                            console.log('WebSocket authenticated:', data.message);

                            // Send a test message to confirm bidirectional communication
                            socket.send(JSON.stringify({
                                type: 'log_message',
                                message: 'Terminal connected and authenticated',
                                message_type: 'info'
                            }));
                        } else if (data.type === 'error') {
                            addLog(`WebSocket error: ${data.message}`, 'error');
                            console.error('WebSocket error message:', data.message);
                        } else {
                            // Log other message types
                            console.log(`WebSocket message (${data.type}):`, data);
                        }
                    } catch (e) {
                        console.error('Error parsing WebSocket message:', e);
                        console.error('Original message:', event.data);
                    }
                };

                socket.onclose = (event) => {
                    connected = false;
                    if (event.wasClean) {
                        addLog(`WebSocket closed: ${event.reason}`, 'warning');
                    } else {
                        addLog('WebSocket connection lost', 'warning');
                    }

                    // Try to reconnect after delay
                    if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
                        reconnectAttempts++;
                        const delay = RECONNECT_DELAY * reconnectAttempts;
                        addLog(`Will try to reconnect WebSocket in ${delay / 1000}s (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`, 'info');

                        clearTimeout(reconnectTimer);
                        reconnectTimer = setTimeout(() => {
                            if (!connected) {
                                connectWebSocket();
                            }
                        }, delay);
                    } else {
                        addLog('Max WebSocket reconnection attempts reached, falling back to long-polling', 'warning');
                        startBusListening();
                    }
                };

                socket.onerror = (error) => {
                    console.error('WebSocket error:', error);
                    addLog('WebSocket error, will try to reconnect', 'error');
                };

                // Set a connection timeout
                setTimeout(() => {
                    if (socket && socket.readyState !== WebSocket.OPEN) {
                        addLog('WebSocket connection timed out', 'error');
                        if (socket.readyState !== WebSocket.CLOSED && socket.readyState !== WebSocket.CLOSING) {
                            socket.close();
                        }
                        startBusListening();
                    }
                }, 10000); // 10 second timeout

            } catch (e) {
                connected = false;
                addLog(`Error setting up WebSocket: ${e.message}`, 'error');
                console.error('WebSocket setup error:', e);
                startBusListening();
            }
        }

        function startBusListening() {
            try {
                addLog('Setting up long-polling fallback...', 'info');
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

                addLog('Terminal connected via bus service (long-polling)', 'success');
            } catch (e) {
                console.warn('Error setting up bus service:', e);
                addLog(`Error setting up bus service: ${e.message}`, 'error');
            }
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
            // Add initial log
            addLog('Terminal service initializing...', 'info');

            // Try WebSocket first
            connectWebSocket();

            // Also start bus listening as a parallel channel
            // The server will send to both to ensure delivery
            startBusListening();
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
