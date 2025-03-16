/** @odoo-module **/
import { browser } from "@web/core/browser/browser";
import { registry } from "@web/core/registry";

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
        const logs = [];
        const listeners = new Set();
        const MAX_LOGS = 1000;

        // Initialize WebSocket if supported
        function connectWebSocket() {
            if (typeof WebSocket === 'undefined') {
                console.log('WebSockets not supported by your browser, using long-polling');
                return;
            }

            try {
                const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
                const wsUrl = `${protocol}://${window.location.host}/csv_import/ws`;

                socket = new WebSocket(wsUrl);

                socket.onopen = () => {
                    connected = true;
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
                        }
                    } catch (e) {
                        console.error('Error parsing WebSocket message:', e);
                    }
                };

                socket.onclose = () => {
                    connected = false;
                    notifyListeners('WebSocket disconnected', 'warning');

                    // Try to reconnect after 5 seconds
                    setTimeout(() => {
                        if (!connected) {
                            connectWebSocket();
                        }
                    }, 5000);
                };

                socket.onerror = (error) => {
                    connected = false;
                    notifyListeners('WebSocket error, falling back to long-polling', 'error');
                    console.error('WebSocket error:', error);
                };
            } catch (e) {
                connected = false;
                notifyListeners('Error setting up WebSocket, using long-polling', 'error');
                console.error('WebSocket setup error:', e);
            }
        }

        // Start listening to bus messages
        function startBusListening() {
            try {
                const channel = `csv_import_logs_${env.services.user.userId}`;
                bus_service.addChannel(channel);

                // Use addEventListener instead of onNotification
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
            } catch (e) {
                console.warn('Error setting up bus service:', e);
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

            // Fallback to RPC
            try {
                await rpc('/csv_import/log', {
                    message,
                    message_type: type
                });
            } catch (error) {
                console.error('Error sending log:', error);
                // Still add the log locally even if server send fails
                addLog(`Failed to send log to server: ${error.message}`, 'warning');
            }
        }

        // Initialize bus listening (skip WebSocket for now)
        try {
            // Don't try WebSocket for now to avoid errors
            connectWebSocket();
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
            }
        };
    }
};

registry.category("services").add("terminal", terminalService);

// /** @odoo-module **/
// import { browser } from "@web/core/browser/browser";
// import { registry } from "@web/core/registry";
// import { session } from "@web/session";

// /**
//  * Terminal Service - Provides a console-like interface for log messages
//  * 
//  * Features:
//  * - Real-time message display with severity levels
//  * - WebSocket support with long-polling fallback
//  * - Message filtering and searching
//  */
// export const terminalService = {
//     dependencies: ['bus_service', 'rpc'],

//     start(env, { bus_service, rpc }) {
//         let socket = null;
//         let connected = false;
//         const logs = [];
//         const listeners = new Set();
//         const MAX_LOGS = 1000;

//         // Initialize WebSocket if supported
//         function connectWebSocket() {
//             if (typeof WebSocket === 'undefined') {
//                 console.log('WebSockets not supported by your browser, using long-polling');
//                 return;
//             }

//             try {
//                 const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
//                 const wsUrl = `${protocol}://${window.location.host}/csv_import/ws`;

//                 // Add connection timeout
//                 const connectionTimeout = setTimeout(() => {
//                     if (socket && socket.readyState !== WebSocket.OPEN) {
//                         console.log('WebSocket connection timeout, falling back to long-polling');
//                         socket.close();
//                         connected = false;
//                         startBusListening(); // Start your fallback mechanism
//                     }
//                 }, 5000); // 5 second timeout

//                 socket = new WebSocket(wsUrl);

//                 socket.onopen = () => {
//                     clearTimeout(connectionTimeout);
//                     connected = true;
//                     // Rest of your existing code...
//                 };

//                 // Rest of your socket event handlers...
//             } catch (e) {
//                 connected = false;
//                 notifyListeners('Error setting up WebSocket, using long-polling', 'error');
//                 startBusListening(); // Start your fallback mechanism
//             }
//         }
//         // function connectWebSocket() {
//         //     if (typeof WebSocket === 'undefined') {
//         //         console.log('WebSockets not supported by your browser, using long-polling');
//         //         return;
//         //     }

//         //     try {
//         //         const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
//         //         const wsUrl = `${protocol}://${window.location.host}/csv_import/ws`;

//         //         socket = new WebSocket(wsUrl);

//         //         socket.onopen = () => {
//         //             connected = true;
//         //             notifyListeners('WebSocket connected', 'success');

//         //             // Send authentication
//         //             socket.send(JSON.stringify({
//         //                 type: 'auth',
//         //                 user_id: session.uid,
//         //                 session_id: session.session_id
//         //             }));
//         //         };

//         //         socket.onmessage = (event) => {
//         //             try {
//         //                 const data = JSON.parse(event.data);
//         //                 if (data.type === 'log_message') {
//         //                     addLog(data.message, data.message_type, data.timestamp);
//         //                 }
//         //             } catch (e) {
//         //                 console.error('Error parsing WebSocket message:', e);
//         //             }
//         //         };

//         //         socket.onclose = () => {
//         //             connected = false;
//         //             notifyListeners('WebSocket disconnected', 'warning');

//         //             // Try to reconnect after 5 seconds
//         //             setTimeout(() => {
//         //                 if (!connected) {
//         //                     connectWebSocket();
//         //                 }
//         //             }, 5000);
//         //         };

//         //         socket.onerror = (error) => {
//         //             connected = false;
//         //             notifyListeners('WebSocket error, falling back to long-polling', 'error');
//         //             console.error('WebSocket error:', error);
//         //         };
//         //     } catch (e) {
//         //         connected = false;
//         //         notifyListeners('Error setting up WebSocket, using long-polling', 'error');
//         //         console.error('WebSocket setup error:', e);
//         //     }
//         // }

//         // Start listening to bus messages
//         function startBusListening() {
//             const channel = `csv_import_logs_${session.uid}`;
//             bus_service.addChannel(channel);

//             // Use addEventListener instead of onNotification
//             bus_service.addEventListener('notification', (ev) => {
//                 const notifications = ev.detail;
//                 if (Array.isArray(notifications)) {
//                     notifications.forEach((notification) => {
//                         if (notification.type === 'log_message' && notification.payload) {
//                             addLog(notification.payload.message, notification.payload.message_type, notification.payload.timestamp);
//                         }
//                     });
//                 }
//             });
//         }

//         // Add a log entry
//         function addLog(message, type = 'info', timestamp = null) {
//             if (!timestamp) {
//                 timestamp = new Date().toLocaleTimeString();
//             }

//             // Add to logs array
//             logs.push({ message, type, timestamp });

//             // Limit logs length
//             if (logs.length > MAX_LOGS) {
//                 logs.shift();
//             }

//             // Notify listeners
//             notifyListeners(message, type, timestamp);
//         }

//         // Notify all registered listeners
//         function notifyListeners(message, type, timestamp) {
//             listeners.forEach(listener => {
//                 try {
//                     listener(message, type, timestamp);
//                 } catch (e) {
//                     console.error('Error in terminal listener:', e);
//                 }
//             });
//         }

//         // Send a log message to the server
//         async function sendLog(message, type = 'info') {
//             // Display locally immediately
//             addLog(message, type);

//             // Try WebSocket first if connected
//             if (connected && socket && socket.readyState === WebSocket.OPEN) {
//                 socket.send(JSON.stringify({
//                     type: 'log_message',
//                     message: message,
//                     message_type: type,
//                     timestamp: new Date().toISOString()
//                 }));
//                 return;
//             }

//             // Fallback to RPC
//             try {
//                 await rpc('/csv_import/log', {
//                     message,
//                     message_type: type
//                 });
//             } catch (error) {
//                 console.error('Error sending log:', error);
//             }
//         }

//         // Initialize WebSocket and Bus listening
//         try {
//             connectWebSocket();
//             startBusListening();
//         } catch (e) {
//             console.error('Error initializing terminal service:', e);
//             // Add initial error log
//             addLog(`Error initializing terminal service: ${e.message}`, 'error');
//         }

//         // Return public interface
//         return {
//             /**
//              * Add a new log entry
//              * 
//              * @param {string} message - The log message
//              * @param {string} [type='info'] - Log type (info, success, warning, error)
//              */
//             addLog,

//             /**
//              * Send a log message to the server and add it locally
//              * 
//              * @param {string} message - The log message
//              * @param {string} [type='info'] - Log type (info, success, warning, error)
//              * @returns {Promise} - Promise resolving when the log is sent
//              */
//             sendLog,

//             /**
//              * Register a listener for new log entries
//              * 
//              * @param {Function} listener - Callback function(message, type, timestamp)
//              * @returns {Function} - Function to unregister the listener
//              */
//             onLog(listener) {
//                 listeners.add(listener);
//                 return () => {
//                     listeners.delete(listener);
//                 };
//             },

//             /**
//              * Get all logged messages
//              * 
//              * @returns {Array} - Array of log entries
//              */
//             getLogs() {
//                 return [...logs];
//             },

//             /**
//              * Clear all logs
//              */
//             clearLogs() {
//                 logs.length = 0;
//                 notifyListeners('Logs cleared', 'info');
//             }
//         };
//     }
// };

// registry.category("services").add("terminal", terminalService);