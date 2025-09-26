/** @odoo-module **/
import { registry } from "@web/core/registry";

/**
 * IComply Terminal Service - Real-time log display terminal (File-based)
 * 
 * Features:
 * - Real-time log message display from server log files
 * - No database storage - reads directly from log files
 * - File position tracking for efficient polling
 * - Bus service integration for live updates
 * - Message filtering and searching
 * - Automatic log refresh from file system
 */
export const icomplyTerminalService = {
    dependencies: ['bus_service', 'rpc'],

    start(env, { bus_service, rpc }) {
        const logs = [];
        const listeners = new Set();
        const MAX_LOGS = 500; // Reduced for better performance
        
        let pollInterval = null;
        let filePosition = 0; // Track file reading position
        let isInitialized = false;
        let isPaused = false;

        /**
         * Format timestamp consistently
         */
        function formatTimestamp(date) {
            if (typeof date === 'string') {
                return date; // Already formatted
            }
            const d = new Date(date);
            const year = d.getFullYear();
            const month = String(d.getMonth() + 1).padStart(2, '0');
            const day = String(d.getDate()).padStart(2, '0');
            const hours = String(d.getHours()).padStart(2, '0');
            const minutes = String(d.getMinutes()).padStart(2, '0');
            const seconds = String(d.getSeconds()).padStart(2, '0');

            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
        }

        /**
         * Add a log entry
         */
        function addLog(message, type = 'info', timestamp = null, level = null, skipNotify = false) {
            if (!timestamp) {
                timestamp = formatTimestamp(new Date());
            }

            const logEntry = { 
                message, 
                type, 
                timestamp: formatTimestamp(timestamp), 
                level: level || type.toUpperCase(),
                id: Date.now() + Math.random() // Simple unique ID
            };

            // Add to logs array
            logs.push(logEntry);

            // Limit logs length for performance
            if (logs.length > MAX_LOGS) {
                logs.shift();
            }

            // Notify listeners unless specifically skipped
            if (!skipNotify) {
                notifyListeners(logEntry);
            }
        }

        /**
         * Notify all registered listeners
         */
        function notifyListeners(logEntry) {
            listeners.forEach(listener => {
                try {
                    listener(logEntry.message, logEntry.type, logEntry.timestamp, logEntry.level);
                } catch (e) {
                    console.error('Error in terminal listener:', e);
                }
            });
        }

        /**
         * Load recent logs from file
         */
        async function loadRecentLogs() {
            try {
                const recentLogs = await rpc('/icomply/logs/recent', { limit: 100 });
                
                // Clear existing logs and add fresh ones
                logs.length = 0;
                
                recentLogs.forEach(log => {
                    addLog(
                        log.message,
                        log.type,
                        log.timestamp,
                        log.level,
                        true // Skip notify during bulk load
                    );
                });

                // Single notification for refresh
                if (isInitialized) {
                    addLog('Logs refreshed from file system', 'info');
                }

                console.log(`Loaded ${recentLogs.length} recent logs from file`);
            } catch (error) {
                console.error('Error loading recent logs:', error);
                addLog(`Error loading logs: ${error.message}`, 'error');
            }
        }

        /**
         * Poll for new logs from file (incremental)
         */
        async function pollNewLogs() {
            if (isPaused) return;

            try {
                const result = await rpc('/icomply/logs/poll', { 
                    last_position: filePosition 
                });

                if (result.logs && result.logs.length > 0) {
                    // Map log levels to terminal types
                    const levelMapping = {
                        'DEBUG': 'info',
                        'INFO': 'info',
                        'WARNING': 'warning', 
                        'ERROR': 'error',
                        'CRITICAL': 'error',
                    };

                    result.logs.forEach(log => {
                        const type = levelMapping[log.level] || 'info';
                        addLog(
                            log.message,
                            type,
                            log.timestamp,
                            log.level
                        );
                    });

                    console.log(`Polled ${result.logs.length} new logs from file`);
                }

                // Update file position
                filePosition = result.position;

            } catch (error) {
                console.error('Error polling new logs:', error);
                // Don't spam error messages for polling failures
                if (isInitialized) {
                    addLog(`Polling error: ${error.message}`, 'warning');
                }
            }
        }

        /**
         * Initialize the terminal service
         */
        async function initialize() {
            try {
                // Load initial logs from file
                await loadRecentLogs();
                
                // Set up bus service for real-time updates (optional enhancement)
                bus_service.addChannel('icomply_logs_realtime');

                // Listen for real-time log broadcasts
                bus_service.addEventListener('notification', (ev) => {
                    const notifications = ev.detail;
                    if (Array.isArray(notifications)) {
                        notifications.forEach((notification) => {
                            if (notification.type === 'new_logs' && notification.payload) {
                                const payload = notification.payload;
                                if (payload.logs && Array.isArray(payload.logs)) {
                                    payload.logs.forEach(log => {
                                        addLog(
                                            log.message, 
                                            log.type, 
                                            log.timestamp, 
                                            log.level
                                        );
                                    });
                                    filePosition = payload.position;
                                }
                            }
                        });
                    }
                });

                // Set up polling for new logs (every 2 seconds)
                pollInterval = setInterval(() => {
                    pollNewLogs();
                }, 2000);

                isInitialized = true;
                addLog('IComply Terminal initialized successfully (File-based)', 'success');

            } catch (error) {
                console.error('Error initializing terminal service:', error);
                addLog(`Terminal initialization error: ${error.message}`, 'error');
            }
        }

        /**
         * Send a log message to the server (optional - for manual logging)
         */
        async function sendLog(message, level = 'INFO', module = null) {
            try {
                // Add locally first for immediate feedback
                const timestamp = formatTimestamp(new Date());
                const type = level.toLowerCase() === 'error' ? 'error' : 
                            level.toLowerCase() === 'warning' ? 'warning' :
                            level.toLowerCase() === 'critical' ? 'error' : 'info';
                
                addLog(message, type, timestamp, level);

                // Send to server (this will write to log file and be picked up by polling)
                await rpc('/icomply/logs/send', {
                    message: message,
                    level: level,
                    module: module
                });

            } catch (error) {
                console.error('Error sending log:', error);
                addLog(`Failed to send log to server: ${error.message}`, 'error');
            }
        }

        /**
         * Get log statistics from file
         */
        async function getStats() {
            try {
                return await rpc('/icomply/logs/stats');
            } catch (error) {
                console.error('Error getting log stats:', error);
                return {};
            }
        }

        /**
         * Cleanup function
         */
        function cleanup() {
            if (pollInterval) {
                clearInterval(pollInterval);
                pollInterval = null;
            }
        }

        // Initialize the service
        initialize();

        // Handle page unload
        window.addEventListener('beforeunload', cleanup);

        // Return public interface
        return {
            /**
             * Add a new log entry locally
             */
            addLog,

            /**
             * Send a log message to the server (will be written to file)
             */
            sendLog,

            /**
             * Register a listener for new log entries
             * @param {Function} listener - Callback function(message, type, timestamp, level)
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
             */
            getLogs() {
                return [...logs];
            },

            /**
             * Clear all local logs (memory only)
             */
            clearLogs() {
                logs.length = 0;
                addLog('Local logs cleared', 'info');
            },

            /**
             * Refresh logs from file system
             */
            async refreshLogs() {
                await loadRecentLogs();
            },

            /**
             * Force poll for new logs immediately
             */
            async pollNow() {
                await pollNewLogs();
            },

            /**
             * Get log statistics from file
             */
            getStats,

            /**
             * Pause/resume polling
             */
            pausePolling() {
                isPaused = true;
                addLog('Log polling paused', 'info');
            },

            resumePolling() {
                isPaused = false;
                addLog('Log polling resumed', 'info');
            },

            isPaused() {
                return isPaused;
            },

            /**
             * Get current file position
             */
            getFilePosition() {
                return filePosition;
            },

            /**
             * Reset file position (will re-read from beginning on next poll)
             */
            resetFilePosition() {
                filePosition = 0;
                addLog('File position reset - will re-read from beginning', 'info');
            },

            /**
             * Manual cleanup
             */
            cleanup,

            /**
             * Check if service is initialized
             */
            isInitialized() {
                return isInitialized;
            },

            /**
             * Get polling status
             */
            getStatus() {
                return {
                    initialized: isInitialized,
                    paused: isPaused,
                    filePosition: filePosition,
                    logCount: logs.length,
                    listenerCount: listeners.size
                };
            }
        };
    }
};

icomplyTerminalService.template = "icomply.Terminal";
// Register the service
registry.category("services").add("icomply_terminal", icomplyTerminalService);