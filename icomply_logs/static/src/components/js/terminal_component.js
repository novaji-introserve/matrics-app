/** @odoo-module **/
import { Component, useState, useRef, onMounted, onWillUnmount } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { registry } from "@web/core/registry";
import { _t } from "@web/core/l10n/translation";

/**
 * IComply Terminal Component (File-based)
 * Displays system logs in a terminal-like interface reading directly from log files
 */
export class IComplyTerminalComponent extends Component {
    setup() {
        // Initialize state
        this.state = useState({
            logs: [],
            searchText: "",
            enabledSeverities: {
                info: true,
                success: true,
                warning: true,
                error: true
            },
            showFilterPanel: false,
            isProcessing: false,
            processingAnimationFrame: 0,
            autoScroll: true,
            showTimestamp: true,
            showLevel: true,
            isPaused: false,
            filePosition: 0,
            connectionStatus: 'disconnected', // disconnected, connecting, connected
            lastUpdateTime: null,
        });

        // Get DOM references
        this.terminalBodyRef = useRef("terminalBody");
        this.logCountRef = useRef("logCount");
        this.filterPanelRef = useRef("filterPanel");
        this.filterInputRef = useRef("filterInput");

        // Processing animation frames
        this.processingFrames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        this.animationInterval = null;
        this.pollInterval = null;

        // Services
        this.rpc = useService('rpc');

        // Try to get terminal service
        try {
            this.terminal = useService("icomply_terminal");
            this.hasTerminalService = true;
        } catch (e) {
            console.warn("IComply terminal service not available, using direct file polling:", e);
            this.hasTerminalService = false;
            // Create basic fallback functionality with direct polling
            this.terminal = {
                getLogs: () => this.state.logs,
                addLog: (message, type, timestamp) => {
                    this.state.logs.push({ 
                        message, 
                        type, 
                        timestamp: timestamp || new Date().toLocaleTimeString(),
                        id: Date.now() + Math.random()
                    });
                    this.scrollToBottom();
                },
                clearLogs: () => { this.state.logs = []; },
                onLog: (listener) => () => {} // No-op unsubscribe function
            };
        }

        // Setup event handlers
        onMounted(() => {
            this.initializeTerminal();
        });

        onWillUnmount(() => {
            this.cleanup();
        });
    }

    /**
     * Initialize terminal component
     */
    async initializeTerminal() {
        try {
            this.state.connectionStatus = 'connecting';
            
            if (this.hasTerminalService) {
                // Get existing logs from service
                this.state.logs = this.terminal.getLogs() || [];

                // Register log listener
                this.unsubscribe = this.terminal.onLog(this.handleNewLog.bind(this));
                
                // Get service status
                const status = this.terminal.getStatus();
                this.state.isPaused = status.paused;
                this.state.filePosition = status.filePosition;
            } else {
                // Load logs directly from server if service not available
                await this.loadLogsFromServer();
                
                // Start direct polling
                this.startDirectPolling();
            }

            // Start processing animation
            this.startProcessingAnimation();

            // Initial scroll to bottom
            this.scrollToBottom();

            this.state.connectionStatus = 'connected';
            this.state.lastUpdateTime = new Date();

        } catch (e) {
            console.error("Error initializing terminal component:", e);
            this.state.connectionStatus = 'disconnected';
            this.addLocalLog("Error initializing terminal: " + e.message, "error");
        }
    }

    /**
     * Start direct polling when service is not available
     */
    startDirectPolling() {
        this.pollInterval = setInterval(async () => {
            if (!this.state.isPaused) {
                await this.pollNewLogs();
            }
        }, 2000); // Poll every 2 seconds
    }

    /**
     * Poll for new logs directly from server
     */
    async pollNewLogs() {
        try {
            const result = await this.rpc('/icomply/logs/poll', { 
                last_position: this.state.filePosition 
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
                    this.addLocalLog(log.message, type, log.timestamp, log.level);
                });

                this.state.lastUpdateTime = new Date();
            }

            // Update file position
            this.state.filePosition = result.position;

        } catch (error) {
            console.error('Error polling new logs:', error);
            this.state.connectionStatus = 'disconnected';
            setTimeout(() => {
                this.state.connectionStatus = 'connecting';
            }, 5000);
        }
    }

    /**
     * Add log locally
     */
    addLocalLog(message, type, timestamp, level) {
        this.state.logs.push({
            message,
            type,
            timestamp: timestamp || new Date().toLocaleTimeString(),
            level: level || type.toUpperCase(),
            id: Date.now() + Math.random()
        });

        // Limit logs to prevent memory issues
        if (this.state.logs.length > 500) {
            this.state.logs.shift();
        }

        // Update processing state
        this.updateProcessingState();

        // Auto-scroll if enabled
        if (this.state.autoScroll) {
            this.scrollToBottom();
        }
    }

    /**
     * Load logs directly from server
     */
    async loadLogsFromServer() {
        try {
            const logs = await this.rpc('/icomply/logs/recent', { limit: 100 });
            this.state.logs = logs.map(log => ({
                message: log.message,
                type: log.type,
                timestamp: log.timestamp,
                level: log.level,
                id: Date.now() + Math.random()
            }));
            this.state.lastUpdateTime = new Date();
        } catch (error) {
            console.error('Error loading logs from server:', error);
            this.state.connectionStatus = 'disconnected';
        }
    }

    /**
     * Start the processing animation interval
     */
    startProcessingAnimation() {
        if (this.animationInterval) {
            clearInterval(this.animationInterval);
        }

        // Update processing state based on logs
        this.updateProcessingState();

        // Start animation interval
        this.animationInterval = setInterval(() => {
            if (this.state.isProcessing) {
                this.state.processingAnimationFrame =
                    (this.state.processingAnimationFrame + 1) % this.processingFrames.length;
            }
        }, 100);
    }

    /**
     * Update processing state based on recent logs
     */
    updateProcessingState() {
        if (!this.state.logs || this.state.logs.length === 0) {
            this.state.isProcessing = false;
            return;
        }

        // Look at recent logs for processing indicators
        const recentLogs = this.state.logs.slice(-5);
        
        const processingKeywords = [
            'processing', 'loading', 'analyzing', 'progress', 'running',
            'executing', 'starting', 'initializing', 'polling', 'fetching'
        ];

        const completionKeywords = [
            'completed', 'finished', 'done', 'success', 'failed', 'error',
            'stopped', 'terminated', 'initialized'
        ];

        // Check the most recent log
        const latestLog = this.state.logs[this.state.logs.length - 1];
        if (latestLog) {
            const message = latestLog.message.toLowerCase();
            
            if (completionKeywords.some(keyword => message.includes(keyword))) {
                this.state.isProcessing = false;
                return;
            }

            if (processingKeywords.some(keyword => message.includes(keyword))) {
                this.state.isProcessing = true;
                return;
            }
        }

        // Check recent logs
        const hasProcessing = recentLogs.some(log =>
            processingKeywords.some(keyword =>
                log.message.toLowerCase().includes(keyword)));

        this.state.isProcessing = hasProcessing;
    }

    /**
     * Get the current processing animation frame
     */
    get processingIndicator() {
        return this.processingFrames[this.state.processingAnimationFrame];
    }

    /**
     * Handle new log message from terminal service
     */
    handleNewLog(message, type, timestamp, level) {
        this.addLocalLog(message, type, timestamp, level);
    }

    /**
     * Scroll terminal body to bottom
     */
    scrollToBottom() {
        if (this.terminalBodyRef.el) {
            setTimeout(() => {
                this.terminalBodyRef.el.scrollTop = this.terminalBodyRef.el.scrollHeight;
            }, 10);
        }
    }

    /**
     * Clear all logs
     */
    clearLogs() {
        this.state.logs = [];
        if (this.terminal && this.terminal.clearLogs) {
            this.terminal.clearLogs();
        }
    }

    /**
     * Refresh logs from server
     */
    async refreshLogs() {
        try {
            this.state.connectionStatus = 'connecting';
            
            if (this.hasTerminalService && this.terminal.refreshLogs) {
                await this.terminal.refreshLogs();
                this.state.logs = this.terminal.getLogs() || [];
            } else {
                await this.loadLogsFromServer();
            }
            
            this.state.connectionStatus = 'connected';
            this.addLocalLog('Logs refreshed successfully', 'success');
        } catch (error) {
            this.state.connectionStatus = 'disconnected';
            this.addLocalLog('Failed to refresh logs: ' + error.message, 'error');
        }
    }

    /**
     * Toggle pause/resume polling
     */
    togglePause() {
        this.state.isPaused = !this.state.isPaused;
        
        if (this.hasTerminalService) {
            if (this.state.isPaused) {
                this.terminal.pausePolling();
            } else {
                this.terminal.resumePolling();
            }
        }
        
        const status = this.state.isPaused ? 'paused' : 'resumed';
        this.addLocalLog(`Log polling ${status}`, 'info');
    }

    /**
     * Reset file position
     */
    resetFilePosition() {
        this.state.filePosition = 0;
        
        if (this.hasTerminalService && this.terminal.resetFilePosition) {
            this.terminal.resetFilePosition();
        }
        
        this.addLocalLog('File position reset - will re-read from beginning', 'info');
    }

    /**
     * Send a test log message
     */
    async sendTestLog() {
        const message = `Test log message at ${new Date().toLocaleTimeString()}`;
        try {
            if (this.hasTerminalService && this.terminal.sendLog) {
                await this.terminal.sendLog(message, 'INFO', 'terminal_test');
            } else {
                // Send directly to server
                await this.rpc('/icomply/logs/send', {
                    message: message,
                    level: 'INFO',
                    module: 'terminal_test'
                });
            }
        } catch (error) {
            this.addLocalLog('Failed to send test log: ' + error.message, 'error');
        }
    }

    /**
     * Toggle filter panel visibility
     */
    toggleFilterPanel() {
        this.state.showFilterPanel = !this.state.showFilterPanel;
    }

    /**
     * Handle filter input change
     */
    onFilterInput(ev) {
        this.state.searchText = ev.target.value;
    }

    /**
     * Toggle severity filter
     */
    toggleSeverity(severity) {
        this.state.enabledSeverities[severity] = !this.state.enabledSeverities[severity];
    }

    /**
     * Toggle auto-scroll
     */
    toggleAutoScroll() {
        this.state.autoScroll = !this.state.autoScroll;
        if (this.state.autoScroll) {
            this.scrollToBottom();
        }
    }

    /**
     * Check if a log entry should be visible based on filters
     */
    isLogVisible(log) {
        // Check severity filter
        if (!this.state.enabledSeverities[log.type]) {
            return false;
        }

        // Check text filter
        if (this.state.searchText) {
            const searchLower = this.state.searchText.toLowerCase();
            return log.message.toLowerCase().includes(searchLower) ||
                   (log.level && log.level.toLowerCase().includes(searchLower));
        }

        return true;
    }

    /**
     * Check if there are any error logs
     */
    get hasErrors() {
        return this.state.logs.some(log => log.type === 'error');
    }

    /**
     * Get visible logs count
     */
    get visibleLogsCount() {
        return this.state.logs.filter(log => this.isLogVisible(log)).length;
    }

    /**
     * Check if all severities are enabled
     */
    get areAllSeveritiesEnabled() {
        if (!this.state.enabledSeverities) return true;
        return Object.values(this.state.enabledSeverities).every(value => Boolean(value));
    }

    /**
     * Get log type CSS class
     */
    getLogTypeClass(log) {
        return `log-type-${log.type}`;
    }

    /**
     * Format log timestamp for display
     */
    formatLogTimestamp(timestamp) {
        if (!timestamp) return '';
        
        try {
            if (typeof timestamp === 'string') {
                return timestamp;
            }
            const date = new Date(timestamp);
            return date.toLocaleTimeString();
        } catch (e) {
            return timestamp;
        }
    }

    /**
     * Get connection status display info
     */
    get connectionStatusInfo() {
        const statusMap = {
            'connected': { text: 'Connected', class: 'text-success', icon: '●' },
            'connecting': { text: 'Connecting...', class: 'text-warning', icon: '◐' },
            'disconnected': { text: 'Disconnected', class: 'text-danger', icon: '●' }
        };
        return statusMap[this.state.connectionStatus] || statusMap['disconnected'];
    }

    /**
     * Get last update time formatted
     */
    get formattedLastUpdate() {
        if (!this.state.lastUpdateTime) return 'Never';
        return this.state.lastUpdateTime.toLocaleTimeString();
    }

    /**
     * Force immediate poll
     */
    async pollNow() {
        if (this.hasTerminalService && this.terminal.pollNow) {
            await this.terminal.pollNow();
        } else {
            await this.pollNewLogs();
        }
        this.addLocalLog('Manual poll completed', 'info');
    }

    /**
     * Cleanup function
     */
    cleanup() {
        // Unregister log listener if it exists
        if (this.unsubscribe) {
            try {
                this.unsubscribe();
            } catch (e) {
                console.error("Error unsubscribing from terminal logs:", e);
            }
        }

        // Stop animation interval
        if (this.animationInterval) {
            clearInterval(this.animationInterval);
        }

        // Stop polling interval
        if (this.pollInterval) {
            clearInterval(this.pollInterval);
        }
    }
}

// Define component template
IComplyTerminalComponent.template = "icomply.Terminal";

// Register the component as a client action
registry.category("actions").add("icomply_terminal", IComplyTerminalComponent);

