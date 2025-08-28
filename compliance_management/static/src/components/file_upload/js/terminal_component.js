/** @odoo-module **/
import { Component, useState, useRef, onMounted, onWillUnmount } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";

// Debug mode - set to false for production
const DEBUG = false;
function logDebug(...args) {
  if (DEBUG) console.log(...args);
}

export class TerminalComponent extends Component {
    setup() {
        // Initialize state with fallback empty data
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
        });

        // Get DOM references
        this.terminalBodyRef = useRef("terminalBody");
        this.logCountRef = useRef("logCount");
        this.filterPanelRef = useRef("filterPanel");
        this.filterInputRef = useRef("filterInput");

        // Processing animation frames
        this.processingFrames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        this.animationInterval = null;

        // Try to get terminal service
        try {
            this.terminal = useService("terminal");

            // Setup event handlers
            onMounted(() => {
                try {
                    // Get existing logs
                    this.state.logs = this.terminal.getLogs() || [];

                    // Register log listener
                    this.unsubscribe = this.terminal.onLog(this.handleNewLog.bind(this));

                    // Start processing animation
                    this.startProcessingAnimation();

                    // Scroll to bottom
                    this.scrollToBottom();
                } catch (e) {
                    logDebug("Error initializing terminal component:", e);
                    // Add default log message on error
                    this.state.logs.push({
                        message: "Terminal service initialized with limited functionality",
                        type: "warning",
                        timestamp: new Date().toLocaleTimeString()
                    });
                }
            });

            onWillUnmount(() => {
                // Unregister log listener if it exists
                if (this.unsubscribe) {
                    try {
                        this.unsubscribe();
                    } catch (e) {
                        logDebug("Error unsubscribing from terminal logs:", e);
                    }
                }

                // Stop animation interval
                if (this.animationInterval) {
                    clearInterval(this.animationInterval);
                }
            });
        } catch (e) {
            logDebug("Terminal service not available:", e);
            // Add default log functionality for when service is not available
            this.terminal = {
                addLog: (message, type = "info", timestamp = null) => {
                    if (!timestamp) {
                        timestamp = new Date().toLocaleTimeString();
                    }
                    this.state.logs.push({ message, type, timestamp });
                    this.scrollToBottom();
                },
                getLogs: () => [],
                clearLogs: () => {
                    this.state.logs = [];
                }
            };

            // Add initial log message
            this.state.logs.push({
                message: "Terminal service not available, using limited functionality",
                type: "warning",
                timestamp: new Date().toLocaleTimeString()
            });

            onMounted(() => {
                this.scrollToBottom();
            });
        }
    }

    /**
     * Start the processing animation interval
     */
    startProcessingAnimation() {
        // Clear any existing interval
        if (this.animationInterval) {
            clearInterval(this.animationInterval);
        }

        // Set initial processing state based on log content
        this.updateProcessingState();

        // Start animation interval - update every 100ms
        this.animationInterval = setInterval(() => {
            // Only animate if processing
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
        // Check if we have logs and the recent ones indicate processing
        if (!this.state.logs || this.state.logs.length === 0) {
            this.state.isProcessing = false;
            return;
        }

        // Look at the most recent logs (last 5)
        const recentLogs = this.state.logs.slice(-5);

        // Check for processing indicators in recent logs
        const processingKeywords = [
            'processing', 'uploading', 'importing', 'reading', 'loading',
            'analyzing', 'progress'
        ];

        // Check if we have any completion indicators
        const completionKeywords = [
            'completed', 'finished', 'done', 'success', 'failed', 'error',
            '100% complete'
        ];

        // Look at most recent log first
        const latestLog = this.state.logs[this.state.logs.length - 1];
        if (latestLog) {
            // If latest log indicates completion, stop processing
            if (completionKeywords.some(keyword =>
                latestLog.message.toLowerCase().includes(keyword))) {
                this.state.isProcessing = false;
                return;
            }

            // If latest log indicates processing, start processing
            if (processingKeywords.some(keyword =>
                latestLog.message.toLowerCase().includes(keyword))) {
                this.state.isProcessing = true;
                return;
            }
        }

        // Check recent logs for processing indicators
        const hasProcessing = recentLogs.some(log =>
            processingKeywords.some(keyword =>
                log.message.toLowerCase().includes(keyword)));

        // Check recent logs for completion indicators
        const hasCompletion = recentLogs.some(log =>
            completionKeywords.some(keyword =>
                log.message.toLowerCase().includes(keyword)));

        // If we have both, use the most recent indicator
        if (hasProcessing && hasCompletion) {
            // Find the index of the most recent processing and completion logs
            const lastProcessingIndex = recentLogs.slice().reverse().findIndex(log =>
                processingKeywords.some(keyword =>
                    log.message.toLowerCase().includes(keyword)));

            const lastCompletionIndex = recentLogs.slice().reverse().findIndex(log =>
                completionKeywords.some(keyword =>
                    log.message.toLowerCase().includes(keyword)));

            // If processing is more recent than completion, we're processing
            this.state.isProcessing = lastProcessingIndex < lastCompletionIndex;
        } else {
            // Otherwise, we're processing if we have processing indicators
            this.state.isProcessing = hasProcessing;
        }
    }

    /**
     * Get the current processing animation frame
     */
    get processingIndicator() {
        return this.processingFrames[this.state.processingAnimationFrame];
    }

    /**
     * Handle new log message from terminal service
     * @param {string} message - Log message
     * @param {string} type - Log type
     * @param {string} timestamp - Log timestamp
     */
    handleNewLog(message, type, timestamp) {
        // Update logs state
        this.state.logs.push({ message, type, timestamp });

        // Update processing state
        this.updateProcessingState();

        // Scroll to bottom
        this.scrollToBottom();
    }

    /**
     * Scroll terminal body to bottom
     */
    scrollToBottom() {
        if (this.terminalBodyRef.el) {
            setTimeout(() => {
                this.terminalBodyRef.el.scrollTop = this.terminalBodyRef.el.scrollHeight;
            }, 0);
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
     * Toggle filter panel visibility
     */
    toggleFilterPanel() {
        this.state.showFilterPanel = !this.state.showFilterPanel;
    }

    /**
     * Handle filter input change
     * @param {Event} ev - Input event
     */
    onFilterInput(ev) {
        this.state.searchText = ev.target.value;
    }

    /**
     * Toggle severity filter
     * @param {string} severity - Severity to toggle
     */
    toggleSeverity(severity) {
        this.state.enabledSeverities[severity] = !this.state.enabledSeverities[severity];
    }

    /**
     * Check if a log entry should be visible based on filters
     * @param {Object} log - Log entry
     * @returns {boolean} - Whether log should be visible
     */
    isLogVisible(log) {
        // Check severity filter
        if (!this.state.enabledSeverities[log.type]) {
            return false;
        }

        // Check text filter
        if (this.state.searchText) {
            return log.message.toLowerCase().includes(this.state.searchText.toLowerCase());
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
     * @returns {number} - Number of visible logs
     */
    get visibleLogsCount() {
        return this.state.logs.filter(log => this.isLogVisible(log)).length;
    }

    /**
     * Check if all severities are enabled
     * @returns {boolean} - Whether all severities are enabled
     */
    get areAllSeveritiesEnabled() {
        // Safely check if all severities are enabled
        if (!this.state.enabledSeverities) return true;
        return Object.values(this.state.enabledSeverities).every(value => Boolean(value));
    }
}

// Define component properties
TerminalComponent.template = "compliance_management.Terminal";
TerminalComponent.props = {};
