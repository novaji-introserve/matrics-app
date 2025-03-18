/** @odoo-module **/
import { Component, useState, useRef, onMounted, onWillUnmount } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";

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
        });

        // Get DOM references
        this.terminalBodyRef = useRef("terminalBody");
        this.logCountRef = useRef("logCount");
        this.filterPanelRef = useRef("filterPanel");
        this.filterInputRef = useRef("filterInput");

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

                    // Scroll to bottom
                    this.scrollToBottom();
                } catch (e) {
                    console.warn("Error initializing terminal component:", e);
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
                        console.warn("Error unsubscribing from terminal logs:", e);
                    }
                }
            });
        } catch (e) {
            console.warn("Terminal service not available:", e);
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
     * Handle new log message from terminal service
     * @param {string} message - Log message
     * @param {string} type - Log type
     * @param {string} timestamp - Log timestamp
     */
    handleNewLog(message, type, timestamp) {
        // Update logs state
        this.state.logs.push({ message, type, timestamp });

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

    /**
     * Get icon HTML for a log type
     * @param {string} type - Log type
     * @returns {string} - Icon class
     */
    getIconClass(type) {
        const icons = {
            error: "fa fa-times-circle text-danger",
            success: "fa fa-check-circle text-success",
            warning: "fa fa-exclamation-triangle text-warning",
            info: "fa fa-info-circle text-info"
        };
        return icons[type] || icons.info;
    }

    /**
     * Get CSS class for a log type
     * @param {string} type - Log type
     * @returns {string} - CSS class
     */
    getLogTypeClass(type) {
        const classes = {
            error: "log-type-error",
            success: "log-type-success",
            warning: "log-type-warning",
            info: "log-type-info"
        };
        return classes[type] || classes.info;
    }
}

// Define component properties
TerminalComponent.template = "compliance_management.Terminal";
TerminalComponent.props = {};