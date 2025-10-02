/** @odoo-module **/
import { Component, useState, useRef, onMounted, onWillUnmount } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { registry } from "@web/core/registry";

/**
 * IComply Terminal Component - Profile-based multi-terminal
 */
export class IComplyTerminalComponent extends Component {
    setup() {
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
            connectionStatus: 'disconnected',
            lastUpdateTime: null,
            pendingLogsCount: 0,
            profileInfo: null,
        });

        // Get profile_id from props
        this.profileId = this.props.action?.params?.profile_id || null;

        this.terminalBodyRef = useRef("terminalBody");
        this.processingFrames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        this.animationInterval = null;

        this.rpc = useService('rpc');

        try {
            this.terminal = useService("icomply_terminal");
            this.hasTerminalService = true;
        } catch (e) {
            console.warn("IComply terminal service not available:", e);
            this.hasTerminalService = false;
        }

        onMounted(() => {
            this.initializeTerminal();
        });

        onWillUnmount(() => {
            this.cleanup();
        });
    }

    async initializeTerminal() {
        try {
            this.state.connectionStatus = 'connecting';
            
            if (!this.profileId) {
                this.addLocalLog("Error: No profile ID specified", "error");
                this.state.connectionStatus = 'disconnected';
                return;
            }

            if (this.hasTerminalService) {
                // Initialize the profile session
                await this.terminal.initProfile(this.profileId);
                
                // Get existing logs
                this.state.logs = this.terminal.getLogs(this.profileId) || [];
                console.log(`Loaded ${this.state.logs.length} existing logs for profile ${this.profileId}`);

                // Register log listener
                this.unsubscribe = this.terminal.onLog(this.profileId, this.handleNewLog.bind(this));
                
                const status = this.terminal.getStatus(this.profileId);
                this.state.isPaused = status.paused;
                this.state.filePosition = status.filePosition;
                this.state.profileInfo = status.profileInfo;
                
                // Apply profile settings
                if (this.state.profileInfo) {
                    this.state.autoScroll = this.state.profileInfo.auto_scroll;
                    this.state.showTimestamp = this.state.profileInfo.show_timestamp;
                    this.state.showLevel = this.state.profileInfo.show_level;
                }
            } else {
                this.addLocalLog("Terminal service not available", "error");
            }

            this.startProcessingAnimation();
            this.scrollToBottom();

            this.state.connectionStatus = 'connected';
            this.state.lastUpdateTime = new Date();

        } catch (e) {
            console.error("Error initializing terminal component:", e);
            this.state.connectionStatus = 'disconnected';
            this.addLocalLog("Error initializing terminal: " + e.message, "error");
        }
    }

    addLocalLog(message, type, timestamp, level, skipScroll = false) {
        this.state.logs.push({
            message,
            type,
            timestamp: timestamp || new Date().toLocaleTimeString(),
            level: level || type.toUpperCase(),
            id: Date.now() + Math.random()
        });

        if (this.state.logs.length > 10000) {
            this.state.logs.shift();
        }

        this.updateProcessingState();

        if (!skipScroll && this.state.autoScroll && !this.state.isPaused) {
            this.scrollToBottom();
        }
    }

    startProcessingAnimation() {
        if (this.animationInterval) {
            clearInterval(this.animationInterval);
        }

        this.updateProcessingState();

        this.animationInterval = setInterval(() => {
            if (this.state.isProcessing) {
                this.state.processingAnimationFrame =
                    (this.state.processingAnimationFrame + 1) % this.processingFrames.length;
            }
        }, 100);
    }

    updateProcessingState() {
        if (!this.state.logs || this.state.logs.length === 0) {
            this.state.isProcessing = false;
            return;
        }

        const processingKeywords = [
            'processing', 'loading', 'analyzing', 'progress', 'running',
            'executing', 'starting', 'initializing', 'polling', 'fetching'
        ];

        const completionKeywords = [
            'completed', 'finished', 'done', 'success', 'failed', 'error',
            'stopped', 'terminated', 'initialized'
        ];

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

        const recentLogs = this.state.logs.slice(-5);
        const hasProcessing = recentLogs.some(log =>
            processingKeywords.some(keyword =>
                log.message.toLowerCase().includes(keyword)));

        this.state.isProcessing = hasProcessing;
    }

    get processingIndicator() {
        return this.processingFrames[this.state.processingAnimationFrame];
    }

    handleNewLog(message, type, timestamp, level) {
        this.addLocalLog(message, type, timestamp, level, this.state.isPaused);
        
        if (this.state.isPaused) {
            this.state.pendingLogsCount++;
        }
    }

    scrollToBottom() {
        if (this.terminalBodyRef.el) {
            setTimeout(() => {
                this.terminalBodyRef.el.scrollTop = this.terminalBodyRef.el.scrollHeight;
            }, 10);
        }
    }

    clearLogs() {
        this.state.logs = [];
        if (this.terminal && this.terminal.clearLogs) {
            this.terminal.clearLogs(this.profileId);
        }
    }

    async refreshLogs() {
        try {
            this.state.connectionStatus = 'connecting';
            
            if (this.hasTerminalService && this.terminal.reloadAllLogs) {
                await this.terminal.reloadAllLogs(this.profileId);
                this.state.logs = this.terminal.getLogs(this.profileId) || [];
            }
            
            this.state.connectionStatus = 'connected';
            this.addLocalLog(`Refreshed with ${this.state.logs.length} total logs`, 'success');
            this.scrollToBottom();
        } catch (error) {
            this.state.connectionStatus = 'disconnected';
            this.addLocalLog('Failed to refresh logs: ' + error.message, 'error');
        }
    }

    togglePause() {
        this.state.isPaused = !this.state.isPaused;
        
        if (this.hasTerminalService) {
            if (this.state.isPaused) {
                this.terminal.pausePolling(this.profileId);
                this.state.pendingLogsCount = 0;
            } else {
                this.terminal.resumePolling(this.profileId);
                const count = this.state.pendingLogsCount;
                this.state.pendingLogsCount = 0;
                this.addLocalLog(`Display resumed - ${count} logs accumulated`, 'success');
                this.scrollToBottom();
            }
        }
    }

    resetFilePosition() {
        this.state.filePosition = 0;
        
        if (this.hasTerminalService && this.terminal.resetFilePosition) {
            this.terminal.resetFilePosition(this.profileId);
        }
    }

    toggleFilterPanel() {
        this.state.showFilterPanel = !this.state.showFilterPanel;
    }

    onFilterInput(ev) {
        this.state.searchText = ev.target.value;
    }

    toggleSeverity(severity) {
        this.state.enabledSeverities[severity] = !this.state.enabledSeverities[severity];
    }

    toggleAutoScroll() {
        this.state.autoScroll = !this.state.autoScroll;
        if (this.state.autoScroll) {
            this.scrollToBottom();
        }
    }

    isLogVisible(log) {
        if (!this.state.enabledSeverities[log.type]) {
            return false;
        }

        if (this.state.searchText) {
            const searchLower = this.state.searchText.toLowerCase();
            return log.message.toLowerCase().includes(searchLower) ||
                   (log.level && log.level.toLowerCase().includes(searchLower));
        }

        return true;
    }

    get hasErrors() {
        return this.state.logs.some(log => log.type === 'error');
    }

    get visibleLogsCount() {
        return this.state.logs.filter(log => this.isLogVisible(log)).length;
    }

    get areAllSeveritiesEnabled() {
        return Object.values(this.state.enabledSeverities).every(value => Boolean(value));
    }

    getLogTypeClass(log) {
        return `log-type-${log.type}`;
    }

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

    get connectionStatusInfo() {
        const statusMap = {
            'connected': { text: 'Connected', class: 'text-success', icon: '●' },
            'connecting': { text: 'Connecting...', class: 'text-warning', icon: '◐' },
            'disconnected': { text: 'Disconnected', class: 'text-danger', icon: '●' }
        };
        return statusMap[this.state.connectionStatus] || statusMap['disconnected'];
    }

    get formattedLastUpdate() {
        if (!this.state.lastUpdateTime) return 'Never';
        return this.state.lastUpdateTime.toLocaleTimeString();
    }

    get pauseButtonText() {
        if (this.state.isPaused && this.state.pendingLogsCount > 0) {
            return `Resume (${this.state.pendingLogsCount} new)`;
        }
        return this.state.isPaused ? 'Resume' : 'Pause';
    }

    get profileName() {
        return this.state.profileInfo?.name || 'Log Terminal';
    }

    get logFilePath() {
        return this.state.profileInfo?.log_file_path || 'N/A';
    }

    async pollNow() {
        if (this.hasTerminalService && this.terminal.pollNow) {
            await this.terminal.pollNow(this.profileId);
        }
        this.addLocalLog('Manual poll completed', 'info');
    }

    async backToProfiles() {
    try {
        this.env.services.action.doAction({
            type: 'ir.actions.act_window',
            res_model: 'icomply.log.profile',
            name: 'Log Profiles',
            view_mode: 'tree,form',
            views: [[false, 'list'], [false, 'form']],
            target: 'current',
        });
    } catch (error) {
        console.error('Error navigating to profiles:', error);
        // Fallback: just close the terminal
        this.env.services.action.doAction({
            type: 'ir.actions.act_window_close'
        });
    }
}

    cleanup() {
        if (this.unsubscribe) {
            try {
                this.unsubscribe();
            } catch (e) {
                console.error("Error unsubscribing from terminal logs:", e);
            }
        }

        if (this.animationInterval) {
            clearInterval(this.animationInterval);
        }

        if (this.hasTerminalService && this.profileId) {
            this.terminal.cleanup(this.profileId);
        }
    }
}

IComplyTerminalComponent.template = "icomply.Terminal";
IComplyTerminalComponent.props = {
    action: { type: Object, optional: true },
    actionId: { type: [Number, String], optional: true },
    className: { type: String, optional: true },
    "*": true,  // Allow any additional props passed by Odoo's action system
};

registry.category("actions").add("icomply_terminal", IComplyTerminalComponent);