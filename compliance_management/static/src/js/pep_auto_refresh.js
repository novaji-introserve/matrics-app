/** @odoo-module **/

// import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
// import { Component, onWillStart, onMounted, onWillUnmount } from "@odoo/owl";
import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";

// This patch adds auto-refresh functionality to PEP forms
patch(FormController.prototype, "compliance_management.PepAutoRefresh", {
    setup() {
        this._super(...arguments);
        
        if (this.props.resModel !== "res.pep") {
            return;
        }
        
        console.log("PEP Auto-refresh: Added to PEP form");
        
        this.orm = useService("orm");
        this.notification = useService("notification");
        this.bus = useService("bus_service");
        
        this.pepMonitoring = {
            active: false,
            intervalId: null,
            processedNotifications: new Set()
        };
        
        this._setupPepButtonHandler();
        
        this._setupBusListener();
        
        const originalOnMounted = this.onMounted;
        this.onMounted = function() {
            if (originalOnMounted) {
                originalOnMounted.call(this);
            }
            
            setTimeout(() => {
                this._checkInitialPepStatus();
            }, 500);
        };
        
        const originalOnWillDestroy = this.onWillDestroy;
        this.onWillDestroy = function() {
            this._stopPepMonitoring();
            if (originalOnWillDestroy) {
                originalOnWillDestroy.call(this);
            }
        };
    },
    
    /**
     * Set up a bus listener for person_update events
     */
    _setupBusListener() {
        // Get current user's partner ID channel
        const channel = 'res.partner';
        const partnerId = this.env.services.user.partnerId;
        
        console.log(`PEP Auto-refresh: Setting up bus listener for channel ${channel}/${partnerId}`);
        
        // Subscribe to bus notifications
        this.bus.addChannel(`${channel}/${partnerId}`);
        
        // Add a listener for the person_update message type
        this.bus.addEventListener('notification', (ev) => {
            if (!this.props.resId) return;
            
            // Process each notification
            for (const notification of ev.detail) {
                // Check if this is our notification type and matches the current record
                if (notification.type === 'person_update') {
                    const payload = notification.payload;
                    
                    // Create a unique notification ID to avoid duplicates
                    const notificationId = `${payload.record_id}_${payload.success}_${Date.now()}`;
                    
                    // Skip if we've already processed this notification
                    if (this.pepMonitoring.processedNotifications.has(notificationId)) {
                        console.log("PEP Auto-refresh: Skipping duplicate notification", payload);
                        continue;
                    }
                    
                    // Mark this notification as processed
                    this.pepMonitoring.processedNotifications.add(notificationId);
                    
                    console.log("PEP Auto-refresh: Received bus notification", payload);
                    
                    // Check if this notification is for the current record
                    if (payload.record_id === this.props.resId) {
                        console.log(`PEP Auto-refresh: Processing notification for record ${payload.record_id}`);
                        
                        // Stop monitoring
                        this._stopPepMonitoring();
                        
                        // Show notification
                        this.notification.add(
                            payload.success 
                                ? "Biography lookup completed successfully." 
                                : `Biography lookup failed: ${payload.message}.`,
                            {
                                title: payload.success ? "Success" : "Error",
                                type: payload.success ? "success" : "danger",
                                sticky: false
                            }
                        );
                        
                        // Update the UI to reflect the new status
                        this._updateFormUI(payload.success ? 'completed' : 'failed', payload.message);
                    }
                }
            }
        });
    },
    
    /**
     * Update the form UI without page refresh
     */
    async _updateFormUI(status, message) {
        try {
            // 1. Reload the record data from the server
            await this.model.root.load();
            
            // 2. Invalidate view to force re-rendering
            this.model.notify();
            
            console.log(`PEP Auto-refresh: UI updated to status ${status}`);
        } catch (error) {
            console.error("PEP Auto-refresh: Error updating UI:", error);
            
            // If updating the UI fails, fall back to page refresh
            this.notification.add(
                "Error updating display. Refreshing page...",
                {
                    type: "warning",
                    sticky: false
                }
            );
            
            setTimeout(() => {
                window.location.reload();
            }, 1500);
        }
    },
    
    /**
     * Monitor button clicks to detect Find Biography
     */
    _setupPepButtonHandler() {
        // Save the original method
        const originalOnButtonClicked = this.onButtonClicked;
        
        // Override the method
        this.onButtonClicked = async function(ev) {
            // Call the original method first
            const result = await originalOnButtonClicked.call(this, ev);
            
            // Get the button name
            const buttonName = ev.name;
            
            // If it's the Find Biography button, set up monitoring
            if (buttonName === "action_find_person") {
                console.log("PEP Auto-refresh: Find Biography button clicked");
                
                // Show a notification that the process has started
                this.notification.add(
                    "Biography lookup started...",
                    {
                        title: "Processing",
                        type: "info",
                        sticky: false
                    }
                );
                
                // Wait a short time for the operation to start
                setTimeout(() => {
                    this._startPepMonitoring();
                }, 500);
            }
            
            return result;
        };
    },
    
    /**
     * Check if the form is already showing a running job
     */
    async _checkInitialPepStatus() {
        if (!this.props.resId) return;
        
        try {
            console.log("PEP Auto-refresh: Checking initial status for record", this.props.resId);
            
            // Use direct RPC call to avoid model.root issues
            const result = await this.orm.call(
                "res.pep",
                "check_job_status",
                [this.props.resId]
            );
            
            console.log("PEP Auto-refresh: Initial status check result:", result);
            
            if (result && result.status === "running") {
                console.log("PEP Auto-refresh: Found form with running job");
                this._startPepMonitoring();
            }
        } catch (error) {
            console.error("PEP Auto-refresh: Error checking initial status:", error);
        }
    },
    
    /**
     * Start monitoring for job completion
     */
    _startPepMonitoring() {
        // Don't start if already monitoring
        if (this.pepMonitoring.active) return;
        
        console.log("PEP Auto-refresh: Starting job monitoring");
        this.pepMonitoring.active = true;
        
        // Reset processed notifications
        this.pepMonitoring.processedNotifications = new Set();
        
        // Check immediately
        this._checkPepJobStatus();
        
        // Then check every 2 seconds
        this.pepMonitoring.intervalId = setInterval(() => {
            this._checkPepJobStatus();
        }, 2000);
    },
    
    /**
     * Stop monitoring
     */
    _stopPepMonitoring() {
        if (this.pepMonitoring.intervalId) {
            clearInterval(this.pepMonitoring.intervalId);
            this.pepMonitoring.intervalId = null;
        }
        
        this.pepMonitoring.active = false;
        console.log("PEP Auto-refresh: Stopped job monitoring");
    },
    
    /**
     * Check the current job status
     */
    async _checkPepJobStatus() {
        if (!this.props.resId) return;
        
        try {
            const result = await this.orm.call(
                "res.pep",
                "check_job_status",
                [this.props.resId]
            );
            
            console.log("PEP Auto-refresh: Job status check result:", result);
            
            // If job is completed or failed, update the UI
            if (result && (result.status === "completed" || result.status === "failed")) {
                // Stop monitoring
                this._stopPepMonitoring();
                
                // Show notification
                this.notification.add(
                    result.status === "completed" 
                        ? "Biography lookup completed successfully." 
                        : `Biography lookup failed: ${result.message}.`,
                    {
                        title: result.status === "completed" ? "Success" : "Error",
                        type: result.status === "completed" ? "success" : "danger",
                        sticky: false
                    }
                );
                
                // Update the UI to reflect the new status
                this._updateFormUI(result.status, result.message);
            }
        } catch (error) {
            console.error("PEP Auto-refresh: Error checking job status:", error);
        }
    }
});
