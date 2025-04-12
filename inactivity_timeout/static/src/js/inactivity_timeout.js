/** @odoo-module **/

import { browser } from "@web/core/browser/browser";
import { registry } from "@web/core/registry";
import { _t } from "@web/core/l10n/translation";

/**
 * Inactivity Timeout Manager
 * Tracks user activity and logs out after a period of inactivity
 * Dynamically fetches configuration from system parameters
 */
class InactivityManager {
    /**
     * @param {Object} env - The environment
     * @param {Object} services - Required services
     */
    constructor(env, services) {
        // Store references
        this.env = env;
        this.notification = services.notification;
        this.rpc = services.rpc;

        // Configuration (defaults, will be overridden by system parameters)
        this.inactivityTimeout = 300 * 1000;  // 5 minutes default timeout
        this.warningTimeout = 60 * 1000;      // 1 minute default warning

        // State variables
        this.activityTimer = null;
        this.warningTimer = null;
        this.warningShown = false;
        this.configRefreshTimer = null;
        this.configRefreshInterval = 60 * 1000; // Check for config changes every minute

        // Events to monitor for activity
        this.events = [
            'mousemove', 'keydown', 'mousedown', 'touchstart',
            'scroll', 'click', 'DOMMouseScroll', 'mousewheel',
            'touchmove', 'MSPointerMove'
        ];

        // Bind methods
        this.resetTimers = this.resetTimers.bind(this);
        this.handleUserActivity = this.handleUserActivity.bind(this);
        this.stayLoggedIn = this.stayLoggedIn.bind(this);
        this.refreshConfiguration = this.refreshConfiguration.bind(this);

        // Load configuration and start
        this.loadConfiguration().then(() => {
            this.startMonitoring();
            // Start periodic config refresh
            this.startConfigRefresh();
        });
    }

    /**
     * Load configuration from server
     * @returns {Promise} - Promise that resolves when configuration is loaded
     */
    async loadConfiguration() {
        try {
            // Add timestamp to prevent caching
            const params = await this.rpc("/web/inactivity/params", {
                timestamp: new Date().getTime()
            });
            console.log("Loaded inactivity parameters:", params);

            if (params && !params.error) {
                const oldInactivityTimeout = this.inactivityTimeout;
                const oldWarningTimeout = this.warningTimeout;

                // Convert from seconds to milliseconds
                this.inactivityTimeout = (params.inactivity_timeout || 300) * 1000;
                this.warningTimeout = (params.warning_timeout || 60) * 1000;

                // If values changed, reset timers
                if (oldInactivityTimeout !== this.inactivityTimeout ||
                    oldWarningTimeout !== this.warningTimeout) {
                    console.log("Configuration changed from:",
                        { old_timeout: oldInactivityTimeout / 1000, old_warning: oldWarningTimeout / 1000 },
                        "to:",
                        { new_timeout: this.inactivityTimeout / 1000, new_warning: this.warningTimeout / 1000 });
                    this.resetTimers();
                }
            } else if (params && params.error) {
                console.error("Error loading inactivity parameters:", params.error);
            }

            return params;
        } catch (error) {
            console.error("Failed to load inactivity parameters:", error);
            // Use defaults if loading fails
            return null;
        }
    }

    /**
     * Start periodic refresh of configuration
     */
    startConfigRefresh() {
        // Clear any existing timer
        if (this.configRefreshTimer) {
            browser.clearTimeout(this.configRefreshTimer);
        }

        // Set new timer to periodically refresh configuration
        this.configRefreshTimer = browser.setInterval(() => {
            this.refreshConfiguration();
        }, this.configRefreshInterval);
    }

    /**
     * Refresh configuration from server
     */
    async refreshConfiguration() {
        console.log("Refreshing inactivity configuration...");
        await this.loadConfiguration();
    }

    /**
     * Start monitoring user activity
     */
    startMonitoring() {
        console.log(`Starting inactivity monitoring: Timeout: ${this.inactivityTimeout / 1000}s, Warning: ${this.warningTimeout / 1000}s`);

        // Add event listeners
        for (const event of this.events) {
            window.addEventListener(event, this.handleUserActivity, true);
        }

        // Set initial timers
        this.resetTimers();
    }

    /**
     * Handle user activity events (debounced)
     */
    handleUserActivity() {
        if (this.debounceTimeout) {
            clearTimeout(this.debounceTimeout);
        }

        this.debounceTimeout = setTimeout(() => {
            this.resetTimers();
        }, 500);
    }

    /**
     * Handler for "Stay Logged In" button
     */
    stayLoggedIn() {
        console.log("User clicked 'Stay Logged In'");
        this.removeCustomNotification();
        this.resetTimers();
    }

    /**
     * Reset the inactivity timers
     */
    resetTimers() {
        // Clear existing timers
        this.clearTimers();

        // Calculate warning time (total inactivity time minus warning period)
        const warningTime = this.inactivityTimeout - this.warningTimeout;

        // Set new warning timer
        this.warningTimer = browser.setTimeout(() => {
            this.showWarning();
        }, warningTime);

        // Set new logout timer
        this.activityTimer = browser.setTimeout(() => {
            this.logoutUser();
        }, this.inactivityTimeout);
    }

    /**
     * Clear all active timers
     */
    clearTimers() {
        if (this.activityTimer) {
            browser.clearTimeout(this.activityTimer);
            this.activityTimer = null;
        }
        if (this.warningTimer) {
            browser.clearTimeout(this.warningTimer);
            this.warningTimer = null;
        }
    }

    /**
     * Remove custom notification
     */
    removeCustomNotification() {
        try {
            // Remove our custom notification
            document.querySelectorAll('.o_inactivity_warning_notification').forEach(el => {
                if (el.parentNode) {
                    el.parentNode.removeChild(el);
                }
            });
        } catch (error) {
            console.error("Error closing notification:", error);
        }
    }

    /**
     * Creates notification DOM structure
     * @param {number} minutesRemaining - Minutes until logout
     * @returns {HTMLElement} The notification element
     */
    createNotificationElement(minutesRemaining) {
        // Create main container with proper classes from SCSS
        const notification = document.createElement('div');
        notification.className = 'o_inactivity_warning_notification p-3 rounded';

        // Create title element
        const title = document.createElement('div');
        title.className = 'o_notification_title mb-2 fw-bold';
        title.textContent = 'Session Expiring Soon';
        notification.appendChild(title);

        // Create content element
        const content = document.createElement('div');
        content.className = 'o_notification_content mb-3';

        // Add text and minutes
        content.textContent = `Your session will expire in ${minutesRemaining} minute(s) due to inactivity.`;
        notification.appendChild(content);

        // Create button
        const button = document.createElement('button');
        button.className = 'o_notification_button btn btn-primary';
        button.textContent = 'Stay Logged In';
        notification.appendChild(button);

        return notification;
    }

    /**
     * Show warning notification
     */
    showWarning() {
        console.log("Showing inactivity warning");

        // Close any existing notification first
        this.removeCustomNotification();

        // Set warning flag
        this.warningShown = true;

        // Calculate minutes remaining
        const minutesRemaining = Math.ceil(this.warningTimeout / 60000);

        // Create notification element
        const notification = this.createNotificationElement(minutesRemaining);

        // Add to document
        document.body.appendChild(notification);

        // Add event listener for stay logged in button
        const self = this;
        const stayButton = notification.querySelector('.o_notification_button');
        stayButton.addEventListener('click', function (e) {
            e.preventDefault();
            self.stayLoggedIn();
        });
    }

    /**
     * Log out the user
     */
    logoutUser() {
        console.log("Logging out due to inactivity");
        this.removeCustomNotification();
        window.location.href = '/web/session/logout?redirect=/web/login?timeout=1';
    }

    /**
     * Clean up resources
     */
    destroy() {
        console.log("Destroying inactivity timeout manager");

        // Remove event listeners
        for (const event of this.events) {
            window.removeEventListener(event, this.handleUserActivity, true);
        }

        // Clear timers
        this.clearTimers();

        if (this.debounceTimeout) {
            clearTimeout(this.debounceTimeout);
        }

        // Clear config refresh timer
        if (this.configRefreshTimer) {
            browser.clearInterval(this.configRefreshTimer);
            this.configRefreshTimer = null;
        }

        // Remove notification if present
        this.removeCustomNotification();
    }
}

// Register as an Odoo service
const inactivityTimeoutService = {
    dependencies: ["notification", "rpc"],
    start(env, services) {
        console.log("Starting inactivity timeout service");
        const manager = new InactivityManager(env, services);

        return {
            destroy: () => {
                manager.destroy();
            }
        };
    }
};

// Add to service registry
registry.category("services").add("inactivity_timeout", inactivityTimeoutService);
