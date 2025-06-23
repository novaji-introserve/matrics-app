/** @odoo-module **/

/**
 * URL Masking Module
 * =================
 * Handles immediate masking of sensitive URLs before encryption is complete.
 * Provides the first layer of protection against URL parameter exposure.
 */

const earlyMaskToken = btoa(Math.random().toString(36)).substring(0, 20);

/**
 * Checks if a URL contains sensitive parameters that should be encrypted
 * 
 * @param {string} url - The URL to check
 * @returns {boolean} True if the URL contains sensitive parameters
 */
export function isSensitiveURL(url) {
    const sensitiveParams = ['action', 'menu_id', 'id', 'active_id', 'active_ids', 'model', 'cids', 'view_type'];
    for (const param of sensitiveParams) {
        if (url.includes(param + '=')) {
            return true;
        }
    }
    return false;
}

/**
 * Masks a URL with a temporary placeholder before encryption is complete
 * 
 * @param {string} url - The URL to mask
 * @param {string} token - The token to use for masking
 * @returns {string} The masked URL
 */
export function maskURL(url, token) {
    const [base, hash] = url.split('#');
    return `${base}#token=${token}`;
}

/**
 * Sets up URL masking hooks for the given service
 * 
 * @param {Object} service - The URL encryption service
 */
export function setupURLMasking(service) {
    const originalPushState = history.pushState;
    const originalReplaceState = history.replaceState;
 
    history.pushState = function() {
        const url = arguments[2];
        if (url && url.includes('#') && !url.includes('token=') && isSensitiveURL(url)) {
            const maskedUrl = maskURL(url, service.maskedToken);
            arguments[2] = maskedUrl;
            service.pendingNavigation = {
                originalUrl: url,
                maskedUrl: maskedUrl,
                type: 'push'
            };
        }
        return originalPushState.apply(this, arguments);
    };
    
    // Hook replaceState to mask URLs immediately
    history.replaceState = function() {
        const url = arguments[2];
        if (url && url.includes('#') && !url.includes('token=') && isSensitiveURL(url)) {
            const maskedUrl = maskURL(url, service.maskedToken);
            arguments[2] = maskedUrl;
            service.pendingNavigation = {
                originalUrl: url,
                maskedUrl: maskedUrl,
                type: 'replace'
            };
        }
        return originalReplaceState.apply(this, arguments);
    };
}

/**
 * Initializes early protection before the service is fully loaded
 * This function runs immediately when the module is imported
 */
export function initializeEarlyProtection() {
    const hash = window.location.hash.substring(1);
    if (hash && !hash.includes('token=') && isSensitiveURL(window.location.href)) {
        const maskedUrl = window.location.pathname + window.location.search + '#token=' + earlyMaskToken;
        window.history.replaceState(null, '', maskedUrl);
    }
}
