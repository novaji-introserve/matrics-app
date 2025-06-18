/** @odoo-module **/

/**
 * URLEncryptionService
 * =====================
 * This service manages the encryption and decryption of URL parameters 
 * for enhanced security in data transmission. It utilizes the Fernet 
 * encryption mechanism and provides fallback methods for RPC calls.
 */

import { registry } from "@web/core/registry";

class URLEncryptionService {
    /**
     * Initializes the service with the given environment.
     * 
     * @param {Object} env - The Odoo environment.
     */
    constructor(env) {
        this.env = env;
        this.rpc = null;
        this.isEncrypting = false;
        this.encryptionCache = new Map();
        this.initialized = false;
    }

    /**
     * Initializes the service, setting up RPC and event listeners.
     * 
     * @returns {Promise<void>}
     */
    async init() {
        if (this.initialized) return;
        
        let attempts = 0;
        while (!this.rpc && attempts < 50) {
            try {
                this.rpc = this.env.services.rpc;
                if (this.rpc) break;
            } catch (e) {
                // RPC service not ready yet
            }
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (!this.rpc) {
            console.warn('URL Encryption: RPC service not available, falling back to fetch');
            this.rpc = this.createFallbackRPC();
        }
        
        this.initialized = true;
        this.setupEventListeners();
        this.startMonitoring();
    }
    
    /**
     * Creates a fallback RPC method using fetch API.
     * 
     * @returns {Function} The fallback RPC function.
     */
    createFallbackRPC() {
        return async (route, params = {}) => {
            try {
                const response = await fetch(route, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Requested-With': 'XMLHttpRequest',
                    },
                    credentials: 'same-origin',
                    body: JSON.stringify({
                        jsonrpc: '2.0',
                        method: 'call',
                        params: params,
                        id: Math.floor(Math.random() * 1000000),
                    }),
                });
                
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                
                const data = await response.json();
                if (data.error) {
                    throw new Error(data.error.message || 'RPC Error');
                }
                
                return data.result;
            } catch (error) {
                console.error('RPC Error:', error);
                throw error;
            }
        };
    }

    /**
     * Encrypts the provided URL parameters.
     * 
     * @param {Object} params - The URL parameters to encrypt.
     * @returns {Promise<Object>} The encrypted parameters.
     */
    async encryptURL(params) {
        if (!this.initialized) {
            await this.init();
        }
        
        try {
            const cacheKey = JSON.stringify(params);
            if (this.encryptionCache.has(cacheKey)) {
                return this.encryptionCache.get(cacheKey);
            }

            const result = await this.rpc('/web/encrypt_url', params);
            if (result && result.success) {
                this.encryptionCache.set(cacheKey, result.params);
                return result.params;
            }
            console.error('URL encryption failed:', result?.error || 'Unknown error');
            return params;
        } catch (error) {
            console.error('URL encryption error:', error);
            return params;
        }
    }

    /**
     * Decrypts the provided URL parameters.
     * 
     * @param {Object} params - The URL parameters to decrypt.
     * @returns {Promise<Object>} The decrypted parameters.
     */
    async decryptURL(params) {
        if (!this.initialized) {
            await this.init();
        }
        
        try {
            if (!params.token) {
                return params;
            }
            
            const result = await this.rpc('/web/decrypt_url', params);
            if (result && result.success) {
                const cleanParams = { ...result.params };
                delete cleanParams.token;
                return cleanParams;
            }
            console.error('URL decryption failed:', result?.error || 'Unknown error');
            return params;
        } catch (error) {
            console.error('URL decryption error:', error);
            return params;
        }
    }

    /**
     * Checks if the URL parameters should be encrypted based on predefined keys.
     * 
     * @param {Object} params - The URL parameters to check.
     * @returns {boolean} True if encryption is needed, false otherwise.
     */
    shouldEncryptParams(params) {
        const encryptKeys = ['action', 'menu_id', 'id', 'active_id', 'active_ids', 'model', 'cids'];
        return encryptKeys.some(key => key in params);
    }

    /**
     * Encrypts the current URL by modifying its hash.
     * 
     * @returns {Promise<void>}
     */
    async encryptCurrentURL() {
        if (this.isEncrypting || !this.initialized) return;
        
        const hash = window.location.hash.substring(1);
        if (!hash) return;

        try {
            const params = new URLSearchParams(hash);
            const paramObj = Object.fromEntries(params.entries());
            
            if (paramObj.token || !this.shouldEncryptParams(paramObj)) {
                return;
            }

            this.isEncrypting = true;
            const encryptedParams = await this.encryptURL(paramObj);
            const newParams = new URLSearchParams(encryptedParams);
            const newHash = '#' + newParams.toString();
            
            if (window.location.hash !== newHash) {
                window.history.replaceState(null, '', window.location.pathname + window.location.search + newHash);
            }
        } catch (error) {
            console.error('Error encrypting current URL:', error);
        } finally {
            this.isEncrypting = false;
        }
    }
    
    /**
     * Decrypts the current URL and stores decrypted parameters globally.
     * 
     * @returns {Promise<Object|null>} The decrypted parameters or null if none.
     */
    async decryptCurrentURL() {
        if (!this.initialized) {
            await this.init();
        }
        
        const hash = window.location.hash.substring(1);
        if (hash) {
            const params = new URLSearchParams(hash);
            const paramObj = Object.fromEntries(params.entries());
            
            if (paramObj.token) {
                const decryptedParams = await this.decryptURL(paramObj);
                
                window.odoo = window.odoo || {};
                window.odoo.decryptedParams = decryptedParams;
                
                return decryptedParams;
            }
        }
        return null;
    }

    /**
     * Sets up event listeners for monitoring URL changes and DOM updates.
     */
    setupEventListeners() {
        // Monitor hash changes
        window.addEventListener('hashchange', () => {
            setTimeout(() => this.encryptCurrentURL(), 100);
        });

        // Monitor for DOM changes that might indicate navigation
        const observer = new MutationObserver(() => {
            setTimeout(() => this.encryptCurrentURL(), 200);
        });

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => {
                if (document.body) {
                    observer.observe(document.body, { 
                        childList: true, 
                        subtree: true 
                    });
                }
            });
        } else if (document.body) {
            observer.observe(document.body, { 
                childList: true, 
                subtree: true 
            });
        }
    }

    /**
     * Starts monitoring the URL for encryption and decryption periodically.
     */
    startMonitoring() {
        // Initial encryption after a delay
        setTimeout(() => this.encryptCurrentURL(), 2000);
        
        // Periodic encryption check
        setInterval(() => {
            if (!this.isEncrypting && this.initialized) {
                this.encryptCurrentURL();
            }
        }, 5000);
    }
}

let urlEncryptionService = null;

// Register as a service with proper dependency handling
registry.category("services").add("url_encryption", {
    dependencies: ["rpc"],
    start(env, { rpc }) {
        urlEncryptionService = new URLEncryptionService(env);
        
        // Initialize the service asynchronously
        urlEncryptionService.init().then(() => {
            urlEncryptionService.decryptCurrentURL();
        }).catch(error => {
            console.error('URL Encryption service initialization failed:', error);
        });
        
        return urlEncryptionService;
    },
});

export { urlEncryptionService };

// Hook system that waits for proper initialization
document.addEventListener('DOMContentLoaded', function() {
    // Wait for Odoo to be fully loaded and services to be available
    const waitForOdooServices = () => {
        return new Promise((resolve) => {
            const checkServices = () => {
                if (window.odoo && 
                    window.odoo.__DEBUG__ && 
                    window.odoo.__DEBUG__.services &&
                    urlEncryptionService &&
                    urlEncryptionService.initialized) {
                    resolve();
                } else {
                    setTimeout(checkServices, 200);
                }
            };
            checkServices();
        });
    };

    waitForOdooServices().then(() => {
        // Hook into navigation events
        const originalPushState = window.history.pushState;
        const originalReplaceState = window.history.replaceState;
        
        window.history.pushState = function(state, title, url) {
            const result = originalPushState.apply(this, arguments);
            setTimeout(() => {
                if (urlEncryptionService) {
                    urlEncryptionService.encryptCurrentURL();
                }
            }, 300);
            return result;
        };
        
        window.history.replaceState = function(state, title, url) {
            const result = originalReplaceState.apply(this, arguments);
            setTimeout(() => {
                if (urlEncryptionService) {
                    urlEncryptionService.encryptCurrentURL();
                }
            }, 300);
            return result;
        };

        // Try to hook into action service if available
        try {
            const actionService = window.odoo.__DEBUG__.services.action;
            if (actionService && actionService.doAction) {
                const originalDoAction = actionService.doAction.bind(actionService);
                actionService.doAction = async function(actionRequest, options = {}) {
                    try {
                        const decryptedParams = await urlEncryptionService.decryptCurrentURL();
                        
                        if (decryptedParams) {
                            // Apply decrypted parameters to the action
                            if (typeof actionRequest === 'number' && decryptedParams.action) {
                                actionRequest = parseInt(decryptedParams.action);
                            }
                            
                            options.additionalContext = options.additionalContext || {};
                            
                            if (decryptedParams.menu_id) {
                                options.additionalContext.menu_id = parseInt(decryptedParams.menu_id);
                            }
                            if (decryptedParams.active_id) {
                                options.additionalContext.active_id = parseInt(decryptedParams.active_id);
                            }
                            if (decryptedParams.active_ids) {
                                const activeIds = typeof decryptedParams.active_ids === 'string' 
                                    ? decryptedParams.active_ids.split(',').map(id => parseInt(id.trim()))
                                    : [parseInt(decryptedParams.active_ids)];
                                options.additionalContext.active_ids = activeIds;
                            }
                        }
                        
                        const result = await originalDoAction(actionRequest, options);
                        setTimeout(() => {
                            if (urlEncryptionService) {
                                urlEncryptionService.encryptCurrentURL();
                            }
                        }, 500);
                        return result;
                    } catch (error) {
                        console.error('URL encryption hook error:', error);
                        return originalDoAction(actionRequest, options);
                    }
                };
            }
        } catch (error) {
            console.warn('Could not hook into action service:', error);
        }
    }).catch(error => {
        console.error('Failed to initialize URL encryption hooks:', error);
    });
});
