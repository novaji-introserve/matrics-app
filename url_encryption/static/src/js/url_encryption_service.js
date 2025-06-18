/** @odoo-module **/

/**
 * URLEncryptionService
 * ====================
 * Core service that manages URL encryption and decryption.
 * Coordinates between different components.
 */

import { setupNavigationInterceptor } from "./navigation_interceptor";
import { setupURLMasking, isSensitiveURL } from "./url_masking";
import { setupActionHook } from "./action_hook";
import { initRPC, createFallbackRPC } from "./rpc_handler";

export class URLEncryptionService {
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
        this.pendingNavigation = null;
        
        this.maskedToken = btoa(Math.random().toString(36)).substring(0, 20);
        
        setupURLMasking(this);
        setupNavigationInterceptor(this);
        
        this._earlyInit();
    }

    /**
     * Early initialization method that doesn't depend on Odoo services
     */
    _earlyInit() {
        this._initRPCAndMonitoring();
        
        window.addEventListener('hashchange', () => this.encryptCurrentURL());
        
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this._onDOMReady());
        } else {
            this._onDOMReady();
        }
    }
    
    /**
     * Handles DOM ready event for early initialization
     */
    _onDOMReady() {
        if (document.body) {
            const observer = new MutationObserver(() => {
                this._processPendingNavigation();
                this.encryptCurrentURL();
            });
            
            observer.observe(document.body, { 
                childList: true, 
                subtree: true 
            });
        }
        
        this._processPendingNavigation();
        this.encryptCurrentURL();
    }
    
    /**
     * Initializes RPC and starts monitoring
     */
    async _initRPCAndMonitoring() {
        this.rpc = await initRPC(this.env, 50, 50);
        
        if (!this.rpc) {
            console.warn('URL Encryption: RPC service not available, falling back to fetch');
            this.rpc = createFallbackRPC();
        }
        
        this.initialized = true;

        this._processPendingNavigation();

        this.encryptCurrentURL();

        setupActionHook(this);

        this._startMonitoring();
    }

    /**
     * Processes any pending navigation that was masked
     */
    async _processPendingNavigation() {
        if (!this.pendingNavigation || !this.rpc) return;
        
        try {
            const { originalUrl, maskedUrl, type } = this.pendingNavigation;
            const [base, hash] = originalUrl.split('#');
            
            if (!hash) return;
            
            const params = new URLSearchParams(hash);
            const paramObj = Object.fromEntries(params.entries());
            
            if (this.shouldEncryptParams(paramObj)) {
                const encryptedParams = await this.encryptURL(paramObj);
                const newParams = new URLSearchParams(encryptedParams);
                const newUrl = base + '#' + newParams.toString();
                
                history.replaceState(null, '', newUrl);
            }
            
            this.pendingNavigation = null;
        } catch (error) {
            console.error('Error processing pending navigation:', error);
            this.pendingNavigation = null;
        }
    }

    /**
     * Fully initializes the service
     * 
     * @returns {Promise<void>}
     */
    async init() {
        if (this.initialized) return;
        await this._initRPCAndMonitoring();
    }

    /**
     * Encrypts the provided URL parameters with optimized caching.
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
                
                if (this.encryptionCache.size > 100) {
                    const oldestKey = this.encryptionCache.keys().next().value;
                    this.encryptionCache.delete(oldestKey);
                }
                
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
            if (!params.token || params.token === this.maskedToken) {
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
        const encryptKeys = ['action', 'menu_id', 'id', 'active_id', 'active_ids', 'model', 'cids', 'view_type'];
        return encryptKeys.some(key => key in params);
    }

    /**
     * Encrypts the current URL by modifying its hash.
     * 
     * @returns {Promise<void>}
     */
    async encryptCurrentURL() {
        if (this.isEncrypting || !this.rpc) return;
        
        const hash = window.location.hash.substring(1);
        if (!hash) return;

        try {
            const params = new URLSearchParams(hash);
            const paramObj = Object.fromEntries(params.entries());
            
            if (paramObj.token) {
                if (paramObj.token === this.maskedToken) {
                    this._processPendingNavigation();
                }
                return;
            }
            
            if (!this.shouldEncryptParams(paramObj)) {
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
            
            if (paramObj.token && paramObj.token !== this.maskedToken) {
                const decryptedParams = await this.decryptURL(paramObj);
                
                window.odoo = window.odoo || {};
                window.odoo.decryptedParams = decryptedParams;
                
                return decryptedParams;
            }
        }
        return null;
    }

    /**
     * Starts monitoring the URL for encryption at shorter intervals.
     */
    _startMonitoring() {
        setInterval(() => {
            if (!this.isEncrypting && this.initialized) {
                this._processPendingNavigation();
                this.encryptCurrentURL();
            }
        }, 1000); 
    }
    
    /**
     * Pre-action URL masking - protects URL even before action is complete
     */
    preActionURLMask() {
        const hash = window.location.hash.substring(1);
        if (hash && !hash.includes('token=')) {
            const maskedUrl = window.location.pathname + window.location.search + '#token=' + this.maskedToken;
            window.history.replaceState(null, '', maskedUrl);
        }
    }
}
