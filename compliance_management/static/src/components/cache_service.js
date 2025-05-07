/** @odoo-module */

import { registry } from "@web/core/registry";

/**
 * Service for interacting with server-side dashboard cache
 * @class ServerCacheService
 */
export class ServerCacheService {
    /**
     * @constructor
     * @param {Object} env - Odoo environment
     */
    constructor(env) {
        this.env = env;
        this.rpc = env.services.rpc;
        this.memoryCache = {}; // In-memory cache for faster repeat access
    }

    /**
     * Get data from cache
     * @param {string} key - Cache key
     * @returns {Promise<Object|null>} - Cache data or null
     */
    async getCache(key) {
        // Check memory cache first
        if (this.memoryCache[key] && this.memoryCache[key].expiry > Date.now()) {
            return this.memoryCache[key].data;
        }

        try {
            const response = await this.rpc('/dashboard/cache/get', { key });
            
            if (response.success && response.data) {
                // Store in memory cache
                this.memoryCache[key] = {
                    data: response.data,
                    expiry: Date.now() + (5 * 60 * 1000) // 5 minutes
                };
                return response.data;
            }
            return null;
        } catch (error) {
            console.error('Error fetching server cache:', error);
            return null;
        }
    }

    /**
     * Set data in cache
     * @param {string} key - Cache key
     * @param {Object} data - Data to cache
     * @returns {Promise<boolean>} - Success status
     */
    async setCache(key, data) {
        try {
            const response = await this.rpc('/dashboard/cache/set', { key, data });
            
            if (response.success) {
                // Update memory cache
                this.memoryCache[key] = {
                    data: data,
                    expiry: Date.now() + (5 * 60 * 1000)
                };
                return true;
            }
            return false;
        } catch (error) {
            console.error('Error setting server cache:', error);
            return false;
        }
    }
}

// Register as a service
const serverCacheService = {
    dependencies: ['rpc'],
    start(env) {
        return new ServerCacheService(env);
    }
};

registry.category('services').add('server_cache', serverCacheService);
