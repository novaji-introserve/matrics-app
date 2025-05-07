/** @odoo-module */

import { registry } from "@web/core/registry";

/**
 * Service for interacting with server-side dashboard cache (user-specific)
 */
export class ServerCacheService {
    constructor(env) {
        this.rpc = env.services.rpc;
        this.memoryCache = {}; // In-memory cache for faster repeat access
    }

    /**
     * Get data from cache
     * @param {string} key - Base cache key
     * @param {number} userId - User ID for user-specific caching
     * @returns {Promise<Object|null>} - Cache data or null
     */
    async getCache(key, userId) {
        // Create user-specific key
        const userKey = `${key}_${userId}`;
        
        // Check memory cache first
        if (this.memoryCache[userKey] && this.memoryCache[userKey].expiry > Date.now()) {
            return this.memoryCache[userKey].data;
        }

        try {
            // Pass the key directly - server side will use current user
            const response = await this.rpc('/dashboard/cache/get', { key });
            
            if (response.success && response.data) {
                // Store in memory cache
                this.memoryCache[userKey] = {
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
     * @param {string} key - Base cache key
     * @param {Object} data - Data to cache
     * @param {number} userId - User ID for user-specific caching
     * @returns {Promise<boolean>} - Success status
     */
    async setCache(key, data, userId) {
        // Create user-specific key
        const userKey = `${key}_${userId}`;
        
        try {
            // The server-side will use the current user
            const response = await this.rpc('/dashboard/cache/set', { key, data });
            
            if (response.success) {
                // Update memory cache
                this.memoryCache[userKey] = {
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
