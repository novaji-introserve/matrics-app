/** @odoo-module */
import { registry } from "@web/core/registry";

// Debug mode - set to false for production
const DEBUG = false;
function logDebug(...args) {
  if (DEBUG) console.log(...args);
}

/**
 * Service for interacting with server-side dashboard cache (user-specific)
 * @class ServerCacheService
 */
export class ServerCacheService {
  /**
   * @constructor
   * @param {Object} env - Odoo environment
   */
  constructor(env) {
    this.rpc = env.services.rpc;
    this.memoryCache = {};
  }
  
  /**
   * Get data from cache
   * @param {string} key - Base cache key
   * @returns {Promise<Object|null>} - Cache data or null
   */
  async getCache(key) {
    const memKey = `${key}`;
    
    if (this.memoryCache[memKey] && this.memoryCache[memKey].expiry > Date.now()) {
      const timeLeft = Math.round((this.memoryCache[memKey].expiry - Date.now()) / 1000);
      logDebug(`Cache hit for ${key} (memory) - ${timeLeft}s left before expiry`);
      return this.memoryCache[memKey].data;
    }
    
    try {
      logDebug(`Checking server cache for ${key}...`);
      const response = await this.rpc('/dashboard/cache/get', { key });
      if (response.success && response.data) {
        logDebug(`Cache hit for ${key} (server) - triggered background refresh`);
        this.memoryCache[memKey] = {
          data: response.data,
          expiry: Date.now() + (40 * 60 * 1000) // 40 minutes
        };
        return response.data;
      }
      logDebug(`Cache miss for ${key}`);
      return null;
    } catch (error) {
      logDebug('Error fetching server cache:', error);
      return null;
    }
  }
  
  /**
   * Set data in cache
   * @param {string} key - Base cache key
   * @param {Object} data - Data to cache
   * @returns {Promise<boolean>} - Success status
   */
  async setCache(key, data) {
    const memKey = `${key}`;
    try {
      const response = await this.rpc('/dashboard/cache/set', { key, data });
      if (response.success) {
        logDebug(`Cache set for ${key}`);
        this.memoryCache[memKey] = {
          data: data,
          expiry: Date.now() + (40 * 60 * 1000) // 40 minutes
        };
        return true;
      }
      return false;
    } catch (error) {
      logDebug('Error setting server cache:', error);
      return false;
    }
  }
  
  /**
   * Invalidate cache entries
   * @param {string} key - Optional specific key to invalidate
   * @returns {Promise<boolean>} - Success status
   */
  async invalidateCache(key = null) {
    try {
      const response = await this.rpc('/dashboard/cache/invalidate', { key });
      if (response.success) {
        if (key) {
          delete this.memoryCache[`${key}`];
          logDebug(`Cache invalidated for ${key}`);
        } else {
          this.memoryCache = {};
          logDebug('All cache invalidated');
        }
        return true;
      }
      return false;
    } catch (error) {
      logDebug('Error invalidating cache:', error);
      return false;
    }
  }
}

const serverCacheService = {
  dependencies: ['rpc'],
  start(env) {
    return new ServerCacheService(env);
  }
};

registry.category('services').add('server_cache', serverCacheService);
