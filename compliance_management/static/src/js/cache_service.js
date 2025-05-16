/** @odoo-module */
import { registry } from "@web/core/registry";

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
    this.memoryCache = {}; // In-memory cache for faster repeat access
  }
  
  /**
   * Get data from cache
   * @param {string} key - Base cache key
   * @returns {Promise<Object|null>} - Cache data or null
   */
  async getCache(key) {
    // Create a memory cache key
    const memKey = `${key}`;
    
    // Check memory cache first - use 10 minutes to match server cache
    if (this.memoryCache[memKey] && this.memoryCache[memKey].expiry > Date.now()) {
      const timeLeft = Math.round((this.memoryCache[memKey].expiry - Date.now()) / 1000);
      console.log(`Cache hit for ${key} (memory) - ${timeLeft}s left before expiry`);
      return this.memoryCache[memKey].data;
    }
    
    try {
      console.log(`Checking server cache for ${key}...`);
      // Pass the key directly - server side will use current user
      const response = await this.rpc('/dashboard/cache/get', { key });
      if (response.success && response.data) {
        console.log(`Cache hit for ${key} (server) - triggered background refresh`);
        // Store in memory cache with 10 minute expiry to match server
        this.memoryCache[memKey] = {
          data: response.data,
          expiry: Date.now() + (10 * 60 * 1000) // 10 minutes
        };
        return response.data;
      }
      console.log(`Cache miss for ${key}`);
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
   * @returns {Promise<boolean>} - Success status
   */
  async setCache(key, data) {
    // Create a memory cache key
    const memKey = `${key}`;
    try {
      // The server-side will use the current user
      const response = await this.rpc('/dashboard/cache/set', { key, data });
      if (response.success) {
        console.log(`Cache set for ${key}`);
        // Update memory cache with 10 minute expiry
        this.memoryCache[memKey] = {
          data: data,
          expiry: Date.now() + (10 * 60 * 1000) // 10 minutes
        };
        return true;
      }
      return false;
    } catch (error) {
      console.error('Error setting server cache:', error);
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
          // Remove from memory cache if specific key
          delete this.memoryCache[`${key}`];
          console.log(`Cache invalidated for ${key}`);
        } else {
          // Clear all memory cache
          this.memoryCache = {};
          console.log('All cache invalidated');
        }
        return true;
      }
      return false;
    } catch (error) {
      console.error('Error invalidating cache:', error);
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









// /** @odoo-module */

// import { registry } from "@web/core/registry";

// /**
//  * Service for interacting with server-side dashboard cache (user-specific)
//  * @class ServerCacheService
//  */
// export class ServerCacheService {
//     /**
//      * @constructor
//      * @param {Object} env - Odoo environment
//      */
//     constructor(env) {
//         this.rpc = env.services.rpc;
//         this.memoryCache = {}; // In-memory cache for faster repeat access
//     }

//     /**
//      * Get data from cache
//      * @param {string} key - Base cache key
//      * @returns {Promise<Object|null>} - Cache data or null
//      */
//     async getCache(key) {
//         // Create a memory cache key
//         const memKey = `${key}`;
        
//         // Check memory cache first
//         if (this.memoryCache[memKey] && this.memoryCache[memKey].expiry > Date.now()) {
//             console.log(`Cache hit for ${key} (memory)`);
//             return this.memoryCache[memKey].data;
//         }

//         try {
//             // Pass the key directly - server side will use current user
//             const response = await this.rpc('/dashboard/cache/get', { key });
            
//             if (response.success && response.data) {
//                 console.log(`Cache hit for ${key} (server)`);
//                 // Store in memory cache
//                 this.memoryCache[memKey] = {
//                     data: response.data,
//                     expiry: Date.now() + (5 * 60 * 1000) // 5 minutes
//                 };
//                 return response.data;
//             }
//             console.log(`Cache miss for ${key}`);
//             return null;
//         } catch (error) {
//             console.error('Error fetching server cache:', error);
//             return null;
//         }
//     }

//     /**
//      * Set data in cache
//      * @param {string} key - Base cache key
//      * @param {Object} data - Data to cache
//      * @returns {Promise<boolean>} - Success status
//      */
//     async setCache(key, data) {
//         // Create a memory cache key
//         const memKey = `${key}`;
        
//         try {
//             // The server-side will use the current user
//             const response = await this.rpc('/dashboard/cache/set', { key, data });
            
//             if (response.success) {
//                 console.log(`Cache set for ${key}`);
//                 // Update memory cache
//                 this.memoryCache[memKey] = {
//                     data: data,
//                     expiry: Date.now() + (5 * 60 * 1000)
//                 };
//                 return true;
//             }
//             return false;
//         } catch (error) {
//             console.error('Error setting server cache:', error);
//             return false;
//         }
//     }
    
//     /**
//      * Invalidate cache entries
//      * @param {string} key - Optional specific key to invalidate 
//      * @returns {Promise<boolean>} - Success status
//      */
//     async invalidateCache(key = null) {
//         try {
//             const response = await this.rpc('/dashboard/cache/invalidate', { key });
            
//             if (response.success) {
//                 if (key) {
//                     // Remove from memory cache if specific key
//                     delete this.memoryCache[`${key}`];
//                     console.log(`Cache invalidated for ${key}`);
//                 } else {
//                     // Clear all memory cache
//                     this.memoryCache = {};
//                     console.log('All cache invalidated');
//                 }
//                 return true;
//             }
//             return false;
//         } catch (error) {
//             console.error('Error invalidating cache:', error);
//             return false;
//         }
//     }
// }

// // Register as a service
// const serverCacheService = {
//     dependencies: ['rpc'],
//     start(env) {
//         return new ServerCacheService(env);
//     }
// };

// registry.category('services').add('server_cache', serverCacheService);
