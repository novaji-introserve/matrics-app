/** @odoo-module **/

/**
 * RPC Handler Module
 * =================
 * Manages RPC communication for encryption/decryption operations.
 * Provides fallback mechanisms when RPC service is not available.
 */

/**
 * Initializes RPC with retry mechanism
 * 
 * @param {Object} env - The Odoo environment
 * @param {number} maxAttempts - Maximum number of attempts to get RPC service
 * @param {number} retryInterval - Interval between retry attempts in ms
 * @returns {Object|null} The RPC service or null if not available
 */
export async function initRPC(env, maxAttempts = 50, retryInterval = 50) {
    let attempts = 0;
    
    while (attempts < maxAttempts) {
        try {
            if (env && env.services && env.services.rpc) {
                return env.services.rpc;
            }
        } catch (e) {
            // RPC service not ready yet
        }
        
        await new Promise(resolve => setTimeout(resolve, retryInterval));
        attempts++;
    }
    
    return null;
}

/**
 * Creates a fallback RPC method using fetch API
 * 
 * @returns {Function} The fallback RPC function
 */
export function createFallbackRPC() {
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
 * Handles an RPC request with retry capability
 * 
 * @param {Function} rpc - The RPC function to use
 * @param {string} route - The route to call
 * @param {Object} params - The parameters to send
 * @param {number} maxRetries - Maximum number of retries
 * @returns {Promise<Object>} The RPC result
 */
export async function handleRPCRequest(rpc, route, params, maxRetries = 2) {
    let retries = 0;
    
    while (retries <= maxRetries) {
        try {
            return await rpc(route, params);
        } catch (error) {
            retries++;
            
            if (retries > maxRetries) {
                throw error;
            }
            
            await new Promise(resolve => setTimeout(resolve, 100 * Math.pow(2, retries)));
        }
    }
}
