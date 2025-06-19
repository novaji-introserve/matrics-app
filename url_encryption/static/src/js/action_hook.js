/** @odoo-module **/

/**
 * Action Hook Module
 * =================
 * Integrates with Odoo's action service to handle parameter decryption
 * and ensure URLs are properly encrypted during action execution.
 */

/**
 * Sets up the action service hook for the given service
 * 
 * @param {Object} service - The URL encryption service
 */
export function setupActionHook(service) {
    try {
        const checkAndHookActionService = () => {
            if (window.odoo && window.odoo.__DEBUG__ && window.odoo.__DEBUG__.services) {
                const actionService = window.odoo.__DEBUG__.services.action;
                if (actionService && actionService.doAction) {
                    const originalDoAction = actionService.doAction.bind(actionService);
                    
                    actionService.doAction = async function(actionRequest, options = {}) {
                        try {
                            const decryptedParams = await service.decryptCurrentURL();
                            
                            if (decryptedParams) {
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
                                if (decryptedParams.model) {
                                    options.additionalContext.model = decryptedParams.model;
                                }
                            }
                            
                            service.preActionURLMask();
                            
                            const result = await originalDoAction(actionRequest, options);
                            
                            service.encryptCurrentURL();
                            
                            return result;
                        } catch (error) {
                            console.error('URL encryption hook error:', error);
                            return originalDoAction(actionRequest, options);
                        }
                    };
                    
                    try {
                        const menuService = window.odoo.__DEBUG__.services.menu;
                        if (menuService && menuService.selectMenu) {
                            const originalSelectMenu = menuService.selectMenu.bind(menuService);
                            
                            menuService.selectMenu = function(menuId, params) {
                                service.preActionURLMask();
                                
                                const result = originalSelectMenu(menuId, params);
                                
                                setTimeout(() => {
                                    service.encryptCurrentURL();
                                }, 100);
                                
                                return result;
                            };
                        }
                    } catch (err) {
                        console.warn('Could not hook into menu service:', err);
                    }
                } else {
                    setTimeout(checkAndHookActionService, 100);
                }
            } else {
                setTimeout(checkAndHookActionService, 100);
            }
        };
        
        checkAndHookActionService();
    } catch (error) {
        console.warn('Could not hook into action service:', error);
    }
}
