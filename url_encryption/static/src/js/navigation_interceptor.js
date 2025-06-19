/** @odoo-module **/

/**
 * Navigation Interceptor Module
 * ============================
 * Intercepts navigation events to ensure URLs are encrypted.
 */

/**
 * Sets up navigation interception hooks
 * 
 * @param {Object} service - The URL encryption service
 */
export function setupNavigationInterceptor(service) {
    document.addEventListener('click', (event) => {
        const anchor = event.target.closest('a');
        if (anchor && anchor.href && anchor.href.includes('#') && !anchor.href.includes('token=')) {
            if (_containsSensitiveParams(anchor.href)) {
                event.preventDefault();
                
                const maskedUrl = _maskURL(anchor.href, service.maskedToken);
                
                service.pendingNavigation = {
                    originalUrl: anchor.href,
                    maskedUrl: maskedUrl,
                    type: 'click'
                };
                
                window.location.href = maskedUrl;
            }
        }
    });
    
    document.addEventListener('submit', (event) => {
        const form = event.target;
        if (form.getAttribute('action') && form.getAttribute('action').includes('#')) {
            const action = form.getAttribute('action');
            if (_containsSensitiveParams(action)) {
                event.preventDefault();
                
                const maskedAction = _maskURL(action, service.maskedToken);
                form.setAttribute('action', maskedAction);
                
                service.pendingNavigation = {
                    originalUrl: action,
                    maskedUrl: maskedAction,
                    type: 'form'
                };
               
                form.submit();
            }
        }
    });
}

/**
 * Checks if a URL contains sensitive parameters
 * 
 * @param {string} url - The URL to check
 * @returns {boolean} True if the URL contains sensitive parameters
 * @private
 */
function _containsSensitiveParams(url) {
    const sensitiveParams = ['action', 'menu_id', 'id', 'active_id', 'active_ids', 'model', 'cids', 'view_type'];
    for (const param of sensitiveParams) {
        if (url.includes(param + '=')) {
            return true;
        }
    }
    return false;
}

/**
 * Masks a URL with a placeholder token
 * 
 * @param {string} url - The URL to mask
 * @param {string} token - The token to use for masking
 * @returns {string} The masked URL
 * @private
 */
function _maskURL(url, token) {
    const [base, hash] = url.split('#');
    return `${base}#token=${token}`;
}
