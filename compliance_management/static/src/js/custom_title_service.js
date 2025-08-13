odoo.define('compliance_management/static/src/js/custom_title_service', function (require) {
    "use strict";
    
    const { registry } = require("@web/core/registry");
    
    /**
     * Custom Title Service
     * This service uses a MutationObserver to watch for title changes
     */
    const customTitleService = {
        dependencies: [],
        start(env) {
            // console.log("Starting custom title service with direct DOM monitoring");
            
            // Function to update the title
            function updateTitle() {
                let currentTitle = document.title;
                
                // Skip if already has our prefix
                if (currentTitle.startsWith('iComply - ')) {
                    return;
                }
                
                // Remove "HR - " prefix if exists
                if (currentTitle.startsWith('HR - ')) {
                    currentTitle = currentTitle.replace('HR - ', '');
                }
                
                // Set our custom title
                const newTitle = "iComply - " + currentTitle;
                document.title = newTitle;
                // console.log("Title updated to:", newTitle);
            }
            
            // Initialize title
            updateTitle();
            
            // Create a MutationObserver to watch for title changes
            const titleObserver = new MutationObserver(function(mutations) {
                mutations.forEach(function(mutation) {
                    if (mutation.type === "childList" || mutation.type === "characterData") {
                        updateTitle();
                    }
                });
            });
            
            // Start observing the document title
            const config = { 
                subtree: true, 
                characterData: true,
                childList: true 
            };
            
            // Use interval as backup approach
            const titleInterval = setInterval(updateTitle, 1000);
            
            // Observe title element
            setTimeout(() => {
                const titleElement = document.querySelector('title');
                if (titleElement) {
                    titleObserver.observe(titleElement, config);
                    // console.log("Title observer attached");
                }
            }, 500);
            
            // Return cleanup function
            return function() {
                titleObserver.disconnect();
                clearInterval(titleInterval);
            };
        }
    };
    
    // Register our service
    registry.category("services").add("custom_title_service", customTitleService);
    
    return customTitleService;
});
