/** @odoo-module **/

import { _t } from "web.core";

/* Custom Settings - Complete customization for Odoo settings pages */

(function() {
    'use strict';
    
    // Log that our script has loaded
    console.log("[CUSTOM SETTINGS] Script loaded");
    
    // Sections to hide
    const SECTIONS_TO_HIDE = [
        'Discuss', 'Statistics', 'Contacts', 
        'Permissions', 'Integrations', 'Developer Tools', 
        'About', 'Performance'
    ];
    
    // Sections to keep - do NOT hide these
    const SECTIONS_TO_KEEP = ['Users', 'Languages', 'Companies', 'Backend Theme'];
    
    /**
     * Hide non-General Settings sidebar items and specific sections
     */
    function customizeSettings() {
        console.log("[CUSTOM SETTINGS] Applying customizations");
        
        // 1. Hide sidebar items except "General Settings"
        let sidebarItems = document.querySelectorAll('.settings_tab .tab');
        sidebarItems.forEach(function(item) {
            // Keep only the "General Settings" tab
            if (!item.textContent.includes('General Settings')) {
                item.style.display = 'none';
                console.log("[CUSTOM SETTINGS] Hidden sidebar item:", item.textContent.trim());
            }
        });
        
        // 2. Hide specific sections based on their exact structure
        
        // Find all h2 elements
        const headings = document.querySelectorAll('h2');
        headings.forEach(function(heading) {
            const headingText = heading.textContent.trim();
            
            // Skip sections we want to keep
            if (SECTIONS_TO_KEEP.includes(headingText)) {
                console.log("[CUSTOM SETTINGS] Keeping section:", headingText);
                return;
            }
            
            if (SECTIONS_TO_HIDE.includes(headingText)) {
                console.log("[CUSTOM SETTINGS] Found section to hide:", headingText);
                
                // Different handling based on the section's structure
                if (headingText === 'Discuss' || headingText === 'Permissions' || 
                    headingText === 'Integrations' || headingText === 'Performance') {
                    // These are direct children of app_settings_block
                    const appBlock = heading.closest('.app_settings_block');
                    if (appBlock) {
                        // Hide the heading
                        heading.style.display = 'none';
                        
                        // Hide the next sibling (the row container)
                        if (heading.nextElementSibling && heading.nextElementSibling.classList.contains('row')) {
                            heading.nextElementSibling.style.display = 'none';
                        }
                        
                        // For sections which are direct children of app_settings_block
                        if (heading.parentElement.classList.contains('app_settings_block')) {
                            // Find any neighboring div containers
                            let nextElem = heading.nextElementSibling;
                            while (nextElem && nextElem.tagName === 'DIV') {
                                nextElem.style.display = 'none';
                                nextElem = nextElem.nextElementSibling;
                            }
                        }
                    }
                } else if (headingText === 'Statistics') {
                    // This is inside a div with id="statistics"
                    const statsBlock = document.getElementById('statistics');
                    if (statsBlock) {
                        statsBlock.style.display = 'none';
                    }
                } else if (headingText === 'Contacts') {
                    // This is inside a div with id="contacts_settings"
                    const contactsBlock = document.getElementById('contacts_settings');
                    if (contactsBlock) {
                        contactsBlock.style.display = 'none';
                    }
                } else if (headingText === 'Developer Tools') {
                    // More complex structure with nested divs
                    const devBlock = document.getElementById('developer_tool');
                    if (devBlock) {
                        // Find the grandparent which is the widget container
                        const widgetContainer = devBlock.closest('.o_widget_res_config_dev_tool');
                        if (widgetContainer) {
                            widgetContainer.style.display = 'none';
                        } else {
                            devBlock.style.display = 'none';
                        }
                    }
                } else if (headingText === 'About') {
                    // This is inside a div with id="about"
                    const aboutBlock = document.getElementById('about');
                    if (aboutBlock) {
                        aboutBlock.style.display = 'none';
                    }
                }
            }
        });
        
        // Handle sections without h2 headings or with different structures
        // Try to hide by position in app_settings_block
        const appSettingsBlock = document.querySelector('.app_settings_block');
        if (appSettingsBlock) {
            // Get all direct h2 children
            const h2Elements = appSettingsBlock.querySelectorAll(':scope > h2');
            
            // Hide specific positions - based on the diagnostic structure
            // 5th h2 is Discuss, 8th is Permissions, 9th is Integrations, 10th is Performance
            const positionsToHide = [4, 7, 8, 9]; // 0-indexed positions
            
            positionsToHide.forEach(position => {
                if (h2Elements[position]) {
                    h2Elements[position].style.display = 'none';
                    
                    // Hide the next container
                    if (h2Elements[position].nextElementSibling && 
                        h2Elements[position].nextElementSibling.classList.contains('row')) {
                        h2Elements[position].nextElementSibling.style.display = 'none';
                    }
                }
            });
        }
        
        // Target sections by data attributes
        document.querySelectorAll('.app_settings_block > div.row.mt16.o_settings_container').forEach(container => {
            const inputs = container.querySelectorAll('input[data-section]');
            inputs.forEach(input => {
                const section = input.getAttribute('data-section');
                if (SECTIONS_TO_HIDE.includes(section)) {
                    container.style.display = 'none';
                }
            });
        });
        
        console.log("[CUSTOM SETTINGS] Customization complete");
    }
    
    // Function to determine if we're on the settings page
    function isSettingsPage() {
        return window.location.href.includes('settings') || 
               document.querySelector('.settings_tab') !== null;
    }
    
    // Apply our customizations when the page loads
    document.addEventListener('DOMContentLoaded', function() {
        if (isSettingsPage()) {
            // Apply with a small delay to ensure DOM is fully loaded
            setTimeout(customizeSettings, 800);
        }
    });
    
    // Also handle navigation events
    let lastUrl = location.href; 
    
    // Create a MutationObserver to watch for DOM changes
    const observer = new MutationObserver(function() {
        if (location.href !== lastUrl) {
            lastUrl = location.href;
            if (isSettingsPage()) {
                // Wait for the settings page to load
                setTimeout(customizeSettings, 800);
            }
        }
        
        // Also check if we're on the settings page and new content was added
        if (isSettingsPage() && document.querySelector('.settings_tab')) {
            customizeSettings();
        }
    });
    
    // Start observing the document body for DOM changes
    observer.observe(document, { childList: true, subtree: true });
    
    // Run immediately if we're already on the settings page
    if (document.readyState === 'complete' || document.readyState === 'interactive') {
        if (isSettingsPage()) {
            setTimeout(customizeSettings, 800);
        }
    }
})();
