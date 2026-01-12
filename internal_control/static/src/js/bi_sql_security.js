/* 
FILE LOCATION: internal_control/static/src/js/bi_sql_security.js
FIXED VERSION FOR ODOO 16
*/

odoo.define('internal_control.bi_sql_security', function (require) {
    'use strict';

    var ListController = require('web.ListController');
    var rpc = require('web.rpc');

    // Override ListController only for bi.sql.view model
    ListController.include({
        
        init: function (parent, model, renderer, params) {
            this._super.apply(this, arguments);
            this._securityChecked = false;
            this._userIsCCO = false;
        },

        start: function () {
            var self = this;
            return this._super.apply(this, arguments).then(function () {
                // Only apply to bi.sql.view model
                if (self.modelName === 'bi.sql.view') {
                    console.log('🎯 BI SQL View detected - checking user access');
                    return self._checkAndApplySecurity();
                }
            });
        },

        reload: function () {
            var self = this;
            return this._super.apply(this, arguments).then(function () {
                // Re-apply security after reload
                if (self.modelName === 'bi.sql.view' && self._securityChecked && !self._userIsCCO) {
                    setTimeout(function() {
                        self._hideCreateButton();
                    }, 200);
                }
            });
        },

        _checkAndApplySecurity: function () {
            var self = this;
            
            // Call Python method to check if user is CCO
            return rpc.query({
                model: 'bi.sql.view',
                method: 'check_user_is_cco',
                args: []
            }).then(function (result) {
                console.log('👤 User access check result:', result);
                
                self._securityChecked = true;
                self._userIsCCO = result.is_cco;
                
                if (!result.is_cco) {
                    console.log('🚫 Non-CCO user detected - applying restrictions');
                    self._applyNonCCORestrictions();
                } else {
                    console.log('✅ CCO user detected - full access granted');
                    self._ensureCCOAccess();
                }
            }).catch(function (error) {
                console.warn('⚠️ Could not check user access:', error);
                // Default to restricted if check fails
                self._applyNonCCORestrictions();
            });
        },

        _applyNonCCORestrictions: function () {
            var self = this;
            
            // Hide NEW button with multiple attempts
            this._hideCreateButton();
            
            // Disable row clicks
            this._disableRowClicks();
            
            // Show access notification
            this._showAccessNotification();
            
            // Keep trying to hide the button for dynamic content
            setTimeout(function() { self._hideCreateButton(); }, 500);
            setTimeout(function() { self._hideCreateButton(); }, 1000);
            setTimeout(function() { self._hideCreateButton(); }, 2000);
        },

        _ensureCCOAccess: function () {
            console.log('🔓 Ensuring full access for CCO user');
            // CCO users get normal functionality
        },

        _hideCreateButton: function () {
            var self = this;
            
            var hideButtons = function() {
                // All possible locations for the NEW button
                var selectors = [
                    '.o_list_button_add',
                    '.o_cp_buttons .o_list_button_add',
                    '.o_control_panel .o_list_button_add',
                    'button[data-hotkey="c"]',
                    '.btn-primary:contains("New")',
                    '.btn-primary:contains("Create")'
                ];
                
                // Hide from control panel
                if (self.$buttons) {
                    selectors.forEach(function(selector) {
                        self.$buttons.find(selector).hide();
                    });
                }
                
                // Hide from DOM
                selectors.forEach(function(selector) {
                    $(selector).hide();
                    self.$el.find(selector).hide();
                });
                
                console.log('🚫 NEW button hidden');
            };
            
            // Apply multiple times to catch dynamic content
            hideButtons();
            setTimeout(hideButtons, 100);
            setTimeout(hideButtons, 300);
        },

        _disableRowClicks: function () {
            var self = this;
            
            setTimeout(function() {
                // Remove existing handlers
                self.$el.off('click.security', 'tbody tr');
                
                // Add our security handler
                self.$el.on('click.security', 'tbody tr', function (e) {
                    var $target = $(e.target);
                    
                    // Allow button clicks (like View Data button)
                    if ($target.is('button') || 
                        $target.closest('button').length ||
                        $target.hasClass('o_list_record_selector') ||
                        $target.closest('.o_list_record_selector').length) {
                        return true;
                    }
                    
                    // Block row clicks for non-CCO users
                    e.preventDefault();
                    e.stopPropagation();
                    
                    // Show access denied message
                    if (self.displayNotification) {
                        self.displayNotification({
                            type: 'warning',
                            title: '🔒 Access Restricted',
                            message: 'Please use the "📊 View Data" button to access reports. Technical configuration is restricted to Chief Compliance Officers.',
                        });
                    }
                    
                    return false;
                });
                
                console.log('🚫 Row clicks disabled for non-CCO user');
            }, 150);
        },

        _showAccessNotification: function () {
            var self = this;
            
            setTimeout(function() {
                if (self.displayNotification) {
                    self.displayNotification({
                        type: 'info',
                        title: '📊 Reports - Read Only Access',
                        message: 'You can view report data but cannot create or modify reports.',
                        sticky: true,
                    });
                }
            }, 1000);
        }
    });

    console.log('🔒 BI SQL Security Module Loaded');
});