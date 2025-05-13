// // File: static/src/js/chatter_position.js
// odoo.define('muk_web_theme_sidebar_invisible.chatter_position', function (require) {
//     "use strict";
    
//     var rpc = require('web.rpc');
//     var session = require('web.session');
//     var core = require('web.core');
    
//     // Add CSS to head to ensure it loads early
//     var styleTag = document.createElement('style');
//     styleTag.innerHTML = `
//         body.o_chatter_position_sided .o_form_view {
//             display: flex !important;
//             flex-direction: row !important;
//             flex-wrap: nowrap !important;
//         }
        
//         body.o_chatter_position_sided .o_form_view .o_form_sheet_bg {
//             flex: 1 1 65% !important;
//             max-width: 65% !important;
//             overflow: auto !important;
//         }
        
//         body.o_chatter_position_sided .o_form_view .o_chatter {
//             flex: 0 0 35% !important;
//             min-width: 320px !important;
//             max-width: 500px !important;
//             padding-left: 16px !important;
//             margin-left: 8px !important;
//             border-left: 1px solid #dee2e6 !important;
//             overflow: auto !important;
//         }
//     `;
//     document.head.appendChild(styleTag);
    
//     // Make a direct RPC call to get user preferences
//     $(document).ready(function() {
//         console.log("Fetching user chatter position preference directly");
        
//         rpc.query({
//             model: 'res.users',
//             method: 'read',
//             args: [[session.uid], ['chatter_position']],
//         }).then(function(result) {
//             console.log("User preferences loaded:", result);
            
//             if (result && result.length && result[0].chatter_position === 'sided') {
//                 $('body').addClass('o_chatter_position_sided');
//                 console.log("Applied o_chatter_position_sided class based on user preference");
//             }
//         }).catch(function(error) {
//             console.error("Failed to load user preferences:", error);
//         });
        
//         // Debug check for session value
//         console.log("Current session values:", session);
//         console.log("Session chatter position:", session.chatter_position);
//     });
    
//     // Also add a FormController hook to ensure the class is applied when forms are opened
//     var FormController = require('web.FormController');
    
//     FormController.include({
//         _update: function() {
//             var self = this;
//             return this._super.apply(this, arguments).then(function() {
//                 // Make RPC call here too in case session wasn't loaded on first page
//                 if (!$('body').hasClass('o_chatter_position_sided')) {
//                     rpc.query({
//                         model: 'res.users',
//                         method: 'read',
//                         args: [[session.uid], ['chatter_position']],
//                     }).then(function(result) {
//                         if (result && result.length && result[0].chatter_position === 'sided') {
//                             $('body').addClass('o_chatter_position_sided');
//                         }
//                     });
//                 }
//             });
//         }
//     });
// });



odoo.define('muk_web_theme_sidebar_invisible.chatter_position', function (require) {
    "use strict";
    
    var rpc = require('web.rpc');
    var session = require('web.session');
    var FormRenderer = require('web.FormRenderer');
    
    // Extend the FormRenderer to move the chatter after it's fully rendered
    FormRenderer.include({
        /**
         * @override
         */
        _renderView: function () {
            var self = this;
            return this._super.apply(this, arguments).then(function () {
                // Check for chatter after rendering is complete
                self._checkForChatter();
                return Promise.resolve();
            });
        },
        
        _checkForChatter: function() {
            var self = this;
            
            // Check if user preference is for sided chatter
            rpc.query({
                model: 'res.users',
                method: 'read',
                args: [[session.uid], ['chatter_position']],
            }).then(function(result) {
                if (result && result.length && result[0].chatter_position === 'sided') {
                    console.log("User has sided chatter preference, checking DOM");
                    
                    // Look for chatter in this form
                    var $chatter = self.$el.find('.o_chatter');
                    
                    if ($chatter.length) {
                        console.log("Found chatter, repositioning");
                        self._moveChatterToSide($chatter);
                    } else {
                        console.log("Chatter not found yet, will try again");
                        // Try again in a moment - sometimes the chatter loads after the form
                        setTimeout(function() {
                            var $chatter = self.$el.find('.o_chatter');
                            if ($chatter.length) {
                                self._moveChatterToSide($chatter);
                            }
                        }, 500);
                    }
                }
            });
        },
        
        _moveChatterToSide: function($chatter) {
            // Get references to key elements
            var $form = this.$el;
            var $sheetBg = $form.find('.o_form_sheet_bg');
            
            if (!$form.length || !$chatter.length || !$sheetBg.length) {
                console.log("Missing required elements:", {
                    form: $form.length,
                    chatter: $chatter.length,
                    sheetBg: $sheetBg.length
                });
                return;
            }
            
            console.log("Moving chatter to side");
            
            // Add a class to mark that we've modified this form
            if (!$form.hasClass('o_form_with_chatter_sided')) {
                $form.addClass('o_form_with_chatter_sided');
                
                // Create a flex container
                $form.css({
                    'display': 'flex',
                    'flex-direction': 'row',
                    'align-items': 'stretch',
                    'height': '100%'
                });
                
                // Style the form sheet background
                $sheetBg.css({
                    'flex': '1 1 65%',
                    'max-width': '65%',
                    'overflow': 'auto'
                });
                
                // Style the chatter
                $chatter.css({
                    'flex': '0 0 35%',
                    'min-width': '320px',
                    'max-width': '500px',
                    'padding-left': '16px',
                    'margin-left': '8px',
                    'border-left': '1px solid #dee2e6',
                    'overflow': 'auto',
                    'height': 'auto'
                });
                
                // Force the chatter to display directly after the form sheet
                $chatter.detach();
                $sheetBg.after($chatter);
                
                console.log("Chatter repositioned successfully");
            }
        }
    });
    
    // Also try to catch dynamics form loads
    $(document).ready(function() {
        // Set up an interval to keep checking for new forms
        setInterval(function() {
            // Only run if user pref is sided
            rpc.query({
                model: 'res.users',
                method: 'read',
                args: [[session.uid], ['chatter_position']],
            }).then(function(result) {
                if (result && result.length && result[0].chatter_position === 'sided') {
                    // Look for forms that haven't been processed yet
                    $('.o_form_view:not(.o_form_with_chatter_sided)').each(function() {
                        var $form = $(this);
                        var $chatter = $form.find('.o_chatter');
                        var $sheetBg = $form.find('.o_form_sheet_bg');
                        
                        if ($chatter.length && $sheetBg.length) {
                            console.log("Found unprocessed form with chatter");
                            
                            // Style the form
                            $form.addClass('o_form_with_chatter_sided');
                            $form.css({
                                'display': 'flex',
                                'flex-direction': 'row',
                                'align-items': 'stretch',
                                'height': '100%'
                            });
                            
                            // Style the form sheet background
                            $sheetBg.css({
                                'flex': '1 1 65%',
                                'max-width': '65%',
                                'overflow': 'auto'
                            });
                            
                            // Style the chatter
                            $chatter.css({
                                'flex': '0 0 35%',
                                'min-width': '320px',
                                'max-width': '500px',
                                'padding-left': '16px',
                                'margin-left': '8px',
                                'border-left': '1px solid #dee2e6',
                                'overflow': 'auto',
                                'height': 'auto'
                            });
                            
                            // Force the chatter to display directly after the form sheet
                            $chatter.detach();
                            $sheetBg.after($chatter);
                            
                            console.log("Processed form with chatter");
                        }
                    });
                }
            });
        }, 1000); // Check every second
    });
});

// odoo.define('muk_web_theme_sidebar_invisible.ChatterPosition', function (require) {
//     "use strict";
    
//     console.log("Loading ChatterPosition module (Odoo 16 version)");
    
//     var FormController = require('web.FormController');
//     var FormRenderer = require('web.FormRenderer');
//     var rpc = require('web.rpc');
//     var session = require('web.session');
    
//     // Store user preferences globally
//     var userChatterPosition = 'normal';
    
//     // Fetch user preferences
//     rpc.query({
//         model: 'res.users',
//         method: 'read',
//         args: [[session.uid], ['chatter_position']],
//     }).then(function(result) {
//         console.log("User preferences loaded:", result);
//         if (result && result.length) {
//             userChatterPosition = result[0].chatter_position;
//             console.log("Set chatter position to:", userChatterPosition);
            
//             // Apply to existing forms
//             setTimeout(function() {
//                 console.log("Checking for existing forms to update");
//                 $('.o_form_view').each(function() {
//                     console.log("Setting data-chatter-position on existing form");
//                     $(this).attr('data-chatter-position', userChatterPosition);
//                 });
//             }, 1000); // Wait for rendering
//         }
//     });
    
//     // Override FormController
//     FormController.include({
//         renderButtons: function() {
//             this._super.apply(this, arguments);
//             console.log("FormController renderButtons called");
            
//             // Set attribute after render
//             var self = this;
//             setTimeout(function() {
//                 if (self.$el) {
//                     console.log("Setting form view data attribute:", userChatterPosition);
//                     self.$el.find('.o_form_view').attr('data-chatter-position', userChatterPosition);
//                 }
//             }, 100);
//         },
        
//         _update: function() {
//             var self = this;
//             return this._super.apply(this, arguments).then(function() {
//                 console.log("FormController _update called");
                
//                 // Set attribute after update
//                 setTimeout(function() {
//                     if (self.$el) {
//                         console.log("Setting form view data attribute after update:", userChatterPosition);
//                         self.$el.find('.o_form_view').attr('data-chatter-position', userChatterPosition);
//                     }
//                 }, 100);
//             });
//         }
//     });
    
//     // Override FormRenderer
//     FormRenderer.include({
//         _renderView: function() {
//             var self = this;
//             return this._super.apply(this, arguments).then(function() {
//                 console.log("FormRenderer _renderView called");
//                 if (self.$el) {
//                     console.log("Setting form view data attribute in renderer:", userChatterPosition);
//                     self.$el.attr('data-chatter-position', userChatterPosition);
//                 }
//             });
//         },
//     });
    
//     // Also try to patch directly after document load
//     $(document).ready(function() {
//         console.log("Document ready - setting up observer");
        
//         // Use MutationObserver to catch newly added forms
//         var observer = new MutationObserver(function(mutations) {
//             mutations.forEach(function(mutation) {
//                 if (mutation.addedNodes && mutation.addedNodes.length) {
//                     var $forms = $(mutation.addedNodes).find('.o_form_view').addBack('.o_form_view');
//                     if ($forms.length) {
//                         console.log("MutationObserver: Found new form views:", $forms.length);
//                         $forms.each(function() {
//                             console.log("Setting data-chatter-position on new form");
//                             $(this).attr('data-chatter-position', userChatterPosition);
//                         });
//                     }
//                 }
//             });
//         });
        
//         // Start observing
//         observer.observe(document.body, {
//             childList: true,
//             subtree: true
//         });
//     });
    
//     console.log("ChatterPosition module initialized");
// });