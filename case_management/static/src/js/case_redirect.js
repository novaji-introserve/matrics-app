// static/src/js/case_redirect.js
odoo.define('case_management.case_redirect', function (require) {
"use strict";

var FormController = require('web.FormController');

FormController.include({
    _onCreate: function () {
        var self = this;
        return this._super.apply(this, arguments).then(function (result) {
            // Check if this is a case with redirect flag
            if (self.modelName === 'case' && 
                self.initialState.context.case_created && 
                self.initialState.context.show_creation_notification) {
                
                // Call the redirect method
                return self._rpc({
                    model: 'case',
                    method: 'get_redirect_action',
                    args: [[result]],  // result is the new record ID
                }).then(function (action) {
                    return self.do_action(action);
                });
            }
            return result;
        });
    }
});

});