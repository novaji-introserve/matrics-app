/** @odoo-module **/

import { FormController } from "@web/views/form/form_controller";
import { patch } from "@web/core/utils/patch";

patch(FormController.prototype, 'case_management_form_controller', {
    async saveRecord() {
        const isNew = !this.model.root.data.id;
        const result = await this._super(...arguments);
        
        if (this.model.root.resModel === 'case' && isNew) {
            this.env.services.notification.add('Case created successfully!', {
                title: 'Success',
                type: 'success',
            });
        }
        
        return result;
    },
});