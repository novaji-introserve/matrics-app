/** @odoo-module **/

import { registry } from "@web/core/registry";
import { formView } from "@web/views/form/form_view";
import { FormController } from "@web/views/form/form_controller";

class CaseFormController extends FormController {
    setup() {
        super.setup();
        const isNew = this.props.resId === undefined;
        if (this.props.context && this.props.context.show_success_message) {
            this.env.services.notification.add("Case created successfully!", {
                title: "Success",
                type: "success",
            });
        }
    }
}

registry.category("views").add("case_form_view", {
    ...formView,
    Controller: CaseFormController,
});