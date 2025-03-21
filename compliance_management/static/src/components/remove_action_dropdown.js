/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { FormController } from "@web/views/form/form_controller";

patch(FormController, "internal_control.FormController", {
  getActionMenuItems() {
    const actionMenuItems = super.getActionMenuItems();
    actionMenuItems.other = actionMenuItems.other.filter(
      (item) => item.key === "archive"
    );
    return actionMenuItems;
  },
});
