/** @odoo-module **/

import { Component, useState } from "@odoo/owl";
import { Dialog } from "@web.core.dialog";
import { _lt } from "@web.core.l10n";

export class Modal extends Component {
  static template = "internal_control.modal"; // Replace with your template
  static components = { Dialog };

  setup() {
    this.state = useState({
      dialogIsOpen: false,
      dialogMessage: _lt("This is a test dialog!"),
      dialogInputValue: "", // For the input field example
    });
  }

  openDialog() {
    this.state.dialogIsOpen = true;
  }

  closeDialog() {
    this.state.dialogIsOpen = false;
    this.state.dialogInputValue = ""; // Clear input on close (optional)
  }

  confirmDialog() {
    // Perform actions on confirmation (e.g., API calls, data updates)
    console.log("Dialog confirmed!", this.state.dialogInputValue); // Access input value
    this.closeDialog();
  }

  cancelDialog() {
    // Perform actions on cancellation (if any)
    console.log("Dialog cancelled!");
    this.closeDialog();
  }

  updateInputValue(ev) {
    this.state.dialogInputValue = ev.target.value;
  }
}

Modal.template = "internal_control.modal";
registry.category("actions").add("custom_modal", Modal);