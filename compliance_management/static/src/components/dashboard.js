/** @odoo-module */

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { Card } from "./card/card";
import { ChartRenderer } from "./chart";
const { Component, useState, useRef, onMounted, onWillStart } = owl;


export class ComplianceDashboard extends Component {
  setup() {
    this.api = useService("orm");
    this.rpc = useService("rpc");
    this.navigate = useService("action");
    this.modalRef = useRef("modalId");
    this.bsModal = null;
    this.state = useState({
      showModal: false, // Initially, the modal is hidden
      modalTitle: "",
      modalContent: "",
      modalInitialized: false
    });

    onMounted(async () => {
      // Initialize the Bootstrap Modal *after* the component is mounted
      const modal = this.modalRef.el;
      if (modal) {
        this.bsModal = new bootstrap.Modal(modal); // Initialize only once
        this.state.modalInitialized = true; // Set the flag to true
      } else {
        console.error("Modal element STILL not found after mount!");
      }
    });

    // onWillStart(async () => {
    //   await this.getcurrentuser();
    //   this.filterByDate();
    // });
  }

  //   async getcurrentuser() {
  //     let result = await this.rpc("/dashboard/user");
  //     this.state.branches_id = result.branch;
  //     this.state.cc = result.group;
  //     this.state.alert_rules_domain = result.alert_rules_domain;
  //   }

  displaybycategory = async (name) => {
      if (this.state.modalInitialized) {
        // Check if bsModal is initialized
        this.bsModal.show();
      } else {
        console.error("Bootstrap Modal not initialized!");
      }
  };

  closeModal = () =>{
     const modal = this.modalRef.el;
     if (modal) {
       const bsModal = bootstrap.Modal.getInstance(modal); // Get the existing instance
       if (bsModal) {
         bsModal.hide();
       }
     } 
  }
}

ComplianceDashboard.template = "owl.ComplianceDashboard";
ComplianceDashboard.components = { Card, ChartRenderer };

registry.category("actions").add("owl.compliance_dashboard", ComplianceDashboard);
