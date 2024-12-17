/** @odoo-module **/

import { registry } from "@web/core/registry";
const { Component, useState, onWillStart, onWillUnmount, useRef, onMounted } = owl;
// import template from './../../../views/dropdown.xml';  // Ensure the correct template path


export class MultilevelDropdown extends Component {
//   static template = template;
  setup() {
    this.menuRef = useRef("menu");

     onMounted(async () => {

       // Enable hover and submenu toggling
       const dropdownSubmenus =
         this.menuRef.el.querySelectorAll(".dropdown-menu");
         
       dropdownSubmenus.forEach((submenu) => {
         submenu.addEventListener("mouseover", () => {
           submenu.classList.add("show");
         });
         submenu.addEventListener("mouseout", () => {
           submenu.classList.remove("show");
         });
       });
     });
  }

 
}

MultilevelDropdown.template = "multilevel.dropdowntemplate";
registry.category("actions").add("multilevel_dropdown_action", MultilevelDropdown);
