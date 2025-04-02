/** @odoo-module **/

import { registry } from '@web/core/registry';
import { ImportFormComponent } from './import_form_component';

// Debug logging to verify loading
console.log("REGISTERING CSV IMPORT CLIENT ACTION");

registry.category("actions").add("compliance_management.csv_import_form", ImportFormComponent);

console.log("CSV IMPORT CLIENT ACTION REGISTERED SUCCESSFULLY");
