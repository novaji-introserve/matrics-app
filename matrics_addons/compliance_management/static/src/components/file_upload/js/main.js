/** @odoo-module **/

import { registry } from '@web/core/registry';
import { ImportFormComponent } from './import_form_component';

// Debug mode - set to false for production
const DEBUG = false;
function logDebug(...args) {
  if (DEBUG) console.log(...args);
}

// Debug logging to verify loading
logDebug("REGISTERING CSV IMPORT CLIENT ACTION");

registry.category("actions").add("compliance_management.csv_import_form", ImportFormComponent);

logDebug("CSV IMPORT CLIENT ACTION REGISTERED SUCCESSFULLY");
